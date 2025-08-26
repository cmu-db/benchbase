#!/usr/bin/python

import json
import sys
import yaml
import os  # Import the 'os' module for path manipulation
from jinja2 import Environment, FileSystemLoader, Undefined
from functools import reduce
from operator import getitem
from collections import OrderedDict


# Custom Undefined handler to return an empty string or leave placeholders as-is
class SilentUndefined(Undefined):
    def _fail_with_undefined_error(self, *args, **kwargs):
        return ''  # Return empty string if undefined

    def __str__(self):
        return self._fail_with_undefined_error()


def jinja2_modification(context, input_yaml):
    """
    Renders the input YAML file as a Jinja2 template, supporting both
    relative and absolute file paths.
    """
    # Get the absolute path of the directory containing the template
    template_dir = os.path.dirname(os.path.abspath(input_yaml))
    # Get the base name of the template file
    template_file = os.path.basename(input_yaml)

    # Initialize Jinja2 environment with the correct template directory
    template_env = Environment(loader=FileSystemLoader(searchpath=template_dir), undefined=SilentUndefined)
    
    # Load the template using its base name
    template = template_env.get_template(template_file)
    
    # Write the rendered content back to the original file path
    with open(input_yaml, 'w') as f:
        f.write(template.render(context))


# Load the YAML using OrderedDict to preserve the order of elements
def ordered_yaml_loader(stream, Loader=yaml.SafeLoader, object_pairs_hook=OrderedDict):
    class OrderedLoader(Loader):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))

    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)


# Custom dumper to preserve empty strings as "" instead of null
def ordered_yaml_dumper(data, stream=None, Dumper=yaml.SafeDumper, **kwds):
    class OrderedDumper(Dumper):
        pass

    # Add custom representer for OrderedDict to preserve order
    def _dict_representer(dumper, data):
        return dumper.represent_dict(data.items())

    # Add custom representer for empty strings to prevent 'null' output
    def _str_representer(dumper, value):
        if not value or value == '' or value == 'null':
            return dumper.represent_scalar('tag:yaml.org,2002:str', '')  # Keep as empty string
        return dumper.represent_scalar('tag:yaml.org,2002:str', value)

    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    OrderedDumper.add_representer(str, _str_representer)  # Ensure empty strings are represented as ""

    return yaml.dump(data, stream, OrderedDumper, **kwds)


def set_nested_item(dataDict, mapList, val):
    """Set item in nested dictionary, handling list indices if present."""
    for i, key in enumerate(mapList[:-1]):
        if isinstance(reduce(getitem, mapList[:i], dataDict), list):
            index = int(key[key.find("[") + 1:key.find("]")])  # Extract index inside brackets
            mapList[i] = index  # Replace the string with an integer index
    reduce(getitem, mapList[:-1], dataDict)[mapList[-1]] = val
    return dataDict


def handle_load_balance_url(url, driver, optimal_threads):
    """
    Handle load-balance parameter in URL based on driver and optimalThreads setting.
    If optimalThreads is true and driver is com.yugabyte.Driver, ensure load-balance=true is present.
    """
    if not optimal_threads or driver != "com.yugabyte.Driver":
        return url
    
    # Check if load-balance is already present in the URL
    if "load-balance=" in url:
        # If load-balance is present, ensure it's set to true
        if "load-balance=false" in url:
            url = url.replace("load-balance=false", "load-balance=true")
        elif "load-balance=true" in url:
            # Already set to true, no change needed
            pass
        else:
            # load-balance is present but with no value, ensure it's true
            # This handles cases like "load-balance" without a value
            url = url.replace("load-balance&", "load-balance=true&")
            url = url.replace("load-balance?", "load-balance=true?")
            if url.endswith("load-balance"):
                url = url.replace("load-balance", "load-balance=true")
    else:
        # load-balance is not present, add it
        if "?" in url:
            url += "&load-balance=true"
        else:
            url += "?load-balance=true"
    
    return url


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <json_context> <yaml_file_path>")
        sys.exit(1)
        
    yaml_file = sys.argv[2]
    
    try:
        data = json.loads(sys.argv[1], strict=False)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON context provided. {e}")
        sys.exit(1)

    if not os.path.exists(yaml_file):
        print(f"Error: YAML file not found at '{yaml_file}'")
        sys.exit(1)

    jinja2_modification(data, yaml_file)

    with open(yaml_file) as f:
        doc = ordered_yaml_loader(f, Loader=yaml.FullLoader)
        
        # Check if optimalThreads is set and handle load-balance URL modification
        optimal_threads = data.get("optimalThreads", False)
        driver = doc.get("driver", "")
        
        for key, value in data.items():
            if key == "warmup":
                doc["works"]["work"]["warmup"] = value
            elif key == "time":
                doc["works"]["work"]["time_secs"] = value
            elif key == "url":
                doc[key] = doc[key].replace("localhost", value)
                if "sslmode" in data:
                    doc[key] = doc[key].replace("disable", "require") if data["sslmode"] else doc[key].replace(
                        "require", "disable")
                if "load-balance" in data and data['load-balance'] == True:
                    doc[key] = doc[key] + "&load-balance=true"
                # Handle optimalThreads load-balance logic
                if optimal_threads:
                    doc[key] = handle_load_balance_url(doc[key], driver, optimal_threads)
            elif key == "optimalThreads":
                # Set the optimalThreads property in the YAML
                doc[key] = value
                # Also handle load-balance URL modification if URL exists
                if "url" in doc:
                    doc["url"] = handle_load_balance_url(doc["url"], driver, value)
            elif key == "setAutoCommit":
                doc["microbenchmark"]["properties"]["setAutoCommit"] = value
            else:
                nested_key = key.replace('[', '.[').split('.')
                set_nested_item(doc, nested_key, value)

        with open(yaml_file, 'w') as fnew:
            ordered_yaml_dumper(doc, fnew, Dumper=yaml.SafeDumper, allow_unicode=True)
    return


if __name__ == "__main__":
    main()
