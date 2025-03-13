"""Testing suite
"""

import xml.etree.ElementTree as ET
import pandas as pd
from configuration.config_parser import XMLParser
from anonymizer import anonymize


MINIMAL_CONFIG = """
    <auto_dp>
        <table name='item'>
            <differential_privacy epsilon='1.0' pre_epsilon='0.0' algorithm='aim'/>
        </table>
    </auto_dp>
"""

FAKE_ONLY_CONFIG = """
    <fake_only>
        <table name="item">
            <!-- Sensitive value handling -->
            <value_faking>
                <column name="name" method="surname" locales="en_US" seed="0"/>
            </value_faking>
        </table>
    </fake_only>
"""

FULL_CONFIG = """
   <full_anon>
        <table name="item">
            <differential_privacy epsilon="1.0" pre_epsilon="0.0" algorithm="mst">
            <!-- Column categorization -->
                <ignore>
                    <column name="id"/>
                </ignore>
                <categorical>
                    <column name="name" />
                    <column name="item" />
                    <column name="timestamp" />
                </categorical>
            <!-- Continuous column fine-tuning -->
                <continuous>
                    <column name="number" bins="1000" lower="1.0" upper="10000.0" />
                </continuous>
            </differential_privacy>
            <!-- Sensitive value handling -->
            <value_faking>
                <column name="name" method="name" locales="en_US" seed="0"/>
            </value_faking>
        </table>
    </full_anon>
"""


def test_full_config():
    """
    Test method for a full config with dp-anonymization,continuous and sensitive values
    """

    parameters = ET.fromstring(FULL_CONFIG)

    full_anon = parameters.find("table")
    config_parser = XMLParser(full_anon)
    anon_config, sens_config, cont_config = config_parser.get_config()

    assert anon_config is not None
    assert sens_config is not None
    assert cont_config is not None

    dataset = pd.read_csv('test_table.csv')

    # Templates Path = None
    dataset_anon = anonymize(
        dataset, anon_config, cont_config, sens_config
    )

    assert dataset_anon is not None
    assert dataset['id'].equals(dataset_anon['id'])
    assert not dataset['item'].equals(dataset_anon['item'])

def test_faking_only():
    """
    Test method for a config that does only apply value faking
    """

    parameters = ET.fromstring(FAKE_ONLY_CONFIG)

    fake_only = parameters.find("table")
    config_parser = XMLParser(fake_only)
    anon_config, sens_config, cont_config = config_parser.get_config()

    dataset = pd.read_csv('test_table.csv')

    assert anon_config is None
    assert sens_config is not None
    assert cont_config is None

    # Templates Path = None
    dataset_anon = anonymize(
        dataset, anon_config, cont_config, sens_config
    )

    assert dataset_anon is not None
    assert dataset['item'].equals(dataset_anon['item'])

def test_minimal_config():
    """Test method for a minimal config where only dp-anonymization is applied. Testing only the config parsing
    """

    parameters = ET.fromstring(MINIMAL_CONFIG)

    only_dp_auto = parameters.find("table")
    config_parser = XMLParser(only_dp_auto)
    anon_config, sens_config, cont_config = config_parser.get_config()

    assert anon_config is not None
    assert sens_config is None
    assert cont_config is None
