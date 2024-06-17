"""Testing suite
"""

import xml.etree.ElementTree as ET
from configuration.config_parser import XMLParser


MINIMAL_CONFIG = """
    <auto_dp>
        <table name='item'>
            <differential_privacy epsilon='1.0' pre_epsilon='0.0' algorithm='aim'/>
        </table>
    </auto_dp>
"""

FULL_CONFIG = """
   <full_anon>
        <table name="item">
            <differential_privacy epsilon="1.0" pre_epsilon="0.0" algorithm="aim">
            <!-- Column categorization -->
                <ignore>
                    <column name="i_id"/>
                </ignore>
                <categorical>
                    <column name="i_name" />
                    <column name="i_data" />
                    <column name="i_im_id" />
                </categorical>
            <!-- Continuous column fine-tuning -->
                <continuous>
                    <column name="i_price" bins="1000" lower="2.0" upper="100.0" /> 
                </continuous>
            </differential_privacy>
            <!-- Sensitive value handling -->
            <value_faking>
                <column name="i_name" method="name" locales="en_US" seed="0"/>
            </value_faking>
        </table>
    </full_anon>
"""


def test_full_config():
    """Test method for a full config with dp-anonymization, continuous and sensitive values"""

    parameters = ET.fromstring(FULL_CONFIG)

    full_anon = parameters.find("table")
    config_parser = XMLParser(full_anon)
    anon_config, sens_config, cont_config = config_parser.get_config()

    assert anon_config is not None
    assert sens_config is not None
    assert cont_config is not None

    assert anon_config.table_name == "item"
    assert anon_config.epsilon == "1.0"
    assert anon_config.preproc_eps == "0.0"
    assert anon_config.algorithm == "aim"


def test_minimal_config():
    """Test method for a minimal config where only dp-anonymization is applied
    """

    parameters = ET.fromstring(MINIMAL_CONFIG)

    only_dp_auto = parameters.find("table")
    config_parser = XMLParser(only_dp_auto)
    anon_config, sens_config, cont_config = config_parser.get_config()

    assert anon_config is not None
    assert sens_config is None
    assert cont_config is None
