import pandas as pd
import xml.etree.ElementTree as ET

from configuration.configurations import SensitiveConfig
from faker import Faker


class SensitiveAnonymizer:
    def __init__(
        self, dataset: pd.DataFrame, sens_config: SensitiveConfig
    ):
        self.dataset = dataset
        self.sens_config = sens_config

    def run_anonymization(self):
        anon_data = self.dataset.copy()

        if self.sens_config:
            for col in self.sens_config.columns:
                anon_data = self.__fake_column(
                    anon_data, col.name, col.method, col.locales, col.seed
                )
        return anon_data

    def __fake_column(self,
        dataset: pd.DataFrame, col_name: str, method: str, locales: list, seed=0
    ):

        if len(locales) > 0:
            fake = Faker(locales)
        else:
            fake = Faker()

        fake.seed_instance(seed)

        sens_dict = {}
        min_len = 0
        max_len = 1
        exists = False

        dataset[col_name] = dataset[col_name].astype(str)

        try:
            # Could use fake.unique but this often results in out-of-uniqueness errors
            faker_function = getattr(fake, method)
            exists = True
        except AttributeError:
            exists = False
            min_len = len(min(dataset[col_name].tolist(), key=len))
            max_len = len(max(dataset[col_name].tolist(), key=len))
            print("Faker method '" + method + "' not found. Resorting to random String")

        collection = dataset[col_name].unique()

        for val in collection:
            if exists:
                fake_value = faker_function()
                sens_dict[val] = fake_value
            else:
                sens_dict[val] = fake.pystr(min_chars=min_len, max_chars=max_len)

        dataset[col_name] = dataset[col_name].map(sens_dict)

        fake.unique.clear()

        return dataset