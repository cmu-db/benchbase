import pandas as pd

from configuration.configurations import SensitiveConfig
from faker import Faker


class SensitiveAnonymizer:
    def __init__(
        self, dataset: pd.DataFrame, sens_config: SensitiveConfig, templates_path: str
    ):
        self.dataset = dataset
        self.sens_config = sens_config
        self.templates_path = templates_path

    def run_anonymization(self):
        anon_data = self.dataset.copy()
        list_of_mappings = []
        if self.sens_config:
            for col in self.sens_config.columns:
                anon_data, mapping = self.__fake_column(
                    anon_data, col.name, col.method, col.locales, col.seed
                )
                list_of_mappings.append(mapping)
                # TODO: Use list of mappings to change templates file
        return anon_data

    def __fake_column(self,
        dataset: pd.DataFrame, col_name: str, method: str, locales: list, seed=0
    ):

        if len(locales) > 0:
            fake = Faker(locales)
        else:
            fake = Faker()

        fake.seed_instance(seed)

        sensDict = {}
        min_len = 0
        max_len = 1
        exists = False

        dataset[col_name] = dataset[col_name].astype(str)

        try:
            fakerFunc = getattr(fake.unique, method)
            exists = True
        except AttributeError:
            exists = False
            min_len = len(min(dataset[col_name].tolist(), key=len))
            max_len = len(max(dataset[col_name].tolist(), key=len))
            print("Faker method '" + method + "' not found. Resorting to random String")

        collection = dataset[col_name].unique()

        for val in collection:
            if exists:
                fakeValue = fakerFunc()
                sensDict[val] = fakeValue
            else:
                sensDict[val] = fake.pystr(min_chars=min_len, max_chars=max_len)

        dataset[col_name] = dataset[col_name].map(sensDict)

        fake.unique.clear()

        return dataset, sensDict
