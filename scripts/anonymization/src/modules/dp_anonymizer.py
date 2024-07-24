import time
from snsynth import Synthesizer
from configuration.configurations import DPConfig,ContinuousConfig
import pandas as pd
from .preprocessor import Preprocessor

class DifferentialPrivacyAnonymizer():
    def __init__(self,  dataset: pd.DataFrame, anon_config: DPConfig, cont_config: ContinuousConfig):
        self.dataset = dataset
        self.anon_config = anon_config
        self.cont_config = cont_config

    def run_anonymization(self):
    
        alg = self.anon_config.algorithm
        eps = float(self.anon_config.epsilon)
        pre_eps = float(self.anon_config.preproc_eps)
        cat = self.anon_config.column_classification.categorical
        cont = self.anon_config.column_classification.continuous
        ordi = self.anon_config.column_classification.ordinal

        saved_columns,saved_indexes = self.__remove_ignorable()

        nullable_flag = self.dataset.isnull().values.any()

        anon_data = pd.DataFrame()

        if eps > 0:
            # For epsilon > 0 we run the anonymization
            synth = Synthesizer.create(alg, epsilon=eps, verbose=True)
            start_time = time.perf_counter()

            # If there is a preprocessing configuration for continuous columns, we need the Preprocessor
            if self.cont_config:
                sample = synth.fit_sample(
                    self.dataset,
                    preprocessor_eps=pre_eps,
                    categorical_columns=cat,
                    continuous_columns=cont,
                    ordinal_columns=ordi,
                    transformer=Preprocessor(self.anon_config).getTransformer(
                        self.dataset, self.cont_config
                    ),
                    nullable=nullable_flag,
                )
                anon_data = pd.DataFrame(sample)
            else:
                sample = synth.fit_sample(
                    self.dataset,
                    preprocessor_eps=pre_eps,
                    categorical_columns=cat,
                    continuous_columns=cont,
                    ordinal_columns=ordi,
                    nullable=nullable_flag,
                )
                anon_data = pd.DataFrame(sample)

            end_time = time.perf_counter()
            print(f"Process took: {(end_time-start_time):0.2f} seconds")

        else:
            print("Epsilon = 0. Anonymization will return the original data")
            anon_data = self.dataset

        anon_data = self.__add_ignorable(anon_data,saved_indexes,saved_columns)

        
        return anon_data
    
    def __remove_ignorable(self):
        saved_columns = []
        saved_indexes = []
        ignore_columns = self.anon_config.column_classification.hidden
        if ignore_columns:
            saved_columns = self.dataset[ignore_columns]
            for col in ignore_columns:
                saved_indexes.append(self.dataset.columns.get_loc(col))
    
        self.dataset = self.dataset.drop(ignore_columns, axis=1)
        return saved_columns,saved_indexes
    
    def __add_ignorable(self,dataset,saved_indexes,saved_columns):
        ignore_columns = self.anon_config.column_classification.hidden
        for ind, col in enumerate(ignore_columns):
            dataset.insert(saved_indexes[ind], col, saved_columns[col])
        return dataset