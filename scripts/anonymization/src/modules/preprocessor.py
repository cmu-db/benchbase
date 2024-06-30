from snsynth.transform import BinTransformer, TableTransformer

from configuration.configurations import DPConfig, ContinuousConfig


class Preprocessor:
    def __init__(self, config: DPConfig):
        self.config = config

    def getTransformer(self, dataset, cont_config):
        """Method that returns a TableTransformer object which can be used by DP mechansisms

        Args:
            dataset (DataFrame): Pandas DataFrame
            algorithm (str): Name of the DP-algorithm
            categorical (str[]): List of categorical column names
            continuous (str[]): List of continuous column names
            ordinal (str[]): List of ordinal column names
            continuousConfig (dict): Configuration of continuous columns

        Returns:
            TableTransformer: A transformer object
        """

        style = "cube"
        if "gan" in self.config.algorithm:
            style = "gan"

        tt = TableTransformer.create(
            dataset,
            nullable=dataset.isnull().values.any(),
            categorical_columns=self.config.categorical,
            continuous_columns=self.config.continuous,
            ordinal_columns=self.config.ordinal,
            style=style,
            constraints=self.getConstraints(cont_config, dataset),
        )
        print("Instantiated Transformer")
        return tt

    def getConstraints(self,cont_config: ContinuousConfig, dataset):
        """Helper method that forms constraints from a list of continuous columns

        Args:
            config (dict): The continuous column configuration
            dataset (DataFrame): Pandas DataFrame

        Returns:
            dict: A dictionary of constraints that will be applied to each specified column
        """
        constraints = {}

        for cont_entry in cont_config.columns:
            col_name = cont_entry.name
            bins = int(cont_entry.bins)

            lower = cont_entry.lower
            upper = cont_entry.upper

            min_bound = None
            max_bound = None

            if lower:
                if "." in lower:
                    min_bound = float(lower)
                else:
                    min_bound = int(lower)
            if upper:
                if "." in upper:
                    max_bound = float(upper)
                else:
                    max_bound = int(upper)

            nullFlag = dataset[col_name].isnull().values.any()
            constraints[col_name] = BinTransformer(
                bins=bins, lower=min_bound, upper=max_bound, nullable=nullFlag
            )

        return constraints
