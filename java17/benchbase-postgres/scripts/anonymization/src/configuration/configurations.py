"""Module that contains all configuration classes
"""


class DPColumnConfig:
    """A class to represent the classification of columns for DP anonymization

    Attributes
    ----------
    hidden : list[str]
          List of column names that will not be anonymized
    categorical : list[str]
          List of categorical column names
    continuous : list[str]
          List of continuous column names
    ordinal : list[str]
          List of ordinal column names

    """

    def __init__(
        self,
        hidden: list[str],
        categorical: list[str],
        continuous: list[str],
        ordinal: list[str],
    ):
        self.hidden = hidden
        self.categorical = categorical
        self.continuous = continuous
        self.ordinal = ordinal


class DPConfig:
    """A class to represent a config that handles DP-Anonymization

    Attributes
    ----------
    table_name : str
          Table name
    epsilon : str
          Privacy budget
    preproc_eps : str
          Privacy budget for preprocessing
    algorithm : str
          Name of the DP-algorithm
    column_classification : DPColumnConfig
          lassification of table columns

    """

    def __init__(
        self,
        table_name: str,
        epsilon: str,
        preproc_eps: str,
        algorithm: str,
        column_classification: DPColumnConfig,
    ):
        self.epsilon = epsilon
        self.table_name = table_name
        self.preproc_eps = preproc_eps
        self.algorithm = algorithm
        self.column_classification = column_classification


class ContinuousEntry:
    """A class to represent a continuous column entry

    Attributes
    ----------
    name : str
          Name of the column
    bins : str
          Number of bins
    lower : str
          Lower bound of values
    upper : str
          Upper bound of values

    """

    def __init__(self, name: str, bins: str, lower: str, upper: str):
        self.name = name
        self.bins = bins
        self.lower = lower
        self.upper = upper


class ContinuousConfig:
    """A class to represent a continuous column config

    Attributes
    ----------
    columns : list[ContinuousEntry]
          A list of continuous entries
    """

    def __init__(self, columns: list[ContinuousEntry]):
        self.columns = columns


class SensitiveEntry:
    """A class to represent a sensitive column entry

    Attributes
    ----------
    name : str
          Name of the column
    method : str
          Faking method
    locales : list[str]
          List of locales
    seed : str
          Randomization seed

    """

    def __init__(self, name: str, method: str, locales: list[str], seed: str):
        self.name = name
        self.method = method
        self.locales = locales
        self.seed = seed


class SensitiveConfig:
    """A class to represent a continuous column config

    Attributes
    ----------
    columns : list[SensitiveEntry]
          Name of the column
    """

    def __init__(self, columns: list[SensitiveEntry]):
        self.columns = columns
