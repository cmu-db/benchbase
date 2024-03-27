class DPConfig:
    def __init__(self,table_name:str,epsilon:str,preproc_eps:str,algorithm:str,hidden:list[str],categorical:list[str],continuous:list[str],ordinal:list[str]):
           self.epsilon = epsilon
           self.table_name = table_name
           self.preproc_eps = preproc_eps
           self.algorithm = algorithm
           self.hidden = hidden
           self.categorical = categorical
           self.continuous = continuous
           self.ordinal = ordinal

class ContinuousEntry:
     def __init__(self,name:str,bins:str,lower:str,upper:str):
          self.name=name
          self.bins=bins
          self.lower=lower
          self.upper=upper
    
class ContinuousConfig:
    def __init__(self,columns:list[ContinuousEntry]):
         self.columns=columns

class SensitiveEntry:
    def __init__(self,name:str,method:str,locales:list[str],seed:str):
         self.name=name
         self.method=method
         self.locales=locales
         self.seed=seed

class SensitiveConfig:
    def __init__(self,columns:list[SensitiveEntry]):
         self.columns=columns
         

