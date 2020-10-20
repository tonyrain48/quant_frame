import numpy as np
from quant.operation import Operation
import pandas as pd
from quant.data import np_nan_array
class OpZscore(Operation):
    '''
    transfer the factor into the form of zscore(= (raw - mean)/std)
    Attention: do op_winsorize before op_zscore
    '''
    def __init__(self):
        super().__init__()
        pass
    
    def do_op(self, daily_alpha_data:np.array,instrument_pool_data, di, data)->np.array:
        non_nan_index = np.where(~np.isnan(daily_alpha_data))[0]
        zscore = daily_alpha_data.copy()
        ts_non_nan = zscore[non_nan_index]
        zscore[non_nan_index] = self._op_zscore(ts_non_nan)
        return zscore
    
    def _op_zscore(self,daily_alpha_data:np.array)->np.array:
        zscore = (daily_alpha_data - np.mean(daily_alpha_data))/np.std(daily_alpha_data)
        return zscore
