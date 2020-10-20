import pandas as pd
import numpy as np
import itertools
from quant.operation import Operation


class OpWinsorize(Operation):
    '''
    recognize extreme value and replace these with up_value or down_value
    
    Params:
        method:
            'mad':
                use Median Absolute Deviation(MAD) instead of mean
                md = median(ts)
                MAD = median(|ts - md|)
                use 1.483*MAD instead of std
                this method is more robust than 'mv'
            'boxplot':
                IOR = Q3 - Q1
                data beyong [Q1 - 3 * IQR, Q3 + 3 * IQR] as abnormal values
                boxplot is not sensitive with extreme values
                when tha data is positive skew and right fat tailed, too much 
                data will divide into extreme values
            'boxplot_skew_adj':
                md = median(ts)
                mc = median(((x_i - md) - (md - x_j)) / (x_i - x_j))
                where x_i > md and x_j < md
                L = ... and U = ...
    
    '''
    def __init__(self,method:str):
        super().__init__()
        self.method = method
        pass
    
    def do_op(self, daily_alpha_data:np.array,instrument_pool_data, di, data)->np.array:
        
        non_nan_index = np.where(~np.isnan(daily_alpha_data))[0]
        ts_copy = daily_alpha_data.copy()
        ts_non_nan = ts_copy[non_nan_index]
        
        
        if self.method == 'mad':
            threshold = self._op_MAD(ts_non_nan)
        elif self.method == 'boxplot':
            threshold = self._op_boxplot(ts_non_nan)
        elif self.method == 'boxplot_adj':
            threshold = self._op_boxplot_adj(ts_non_nan)
        else:
            raise ValueError('No method called :{},please check the input method!'.format(self.method))
        self._do_stats(ts_non_nan,threshold[0],threshold[1])
        ts_mod = self._do_winsorize(ts_non_nan,threshold[0],threshold[1])
        
        daily_alpha_data[non_nan_index] = ts_mod
        return daily_alpha_data
        pass
    
    
    def _op_MAD(self, daily_alpha_data:np.array)->list:
        md = np.median(daily_alpha_data)
        md_array = np.ones(len(daily_alpha_data)) * md
        mad = pd.Series(daily_alpha_data - md_array).abs().median()
        up = md + 3 * 1.483 * mad
        down = md - 3 * 1.483 * mad
        return up,down
        
    
    def _op_boxplot(self, daily_alpha_data:np.array)->list:
        q1 = np.percentile(daily_alpha_data,25)
        q3 = np.percentile(daily_alpha_data,75)
        iqr = q3 - q1
        up = q3 + 3 * iqr
        down = q1 - 3* iqr
        return up,down
           
    def _op_boxplot_adj(self, daily_alpha_data:np.array)->list:
        md = np.median(daily_alpha_data)
        ts = daily_alpha_data.copy()
        x_u,x_d = ts[ts > md], ts[ts < md]
        h = [[(x_u[k] + x_d[i] - 2 * md )/(x_u[k] + x_d[i]) for i in range(len(x_d))] for k in range(len(x_u))]
        h_mod = list(itertools.chain.from_iterable(h))
        mc = np.median(h_mod)
        q1 = np.percentile(daily_alpha_data,25)
        q3 = np.percentile(daily_alpha_data,75)
        iqr = q3 - q1
        if mc >= 0:
            down = q1 - 1.5 * np.exp(-3.5 * mc) * iqr
            up = q3 + 1.5 * np.exp(4 * mc) * iqr
        else:
            down = q1 - 1.5 * np.exp(-4 * mc) * iqr
            up = q3 + 1.5 * np.exp(3.5 * mc) * iqr
        return up,down
    
    def _do_winsorize(self,daily_alpha_data:np.array,up:float,down:float)->np.array:
        ts = daily_alpha_data.copy()
        ts[ts>up] = up
        ts[ts<down] = down
        return ts
    
    def _do_stats(self,daily_alpha_data:np.array,up:float,down:float)->float:
        ts = daily_alpha_data.copy()
        if len(ts)>0:
            extreme_ratio = 1.0 * (len(ts[ts>up]) + len(ts[ts<down]))/len(ts)  
        else:
            extreme_ratio = np.nan
        print("{} of data is regarded as extreme value in the mode of {}".format(extreme_ratio,self.method))
        pass
