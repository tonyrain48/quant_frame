#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from quant.operation import Operation
from quant.data import np_nan_array
class OpStats(Operation):
    def __init__(self, industry='ZXS', header=''):
        super().__init__()
        self.industry = industry
        self.header = header
        pass
    
    def _breadth(self,daily_alpha_data):
        valid = np.argwhere(~np.isnan(daily_alpha_data)) 
        norm1 = np.linalg.norm(daily_alpha_data[valid],ord=1)
        norm2 = np.linalg.norm(daily_alpha_data[valid])
        return np.power(norm1/norm2,2)

    def _industry(self, daily_alpha_data, instrument_pool_data, di, data):
        valid = np.argwhere(~np.isnan(daily_alpha_data))
        industry_data = np_nan_array(shape=daily_alpha_data.shape, dtype='int32')
        industry_data[valid] = data[self.industry].data[di][valid]
        sum1 = 0
        sum2 = 0
        for i in range(np.max(industry_data[valid])+1):
            index_i = np.argwhere(industry_data == i)
            ts = pd.DataFrame(daily_alpha_data[index_i])
            sum1 += np.abs(ts.sum()[0])
            sum2 += ts.abs().sum()[0]
        return sum1/sum2
        
    def _print(self,daily_alpha_data,instrument_pool_data, di ,data):
        print('{}:the breadth of alpha data is {}'.format(self.header,self._breadth(daily_alpha_data)))
        print('{}:the all industry exposure of alpha is {}'.format(self.header,self._industry(daily_alpha_data, instrument_pool_data, di ,data)))
        pass
        
    def do_op(self, daily_alpha_data, instrument_pool_data, di, data):
        self._print(daily_alpha_data, instrument_pool_data, di, data)
        return daily_alpha_data
    
    
