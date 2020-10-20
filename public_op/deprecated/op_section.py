# Created by yzhou and zshang
from quant.operation import Operation
from quant.data import np_nan_array
import numpy as np
import pandas as pd

class OpSection(Operation):
    def __init__(self, q, qnum = 10):
        self.q = q
        self.qnum = qnum

    def do_op(self, daily_alpha_data, instrument_pool_data, di, data):
        arr = np_nan_array(shape = daily_alpha_data.shape, dtype = "float32")
        alpha_valid = ~np.isnan(daily_alpha_data)
        valid_count = np.nansum(alpha_valid)
        arr[alpha_valid] = 0.0
        alpha_rank = np.array(pd.DataFrame(daily_alpha_data).rank(ascending=True)[0].tolist())
        section_index = np.array([all([x >= (self.q-1)/self.qnum*valid_count + 0.5, x < self.q/self.qnum*valid_count + 0.5]) for x in alpha_rank])
        arr[section_index] = 1.0
        return arr

if __name__ == '__main__':
    print('')
