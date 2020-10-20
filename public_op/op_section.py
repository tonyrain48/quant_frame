from quant.operation import Operation
from quant.data import np_nan_array
import numpy as np
import pandas as pd

class OpSection(Operation):
    '''
    对股票进行分组
    这里目前是返回某一组的因子值为1，其它因子值为空  也即变成了等权

    To do： 返回一个向量，值为因子值对应的分组
            增加市值加权
    '''
    def __init__(self, q, qnum = 10):
        super().__init__()
        self.q = q
        self.qnum = qnum

    def do_op(self, daily_alpha_data, instrument_pool_data, di, data):
        arr = np_nan_array(shape = daily_alpha_data.shape, dtype = "float64")
        arr[~np.isnan(daily_alpha_data)] = 0.0
        alpha_rank = pd.Series(daily_alpha_data).rank(pct = 1)
        arr[(alpha_rank > (self.q - 1)/self.qnum) & (alpha_rank <= self.q/self.qnum)] = 1
        return arr

if __name__ == '__main__':
    nrow, ncol = 1, 30
    data = np.random.uniform(-1, 1, size = (nrow, ncol))[0]
    nan_num = np.random.randint(1, 30, size = 1)[0]
    nan_index = np.random.choice(ncol, nan_num, replace = False)
    data[nan_index] = np.nan
    print('raw data is {}'.format(data))
    print('number of nan is {}'.format(nan_num))
    print('------op1:{}'.format(OpSection(1,3).do_op(data, None, None, None)))
    print('------op1:{}'.format(OpSection(1).do_op(data, None, None, None))) 
