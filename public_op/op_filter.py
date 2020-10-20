from quant.operation import Operation
from quant.data import np_nan_array
import numpy as np

class OpFilter(Operation):
    def do_op(self, daily_alpha_data, instrument_pool_data, di, data):
        super().__init__()
        res = daily_alpha_data.copy()
        #UNTESTED !!!!
        res[instrument_pool_data[di-1] == False] = np.nan
        return res


if __name__ == '__main__':
    nrow, ncol = [1, 10]
    data = np.random.uniform(0,10,size = (nrow, ncol))[0]
    ip = np.random.randint(0, 2, size = (nrow, ncol))[0]
    op = OpFilter()
    print('data = {}'.format(data))
    print('ip = {}'.format(ip))
    print('{}'.format(op.do_op(data, ip, None, None)))
