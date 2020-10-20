import numpy as np
import pandas as pd
from quant.data import np_nan_array
from quant.helpers import debug_print
from quant.operation import Operation

class OpDebug(Operation):
    def __init__(self, header=''):
        super().__init__()
        self.header = header
        pass

    def do_op(self, daily_alpha_data, instrument_pool_data, di, data):
        debug_print(daily_alpha_data, self.header)
        return daily_alpha_data
