from quant.operation import Operation
from quant.alpha import AbstractAlphaEngine
from quant.data import np_nan_array
import pandas as pd


class OpOmega(Operation):
    def __init__(self, q, op_required_dims, qnum):
        super().__init__()
        self.op_required_dims = op_required_dims
        self.q = q
        self.qnum = qnum
        pass

    def do_op(self, daily_alpha_data, instrument_pool_data, di, data):
        omega = self.calculate_omega(instrument_pool_data, di, data)
        alpha = np_nan_array(shape=omega.shape, dtype="float64")
        omega_rank = pd.Series(omega).rank(pct=1)
        omega_valid = (omega_rank > (self.q-1)/self.qnum) & (omega_rank <= self.q/self.qnum)
        alpha[omega_valid] = daily_alpha_data[omega_valid]

        return alpha

    def calculate_omega(self, instrument_pool_data, di, data, delay):
        pass


class OpDimOmega(OpOmega):
    def __init__(self, q, op_dim, qnum=10):
        super().__init__(q, [op_dim], qnum)
        self.op_dim = op_dim
        pass

    def calculate_omega(self, instrument_pool_data, di, data, delay=1):
        valid = instrument_pool_data.data[di-delay]
        omega = np_nan_array(shape=valid.shape, dtype="float64")

        omega[valid] = data[self.op_dim].data[di-delay][valid]
        return omega

