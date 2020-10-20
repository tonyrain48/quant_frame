import numpy as np
from quant.operation import Operation
from quant.data import np_nan_array


class OpNeutral(Operation):
    def __init__(self, neutral_industry, market_neutral=False,discrete_group=True):
        self.neutral_industry = neutral_industry
        self.market_neutral = market_neutral
        self.discrete_group = discrete_group
        pass

    def do_op(self, daily_alpha, instrument_pool_data, di, data):
        # both alpha and industry_data are TensorDimData on one day, they are numpy ndarrays with NAN possibly
        # none_group means group neutral for None value
        valid = np.argwhere(~np.isnan(daily_alpha))
        if self.market_neutral:
            average_alpha = sum (daily_alpha[valid]) / len(valid)
            daily_alpha[valid] = daily_alpha[valid] - average_alpha
        else:
            industry_data = np_nan_array(shape=daily_alpha.shape, dtype="int32")
            industry_data[valid] = data[self.neutral_industry].data[di][valid]

            for i in range(np.max(industry_data[valid]) + 1):
                index_i = np.argwhere(industry_data == i)
                if len(index_i):
                    average_i = sum (daily_alpha[index_i]) / len(index_i)
                    daily_alpha[index_i] = daily_alpha[index_i] - average_i

            index_none = np.argwhere(industry_data == -1)
            if len(index_none):
                if not self.discrete_group:
                    average_none = sum (daily_alpha[index_none]) / len(index_none)
                    daily_alpha[index_none] = daily_alpha[index_none] - average_none
                else:
                    daily_alpha[index_none] = 0
        return daily_alpha
