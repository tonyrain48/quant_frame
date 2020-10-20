import numpy as np
import pandas as pd
from quant.data import np_nan_array
from quant.operation import Operation

class OpPower(Operation):
    def __init__(self, power = 1.0, rank_type = 1):
        """
        :param rank_type: 0.no rank
                           1.rank data to [-1,1] 
                           2.rank data to [0,1]
                           3.rank data to [-1,0]
                           4.rank data to [-1,1] respectively with positive and negative group
        :param power:
        """
        super().__init__()
        self.rank_type = rank_type
        self.power = power
        pass

    def do_op(self, daily_alpha_data, instrument_pool_data, di, data):
        if self.rank_type == 0:
            self._op_power(daily_alpha_data)
        elif self.rank_type == 4:
            oprank_group_alpha = self._op_rank_with_group(daily_alpha_data)
            self._op_power(oprank_group_alpha)
        else:
            oprank_alpha = self._op_rank(daily_alpha_data)
            self._op_power(oprank_alpha)
        return self.oppower_alpha

        pass

    def _op_rank(self,daily_alpha_data):
        minvalue = -1
        maxvalue = 1
        if self.rank_type == 2:
            minvalue = 0
        elif self.rank_type == 3:
            maxvalue = 0

        rankdata = np.array(pd.DataFrame(daily_alpha_data).rank(ascending=True)[0].tolist())
        oprank_alpha = minvalue + (maxvalue - minvalue) / (np.sum(~np.isnan(daily_alpha_data)) - 1) * (rankdata-1)

        oprank_alpha = np.array(oprank_alpha)

        return oprank_alpha

    def _op_rank_with_group(self,daily_alpha_data):
        if self.rank_type == 4:
            minvalue = -1
            maxvalue = 1
            midvalue = 0
            oprank_group_alpha = daily_alpha_data

        positive_alpha_index = np.where(daily_alpha_data > 0)[0]
        positive_alpha = daily_alpha_data[positive_alpha_index]
        negative_alpha_index = np.where(daily_alpha_data < 0)[0]
        negative_alpha = daily_alpha_data[negative_alpha_index]


        if len(positive_alpha) > 0:
            rankdata_positive = np.array(pd.DataFrame(positive_alpha).rank(ascending=True)[0].tolist())
            oprank_alpha_positive = midvalue + (maxvalue - midvalue) / (np.sum(~np.isnan(positive_alpha))) * rankdata_positive
            oprank_group_alpha[positive_alpha_index] = oprank_alpha_positive

        if len(negative_alpha) > 0:
            rankdata_negative = np.array(pd.DataFrame(negative_alpha).rank(ascending=True)[0].tolist())
            oprank_alpha_negative = minvalue + (midvalue - minvalue) / (np.sum(~np.isnan(negative_alpha))) * (rankdata_negative - 1)
            oprank_group_alpha[negative_alpha_index] = oprank_alpha_negative

        oprank_group_alpha = np.array(oprank_group_alpha)

        return oprank_group_alpha

    def _op_power(self, alpha):
        self.oppower_alpha = np_nan_array(shape = alpha.shape, dtype="float64")
        zeros_index = np.where(alpha == 0)[0]
        self.oppower_alpha = alpha / abs(alpha) * np.power(abs(alpha), self.power)
        self.oppower_alpha[zeros_index] = 0
        # for i in range(len(valid_index)):
        #     if alpha[i] == 0 | np.isnan(alpha[i]):
        #         self.oppower_alpha[i] = alpha[i]
        #     else:
        #         self.oppower_alpha = alpha/abs(alpha) * np.power(abs(alpha), self.power)
        return self.oppower_alpha

