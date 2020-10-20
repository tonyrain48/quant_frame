import numpy as np
from quant.operation import Operation
from quant.data import np_nan_array


class OpNeutral(Operation):
    '''
    行业中性
    neutral_industry： 所选行业（名称）
    industry_data：对应股票所在的行业序号，用整数表示
    '''
    
    
    def __init__(self, neutral_industry, market_neutral=False, discrete_group=False, delay=1):
        super().__init__()
        self.neutral_industry = neutral_industry
        self.market_neutral = market_neutral
        self.discrete_group = discrete_group
        self.delay = delay
        pass

    def do_op(self, daily_alpha, instrument_pool_data, di, data):
        # both alpha and industry_data are TensorDimData on one day, they are numpy ndarrays with NAN possibly
        # none_group means group neutral for None value
        valid = np.argwhere(~np.isnan(daily_alpha)) 
        if len(valid):
            
            # 市场中性
            if self.market_neutral:
                daily_alpha[valid] = daily_alpha[valid] - np.mean(daily_alpha[valid])  # 去均值
            else:
                # 提取行业数据
                industry_data = np_nan_array(shape=daily_alpha.shape, dtype="int32")
                industry_data[valid] = data[self.neutral_industry].data[di-self.delay][valid]
                # 行业中性
                for i in range(np.max(industry_data[valid]) + 1):
                    index_i = np.argwhere(industry_data == i)
                    daily_alpha[index_i] = daily_alpha[index_i] - np.mean(daily_alpha[index_i])
                
                # 有些股票没有被分到某一个行业中，可以把这部分股票剔除或者按同一个行业处理
                index_none = np.argwhere(industry_data == -1)
                if self.discrete_group:
                    daily_alpha[index_none] = 0
                else:
                    daily_alpha[index_none] = daily_alpha[index_none] - np.mean(daily_alpha[index_none])
        else:
            pass

        return daily_alpha
