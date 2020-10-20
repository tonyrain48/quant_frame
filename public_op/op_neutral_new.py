# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 14:01:57 2020

中性化：新增其他因子中性

@author: quhon
"""

import numpy as np
from quant.operation import Operation
from quant.data import np_nan_array
from quant.helpers import transform_one_hot,nan_neutral

class OpNeutralNew(Operation):
    '''
    中性化
    neutral_industry：str， 所选行业（名称）
    cap_neutral: str，使用何种市值数据，一般用总市值 
    other_neutral: list，想要中性化的因子，列表格式
    
    '''
    
    
    def __init__(self, neutral_industry,cap_neutral,other_neutral=[], market_neutral=False, discrete_group=False, delay=1):
        super().__init__()
        self.neutral_industry = neutral_industry
        self.market_neutral = market_neutral
        self.discrete_group = discrete_group
        self.cap_neutral = cap_neutral
        self.other_neutral = other_neutral
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
                # 行业中性
                industry_data = data[self.neutral_industry].data[di-self.delay][valid]
                X = transform_one_hot(industry_data.ravel())
                y = daily_alpha[valid]
                
                # 可能存在 股票池中不包括某个行业，则该行业会全为0， 需要剔除。否则不满秩
                X = X[:,np.sum(X,axis=0)!= 0]
                
                # 市值中性
                if self.cap_neutral:
                    cap_data = data[self.cap_neutral].data[di-self.delay][valid]
                    X = np.hstack((X,cap_data))
                
                # 其它因子中性
                if self.other_neutral:
                    for data_name in self.other_neutral:
                        tmp_data = data[data_name].data[di-self.delay][valid]
                        X = np.hstack((X,tmp_data))

                # 回归取残差
                daily_alpha[valid] = y - np.dot(X,np.dot(np.dot(np.linalg.inv(np.dot(X.T,X)),X.T),y))
                
                # 有些股票没有被分到某一个行业中，可以把这部分股票剔除或者按同一个行业处理
                index_none = np.argwhere(industry_data == -1)
                if self.discrete_group:
                    daily_alpha[index_none] = 0
                else:
                    daily_alpha[index_none] = daily_alpha[index_none] - np.mean(daily_alpha[index_none])
        else:
            pass

        return daily_alpha
