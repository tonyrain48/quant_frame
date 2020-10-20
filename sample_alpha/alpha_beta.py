# -*- coding: utf-8 -*-

from quant.data import np_nan_array
from quant.alpha import AbstractAlphaEngine
import numpy as np
from helpers import nan_cov

class AlphaBeta(AbstractAlphaEngine):
    def __init__(self, user_name, project_name, alpha_name, universe, instrument_pool,days,index_code, op_list=[], required_dims=[] , need_to_dump = False, delay = 1):
        super().__init__(user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims, need_to_dump) 
        self.days = days
        self.index_code = index_code
        self.delay= delay
        pass

    def _get_intrinsic_required_dims(self):
        return ["CLOSEPRICE", "CUM_ADJ_FACTOR",'S_DQ_CLOSE']

    def do_calculate_one_day_alpha(self, instrument_pool_data, di, data):
        delay = self.delay
        index_code = self.index_code
        valid = instrument_pool_data.data[di-delay]
        alpha = np_nan_array(shape=valid.shape, dtype="float64")
        
        index_ind = self.universe.secu_code_to_index[index_code]
        

        close = data["CLOSEPRICE"].data * data["CUM_ADJ_FACTOR"].data
        
        # 计算收益率
        ret_stk = close[di-delay-self.days:di-delay,valid] / close[di-delay-self.days-1:di-delay-1,valid] - 1
        
        # 剔除无效数据 重新计算valid
        new_valid = ~np.isnan(ret_stk.sum(axis=0))
        valid[valid] = new_valid
        ret_stk = ret_stk[:,new_valid]
        
        # 剔除长时间停牌的数据（收益率为0）
        new_valid2 = ~((ret_stk == 0).sum(axis=0) > self.days * 0.8)
        valid[valid] = new_valid2
        ret_stk = ret_stk[:,new_valid2]
        
        # 指数不需要复权
        mkt_close = data['S_DQ_CLOSE'].data
        
        # 分别计算指数收益率和股票收益率
        ret_mkt = mkt_close[di-delay-self.days:di-delay,index_ind] / mkt_close[di-delay-self.days-1:di-delay-1,index_ind] - 1
        
        # 计算beta
        X = ret_stk
        y = ret_mkt
        beta = np.dot(np.dot(np.linalg.inv(np.dot(X.T,X)),X.T),y)
        
        
        
        alpha[valid] = beta

        return alpha