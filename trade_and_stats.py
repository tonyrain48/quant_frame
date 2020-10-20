import numpy as np
import os.path
from quant.constants import WORK_BASE_DIR, DATE_FORMAT_STRING, TITLE_FOR_EXP, TITLE_FOR_ATT
from quant.data import np_nan_array
from quant.helpers import Logger, CMD, parse_dim_index, nan_cov, debug_print, transform_one_hot


class AbstractTradeAndStatsEngine(object):
    def __init__(self, user_name, project_name, universe, pnl_sub_path):
        """
        :param user_name: 
        :param project_name:
        :param universe: 
        :param required_dims: 
        :param intrument_pool: it'a also treated as a dim data, the name
        :param instrument_pool_path:
        """
        self.user_name = user_name
        self.project_name = project_name
        self.universe = universe
        self.pnl_sub_path = pnl_sub_path
        self.log_prefix = "[" + self.__class__.__name__ + "]"
        self.f = {}
        self.result_list = ['TRADE_DT','PNL','LONG_SIZE','SHORT_SIZE','NET_SIZE','TVR_VOLUME','TVR_VALUE','LONG_NUM','SHORT_NUM','IC']
        
        self.tmp_data = {}
        self.tmp_alpha_data = {}
        for result_name in self.result_list:
            self.tmp_alpha_data[result_name] = []
        pass
    
    # 创建一个文件，用来写入需要保存的数据
    def open_pnl_files(self, alpha_names, file_styles):
        '''
        alpha_names: 待回测的alpha名称
        file_styles: 暂时默认 [pnl]
        '''
        for alpha_name in alpha_names:
            for file_style in file_styles:

                file_path = os.path.join(WORK_BASE_DIR, "{}/{}/pnl/{}/{}.{}".format(self.user_name, self.project_name, self.pnl_sub_path, alpha_name, file_style))
                dir_name = os.path.dirname(file_path)
                if not os.path.exists(dir_name):
                    os.makedirs(dir_name)
                
                self.tmp_data[alpha_name + file_style] = self.tmp_alpha_data
                self.f[alpha_name + file_style] = open(file_path, "w")
                self.f[alpha_name + 'pnl'].write('|'.join(self.result_list) + '\n')
        pass

    def close_pnl_files(self):
        for alpha_name, opened_file in self.f.items():
            opened_file.close()
        pass

    def do_trade_and_stats(self, di, current_date, alpha_name, start_di, current_day_alpha_data, instrument_pool_data, dim_data, prevday_daily_pos, size, multiple, trade_cost, alpha_delay, do_attribution, common_factor_list, covariance_data, factor_return):
        pass


class TradeAndStatsSimple(AbstractTradeAndStatsEngine):
    def __init__(self, user_name, project_name, universe, pnl_sub_path, consider_suspend=True):
        super().__init__(user_name, project_name, universe, pnl_sub_path)
        self.consider_suspend = consider_suspend
    
    
    def do_trade_and_stats(self, di, current_date, alpha_name, start_di, current_day_alpha_data, instrument_pool_data, dim_data,\
                           prevday_daily_pos, size,multiple, trade_cost, alpha_delay, do_attribution, common_factor_list, covariance_data, factor_return):
        
        # 提取用于回测的基础数据  收盘价  复权因子  收益率等
        close_data = dim_data["CLOSEPRICE"].data[di]
        adj_factor = dim_data["ADJ_FACTOR"].data[di]
        returns = dim_data["RETURN"].data[di]

        # 把相应的alpha值保存到pooled_alpha_data变量中 
        pooled_alpha_data = np_nan_array(shape=current_day_alpha_data.shape, dtype='float64') # 初始化 n×1的向量
        pooled_alpha_data[instrument_pool_data.data[di - alpha_delay]] = current_day_alpha_data[instrument_pool_data.data[di - alpha_delay]]

        #white list all index ii
        index_code_list = ['000300.SH', '000905.SH']
        index_ii_list = [self.universe.secu_code_to_index[x] for x in index_code_list]
        for index_ii in index_ii_list:
            pooled_alpha_data[index_ii] = current_day_alpha_data[index_ii]
        
        # 计算每只股票的持仓（股）
        scaled_alpha = np.multiply(size,np.divide(pooled_alpha_data, max(np.nansum(np.abs(pooled_alpha_data)),1))) # 先将因子值归一化，再乘以size，得到理论投资金额
        daily_pos = np.multiply(np.round(np.divide(np.divide(scaled_alpha, close_data), multiple)), multiple)      # 用100股调整，得到实际投资金额
        
        
        
        # 检查是否停牌
        if self.consider_suspend:
            trade_state_data = dim_data["TRADESTATE"].data[di]
            suspend_valid = np.array(trade_state_data)=='Suspend'
            trade_valid = np.logical_not(suspend_valid)
            # 计算真实的 long short
            long_valid = scaled_alpha>0
            short_valid = scaled_alpha<=0

            size_long = np.nansum(scaled_alpha[long_valid])
            size_short = np.nansum(scaled_alpha[short_valid])

            if di == start_di:
                prevday_daily_pos = np_nan_array(shape=current_day_alpha_data.shape, dtype='float64')
            daily_pos[suspend_valid] = np.multiply(prevday_daily_pos[suspend_valid], adj_factor[suspend_valid])

            suspend_long_valid = np.logical_and(suspend_valid, prevday_daily_pos>0)
            suspend_short_valid = np.logical_and(suspend_valid, prevday_daily_pos<=0)
            size_long_suspend = np.nansum(np.multiply(daily_pos[suspend_long_valid], close_data[suspend_long_valid]))
            size_short_suspend = np.nansum(np.multiply(daily_pos[suspend_short_valid], close_data[suspend_short_valid]))
            size_long_trade = size_long - size_long_suspend
            size_short_trade = size_short - size_short_suspend
            long_trade_valid = np.logical_and(trade_valid, long_valid)
            short_trade_valid = np.logical_and(trade_valid, short_valid)
            scaled_alpha[long_trade_valid] = np.divide(scaled_alpha[long_trade_valid], np.nansum(np.abs(scaled_alpha[long_trade_valid])))
            scaled_alpha[short_trade_valid] = np.divide(scaled_alpha[short_trade_valid], np.nansum(np.abs(scaled_alpha[short_trade_valid])))
            daily_pos[long_trade_valid] = np.round(np.divide(size_long_trade*scaled_alpha[long_trade_valid], close_data[long_trade_valid])/multiple)*multiple
            daily_pos[short_trade_valid] = -np.round(np.divide(size_short_trade*scaled_alpha[short_trade_valid], close_data[short_trade_valid])/multiple)*multiple
        
        #================交易并且更新净值==================
        
        # 计算换手率 以及 手续费
        prevday_close_data = dim_data["CLOSEPRICE"].data[di-1]
        '''
        ic_alpha: 真实的投资金额（最小单位100股调整之后的投资金额）
        '''
        
        if di == start_di:
            ic_alpha = np.zeros(shape=pooled_alpha_data.shape)
            tvr_shares = np.abs(daily_pos)
        else: 
            ic_alpha = np.multiply(prevday_daily_pos, prevday_close_data)
            tvr_shares = np.abs(daily_pos - np.multiply(prevday_daily_pos, adj_factor))
            # 计算换手率的时候  需要使用 累计复权因子 × 前一天的持仓股数
        
        tvr_value = np.nansum(np.multiply(tvr_shares, close_data))
        commission = tvr_value * trade_cost
        
        # 计算pnl
#        pnl_before_commission = np.multiply(ic_alpha, returns)
        pnl_before_commission = np.nansum(np.multiply(ic_alpha, returns))
        pnl_after_commission = pnl_before_commission - commission

        # 
        daily_pos_money = np.multiply(daily_pos, close_data) # 每只股票实际投资的钱数
        long_valid = daily_pos_money > 0.0   # 做多的股票
        long_size = round(np.sum(daily_pos_money[long_valid]), 2)  # 计算多头持仓
        short_valid = daily_pos_money < 0.0  # 做空的股票
        short_size = round(np.sum(daily_pos_money[short_valid]), 2)  # 计算空头持仓

        # 保存回测数据
        # todo : 增加rankic
        pnl_stats = [current_date.strftime(DATE_FORMAT_STRING), round(pnl_after_commission, 2),
                     long_size, short_size, long_size - short_size, int(np.nansum(tvr_shares)),
                     round(tvr_value, 2), len([0 for x in daily_pos if x > 0]), len([0 for x in daily_pos if x < 0]),
                     round(nan_cov(ic_alpha, returns)[1], 6)]

        # 输出日志
        Logger.debug(self.log_prefix, ' '.join(map(str, pnl_stats)))
        self.f[alpha_name + 'pnl'].write('|'.join(map(str, pnl_stats)) + '\n')
        
        if not do_attribution:
            return daily_pos
        
        # Attribution and Exposure
        specirisk_data = dim_data["SPECIRISK"].data[di-1]
        common_factor_exposure = []
        common_factor_covariance_data = []
        for factor_index, common_factor in enumerate(common_factor_list):
            common_factor_exposure.append(dim_data[common_factor].data[di-1])
            common_factor_covariance_data.append(covariance_data[common_factor][di-1])
        common_factor_exposure = np.array(common_factor_exposure)
        common_factor_covariance_data = np.array(common_factor_covariance_data)

        common_factor_exposure = np.nan_to_num(common_factor_exposure)
        if di == start_di:
            prevday_daily_pos = np.zeros(shape=pooled_alpha_data.shape)
        prevday_daily_pos_money = np.nan_to_num(np.multiply(prevday_daily_pos, prevday_close_data))
        exposure_for_position = np.nansum(np.multiply(prevday_daily_pos_money, common_factor_exposure), axis = 1)
        factor_pnl_for_position = np.multiply(exposure_for_position, factor_return[di])
        total_factor_pnl = np.nansum(factor_pnl_for_position)
        specireturn_pnl = np.nansum(pnl_after_commission) - total_factor_pnl
        att_stats = [current_date.strftime(DATE_FORMAT_STRING), long_size - short_size, round(np.nansum(pnl_after_commission), 2),
                     round(total_factor_pnl, 2), round(specireturn_pnl, 2)]
        att_stats = att_stats + factor_pnl_for_position.tolist()
        if di == start_di:
            self.f[alpha_name + 'att'].write(' '.join(TITLE_FOR_ATT + common_factor_list) + '\n')
        self.f[alpha_name + 'att'].write(' '.join(map(str, att_stats)) + '\n')

        specirisk_data = np.nan_to_num(specirisk_data)
        factor_risk = np.dot(np.dot(prevday_daily_pos_money, np.dot(np.dot(common_factor_exposure.T, common_factor_covariance_data), common_factor_exposure)), prevday_daily_pos_money.T) / 10000
        specific_risk = np.dot(np.dot(prevday_daily_pos_money, np.diag(np.power(specirisk_data, 2))), prevday_daily_pos_money.T) / 10000

        factor_exposure = np.dot(prevday_daily_pos_money, common_factor_exposure.T)
        risk_contribution = np.multiply(np.dot(common_factor_covariance_data, factor_exposure.T), factor_exposure.T) / 10000

        exp_stats = [current_date.strftime(DATE_FORMAT_STRING), long_size - short_size, round(factor_risk + specific_risk, 2),
                     round(factor_risk, 2), round(specific_risk, 2)]
        exp_stats = exp_stats + exposure_for_position.tolist() + np.nan_to_num(risk_contribution).tolist()
        if di == start_di:
            self.f[alpha_name + 'exp'].write(' '.join(TITLE_FOR_EXP + [x + '_Exp' for x in common_factor_list] + [x + '_Risk' for x in common_factor_list]) + '\n')
        self.f[alpha_name + 'exp'].write(' '.join(map(str, exp_stats)) + '\n')

        return daily_pos
    
    
class TradeAndStatsPure(AbstractTradeAndStatsEngine):
    def __init__(self, user_name, project_name, universe, pnl_sub_path, consider_suspend=True):
        super().__init__(user_name, project_name, universe, pnl_sub_path)
        self.consider_suspend = consider_suspend

    def do_trade_and_stats(self, di, current_date, alpha_name, start_di, current_day_alpha_data, instrument_pool_data, dim_data,\
                           prevday_daily_pos, size,multiple, trade_cost, alpha_delay, do_attribution, common_factor_list, covariance_data, factor_return):
        # 提取用于回测的基础数据  收盘价  复权因子  收益率等
        close_data = dim_data["CLOSEPRICE"].data[di]
        adj_factor = dim_data["ADJ_FACTOR"].data[di]
        returns = dim_data["RETURN"].data[di]
        indus_cap = dim_data['INDUSCAP'].data[di] # 行业市值数据有问题， 需要查看。 (universe中的股票，没有行业对应 或者 没有市值数据对应)
        valid = instrument_pool_data.data[di - alpha_delay]

        # 初始化final_alpha
        pooled_alpha_data = np_nan_array(shape=current_day_alpha_data.shape, dtype='float64') # 初始化 n×1的向量
        
        
        # 需要其他因子数据  一起放到X中， 这里需要扩展alpha_array,第一列为目标因子，其他列为方程中的其他因子
        
        # 计算X （alpha、industry_dummy、country） (done)
        alpha_array = current_day_alpha_data[valid]
        
        indus_data = dim_data['ZXF'].data[di][valid]
        indus_dummy_var = transform_one_hot(indus_data)
        country = np.ones(shape = (len(alpha_array),1))
        
        X = np.hstack([alpha_array.reshape(-1,1),country,indus_dummy_var])
        
        # 计算V  （股票的市值倒数） （done）
        cap_data = dim_data['TOTALSHARES'].data[di][valid]
        weight = np.sqrt(cap_data) / np.sqrt(cap_data).sum()
        v_n = weight.ravel()
        V = np.diag(v_n)
        
        
        # 计算行业的市值权重 （约束条件：行业市值加权和为1） (todo)
        sum_cap = np.sum(np.unique(indus_cap[valid]))
        weight = indus_cap[valid] / sum_cap
        weight_dict = dict(zip(indus_data,weight))
        
        indus_keys= [i for i in weight_dict.keys()]
        indus_keys.sort()
        
        weight_array = np_nan_array(shape = (len(indus_keys)), dtype='float64')
        for i,w in enumerate(indus_keys):
            weight_array[i] = weight_dict[w]
        
        adj_weight = weight_array / weight_array[-1]
        adj_weight[-1] = 0
        adj_weight = adj_weight * -1
        
        
        
        # 计算R  （对X做变形）
        cons = np.diag(np.ones(X.shape[1]))
        indus_num = indus_dummy_var.shape[1]
        cons[-1:,-indus_num:] = adj_weight
        R = cons[:,:-1]
        
        
        # 计算omega = R(R'X'VXR)-1R'X'V  (todo)
        omega1 = np.linalg.inv(np.dot(np.dot(np.dot(np.dot(R.T,X.T),V),X),R))
        omega = np.dot(np.dot(np.dot(np.dot(R,omega1),R.T),X.T),V)
        
        final_alpha = omega[0,:]
        pooled_alpha_data[valid] = final_alpha
        
        
        # 计算每只股票的持仓金额
        daily_pos = np.multiply(size,np.divide(pooled_alpha_data, max(np.nansum(np.abs(pooled_alpha_data)),1))) # 先将因子值归一化，再乘以size，得到理论投资金额 
        
        
        #================交易并且更新净值==================
        
        # 计算换手率 以及 手续费
        prevday_close_data = dim_data["CLOSEPRICE"].data[di-1]
        '''
        ic_alpha: 真实的投资金额（最小单位100股调整之后的投资金额）
        '''
        
        if di == start_di:
            ic_alpha = np.zeros(shape=pooled_alpha_data.shape)
            tvr_shares = np.abs(daily_pos)
        else: 
            ic_alpha = np.multiply(prevday_daily_pos, prevday_close_data)
            tvr_shares = np.abs(daily_pos - np.multiply(prevday_daily_pos, adj_factor))
            # 计算换手率的时候  需要使用 累计复权因子 × 前一天的持仓股数
        
        tvr_value = np.nansum(np.multiply(tvr_shares, close_data))
        commission = tvr_value * trade_cost
        
        # 计算pnl
#        pnl_before_commission = np.multiply(ic_alpha, returns)
        pnl_before_commission = np.nansum(np.multiply(ic_alpha, returns))
        pnl_after_commission = pnl_before_commission - commission

        # 
        daily_pos_money = np.multiply(daily_pos, close_data) # 每只股票实际投资的钱数
        long_valid = daily_pos_money > 0.0   # 做多的股票
        long_size = round(np.sum(daily_pos_money[long_valid]), 2)  # 计算多头持仓
        short_valid = daily_pos_money < 0.0  # 做空的股票
        short_size = round(np.sum(daily_pos_money[short_valid]), 2)  # 计算空头持仓

        # 保存回测数据
        # todo : 增加rankic
        pnl_stats = [current_date.strftime(DATE_FORMAT_STRING), round(pnl_after_commission, 2),
                     long_size, short_size, long_size - short_size, int(np.nansum(tvr_shares)),
                     round(tvr_value, 2), len([0 for x in daily_pos if x > 0]), len([0 for x in daily_pos if x < 0]),
                     round(nan_cov(ic_alpha, returns)[1], 6)]

        # 输出日志
        Logger.debug(self.log_prefix, ' '.join(map(str, pnl_stats)))
        self.f[alpha_name + 'pnl'].write('|'.join(map(str, pnl_stats)) + '\n')
        
        
        return daily_pos

