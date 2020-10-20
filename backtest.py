import time
from datetime import date
import os.path
import numpy as np
import pickle
from quant.constants import WORK_BASE_DIR, COMMON_FACTOR_FOR_TL, COMMON_FACTOR_FOR_MSCI, COV_FACTOR_FOR_TL, COV_FACTOR_FOR_MSCI
from quant.data import DimData, TensorDimData, np_nan_array
from quant.helpers import Logger
from quant.universe import Universe
from quant.alpha import SampleAlpha1, SampleAlpha2

class BackTestEngine(object):
    def __init__(self, user_name, project_name, universe, size, multiple, trade_cost, test_alphas, trade_and_stats_engine, public_dim_path_template={}, prevday_pos_path="", do_attribution=False, barra_model=""):
        """
        :param user_name:
        :param project_name:
        :param size: 总金额
        :param multiple: 最小投资单位（100股）
        :param trade_cost: 手续费
        :param test_alphas: [alpha……] 需要回测的因子
        :param trade_engine
        :param stats_engine
        """
        
        self.user_name = user_name
        self.project_name = project_name
        self.universe = universe
        self.size = size
        self.multiple = multiple
        self.trade_cost = trade_cost
        self.test_alphas = test_alphas
        self.trade_and_stats_engine = trade_and_stats_engine
        self.public_dim_path_template = public_dim_path_template
        self.prevday_pos_path = prevday_pos_path
        self.prevday_pos = None
        self.do_attribution = do_attribution
        self.barra_model = barra_model
        self.log_prefix = "[" + self.__class__.__name__ + "]"

        self.file_styles = ['pnl', 'att', 'exp'] if do_attribution else ['pnl']
        self.required_dims = {"CLOSEPRICE", "ADJ_FACTOR", "RETURN", "TRADESTATE",'TOTALSHARES','INDUSCAP'}
        self.instrument_pools = set()
        
        
        for alpha in test_alphas:
            self.required_dims = self.required_dims | set(alpha.required_dims)
            for op in alpha.op_list:
                self.required_dims = self.required_dims | set(op.required_dims)
            self.instrument_pools.add(alpha.instrument_pool)

        if not do_attribution:
            self.common_factor_list = None 
            self.covariance_factor = None 
            self.barra_required_dims = None 
            self.barra_covariance_data = None
            self.factor_return = None
        elif self.barra_model == 'MSCI':
            self.required_dims = self.required_dims | set(COMMON_FACTOR_FOR_MSCI) | {"SPECIRISK"}
            self.common_factor_list = COMMON_FACTOR_FOR_MSCI
            self.covariance_factor = COV_FACTOR_FOR_MSCI
            self.barra_required_dims = set([factor + '_COV' for factor in COMMON_FACTOR_FOR_MSCI])
        else:
            self.required_dims = self.required_dims | set(COMMON_FACTOR_FOR_TL) | {"SPECIRISK"}
            self.common_factor_list = COMMON_FACTOR_FOR_TL
            self.covariance_factor = COV_FACTOR_FOR_TL
            self.barra_required_dims = set([factor + '_COV' for factor in COMMON_FACTOR_FOR_TL])

        self.dim_data_path_template = os.path.join(WORK_BASE_DIR, "{}/{}/dim/".format(user_name, project_name) + "{}.bin")
        self.alpha_data_path_template = os.path.join(WORK_BASE_DIR, "{}/{}/alpha/".format(user_name, project_name) + "{}.bin")
        self._load_tensor_dim_data()
        self._load_instrument_pool_data()
        self._get_prevday_pos()
        if do_attribution: 
            self._load_barra_covariance_data(self.common_factor_list, self.covariance_factor)
            self._load_barra_factor_return(self.common_factor_list)

        pass

    def _load_tensor_dim_data(self):
        '''
        加载数据
        dim_data : {'close':[(date1,{sec:values}),(date2,{sec:values}),……],
                    'low':[(date1,{sec:values}),(date2,{sec:values}),……],
                    ……
                    }
        '''
        self.dim_data = {}
        for dim in self.required_dims:
            dim_data = DimData.new_dim_data_from_file(self._get_required_dim_file_path(dim))
            self.dim_data[dim] = TensorDimData.from_dim_data(dim_data, self.universe)
        pass
    
    # 加载股票池的数据
    def _load_instrument_pool_data(self):
        '''
        instrument_pool_data 是一个字典，key是股票池的名字
        values为一个Booleans组成的矩阵，每行对应相应的日期。列对应股票是否在这个股票池中。
        '''
        
        self.instrument_pool_data = {}
        for pool_name in self.instrument_pools:
            dim_data = DimData.new_dim_data_from_file(self._get_required_dim_file_path(pool_name))
            self.instrument_pool_data[pool_name] = TensorDimData.from_dim_data(dim_data, self.universe)
        pass

    def _get_required_dim_file_path(self, dim):
        if dim in self.public_dim_path_template:
            return self.public_dim_path_template[dim].format(dim)
        else:
            return self.dim_data_path_template.format(dim)

    def _get_prevday_pos(self):
        if len(self.prevday_pos_path) == 0:
            return
        self.prevday_pos = np_nan_array(shape=self.dim_data["CLOSEPRICE"].data[0].shape, dtype="float64")
        with open(self.prevday_pos_path, "r") as f:
            lines = f.readlines()
            for li, line in enumerate(lines):
                line_list = line.strip().split(",")
                if li == 0:
                    continue
                original_secu_code = line_list[2]
                secu_code = None
                if len(original_secu_code) != 6 or (original_secu_code[0:2] != "00" and original_secu_code[0:2] != "30" and original_secu_code[0:2] != "60"):
                    Logger.warn("PREVDAY", "Invalid original secu code: {}".format(original_secu_code))
                    continue
                else:
                    if original_secu_code[0:2] == "00" or original_secu_code[0:2] == "30":
                        secu_code = original_secu_code + ".SZ"
                    else:
                        secu_code = original_secu_code + ".SH"
                if secu_code not in self.universe.secu_code_to_index:
                    Logger.warn("PREVDAY", "secu code not in universe: {}, original secu code: {}".format(secu_code, original_secu_code))
                    continue
                position = float(line_list[3])
                self.prevday_pos[self.universe.secu_code_to_index[secu_code]] = position

    def _load_barra_covariance_data(self, common_factor_list, covariance_factor):
        self.barra_covariance_data = {}
        for common_factor in common_factor_list:
            Logger.debug(self.log_prefix, "Load covariance data for {}".format(common_factor))
            f = open(os.path.join(WORK_BASE_DIR, "{}/{}/dim/{}_COV.bin".format(self.user_name, self.project_name, common_factor)), "rb")
            _ = pickle.load(f)
            _ = pickle.load(f)
            cov_data = pickle.load(f)
            data = np.zeros([len(cov_data), len(covariance_factor)])
            for di in range(len(cov_data)):
                for factor_index, cov_factor in enumerate(covariance_factor):
                    data[di][factor_index] = cov_data[di][1][cov_factor]
            self.barra_covariance_data[common_factor] = data
        pass

    def _load_barra_factor_return(self, common_factor_list):
        Logger.debug(self.log_prefix, "Load factor return data")
        f = open(os.path.join(WORK_BASE_DIR, "{}/{}/dim/FACTOR_RETURN.bin".format(self.user_name, self.project_name)), "rb")
        _ = pickle.load(f)
        _ = pickle.load(f)
        factor_return = pickle.load(f)
        self.factor_return = np.zeros([len(factor_return), len(common_factor_list)])
        for di in range(len(factor_return)):
            for factor_index, common_factor in enumerate(common_factor_list):
                self.factor_return[di][factor_index] = factor_return[di][1][common_factor]
        pass
    
    # 回测运行主程序
    def run_test(self):
        # 初始化用于保存alpha的变量
        alpha_data_to_dump = {}
        
        # 遍历需要回测的alpha
        for alpha_index, alpha in enumerate(self.test_alphas):
            # 是否需要保存
            if alpha.need_to_dump:
                alpha_data_to_dump[alpha_index] = []
        # 创建一个文件夹/文件 用来保存alpha
        self.trade_and_stats_engine.open_pnl_files([alpha.alpha_name for alpha in self.test_alphas], self.file_styles)
        
        # 初始化用于保存每天pos的变量
        prev_daily_pos = {}
        for alpha_index, _ in enumerate(self.test_alphas):
            prev_daily_pos[alpha_index] = None
        
        # 遍历每一天
        for di, current_date in enumerate(self.universe.date_list):
            
            # 如果日期不在回测范围内，continue，并且在保存的alpha中用nan填充
            if di < self.universe.start_di or di > self.universe.end_di:
                for alpha_index, alpha in enumerate(self.test_alphas):
                    if alpha.need_to_dump:
                        fake_alpha = np_nan_array(shape=self.dim_data["CLOSEPRICE"].data[0].shape, dtype="float64")
                        alpha_data_to_dump[alpha_index].append(fake_alpha)
                continue
            
            # 日期在回测范围内的时候  
            for alpha_index, alpha in enumerate(self.test_alphas):
                
                # 计算di天的alpha值
                current_day_alpha_data = alpha.calculate_one_day_alpha(
                    self.instrument_pool_data[alpha.instrument_pool],
                    di,
                    self.dim_data)
                
                # 计算每天持仓信息  
                daily_pos = self.trade_and_stats_engine.do_trade_and_stats(
                    di,
                    current_date,
                    alpha.alpha_name,
                    self.universe.start_di,
                    current_day_alpha_data,
                    self.instrument_pool_data[alpha.instrument_pool],
                    self.dim_data,
                    self.prevday_pos if di == self.universe.end_di and self.prevday_pos_path else prev_daily_pos[alpha_index],
                    self.size, self.multiple, self.trade_cost, alpha.delay, self.do_attribution, self.common_factor_list, self.barra_covariance_data, self.factor_return)
                
                # 保存到prev_daily_pos变量中，以备后续计算
                prev_daily_pos[alpha_index] = daily_pos
                
                # 保存因子值
                if alpha.need_to_dump:
                    alpha_data_to_dump[alpha_index].append(current_day_alpha_data)
        self.trade_and_stats_engine.close_pnl_files()
        
        
        for alpha_index, alpha_data in alpha_data_to_dump.items():
            dim_data = DimData.from_alpha(alpha_data, self.universe)
            dim_data.save_dim_data_to_file(self.alpha_data_path_template.format(self.test_alphas[alpha_index].alpha_name))

        pass


if __name__ == '__main__':
    from quant.trade_and_stats import SampleTradeAndStatsEngine

    t0 = time.process_time()
    user_name = "hqu"
    project_name = "universe_test"
    required_data_sources = ["Stock", "Index"]
    start_date = date(2018, 9, 21)
    end_date = date(2018, 9, 26)
    back_days = 1
    end_days = 0

    universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)

    size = 2E8
    multiple = 100
    trade_cost = 0

    instrument_pool = "SAMPLE_INSTRUMENT_POOL"
    sample_alpha1 = SampleAlpha1(user_name, project_name, "sample_alpha_1", universe, ["HIGHPRICE"], instrument_pool, True)
    sample_alpha2 = SampleAlpha2(user_name, project_name, "sample_alpha_2", universe, ["LOWPRICE"], instrument_pool, True)
    pnl_sub_path = "test_pnl"
    test_alphas = [sample_alpha1, sample_alpha2]
    trade_and_stats_engine = SampleTradeAndStatsEngine(user_name, project_name, universe, pnl_sub_path)
    backtest_engine = BackTestEngine(user_name, project_name, universe, size, multiple, trade_cost, test_alphas, trade_and_stats_engine, do_attribution=True, barra_model = 'MSCI')
    backtest_engine.run_test()

    t1 = time.process_time()
    Logger.info("", "{} seconds".format(t1 - t0))
