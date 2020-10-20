
#前复权成交量的n日平均
from quant.definitions import DataType
from quant.data_loader import AbstractDimDataLoader
from quant.signature import Signature
from quant.helpers import Logger,CMD
from quant.data import DimData
from quant.constants import DATE_FORMAT_STRING
import pickle
import numpy as np
import collections
import os
from datetime import date


class AdjVolumeDimDataLoader (AbstractDimDataLoader):
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, required_dims=["TURNOVERVOLUME", "CUM_ADJ_FACTOR", "TRADESTATE"], new_dims=["ADV60"], days=60, suspend_pass_rate=1/3):
        super().__init__(user_name, project_name, data_loader_name, universe, version_number, required_dims, new_dims)
        self.data_loader_name="ADV"+str(days)
        self.new_dims=["ADV"+str(days)]
        self.days=days
        self.suspend_pass_rate=suspend_pass_rate

    def _dim_definitions(self):
        return {new_dim: DataType.Float64 for new_dim in self.new_dims}

    def _check_parameter(self, signature, old_signature):
        return signature.days == old_signature.days and signature.suspend_pass_rate == old_signature.suspend_pass_rate

    def _load_all(self):
        Logger.debug(self.log_prefix, "Start load all")
        required_dim_datas = {}
        dim_datas = {}
        for dim in self.required_dims:
            required_dim_datas[dim] = DimData.strict_dim_data_from_file_and_universe(self._get_required_dim_file_path(dim), self.universe)
        for new_dim in self.new_dims:
            dim_datas[new_dim] = DimData(self.version_number, self.dim_definitions[new_dim].value.name)
        self._calculate_new_dim_data(required_dim_datas, dim_datas)
        for new_dim in self.new_dims:
            dim_datas[new_dim].save_dim_data_to_file(self.dim_data_path_template.format(new_dim))
        Logger.debug(self.log_prefix, "Finished Load all")
        pass

    def _calculate_new_dim_data(self,required_dim_datas,dim_datas):
        count_deques = {}
        volume_deques = {}
        cum_adj_deques = {}
        for di,current_date in enumerate(self.universe.date_list):
            Logger.debug(self.log_prefix, current_date.strftime(DATE_FORMAT_STRING))
            date_dim_data = {}
            for secu_code in self.universe.secu_code_list:
                if di<self.days-1:
                    if di==0:
                        if secu_code not in required_dim_datas["TRADESTATE"].data[di][1]:
                            count_deques[secu_code]=collections.deque([0])
                            volume_deques[secu_code] = collections.deque([0])
                            cum_adj_deques[secu_code] = collections.deque([required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code]])
                        elif required_dim_datas["TRADESTATE"].data[di][1][secu_code]=='Suspend':
                            count_deques[secu_code]=collections.deque([0])
                            volume_deques[secu_code] = collections.deque([0])
                            cum_adj_deques[secu_code] = collections.deque([required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code]])
                        else:
                            count_deques[secu_code] = collections.deque([1])
                            volume_deques[secu_code] = collections.deque([required_dim_datas["TURNOVERVOLUME"].data[di][1][secu_code] \
                                    if required_dim_datas["TURNOVERVOLUME"].data[di][1][secu_code] else 0])
                            cum_adj_deques[secu_code] = collections.deque([required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code]])

                    else:
                        if secu_code not in required_dim_datas["TRADESTATE"].data[di][1]:
                            count_deques[secu_code].append(count_deques[secu_code][-1]+0)
                            volume_deques[secu_code].append(0)
                            cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                        elif required_dim_datas["TRADESTATE"].data[di][1][secu_code] == 'Suspend':
                            count_deques[secu_code].append(count_deques[secu_code][-1] + 0)
                            volume_deques[secu_code].append(0)
                            cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                        else:
                            count_deques[secu_code].append(count_deques[secu_code][-1] + 1)
                            volume_deques[secu_code].append(required_dim_datas["TURNOVERVOLUME"].data[di][1][secu_code] \
                                    if required_dim_datas["TURNOVERVOLUME"].data[di][1][secu_code] else 0)
                            cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                    date_dim_data[secu_code]=np.nan
                else:
                    if secu_code not in required_dim_datas["TRADESTATE"].data[di][1]:
                        count_deques[secu_code].append(count_deques[secu_code][-1] + 0)
                        volume_deques[secu_code].append(0)
                        cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                    elif required_dim_datas["TRADESTATE"].data[di][1] == 'Suspend':
                        count_deques[secu_code].append(count_deques[secu_code][-1] + 0)
                        volume_deques[secu_code].append(0)
                        cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                    else:
                        count_deques[secu_code].append(count_deques[secu_code][-1] + 1)
                        volume_deques[secu_code].append(required_dim_datas["TURNOVERVOLUME"].data[di][1][secu_code] \
                                if required_dim_datas["TURNOVERVOLUME"].data[di][1][secu_code] else 0)
                        cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                    if di==self.days-1:
                        if count_deques[secu_code][di]-count_deques[secu_code][0]+1>=self.days-self.days*self.suspend_pass_rate:
                            date_dim_data[secu_code]=np.dot(volume_deques[secu_code],1/np.array(cum_adj_deques[secu_code]))*cum_adj_deques[secu_code][-1]/(count_deques[secu_code][-1])
                        else:
                            date_dim_data[secu_code]=np.nan
                    else:
                        prev_sum_adv=dim_datas[self.new_dims[0]].data[di-1][1][secu_code]*(count_deques[secu_code][-2]-count_deques[secu_code].popleft()+1)
                        if count_deques[secu_code][-1]-count_deques[secu_code][0]+1>=self.days-self.days*self.suspend_pass_rate:
                            date_dim_data[secu_code]=(prev_sum_adv/cum_adj_deques[secu_code][-2]*cum_adj_deques[secu_code][-1]- \
                                                     volume_deques[secu_code].popleft()/cum_adj_deques[secu_code].popleft()*cum_adj_deques[secu_code][-1]+volume_deques[secu_code][-1])/(count_deques[secu_code][-1]-count_deques[secu_code][0]+1)
                        else:
                            date_dim_data[secu_code]=np.nan
                if current_date==date(2018,9,13)  and secu_code=='000001.SZ':
                    print('aaa')
                    print('yeah')
            dim_datas[self.new_dims[0]].append_date_data(current_date,date_dim_data)

    def do_load(self):

        signature_path = self._signature_path_name(self.user_name, self.project_name, self.data_loader_name)
        universe_path = self._universe_path_name(self.user_name, self.project_name)
        signature = AdvSignature(self.universe.start_date, self.universe.end_date, self.universe.back_days, self.universe.end_days,self.days,self.suspend_pass_rate)
        old_signature = AdvSignature.new_signature_from_file(signature_path)

        self.dim_definitions = self._dim_definitions()

        if not self._check_dependent_data_version(self._dependent_data_paths(), self.version_number):
            Logger.info(self.log_prefix, "Dependent data has changed. Need to reload all")
            calulate_all = CMD.proceedWhenY(self.log_prefix, "Detect newly updated data, recalculate or not")
            if calulate_all:
                self._load_all()
                self.universe.save_universe(universe_path)
                signature.save_signature(signature_path)
            else:
                Logger.info(self.log_prefix, "Manually skip reload")
            return
        elif not self._check_dim_data_existance(self.new_dims) or not os.path.exists(universe_path):
            Logger.info(self.log_prefix, "Unloaded dim detected or universe not created correctly. Need to load all")
            self._load_all()
            self.universe.save_universe(universe_path)
            signature.save_signature(signature_path)
            return
        elif signature.check(old_signature):
            Logger.info(self.log_prefix, "No change. Skip reload")
            return
        elif not self._check_parameter(signature,old_signature):
            Logger.info(self.log_prefix, "Some parameters change. Try to load all")
            self._load_all()
            self.universe.save_universe(universe_path)
            signature.save_signature(signature_path)
            return
        elif not self._check_dim_data_range(self.new_dims, self.universe):
            Logger.info(self.log_prefix, "Parameters don't change,date list expanded. Try to load all")
            self._load_all()
            self.universe.save_universe(universe_path)
            signature.save_signature(signature_path)
            return
        elif not self._check_dim_data_range_head(self.new_dims, self.universe):
            calulate_all = CMD.proceedWhenY(self.log_prefix, "Parameters don't change,the start of date list is pushed back, recalculate or not")
            if calulate_all:
                self._load_all()
                self.universe.save_universe(universe_path)
                signature.save_signature(signature_path)
            else:
                Logger.info(self.log_prefix, "Manually skip reload although the start of date list is pushed back")
            return
        else:
            Logger.info(self.log_prefix, "Parameters don't change,the end date already covered. Skip reload")
            self.universe.save_universe(universe_path)
            signature.save_signature(signature_path)
            return

    def _check_dim_data_range_head(self, dims, universe):
        if not universe.date_list:
            raise Exception("Universe has empty date list")
        start_date = universe.date_list[0]
        for dim in dims:
            dim_data_path = self.dim_data_path_template.format(dim)
            if not os.path.exists(dim_data_path):
                raise Exception("Dim data {} should exist!".format(dim_data_path))
            dim_data = DimData.new_dim_data_from_file(dim_data_path)
            if not dim_data.data:
                return False
            if start_date > dim_data.data[0][0]:
                return False
        return True


class AdvSignature (Signature):
    def __init__(self, start_date, end_date, back_days, end_days, days, suspend_pass_rate):
        super().__init__(start_date, end_date, back_days, end_days)
        self.days = days
        self.suspend_pass_rate = suspend_pass_rate

    def check(self, other_signature):
        return self.start_date == other_signature.start_date \
               and self.end_date == other_signature.end_date \
               and self.back_days == other_signature.back_days \
               and self.end_days == other_signature.end_days \
               and self.days == other_signature.days \
               and self.suspend_pass_rate == other_signature.suspend_pass_rate

    def _write_signature_to_file(self, f):
        super()._write_signature_to_file(f)
        pickle.dump(self.days ,f)
        pickle.dump(self.suspend_pass_rate ,f)

    def _read_signature_from_file(self, f):
        super()._read_signature_from_file(f)
        self.days = pickle.load(f)
        self.suspend_pass_rate = pickle.load(f)

    @staticmethod
    def new_signature_from_file(file_path):
        signature = AdvSignature(None, None, 0, 0, 0, 0)
        signature.load_signature(file_path)
        return signature




if __name__ == "__main__":
        import time
        from datetime import date
        import os.path
        from quant.constants import WORK_BASE_DIR, DATA_BASE_DIR
        from quant.universe import Universe
        from quant.universe_generator import UniverseGenerator
        from quant.alpha import SampleAlpha1, SampleAlpha2
        from quant.data import ProjectData
        from quant.backtest import BackTestEngine
        from quant.helpers import Logger
        from quant.trade_and_stats import SampleTradeAndStatsEngine
        from quant.public_dl.dl_adjfactor2 import AdjFactorDimDataLoader
        from quant.public_dl.dl_return import ReturnDimDataLoader
        from quant.data_loader import BaseDataLoader, SampleDimDataLoader, SampleInstrumentPoolDataLoader
        t0 = time.process_time()
        # Project set up
        user_name = "jinxing"
        project_name = "all_in_one_test"
        required_data_sources = ["Stock", "Index"]
        start_date = date(2018, 4, 5)
        end_date = date(2018, 9, 28)
        back_days = 60
        end_days = 0

        # Generate universe
        universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date, back_days, end_days)
        universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
        universe = Universe.new_universe_from_file(universe_path)

        # Get and update version number
        version_number = ProjectData.read_version_number(user_name, project_name)
        ProjectData.save_version_number(user_name, project_name, version_number + 1)

        # BaseData setup
        stock_path_template = os.path.join(DATA_BASE_DIR, "cooked/BaseData/{}/{}/{}/BASEDATA.txt")
        index_path_template = os.path.join(DATA_BASE_DIR, "raw/WIND/IndexQuote/{}/{}/{}/AINDEXEODPRICES.txt")
        dims_to_load = ["PREVCLOSEPRICE", "TRADESTATE","TURNOVERVOLUME","OPENPRICE", "HIGHPRICE", "LOWPRICE", "CLOSEPRICE", "SECUNAME", "BASESHARES", "S_DQ_PRECLOSE","DIVIDEND","ACTUALPLARATIO","SPLIT","PLAPRICE"]

        base_data_loader = BaseDataLoader(user_name, project_name, 'mine',universe, version_number,
                                                  [stock_path_template, index_path_template], dims_to_load)
        sample_dim_data_loader = SampleDimDataLoader(user_name, project_name, 'sddl', universe, version_number, required_dims=["HIGHPRICE", "LOWPRICE"], new_dims=["AVERAGE_HIGH_LOW"])
        sample_dim_data_loader.do_load()
        sample_ip_data_loader = SampleInstrumentPoolDataLoader(user_name, project_name,'sipdl', universe, version_number, required_dims=["HIGHPRICE"], new_dims=["SAMPLE_INSTRUMENT_POOL"])
        sample_ip_data_loader.do_load()
        adj_factor_dim_data_loader = AdjFactorDimDataLoader(user_name,project_name,'adjfdl',universe,version_number)
        adj_factor_dim_data_loader.do_load()
        return_dim_data_loader=ReturnDimDataLoader(user_name,project_name,'retdl',universe,version_number,["ADJ_FACTOR","CLOSEPRICE"],["RETURN"])
        return_dim_data_loader.do_load()
        adj_volume_dim_data_loader=AdjVolumeDimDataLoader(user_name,project_name, 'advdl', universe,version_number,suspend_pass_rate=1/3,days=60)
        adj_volume_dim_data_loader.do_load()
        # BackTest setup, alphas, trade, stats
        size = 2E8
        multiple = 100
        trade_cost = 0
        instrument_pool = "SAMPLE_INSTRUMENT_POOL"
        sample_alpha1 = SampleAlpha1(user_name, project_name, "sample_alpha_1", universe, ["TURNOVERVALUE"], instrument_pool, True)
        sample_alpha2 = SampleAlpha2(user_name, project_name, "sample_alpha_2", universe, ["ADJ_FACTOR"], instrument_pool, True)
        pnl_sub_path = "test_pnl"
        test_alphas = [sample_alpha1]

        trade_and_stats_engine = SampleTradeAndStatsEngine(user_name, project_name, universe, pnl_sub_path)
        backtest_engine = BackTestEngine(user_name, project_name, universe, size, multiple, trade_cost, test_alphas, trade_and_stats_engine)
        backtest_engine.run_test()
        t1 = time.process_time()
        Logger.info("", "{} seconds".format(t1 - t0))



