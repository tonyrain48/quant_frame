
import math
import numpy as np
import os.path
import time
import datetime
import collections
from datetime import date
from quant.universe import Universe
from quant.constants import DATA_BASE_DIR, WORK_BASE_DIR
from quant.data import ProjectData, DimData, TensorDimData
from quant.data_loader import AbstractDimDataLoader, BaseDataLoader
from quant.helpers import Logger, CMD
from quant.definitions import DataType
from quant.universe_generator import UniverseGenerator

from quant.public_dl.sig_topliquid import TopLiquidSignature


class TopLiquidDataLoader(AbstractDimDataLoader):

    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, required_dims, new_dims,
                 WindowDays=60, Lookback=40, Passrate=100, Sticky=40, Delta=0.1, top="1500",
                 option = False, MinVol=0, MaxVol=1 * math.exp(20), MinPrice=0, MaxPrice=5000,
                 MinCap=0, NoST=True, Cyb=True, FilterSuspend=False):
        super().__init__(user_name, project_name, data_loader_name, universe, version_number, required_dims, new_dims)

        self.data_loader_name = data_loader_name
        self.WindowDays = WindowDays     # "计算窗口长度"
        self.Lookback = Lookback         # "有效回看天数"
        self.Passrate = Passrate         # "有效回看天数中goodrecord通过率"
        self.Sticky = Sticky             # "连续不满足天数"
        self.Delta = Delta               # "容忍区间，进行考察goodrecord的区间，在top*(1-Delta),则goodrecord会增加1，在调整top外的股票进入top时，也参考这个指标"
        self.t = top                     # "TOP的股票数量"
        self.option = option             # "是否考虑以下6个筛选条件"
        self.MinVol = MinVol             # 最小成交量限制
        self.MaxVol = MaxVol             # 最大成交量限制
        self.MinPrice = MinPrice         # "最小价格限制"
        self.MaxPrice = MaxPrice         # "最大价格限制"
        self.MinCap = MinCap             # "最小市值限制"
        self.NoST = NoST                 # "是否过滤ST"
        self.Cyb = Cyb                   # "是否过滤创业板"
        self.FilterSuspend = FilterSuspend  # "是否过滤停牌"
        self.log_prefix = "[" + self.__class__.__name__ + "]"
        self.dim_data_path_template = os.path.join(WORK_BASE_DIR,"{}/{}/dim/".format(user_name, project_name) + "{}.bin")
        
        # 根据需求选择required_dims
        if not self.option:
            self.required_dims = ["TURNOVERVALUE"]
        else:
            self.required_dims = ["TURNOVERVALUE", "TURNOVERVOLUME", "SPECIALTRADETYPE", "CLOSEPRICE", "TOTALSHARES"]
        if self.FilterSuspend:
            self.required_dims.append("TRADESTATE")
        if str.upper(self.t) == "ALL":
            self.TopNum = len(self.universe.secu_code_list)
            self.required_dims.append("LISTEDDATE")
        else:
            self.TopNum = int(self.t)

        self.dim_data = {dim_name: [] for dim_name in self.required_dims}

        pass

    def do_load(self):
        
        # 必须参数
        requiredpara = [self.WindowDays, self.Lookback, self.Passrate, self.Sticky, self.Delta, self.t, self.Cyb,
                        self.FilterSuspend]
        # 可选参数
        optionpara = [self.MinVol, self.MaxVol, self.MinPrice, self.MaxPrice, self.MinCap, self.NoST]
        # signature
        signature_path = self._signature_path_name(self.user_name, self.project_name, self.data_loader_name)
        universe_path = self._universe_path_name(self.user_name, self.project_name)
        signature = TopLiquidSignature(self.universe.start_date, self.universe.end_date, self.universe.back_days,
                                       self.universe.end_days, requiredpara, self.option, optionpara)
        old_signature = TopLiquidSignature.new_topliquid_data_signature_from_file(signature_path)

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
        elif not self._check_parameter(signature, old_signature):
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
            calulate_all = CMD.proceedWhenY(self.log_prefix,
                                            "Parameters don't change,the start of date list is pushed back, recalculate or not")
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

    def _load_all(self):
        Logger.debug(self.log_prefix, "Start load all")
        required_dim_datas = {}
        dim_datas = {}
        for dim in self.required_dims:
            required_dim_datas[dim] = DimData.strict_dim_data_from_file_and_universe(self.dim_data_path_template.format(dim), self.universe)
        for new_dim in self.new_dims:
            dim_datas[new_dim] = DimData(self.version_number, self.dim_definitions[new_dim].value.name)
        self._calculate_new_dim_data(required_dim_datas, dim_datas)
        for new_dim in self.new_dims:
            dim_datas[new_dim].save_dim_data_to_file(self.dim_data_path_template.format(new_dim))
        Logger.debug(self.log_prefix, "Finished Load all")
        pass

    def _dim_definitions(self):
        """
        :return: {dim_name: data_type}, which describe the dim definition that describe the dim to load
        """
        return {self.new_dims[0]: DataType.Bool}

    def _check_dim_data_range_head(self, dims, universe):
        """
        Check whether the dependent data has already covered the date list in the universe
        """
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

    def _check_parameter(self, signature, old_signature):
        if signature.option == old_signature.option == False:
            return signature.requiredpara == old_signature.requiredpara \
                   and signature.option == old_signature.option
        else:
            return signature.requiredpara == old_signature.requiredpara \
                   and signature.option == old_signature.option \
                   and signature.optionpara == old_signature.optionpara

    def _calculate_new_dim_data(self, required_dim_datas, dim_datas):

        for dim in required_dim_datas:
            self.dim_data[dim] = TensorDimData.from_dim_data(required_dim_datas[dim], self.universe).data

        outmark = np.zeros(len(self.universe.secu_code_list), dtype='bool')
        outporale = np.zeros(len(self.universe.secu_code_list)) + self.Sticky
        goodrecord = np.zeros(len(self.universe.secu_code_list))
        hist_status = np.zeros(len(self.universe.secu_code_list), dtype='bool')
        self.valid = np.ones(shape=[len(self.universe.date_list), len(self.universe.secu_code_list)]) * np.nan
        histrank = []

        for di, current_date in enumerate(self.universe.date_list):
            if di < self.WindowDays - 1:
                temp = dict(zip(self.universe.secu_code_list, np.zeros(len(self.universe.secu_code_list)) * np.nan))
            else:
                temp = dict(zip(self.universe.secu_code_list, np.zeros(len(self.universe.secu_code_list), dtype='bool')))
            Inst = 0
            if di >= self.WindowDays - 1:
                filter = np.zeros(len(self.universe.secu_code_list), dtype='bool')
                if self.option:
                    if di == self.WindowDays - 1:
                        sumclose = np.zeros(len(self.universe.secu_code_list))
                        sumvol = np.zeros(len(self.universe.secu_code_list))
                        sumshare = np.zeros(len(self.universe.secu_code_list))
                        ndays = np.zeros(len(self.universe.secu_code_list))
                        hist_close = []
                        hist_vol = []
                        hist_share = []
                        for ii, secu_code in enumerate(self.universe.secu_code_list):
                            self.valid[di][ii] = False
                            history_close = collections.deque(maxlen = self.WindowDays)
                            history_vol = collections.deque(maxlen = self.WindowDays)
                            history_share = collections.deque(maxlen = self.WindowDays)
                            for dd in range(di + 1):
                                history_close.append(self.dim_data["CLOSEPRICE"][dd][ii])
                                history_vol.append(self.dim_data["TURNOVERVOLUME"][dd][ii])
                                history_share.append(self.dim_data["TOTALSHARES"][dd][ii])
                                if np.isfinite(self.dim_data["CLOSEPRICE"][dd][ii]) & \
                                        np.isfinite(self.dim_data["TURNOVERVOLUME"][dd][ii]) & np.isfinite(self.dim_data["TOTALSHARES"][dd][ii]):
                                    sumclose[ii] += self.dim_data["CLOSEPRICE"][dd][ii]
                                    sumvol[ii] += self.dim_data["TURNOVERVOLUME"][dd][ii]
                                    sumshare[ii] += self.dim_data["TOTALSHARES"][dd][ii]
                                    ndays[ii] += 1
                            hist_close.append(history_close)
                            hist_vol.append(history_vol)
                            hist_share.append(history_share)
                            if (ndays[ii] > 0) & (self.dim_data["SPECIALTRADETYPE"][di][ii] is not None):
                                if ((sumclose[ii] / ndays[ii] < self.MinPrice) | (sumvol[ii] / ndays[ii] < self.MinVol) | (sumshare[ii] / ndays[ii] < self.MinCap) \
                                        | (sumclose[ii] / ndays[ii] > self.MaxPrice) | ((self.dim_data["SPECIALTRADETYPE"][di - 1][ii] != '0') & self.NoST)):
                                    filter[ii] = True; self.valid[di][ii] = False; outmark[ii] = False; outporale[ii] = self.Sticky
                                    # print(current_date, ii,secu_code)
                            else:
                                filter[ii] = True; self.valid[di][ii] = False; outmark[ii] = False; outporale[ii] = self.Sticky
                    else:
                        for ii in range(len(self.universe.secu_code_list)):
                            if np.isfinite(hist_close[ii][0]) & np.isfinite(hist_vol[ii][0]) & np.isfinite(hist_share[ii][0]):
                                sumclose[ii] -= hist_close[ii][0]
                                sumvol[ii] -= hist_vol[ii][0]
                                sumshare[ii] -= hist_share[ii][0]
                                ndays[ii] -= 1
                            if np.isfinite(self.dim_data["CLOSEPRICE"][di][ii]) & \
                                    np.isfinite(self.dim_data["TURNOVERVOLUME"][di][ii]) & np.isfinite(self.dim_data["TOTALSHARES"][di][ii]):
                                sumclose[ii] += self.dim_data["CLOSEPRICE"][di][ii]
                                sumvol[ii] += self.dim_data["TURNOVERVOLUME"][di][ii]
                                sumshare[ii] += self.dim_data["TOTALSHARES"][di][ii]
                                ndays[ii] += 1
                            hist_close[ii].append(self.dim_data["CLOSEPRICE"][di][ii])
                            hist_vol[ii].append(self.dim_data["TURNOVERVOLUME"][di][ii])
                            hist_share[ii].append(self.dim_data["TOTALSHARES"][di][ii])
                            if (ndays[ii] > 0) & (self.dim_data["SPECIALTRADETYPE"][di][ii] is not None):
                                if ((sumclose[ii] / ndays[ii] < self.MinPrice) | (sumvol[ii] / ndays[ii] < self.MinVol) | (sumshare[ii] / ndays[ii] < self.MinCap) \
                                        | (sumclose[ii] / ndays[ii] > self.MaxPrice) | ((self.dim_data["SPECIALTRADETYPE"][di - 1][ii] != '0') & self.NoST)):
                                    filter[ii] = True; self.valid[di][ii] = False; outmark[ii] = False; outporale[ii] = self.Sticky
                                    # print(current_date, secu_code)
                            else:
                                filter[ii] = True; self.valid[di][ii] = False; outmark[ii] = False; outporale[ii] = self.Sticky

                # re - calc value
                if di == self.WindowDays - 1:
                    sumvalue = np.zeros(len(self.universe.secu_code_list))
                    hist_sumvalue = np.zeros(len(self.universe.secu_code_list))
                    hist_tvrval = []
                    hist_sumdays = np.zeros(len(self.universe.secu_code_list))
                    for ii in range(len(self.universe.secu_code_list)):
                        hist = collections.deque(maxlen=self.WindowDays)
                        for dd in range(di + 1):
                            hist.append(self.dim_data["TURNOVERVALUE"][dd][ii])
                            if np.isfinite(self.dim_data["TURNOVERVALUE"][dd][ii]):
                                hist_sumvalue[ii] += self.dim_data["TURNOVERVALUE"][dd][ii]
                                hist_sumdays[ii] += 1
                        hist_tvrval.append(hist)
                        if (int(hist_sumdays[ii]) > (self.WindowDays / 3)) & (not filter[ii]):
                            sumvalue[ii] = hist_sumvalue[ii] / hist_sumdays[ii]
                        else:
                            sumvalue[ii] = 0
                else:
                    for ii in range(len(self.universe.secu_code_list)):
                        if np.isfinite(hist_tvrval[ii][0]):
                            hist_sumvalue[ii] = hist_sumvalue[ii] - hist_tvrval[ii][0]
                            hist_sumdays[ii] = hist_sumdays[ii] - 1
                        if np.isfinite(self.dim_data["TURNOVERVALUE"][di][ii]):
                            hist_sumvalue[ii] = hist_sumvalue[ii] + self.dim_data["TURNOVERVALUE"][di][ii]
                            hist_sumdays[ii] = hist_sumdays[ii] + 1
                        hist_tvrval[ii].append(self.dim_data["TURNOVERVALUE"][di][ii])
                        if (int(hist_sumdays[ii]) > (self.WindowDays / 3)) & (not filter[ii]):
                            sumvalue[ii] = hist_sumvalue[ii] / hist_sumdays[ii]
                        else:
                            sumvalue[ii] = 0

                ind = np.argsort(-sumvalue)
                rk = np.zeros(len(sumvalue))
                for i in range(len(sumvalue)):
                    rk[ind[i]] = i + 1
                histrank.append(rk.tolist())

                for rr in range(len(ind)):
                    ii = ind[rr]
                    secu_code = self.universe.secu_code_list[ii]
                    # count goodrecord in lookbackdays
                    if rr < self.TopNum * (1.0 - self.Delta):
                        goodrecord[ii] += 1
                    if (len(histrank) == (self.Lookback + 1)) & (histrank[0][ii] < (self.TopNum * (1.0 - self.Delta))):
                        goodrecord[ii] -= 1

                    # init
                    if di == self.WindowDays - 1:
                        if not filter[ii]:
                            if rr >= self.TopNum:
                                self.valid[di][ii] = False
                            else:
                                self.valid[di][ii] = True
                        continue

                    if filter[ii]:
                        continue

                    # inherit di - 1
                    self.valid[di][ii] = self.valid[di - 1][ii]

                    # check for out
                    if self.valid[di - 1][ii]:
                        if rr >= self.TopNum * (1.0 + self.Delta):
                            outmark[ii] = True
                            outporale[ii] -= 1
                        elif outmark[ii]:
                            outmark[ii] = False
                            outporale[ii] = self.Sticky
                        if outporale[ii] == 0:
                            self.valid[di][ii] = False
                            outmark[ii] = False
                            outporale[ii] = self.Sticky
                    else:
                        if goodrecord[ii] >= (self.Lookback * self.Passrate) / 100:
                            self.valid[di][ii] = True

                if len(histrank) == self.Lookback + 1:
                    histrank.pop(0)

                for rr in range(len(ind)):
                    ii = ind[rr]
                    secu_code = self.universe.secu_code_list[ii]

                    # remove CHUANGYEBAN
                    if not self.Cyb:
                        ticker = self.universe.secu_code_list[ii]
                        token = ticker[0: 3]
                        if token == '300':
                            self.valid[di][ii] = False

                    # special routine for suspend
                    if self.FilterSuspend:
                        if self.dim_data["TRADESTATE"][di][ii] == 'Suspend':
                            self.valid[di][ii] = False
                            if not self.dim_data["TRADESTATE"][di - 1][ii] == 'Suspend':
                                hist_status[ii] = self.valid[di - 1][ii]
                            outmark[ii] = False
                            outporale[ii] = self.Sticky
                        elif self.dim_data["TRADESTATE"][di - 1][ii] == 'Suspend':
                            if hist_status[ii] == (rr < self.TopNum):
                                self.valid[di][ii] = hist_status[ii]

                    # remove Index
                    if self.universe.secu_code_to_source[secu_code] == 'Index':
                        self.valid[di][ii] = False

                    # compare listeddate
                    if str.upper(self.t) == "ALL":
                        if not self.dim_data["LISTEDDATE"][di][ii]:
                            self.valid[di][ii] = False
                        else:
                            if datetime.datetime.strptime(self.dim_data["LISTEDDATE"][di][ii], '%Y%m%d').date() > current_date:
                                self.valid[di][ii] = False

                    if self.valid[di][ii]:
                        Inst += 1

                    if self.valid[di][ii] != self.valid[di][ii]:
                        temp[secu_code] = np.nan
                    else:
                        temp[secu_code] = True if self.valid[di][ii] else False

            dim_datas[self.new_dims[0]].append_date_data(current_date, temp)

            Logger.debug(self.log_prefix, "TopLiquid updated {} instruments in {}".format(Inst, current_date))

        return


if __name__ == "__main__":
    user_name = "hqu"
    project_name = "toptest4"
    required_data_sources = ["Stock", "Index"]
    start_date = date(2012, 1, 1)
    end_date = date(2017, 6, 30)
    back_days = 60
    end_days = 0
    universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date,
                                           back_days, end_days)

    universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)
    stock_path_template = os.path.join(DATA_BASE_DIR, "cooked/BaseData/{}/{}/{}/BASEDATA.txt")
    index_path_template = os.path.join(DATA_BASE_DIR, "raw/WIND/IndexQuote/{}/{}/{}/AINDEXEODPRICES.txt")
    dims_to_load = ["TURNOVERVALUE", "TURNOVERVOLUME", "SPECIALTRADETYPE", "CLOSEPRICE", "TOTALSHARES", "TOTALFLOATSHARES", "TRADESTATE"]
    version_number = ProjectData.read_version_number(user_name, project_name)
    ProjectData.save_version_number(user_name, project_name, version_number + 1)
    # base_data_loader = OriginalBaseDataLoader(user_name, project_name, universe, version_number, stock_path_template,
    #                                   index_path_template, dims_to_load)
    another_base_data_loader = BaseDataLoader(user_name, project_name, '', universe, version_number,
                                              [stock_path_template, index_path_template], dims_to_load)
    # sample_dim_data_loader = SampleDimDataLoader(user_name, project_name, universe, version_number, required_dims=["HIGHPRICE", "LOWPRICE"], new_dims=["AVERAGE_HIGH_LOW"])
    # sample_dim_data_loader.do_load()
    t0 = time.process_time()
    sample_ip_data_loader = TopLiquidDataLoader(user_name, project_name, "1500topliquid", universe, version_number, [],['TOP1500'], option = True, MaxVol = np.inf, NoST = True)
    sample_ip_data_loader.do_load()

    t1 = time.process_time()
    Logger.info("", "{} seconds".format(t1 - t0))
