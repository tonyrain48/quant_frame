#前复权成交量的n日平均

from quant.definitions import DataType
from quant.data_loader import AbstractDimDataLoader
from quant.signature import Signature
from quant.helpers import Logger,CMD
from quant.data import DimData,TensorDimData
from quant.constants import DATE_FORMAT_STRING
import pickle
import numpy as np
import collections
import os
import math
from datetime import date


class AdjVolumeDimDataLoader (AbstractDimDataLoader):
    def __init__(self, user_name, project_name, data_loader_name, universe,  version_number, required_dims=["TURNOVERVOLUME","CUM_ADJ_FACTOR","TRADESTATE"], new_dims=['ADV60'],days=[60],suspend_pass_rate=1/3):
        super().__init__(user_name,project_name, data_loader_name, universe,version_number,required_dims,new_dims)
        self.days_list = days
        if self.days_list!=[60]:
            self.new_dims=["ADV"+str(days) for days in self.days_list]
        self.suspend_pass_rate=suspend_pass_rate

    def _dim_definitions(self):
        return {"ADV"+str(days) : DataType.Float64 for days in self.days_list}

    def _check_parameter(self, signature, old_signature):
        return signature.days == old_signature.days and signature.suspend_pass_rate == old_signature.suspend_pass_rate

    def _load_all(self,days,i):
        Logger.debug(self.log_prefix, "Start load all")
        required_dim_datas = {}
        dim_datas = {}
        for dim in self.required_dims:
            required_dim_datas[dim] = DimData.strict_dim_data_from_file_and_universe(self._get_required_dim_file_path(dim), self.universe)
        new_dim=self.new_dims[i]
        dim_datas[new_dim] = DimData(self.version_number, self.dim_definitions[new_dim].value.name)
        self._calculate_new_dim_data(required_dim_datas, dim_datas,days)

        dim_datas[new_dim].save_dim_data_to_file(self.dim_data_path_template.format(new_dim))
        Logger.debug(self.log_prefix, "Finished Load all")
        pass

    def _calculate_new_dim_data(self,required_dim_datas,dim_datas,days):
        count_deques = {}
        volume_deques = {}
        cum_adj_deques = {}
        for di,current_date in enumerate(self.universe.date_list):
            # Logger.debug(self.log_prefix, current_date.strftime(DATE_FORMAT_STRING))
            date_dim_data = {}
            count_valid=0
            count_invalid=0
            for secu_code in self.universe.secu_code_list:
                if di<days-1:
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
                            count_deques[secu_code].append(0)
                            volume_deques[secu_code].append(0)
                            cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                        elif required_dim_datas["TRADESTATE"].data[di][1][secu_code] == 'Suspend':
                            count_deques[secu_code].append(0)
                            volume_deques[secu_code].append(0)
                            cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                        else:
                            count_deques[secu_code].append(1)
                            volume_deques[secu_code].append(required_dim_datas["TURNOVERVOLUME"].data[di][1][secu_code] \
                                    if required_dim_datas["TURNOVERVOLUME"].data[di][1][secu_code] else 0)
                            cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                    date_dim_data[secu_code]=np.nan
                else:
                    if secu_code not in required_dim_datas["TRADESTATE"].data[di][1]:
                        count_deques[secu_code].append(0)
                        volume_deques[secu_code].append(0)
                        cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                    elif required_dim_datas["TRADESTATE"].data[di][1][secu_code] == 'Suspend':
                        count_deques[secu_code].append(0)
                        volume_deques[secu_code].append(0)
                        cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                    else:
                        count_deques[secu_code].append(1)
                        volume_deques[secu_code].append(required_dim_datas["TURNOVERVOLUME"].data[di][1][secu_code] \
                                if required_dim_datas["TURNOVERVOLUME"].data[di][1][secu_code] else 0)
                        cum_adj_deques[secu_code].append(required_dim_datas["CUM_ADJ_FACTOR"].data[di][1][secu_code])
                    if di==days-1:
                        valid_days=sum(count_deques[secu_code])
                        if valid_days>=days-days*self.suspend_pass_rate:
                            date_dim_data[secu_code]=np.dot(volume_deques[secu_code],1/np.array(cum_adj_deques[secu_code]))* \
                                                     cum_adj_deques[secu_code][-1]/valid_days
                            count_valid=count_valid+1
                        else:
                            count_invalid=count_invalid+1
                            date_dim_data[secu_code]=np.nan

                    else:
                        prev_sum_adv=dim_datas["ADV"+str(days)].data[di-1][1][secu_code]*(sum(count_deques[secu_code])-count_deques[secu_code][-1])
                        count_deques[secu_code].popleft()
                        valid_days=sum(count_deques[secu_code])
                        if valid_days>=days-days*self.suspend_pass_rate:
                            if not math.isnan(prev_sum_adv):
                                date_dim_data[secu_code]=(prev_sum_adv/cum_adj_deques[secu_code][-2]*cum_adj_deques[secu_code][-1]- \
                                                    volume_deques[secu_code].popleft()/cum_adj_deques[secu_code].popleft()*cum_adj_deques[secu_code][-1]+volume_deques[secu_code][-1])/valid_days
                            else:
                                volume_deques[secu_code].popleft()
                                cum_adj_deques[secu_code].popleft()
                                date_dim_data[secu_code]=np.dot(volume_deques[secu_code],1/np.array(cum_adj_deques[secu_code]))*\
                                                         cum_adj_deques[secu_code][-1]/valid_days
                                np.dot(volume_deques[secu_code], 1 / np.array(cum_adj_deques[secu_code])) * \
                                required_dim_datas["CUM_ADJ_FACTOR"].data[-1][1][secu_code] / valid_days
                            count_valid=count_valid+1
                        else:
                            volume_deques[secu_code].popleft()
                            cum_adj_deques[secu_code].popleft()
                            date_dim_data[secu_code]=np.nan
                            count_invalid = count_invalid+1
            dim_datas["ADV"+str(days)].append_date_data(current_date,date_dim_data)
            Logger.debug(self.log_prefix, "ADV{} updated {} valid instruments and {} invalid instruments in {}".format(days, count_valid, count_invalid, current_date))

    def do_load(self):
        len_name_list=len(self.data_loader_name)
        for (i,name) in enumerate(self.data_loader_name):
            days=int(name[3:])
            signature_path = self._signature_path_name(self.user_name, self.project_name, name)
            universe_path = self._universe_path_name(self.user_name, self.project_name)
            signature = AdvSignature(self.universe.start_date, self.universe.end_date, self.universe.back_days, self.universe.end_days,days,self.suspend_pass_rate)
            old_signature = AdvSignature.new_signature_from_file(signature_path)

            self.dim_definitions = self._dim_definitions()

            if not self._check_dependent_data_version(self._dependent_data_paths(), self.version_number):
                Logger.info(self.log_prefix, "Dependent data has changed. Need to reload all")
                calulate_all = CMD.proceedWhenY(self.log_prefix, "Detect newly updated data, recalculate or not")
                if calulate_all:
                    self._load_all(days,i)
                    self.universe.save_universe(universe_path)
                    signature.save_signature(signature_path)
                else:
                    Logger.info(self.log_prefix, "Manually skip reload")
                if i==len_name_list-1:
                    return
                else:
                    continue
            elif not self._check_dim_data_existance(self.new_dims) or not os.path.exists(universe_path):
                Logger.info(self.log_prefix, "Unloaded dim detected or universe not created correctly. Need to load all")
                self._load_all(days,i)
                self.universe.save_universe(universe_path)
                signature.save_signature(signature_path)
                if i == len_name_list - 1:
                    return
                else:
                    continue
            elif signature.check(old_signature):
                Logger.info(self.log_prefix, "No change. Skip reload")
                if i == len_name_list - 1:
                    return
                else:
                    continue
            elif not self._check_parameter(signature,old_signature):
                Logger.info(self.log_prefix, "Some parameters change. Try to load all")
                self._load_all(days,i)
                self.universe.save_universe(universe_path)
                signature.save_signature(signature_path)
                if i == len_name_list - 1:
                    return
                else:
                    continue
            elif not self._check_dim_data_range(self.new_dims, self.universe):
                Logger.info(self.log_prefix, "Parameters don't change,date list expanded. Try to load all")
                self._load_all(days,i)
                self.universe.save_universe(universe_path)
                signature.save_signature(signature_path)
                if i == len_name_list - 1:
                    return
                else:
                    continue
            elif not self._check_dim_data_range_head(self.new_dims, self.universe):
                calulate_all = CMD.proceedWhenY(self.log_prefix, "Parameters don't change,the start of date list is pushed back, recalculate or not")
                if calulate_all:
                    self._load_all(days,i)
                    self.universe.save_universe(universe_path)
                    signature.save_signature(signature_path)
                else:
                    Logger.info(self.log_prefix, "Manually skip reload although the start of date list is pushed back")
                if i == len_name_list - 1:
                    return
                else:
                    continue
            else:
                Logger.info(self.log_prefix, "Parameters don't change,the end date already covered. Skip reload")
                self.universe.save_universe(universe_path)
                signature.save_signature(signature_path)
                if i == len_name_list - 1:
                    return
                else:
                    continue

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
