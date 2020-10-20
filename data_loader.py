import sys
sys.path.append(r'D:\ms_intern\quant_frame\quant')
import os.path
import time
import traceback
import sys
import numpy as np
from datetime import date
from quant.signature import Signature, RawDataLoaderSignature, DimDataLoaderSignature
from quant.universe import Universe
from quant.constants import DATA_BASE_DIR, WORK_BASE_DIR, COLUMN_DELIMITER, DATE_FORMAT_STRING
from quant.data import ProjectData, DimData, MSData
from quant.helpers import Logger, CMD, parse_dim_index
from quant.definitions import DataType
from quant.universe_generator import UniverseGenerator


class AbstractDimDataLoader(object):
    """
    Abstract data loader to load data directly from the the already loaded dim data
    """
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, required_dims, new_dims,
                 public_dim_path_template={}, use_public_data=False):
        self.user_name = user_name
        self.project_name = project_name
        self.data_loader_name = data_loader_name
        self.universe = universe
        self.version_number = version_number
        self.required_dims = required_dims   # data_path
        self.new_dims = new_dims             # data_name
        self.log_prefix = "[" + self.__class__.__name__ + "]"
        self.dim_data_path_template = os.path.join(WORK_BASE_DIR, "{}/{}/dim/".format(user_name, project_name) + "{}.bin")

        self.public_dim_path_template = public_dim_path_template
        self.use_public_data = use_public_data
        self.required_dim_public_path_template = {dim: public_dim_path_template[dim] for dim in required_dims if dim in public_dim_path_template}
        self.true_new_dims = {dim for dim in new_dims if dim not in public_dim_path_template} if use_public_data else new_dims
        for not_used_public_dim in self.true_new_dims:
            if not_used_public_dim in public_dim_path_template:
                public_dim_path_template.pop(not_used_public_dim)
        pass
        if use_public_data and len(self.true_new_dims) > 0:
            Logger.warn(self.log_prefix, "Dims {} NOT in public data.".format(self.true_new_dims))

    def do_load(self):
        signature_path = self._signature_path_name(self.user_name, self.project_name, self.data_loader_name)
        signature = DimDataLoaderSignature(self.universe.start_date, self.universe.end_date,
                                           self.universe.back_days, self.universe.end_days,
                                           self.required_dim_public_path_template)
        old_signature = DimDataLoaderSignature.new_dim_data_signature_from_file(signature_path)

        self.dim_definitions = self._dim_definitions()
        if not self._check_dependent_data_version(self._dependent_data_paths(), self.version_number):
            Logger.info(self.log_prefix, "Dependent data has changed. Need to reload all")
            calulate_all = CMD.proceedWhenY(self.log_prefix, "Detect newly updated data, recalculate or not")
            if calulate_all:
                self._load_all()
                signature.save_signature(signature_path)
            else:
                Logger.info(self.log_prefix, "Manually skip reload")
            return
        elif not self._check_dim_data_existance(self.true_new_dims):
            Logger.info(self.log_prefix, "Unloaded dim detected not created correctly. Need to load all")
            self._load_all()
            signature.save_signature(signature_path)
            return
        elif signature.check(old_signature):
            Logger.info(self.log_prefix, "No change. Skip reload")
            return
        elif not signature.check_outside_date(old_signature):
            Logger.info(self.log_prefix, "Signature change outside date list. Reload all")
            self._load_all()
            signature.save_signature(signature_path)
        elif not self._check_dim_data_range(self.new_dims, self.universe):
            dim_data_path = self.dim_data_path_template.format(self.new_dims[0])
            dim_data = DimData.new_dim_data_from_file(dim_data_path)
            dim_data_start_date = dim_data.get_data_start_date()
            dim_data_end_date = dim_data.get_data_end_date()
            universe_start_date = self.universe.date_list[0]
            universe_end_date = self.universe.date_list[-1]
            if universe_end_date < dim_data_start_date or universe_start_date > dim_data_end_date:
                Logger.info(self.log_prefix, "Dectect totally independent data.")
                hint = "Detect totally independent data, existing new_dims: " + str(dim_data_start_date) +\
                       "to" + str (dim_data_end_date) + " , go on to calculate and save new data(Y) or quit(N)"
                calulate_all = CMD.proceedWhenY(self.log_prefix, hint)
                if calulate_all:
                    Logger.info(self.log_prefix, "Calculate and save new data")
                    self._load_all()
                    signature.save_signature(signature_path)
                else:
                    sys.exit(0)
                return

            else:
                Logger.info(self.log_prefix, "Date list changed. Try to load partial")
                head_date_list, tail_date_list = self._get_head_and_tail_list()
                self._load_partial(head_date_list, tail_date_list)
                signature.save_signature(signature_path)
                return

        else:
            Logger.info(self.log_prefix, "Date range already covered. Skip reload")
            signature.save_signature(signature_path)
            return

    def _get_required_dim_file_path(self, dim):
        if dim in self.public_dim_path_template:
            return self.public_dim_path_template[dim].format(dim)
        else:
            return self.dim_data_path_template.format(dim)

    def _load_all(self):
        Logger.debug(self.log_prefix, "Start load all")
        required_dim_datas = {}
        dim_datas = {}
        for dim in self.required_dims:
            required_dim_file_path = self._get_required_dim_file_path(dim)
            print("loading dim {} from {}".format(dim, required_dim_file_path))
            required_dim_datas[dim] = DimData.strict_dim_data_from_file_and_universe(required_dim_file_path, self.universe)
        for new_dim in self.new_dims:
            dim_datas[new_dim] = DimData(self.version_number, self.dim_definitions[new_dim].value.name)

        for di, current_date in enumerate(self.universe.date_list):
            self._calculate_one_day_with_log(required_dim_datas, dim_datas, di, current_date)
        for new_dim in self.new_dims:
            dim_datas[new_dim].save_dim_data_to_file(self.dim_data_path_template.format(new_dim))
        Logger.debug(self.log_prefix, "Finished Load all")
        pass

    def _load_partial(self, head_date_list, tail_date_list):
        Logger.debug(self.log_prefix, "Start load partial")
        if not head_date_list and not tail_date_list:
            Logger.debug(self.log_prefix, "empty head_date_list and empty_tail_list. Skip")
            return
        required_dim_datas = {}
        dim_datas = {}
        for dim in self.required_dims:
            required_dim_datas[dim] = DimData.strict_dim_data_from_file_and_universe(self._get_required_dim_file_path(dim), self.universe)
        for new_dim in self.new_dims:
            dim_datas[new_dim] = DimData.new_dim_data_from_file(self.dim_data_path_template.format(new_dim))
            # when load partial, the version number should not be updated
        if head_date_list:
            head_dim_data = {}
            for dim in self.new_dims:
                head_dim_data[dim] = DimData(self.version_number, self.dim_definitions[dim].value.name)
            for current_date in head_date_list:
                di = self.universe.date_list.index(current_date)
                self._calculate_one_day_with_log(required_dim_datas, head_dim_data, di, current_date)
            for dim in self.new_dims:
                dim_datas[dim].merge_dim_data_at_head(head_dim_data[dim])
        if tail_date_list:
            tail_dim_data = {}
            for dim in self.new_dims:
                tail_dim_data[dim] = DimData(self.version_number, self.dim_definitions[dim].value.name)
            for current_date in tail_date_list:
                di = self.universe.date_list.index(current_date)
                self._calculate_one_day_with_log(required_dim_datas, tail_dim_data, di, current_date)
            for dim in self.new_dims:
                dim_datas[dim].merge_dim_data_at_tail(tail_dim_data[dim])
        for dim in self.new_dims:
            dim_datas[dim].save_dim_data_to_file(self.dim_data_path_template.format(dim))
        Logger.debug(self.log_prefix, "Finished load partial")
        pass

    def _dim_definitions(self):
        """
        :return: {dim_name: data_type}, which describe the dim definition that describe the dim to load
        """
        return {}

    def _one_day_log(self, required_dim_datas, dim_datas, di, current_date, other_info_list=[]):
        Logger.debug(self.log_prefix, current_date.strftime(DATE_FORMAT_STRING) + " DEFAULT LOG")

    def _calculate_one_day_with_log(self, required_dim_datas, dim_datas, di, current_date):
        other_info_list = []
        if current_date == self.universe.date_list[-1]:
            try:
                other_info_list = self._calculate_one_day(required_dim_datas, dim_datas, di, current_date)
                self._one_day_log(required_dim_datas, dim_datas, di, current_date, other_info_list)
            except Exception as e:
                print(traceback.print_exc())
                Logger.warn(self.log_prefix, "Last date {} data not exist!".format(current_date))
                # Need to do something here in DimDataLoader
                for dim in self.new_dims:
                    dim_datas[dim].append_date_data(current_date, {})
            return
        other_info_list = self._calculate_one_day(required_dim_datas, dim_datas, di, current_date)
        self._one_day_log(required_dim_datas, dim_datas, di, current_date, other_info_list)

    def _calculate_one_day(self, required_dim_datas, dim_datas, di, current_date):
        """
        Fill dim_datas for current_date. Need to be implemented by sub-class
        :param required_dim_datas: {dim_name: dim_data} that are the required dimension
        :param dim_datas: {dim_name: dim_data} that are the data to be loaded
        :param di: date index
        :param current_date:
        """
        return []
        pass

    def _signature_path_name(self, user_name, project_name, data_loader_name):
        return os.path.join(WORK_BASE_DIR, "{}/{}/signature/{}_{}_signature.bin".format(user_name, project_name, self.__class__.__name__, data_loader_name))

    def _universe_path_name(self, user_name, project_name):
        return os.path.join(WORK_BASE_DIR, "{}/{}/universe/{}_universe.bin".format(user_name, project_name, self.__class__.__name__))

    
    def _check_dependent_data_version(self, data_paths, version_number):
        for dim, data_path in data_paths.items():
            if dim in self.required_dim_public_path_template:
                continue
            if MSData.data_has_been_updated(data_path, version_number):
                return False
        return True

    def _check_dim_data_existance(self, dims):
        for dim in dims:
            if not os.path.exists(self.dim_data_path_template.format(dim)):
                return False
        return True

    def _check_dim_data_range(self, dims, universe):
        """
        Check whether the dependent data has already covered the date list in the universe
        """
        if not universe.date_list:
            raise Exception("Universe has empty date list")
        start_date = universe.date_list[0]
        end_date = universe.date_list[-1]
        for dim in dims:
            dim_data_path = self.dim_data_path_template.format(dim)
            if not os.path.exists(dim_data_path):
                raise Exception("Dim data {} should exist!".format(dim_data_path))
            dim_data = DimData.new_dim_data_from_file(dim_data_path)
            if not dim_data.data:
                return False
            if start_date < dim_data.data[0][0] or end_date > dim_data.data[-1][0]:
                return False
        return True

    def _get_head_and_tail_list(self):
        if not self.universe.date_list:
            raise Exception("Universe has empty date list")
        head_date_list = []
        tail_date_list = []
        if len(self.new_dims) == 0:
            return head_date_list, tail_date_list

        dim = self.new_dims[0]
        dim_data_path = self.dim_data_path_template.format(dim)
        if not os.path.exists(dim_data_path):
            raise Exception("Dim data {} should exist!".format(dim_data_path))
        dim_data = DimData.new_dim_data_from_file(dim_data_path)
        dim_data_date_list = dim_data.get_date_list()
        head_index = None
        tail_index = None
        if dim_data_date_list[0] in self.universe.date_list:
            head_index = self.universe.date_list.index(dim_data_date_list[0])
        if dim_data_date_list[-1] in self.universe.date_list:
            tail_index = self.universe.date_list.index(dim_data_date_list[-1])
        head_date_list = self.universe.date_list[:head_index] if head_index else []
        tail_date_list = self.universe.date_list[tail_index + 1:] if tail_index else []
        return head_date_list, tail_date_list

    def _dependent_data_paths(self):
        """
        :return: [dim: data_path ...]
        """
        return {dim: self._get_required_dim_file_path(dim) for dim in self.required_dims}


class SampleDimDataLoader(AbstractDimDataLoader):
    def _dim_definitions(self):
        """
        :return: {dim_name: data_type}, which describe the dim definition that describe the dim to load
        """
        return {"AVERAGE_HIGH_LOW": DataType.Float64}

    def _calculate_one_day(self, required_dim_datas, dim_datas, di, current_date):
        """
        Fill dim_datas for current_date. Need to be implemented by sub-class
        :param required_dim_datas: {dim_name: dim_data} that are the required dimension
        :param dim_datas: {dim_name: dim_data} that are the data to be loaded
        """
        date_dim_data = {}
        for secu_code in self.universe.secu_code_list:
            if secu_code not in required_dim_datas["HIGHPRICE"].data[di][1] and secu_code not in required_dim_datas["LOWPRICE"].data[di]:
                continue
            high_price = required_dim_datas["HIGHPRICE"].data[di][1][secu_code] if secu_code in required_dim_datas["HIGHPRICE"].data[di][1] else None
            low_price = required_dim_datas["LOWPRICE"].data[di][1][secu_code] if secu_code in required_dim_datas["LOWPRICE"].data[di][1] else None
            date_dim_data[secu_code] = (high_price + low_price) / 2 if (high_price is not None and low_price is not None) else None
        dim_datas["AVERAGE_HIGH_LOW"].append_date_data(current_date, date_dim_data)


class SampleInstrumentPoolDataLoader(AbstractDimDataLoader):
    def _dim_definitions(self):
        """
        :return: {dim_name: data_type}, which describe the dim definition that describe the dim to load
        """
        return {"SAMPLE_INSTRUMENT_POOL": DataType.Bool}

    def _calculate_one_day(self, required_dim_datas, dim_datas, di, current_date):
        """
        Fill dim_datas for current_date. Need to be implemented by sub-class
        :param required_dim_datas: {dim_name: dim_data} that are the required dimension
        :param dim_datas: {dim_name: dim_data} that are the data to be loaded
        """
        date_dim_data = {}
        for secu_code in self.universe.secu_code_list:
            if secu_code not in required_dim_datas["HIGHPRICE"].data[di][1]:
                continue
            high_price = required_dim_datas["HIGHPRICE"].data[di][1][secu_code] if secu_code in required_dim_datas["HIGHPRICE"].data[di][1] else None
            date_dim_data[secu_code] = high_price > 20.0 if (high_price is not None) else False
        dim_datas["SAMPLE_INSTRUMENT_POOL"].append_date_data(current_date, date_dim_data)


class AbstractRawDataLoader(object):
    """
    Abstract data loader to load from raw (txt) file
    """
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, dim_to_new_name={},
                 public_dim_path_template={}, use_public_data=False):
        """
        :param user_name: project owner
        :param project_name: name of the project. universe will be project-wise
        :param universe: the universe object for loading the data
        :param path_templates: all the path templates that this data loader is using   e.g:[stock_path,index_path]
        :param dims_to_load: the dims that this run would like to load
        :param dim_to_new_name: {dim_name: new_dim_name}
        :param public_dim_path_template: {dim_name: dim's public_data_path}
        :param use_public_data: boolean
        """
        # 生成path模板
        self.log_prefix = "[" + self.__class__.__name__ + "]"
        signature_path = os.path.join(WORK_BASE_DIR, "{}/{}/signature/{}_{}_signature.bin".format(user_name, project_name, self.__class__.__name__, data_loader_name))
        self.dim_data_path_template = os.path.join(WORK_BASE_DIR, "{}/{}/dim/".format(user_name, project_name) + "{}.bin")

        self.public_dim_path_template = public_dim_path_template
        self.use_public_data = use_public_data
        
        # 如果use_public_data 则为{dim……}  否则为dims_to_load = ['close','low'……]
        dims_to_truly_load = {dim for dim in dims_to_load if dim not in public_dim_path_template} if use_public_data else dims_to_load
        for none_public_dim in dims_to_truly_load:
            if none_public_dim in public_dim_path_template:
                public_dim_path_template.pop(none_public_dim)
        
        # 生成用来比对数据的signature 
        signature = RawDataLoaderSignature(universe.start_date, universe.end_date, universe.back_days, universe.end_days, path_templates, dims_to_load, dim_to_new_name)
        old_signature = RawDataLoaderSignature.new_raw_data_signature_from_file(signature_path)
        
        
#========================检查并加载数据=================================
        self.dim_definitions = self._all_dim_definitions() # 定义数据类型
        # 如果数据不存在 需要重新加载
        if not self._check_dim_data_existance(dims_to_truly_load, dim_to_new_name):
            Logger.info(self.log_prefix, "Unloaded dim detected or universe not created correctly. Need to load all")
            self._load_all(universe, version_number, path_templates, dims_to_truly_load, dim_to_new_name)
            signature.save_signature(signature_path)
            return
        # 如果数据没有变化，则不操作
        elif signature.check(old_signature):
            Logger.info(self.log_prefix, "No change. Skip reload")
            return
        # 如果新的时间范围超过原来的时间范围，则重新加载（功能暂未实现）
        elif not signature.check_outside_date(old_signature):
            Logger.info(self.log_prefix, "Signature change outside date list. Reload all")
            self._load_all(universe, version_number, path_templates, dims_to_truly_load, dim_to_new_name)
            signature.save_signature(signature_path)
            return
        # 如果有某些数据时间范围 没有 都覆盖指定的时间范围
        elif not self._check_dim_data_range(dims_to_truly_load, universe, dim_to_new_name):
            dim = dims_to_truly_load[0]
            dim_data_path = self.dim_data_path_template.format(dim_to_new_name[dim] if dim in dim_to_new_name else dim)
            dim_data = DimData.new_dim_data_from_file(dim_data_path)
            dim_data_start_date = dim_data.get_data_start_date()
            dim_data_end_date = dim_data.get_data_end_date()
            universe_start_date = universe.date_list[0]
            universe_end_date = universe.date_list[-1]
            if universe_end_date < dim_data_start_date or universe_start_date > dim_data_end_date:
                Logger.info(self.log_prefix, "Dectect totally independent data.")
                hint = "Detect totally independent data, existing dims_to_load: " + str(dim_data_start_date) + \
                       "to" + str(dim_data_end_date) + " , go on to calculate and save new data(Y) or quit(N)"
                calulate_all = CMD.proceedWhenY(self.log_prefix, hint)
                if calulate_all:
                    Logger.info(self.log_prefix, "Calculate and save new data")
                    self._load_all(universe, version_number, path_templates, dims_to_truly_load, dim_to_new_name)
                    signature.save_signature(signature_path)
                else:
                    sys.exit(0)
                return
            else:
                Logger.info(self.log_prefix, "Date list expanded. Try to load partial")
                head_date_list, tail_date_list = self._get_head_and_tail_list(dims_to_truly_load, universe, dim_to_new_name)
                self._load_partial(universe, version_number, path_templates, dims_to_truly_load, head_date_list,
                                   tail_date_list, dim_to_new_name)
                signature.save_signature(signature_path)
                return
        else:
            Logger.info(self.log_prefix, "Date range already covered. Skip reload")
            signature.save_signature(signature_path)
            return
        pass

    def do_load(self):
        pass
    
    # 检查data是否已经存在
    def _check_dim_data_existance(self, dims, dim_to_new_name):
        if self.use_public_data:
            for dim in dims:
                if dim in self.public_dim_path_template:
                    if not os.path.exists(self.public_dim_path_template[dim].format(dim_to_new_name[dim] if dim in dim_to_new_name else dim)):
                        return False
                else:
                    if not os.path.exists(self.dim_data_path_template.format(dim_to_new_name[dim] if dim in dim_to_new_name else dim)):
                        return False
        else:
            for dim in dims:
                if not os.path.exists(self.dim_data_path_template.format(dim_to_new_name[dim] if dim in dim_to_new_name else dim)):
                    return False
        return True

    def _check_dim_data_range(self, dims, universe, dim_to_new_name):
        """
        Check whether the data to load has already covered the date list in the universe
        检查已有的数据时间范围是否已经覆盖了指定的时间范围
        """
        if not universe.date_list:
            raise Exception("Universe has empty date list")
        start_date = universe.date_list[0]
        end_date = universe.date_list[-1]
        for dim in dims:
            dim_data_path = self.dim_data_path_template.format(dim_to_new_name[dim] if dim in dim_to_new_name else dim)
            if not os.path.exists(dim_data_path):
                raise Exception("Dim data {} should exist!".format(dim_data_path))
            dim_data = DimData.new_dim_data_from_file(dim_data_path)
            if not dim_data.data:
                return False
            if start_date < dim_data.data[0][0] or end_date > dim_data.data[-1][0]:
                return False
        return True

    def _get_head_and_tail_list(self, dims_to_load, universe, dim_to_new_name):
        if not universe.date_list:
            raise Exception("Universe has empty date list")
        head_date_list = []
        tail_date_list = []
        if len(dims_to_load) == 0:
            return head_date_list, tail_date_list

        dim = dims_to_load[0]
        dim_data_path = self.dim_data_path_template.format(dim_to_new_name[dim] if dim in dim_to_new_name else dim)
        if not os.path.exists(dim_data_path):
            raise Exception("Dim data {} should exist!".format(dim_data_path))
        dim_data = DimData.new_dim_data_from_file(dim_data_path)
        dim_data_date_list = dim_data.get_date_list()
        head_index = None
        tail_index = None
        if dim_data_date_list[0] in universe.date_list:
            head_index = universe.date_list.index(dim_data_date_list[0])
        if dim_data_date_list[-1] in universe.date_list:
            tail_index = universe.date_list.index(dim_data_date_list[-1])
        head_date_list = universe.date_list[:head_index] if head_index else []
        tail_date_list = universe.date_list[tail_index + 1:] if tail_index else []
        return head_date_list, tail_date_list

    def _load_all(self, universe, version_number, path_templates, dims, dim_to_new_name):
        '''
        path_templates:  [stock_path,index_path,……]
        dims:            ['close',...]
        dim_to_new_name: []
        -------------------
        dim_datas : {'close':DimData,
                     'low':DimData,
                     ...
                     }
        
        '''
        
        Logger.debug(self.log_prefix, "Start load all")
        dim_datas = {} # 初始化数据变量
        for dim in dims:
            dim_datas[dim] = DimData(version_number, self.dim_definitions[dim].value.name)  # DimData(version_number,datatype)
        for current_date in universe.date_list:
            date_data = {} # 初始化 date_data
            for dim in dims: 
                date_data[dim] = {}
            self._load_one_date_data_with_log(universe, path_templates, current_date, date_data, dims) # 填满date_data[dim]
            for dim in dims:
                dim_datas[dim].append_date_data(current_date, date_data[dim])
        # 保存数据 （读取多少数据 就存多少个bin文件）
        for dim in dims:
            dim_datas[dim].save_dim_data_to_file(self.dim_data_path_template.format(dim_to_new_name[dim] if dim in dim_to_new_name else dim))
        Logger.debug(self.log_prefix, "Finished Load all")

    def _load_partial(self, universe, version_number, path_templates, dims, head_date_list, tail_date_list, dim_to_new_name):
        Logger.debug(self.log_prefix, "Start load partial")
        dim_datas = {}
        for dim in dims:
            dim_datas[dim] = DimData.new_dim_data_from_file(self.dim_data_path_template.format(dim_to_new_name[dim] if dim in dim_to_new_name else dim))
            # when load partial, the version number should not be updated
        if head_date_list:
            head_dim_data = {}
            for dim in dims:
                head_dim_data[dim] = DimData(version_number, self.dim_definitions[dim].value.name)
            for current_date in head_date_list:
                date_data = {}
                for dim in dims:
                    date_data[dim] = {}
                self._load_one_date_data_with_log(universe, path_templates, current_date, date_data, dims)
                for dim in dims:
                    head_dim_data[dim].append_date_data(current_date, date_data[dim])
            for dim in dims:
                dim_datas[dim].merge_dim_data_at_head(head_dim_data[dim])
        if tail_date_list:
            tail_dim_data = {}
            for dim in dims:
                tail_dim_data[dim] = DimData(version_number, self.dim_definitions[dim].value.name)
            for current_date in tail_date_list:
                date_data = {}
                for dim in dims:
                    date_data[dim] = {}
                self._load_one_date_data_with_log(universe, path_templates, current_date, date_data, dims)
                for dim in dims:
                    tail_dim_data[dim].append_date_data(current_date, date_data[dim])
            for dim in dims:
                dim_datas[dim].merge_dim_data_at_tail(tail_dim_data[dim])
        for dim in dims:
            dim_datas[dim].save_dim_data_to_file(self.dim_data_path_template.format(dim_to_new_name[dim] if dim in dim_to_new_name else dim))
        Logger.debug(self.log_prefix, "Finished load partial")
        pass

    def _one_day_log(self, universe, path_templates, current_date, date_data, dims, other_info_list=[]):
        Logger.debug(self.log_prefix, current_date.strftime(DATE_FORMAT_STRING) + " DEFAULT LOG")

    def _load_one_date_data_with_log(self, universe, path_templates, current_date, date_data, dims):
        '''
        加载一天数据
        '''
        other_info_list = []
        # 判断是否为最后一天
        if current_date == universe.date_list[-1]:
            try:
                other_info_list = self._load_one_date_data(universe, path_templates, current_date, date_data, dims)
            except:
                Logger.warn(self.log_prefix, "Last date {} data not exist!".format(current_date))
        else:
            other_info_list = self._load_one_date_data(universe, path_templates, current_date, date_data, dims)
        self._one_day_log(universe, path_templates, current_date, date_data, dims, other_info_list)

    def _load_one_date_data(self, universe, path_templates, current_date, date_data, dims):
        """
        Would read from txt file and save it into date_data. May be different one how to use the templates
        :param universe:
        :param current_date:
        :param date_data:
        :param dims: dims that about to load from raw data
        """
        return []
        pass

    def _read_one_file(self, f, date_data, dims):
        lines = f.readlines()
        dim_index = {}
        for li, line in enumerate(lines):
            line_list = line.strip().split(COLUMN_DELIMITER)
            if li == 0:
                dim_index = parse_dim_index(line_list)
                continue
            self._read_one_row(date_data, line_list, dim_index, dims)

    def _read_one_row(self, date_data, line_list, dim_index, dims):
        secu_code = line_list[0]
        for dim in dims:
            if dim not in dim_index:
                continue
            date_data[dim][secu_code] = self.dim_definitions[dim].value.parser(line_list[dim_index[dim]])

        pass

    def _all_dim_definitions(self):
        return {}


class BaseDataLoader(AbstractRawDataLoader):
    """
    path_template: [stock_path_template, index_path_template]
    """
    def _load_one_date_data(self, universe, path_templates, current_date, date_data, dims):
        """
        Would read from txt file and save it into date_data. May be different due to different universe set up
        :param universe:
        :param current_date:
        :param date_data:
        :param dims: dims that about to load from raw data
        """
        if universe.stock_secu_code_number:
            file_path = path_templates[0].format(current_date.year, current_date.month, current_date.day)
            with open(file_path, "r", encoding='utf-8') as f:
                self._read_one_file(f, date_data, dims)
        if universe.index_secu_code_list:
            file_path = path_templates[1].format(current_date.year, current_date.month, current_date.day)
            with open(file_path, "r", encoding='utf-8') as f:
                self._read_one_file(f, date_data, dims)
        pass

    def _all_dim_definitions(self):
        return {
            "PREVCLOSEPRICE": DataType.Float64, "OPENPRICE": DataType.Float64, "HIGHPRICE": DataType.Float64, "LOWPRICE": DataType.Float64, "CLOSEPRICE": DataType.Float64,
            "TURNOVERVOLUME": DataType.Float64, "TURNOVERVALUE": DataType.Float64,
            "SECUMARKET": DataType.Str, "LISTEDDATE": DataType.Str, "TRADESTATE": DataType.Str, "SPECIALTRADETYPE": DataType.Str,
            "SPLIT": DataType.Float64, "DIVIDEND": DataType.Float64, "ACTUALPLARATIO": DataType.Float64, "PLAPRICE": DataType.Float64,
            "BASESHARES": DataType.Float64, "ACTUALPLAVOL": DataType.Float64, "TOTALSHARES": DataType.Float64, "TOTALFLOATSHARES": DataType.Float64, "FREEFLOATSHARES": DataType.Float64,
            "SECUNAME": DataType.Str, "CHINAME": DataType.Str,
            "S_DQ_PRECLOSE": DataType.Float64, "S_DQ_CLOSE": DataType.Float64
        }


class OtherBaseDataLoader(AbstractRawDataLoader):
    """
    path_template: [stock_path_template, index_path_template]
    """
    def _load_one_date_data(self, universe, path_templates, current_date, date_data, dims):
        """
        Would read from txt file and save it into date_data. May be different due to different universe set up
        :param universe:
        :param current_date:
        :param date_data:
        :param dims: dims that about to load from raw data
        """
        if universe.stock_secu_code_number:
            file_path = path_templates.format(current_date.year, current_date.month, current_date.day)
            with open(file_path, "r", encoding='utf-8') as f:
                self._read_one_file(f, date_data, dims)

        pass

    def _read_one_row(self, date_data, line_list, dim_index, dims):
        secu_code = line_list[0]
        for dim in dims:
            if dim not in dim_index:
                continue
            if line_list[dim_index[dim]] == '':
                date_data[dim][secu_code] = np.nan
            else:
                date_data[dim][secu_code] = self.dim_definitions[dim].value.parser(line_list[dim_index[dim]])



    def _all_dim_definitions(self):
        return {
            "TOT_ASSETS": DataType.Float64}



if __name__ == "__main__":
    t0 = time.process_time()
    user_name = "hqu"
    project_name = "universe_test"
    required_data_sources = ["Stock"]
    start_date = date(2017, 1, 4)
    end_date = date(2017, 1, 14)
    back_days = 1
    end_days = 0
    universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date, back_days, end_days)
    
    universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)
    stock_path_template = os.path.join(DATA_BASE_DIR, "cooked/BaseData/{}/{}/{}/BASEDATA_new.txt")
    index_path_template = os.path.join(DATA_BASE_DIR, "raw/WIND/IndexQuote/{}/{}/{}/AINDEXEODPRICES.txt")
    dims_to_load = ["PREVCLOSEPRICE", "OPENPRICE", "HIGHPRICE", "LOWPRICE", "CLOSEPRICE", "SECUNAME", "BASESHARES", "S_DQ_PRECLOSE"]
    version_number = ProjectData.read_version_number(user_name, project_name)
    ProjectData.save_version_number(user_name, project_name, version_number + 1)
    # base_data_loader = OriginalBaseDataLoader(user_name, project_name, universe, version_number, stock_path_template,
    #                                   index_path_template, dims_to_load)
    another_base_data_loader = BaseDataLoader(user_name, project_name, "abdl", universe, version_number,
                                                      [stock_path_template], dims_to_load)

    # sample_dim_data_loader = SampleDimDataLoader(user_name, project_name, "sampledimdl", universe, version_number, required_dims=["HIGHPRICE", "LOWPRICE"], new_dims=["AVERAGE_HIGH_LOW"])
    # sample_dim_data_loader.do_load()
    # sample_ip_data_loader = SampleInstrumentPoolDataLoader(user_name, project_name, "sampleipdl", universe, version_number, required_dims=["HIGHPRICE"], new_dims=["SAMPLE_INSTRUMENT_POOL"])
    # sample_ip_data_loader.do_load()
    t1 = time.process_time()
    Logger.info("", "{} seconds".format(t1 - t0))
