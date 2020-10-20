import os
import os.path
import numpy as np
import pickle
import time
from quant.constants import WORK_BASE_DIR, DATE_FORMAT_STRING
from quant.definitions import DataType
from quant.universe import Universe


def np_nan_array(shape, dtype):
    # Be very careful when using non float dtype! np.nan would give dtype=int a value of inf, and dtype=bool a value of True
    nan_array = np.zeros(shape=shape, dtype=dtype)
    if dtype == "bool_" or dtype == "bool":
        return nan_array
    nan_array[:] = np.nan
    return nan_array


class MSData(object):
    @staticmethod
    def data_has_been_updated(file_path, new_version_number):
        if not os.path.exists(file_path):
            return True
        with open(file_path, "rb") as f:
            version_number = pickle.load(f)
            return version_number == new_version_number


class ProjectData(MSData):
    """
    We use txt file in Project Data for transparency
    """
    def __init__(self, version_number):
        self.version_number = version_number

    def save(self, file_path):
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        with open(file_path, "w") as f:
            f.write(str(self.version_number) + "\n")

    def load(self, file_path):
        if not os.path.exists(file_path):
            return
        with open(file_path, "r") as f:
            self.version_number = int(f.readline().strip())

    @staticmethod
    def project_file_path(user_name, project_name):
        return os.path.join(WORK_BASE_DIR, "{}/{}/project_version.txt".format(user_name, project_name))

    @staticmethod
    def save_version_number(user_name, project_name, version_number):
        project_data = ProjectData(version_number)
        project_data.save(ProjectData.project_file_path(user_name, project_name))

    @staticmethod
    def read_version_number(user_name, project_name):
        file_path = ProjectData.project_file_path(user_name, project_name)
        if not os.path.exists(file_path):
            return 0
        with open(file_path, "r") as f:
            try:
                return int(f.readline().strip())
            except:
                raise Exception("Read project version number error", file_path)


class DimData(MSData):
    '''
    用来读数据与存数据
    '''
    def __init__(self, version_number, data_type_name):
        """
        data: [(date, date_data)], where date_data is {secu_code: value}
        """
        self.version_number = version_number
        self.data = []
        self.data_type_name = data_type_name
        pass

    def append_date_data(self, current_date, date_data):
        self.data.append((current_date, date_data))

    def merge_dim_data_at_head(self, head_dim_data):
        data = head_dim_data.data
        data.extend(self.data)
        self.data = data

    def merge_dim_data_at_tail(self, tail_dim_data):
        self.data.extend(tail_dim_data.data)

    def get_date_list(self):
        return [d for (d, v) in self.data]

    def get_data_start_date(self):
        return self.data[0][0]

    def get_data_end_date(self):
        return self.data[-1][0]

    def save_dim_data_to_file(self, file_path):
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        with open(file_path, "wb") as f:
            pickle.dump(self.version_number, f)
            pickle.dump(self.data_type_name, f)
            pickle.dump(self.data, f)
    
    def save_alpha_to_file(self, file_path_template,alpha_name):
        
        for i, items in enumerate(self.data):
            if i == 0:
                continue
            date = items[0]
            data = items[1]
            file_path = file_path_template.format(date.year,date.month,date.day,alpha_name)
            dir_name = os.path.dirname(file_path)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            with open(file_path, 'w',encoding='utf-8') as f:
                f.write('SEC_CODE|ALPHA_VALUES\n')
                for k,v in data.items():
                    f.write(str(k) + '|' + str(v) + '\n')
    
    def load_dim_data(self, file_path):
        if not os.path.exists(file_path):
            print("[ERROR] Loading dim data: file_path {} does not exist!".format(file_path))
            return
        with open(file_path, "rb") as f:
            self.version_number = pickle.load(f)
            self.data_type_name = pickle.load(f)
            self.data = pickle.load(f)
            print(file_path)


    def retrive_strict_data_by_universe(self, universe):
        while self.data[0][0] < universe.date_list[0]:
            del self.data[0]
        while self.data[-1][0] > universe.date_list[-1]:
            del self.data[-1]


    def show_data_info(self):
        print("[INFO]Version number:{}".format(self.version_number))
        print("[INFO]Data type:{}".format(self.data_type_name))
        for current_date, date_data in self.data:
            res_list = [current_date.strftime(DATE_FORMAT_STRING)]
            for secu_code, value in date_data.items():
                res_list.append(secu_code + ":" + str(value))
            print("[DATA]" + "|".join(res_list))

    @staticmethod
    def from_alpha(alpha_data, universe):
        dim_data = DimData(-1, "float64")
        for di, current_date in enumerate(universe.date_list):
            date_data = {}
            for secu_code, ii in universe.secu_code_to_index.items():
                if ~np.isnan(alpha_data[di][ii]):
                    date_data[secu_code] = alpha_data[di][ii]
            dim_data.append_date_data(current_date, date_data)
        return dim_data


    @staticmethod
    def new_dim_data_from_file(file_path):
        dim_data = DimData(-1, "none") # 初始化
        dim_data.load_dim_data(file_path)
        return dim_data


    @staticmethod
    def strict_dim_data_from_file_and_universe(file_path, universe):
        dim_data = DimData(-1, "none")
        dim_data.load_dim_data(file_path)
        dim_data.retrive_strict_data_by_universe(universe)
        return dim_data


    @staticmethod
    def data_has_been_updated(file_path, new_version_number):
        if not os.path.exists(file_path):
            return False
        with open(file_path, "rb") as f:
            version_number = pickle.load(f)
            return version_number == new_version_number


class TensorDimData(MSData):
    '''
    把dim_data转成矩阵形式
    -------------------------------------------
    矩阵    rows:日期   维度与universe.date_list相同
        columns: 股票  维度与universe.secu_code_list相同
    '''
    def __init__(self):
        self.data = None
        pass
    
    @staticmethod
    def from_dim_data(dim_data, universe):
        if dim_data.data_type_name not in DataType.NAME_TO_TYPE.value:
            raise Exception("{} is not a valid data type name".format(dim_data.data_type_name))
        tensor_dim_data = TensorDimData()
        tensor_dim_data.data_type_name = dim_data.data_type_name
        
        # 如果是numpy格式，把数据转成矩阵
        if DataType.NAME_TO_TYPE.value[dim_data.data_type_name].is_numpy:
            tensor_dim_data.data = np_nan_array(shape=(len(universe.date_list), len(universe.secu_code_list)), dtype=dim_data.data_type_name)
            for current_date, secu_code_to_value in dim_data.data:
                if current_date not in universe.date_to_di:
                    continue
                di = universe.date_to_di[current_date]
                for secu_code, value in secu_code_to_value.items():
                    if secu_code not in universe.secu_code_to_index:
                        continue
                    tensor_dim_data.data[di][universe.secu_code_to_index[secu_code]] = value
        # 如果不是numpy格式 则不是基础数据，
        else:
            tensor_dim_data.data = []
            for _ in universe.date_list:
                tensor_dim_data.data.append([None for _ in range(len(universe.secu_code_list))])
            for current_date, secu_code_to_value in dim_data.data:
                if current_date not in universe.date_to_di:
                    continue
                di = universe.date_to_di[current_date]
                for secu_code, value in secu_code_to_value.items():
                    if secu_code not in universe.secu_code_to_index:
                        continue
                    tensor_dim_data.data[di][universe.secu_code_to_index[secu_code]] = value
        return tensor_dim_data


if __name__ == "__main__":
    user_name = "hqu"
    project_name = "NewTest"
    universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)

    dim_data = DimData.new_dim_data_from_file(os.path.join(WORK_BASE_DIR, "{}/{}/dim/{}.bin".format(user_name, project_name, "ZXS")))
    # dim_data.show_data_info()
    # dim_data = DimData.new_dim_data_from_file(os.path.join(WORK_BASE_DIR, "{}/{}/dim/{}.bin".format(user_name, project_name, "AVERAGE_HIGH_LOW")))
    # dim_data.show_data_info()
    # dim_data = DimData.new_dim_data_from_file(os.path.join(WORK_BASE_DIR, "{}/{}/dim/{}.bin".format(user_name, project_name, "SAMPLE_INSTRUMENT_POOL")))
    # dim_data.show_data_info()
    # alpha_data = DimData.new_dim_data_from_file(os.path.join(WORK_BASE_DIR, "{}/{}/alpha/{}.bin".format(user_name, project_name, "sample_alpha_1")))
    # alpha_data.show_data_info()

    t0 = time.process_time()
    tensor_dim_data = TensorDimData.from_dim_data(dim_data, universe)
    t1 = time.process_time()
    print("tensor dim data load {} seconds".format(t1 - t0))
