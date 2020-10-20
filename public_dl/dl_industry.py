import sys
sys.path.append(r'F:\work\quant')
import os.path
import pickle
from datetime import date
from quant.universe import Universe
from quant.constants import DATA_BASE_DIR, WORK_BASE_DIR
from quant.data import ProjectData, DimData, TensorDimData , np_nan_array
from quant.data_loader import AbstractDimDataLoader,BaseDataLoader, AbstractRawDataLoader
from quant.helpers import Logger, CMD
from quant.definitions import DataType
from quant.universe_generator import UniverseGenerator
import numpy as np
import time
# 行业整理
class IndustryDataLoader(AbstractRawDataLoader):

    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load):
        self.dim_industry_code_dic = {}
        for dim in dims_to_load:
            dim_data_dic_path = os.path.join(WORK_BASE_DIR, "{}/{}/dim/".format(user_name, project_name)
                                             + "{}_dic.bin".format(dim))
            if not os.path.exists(dim_data_dic_path):
                self.dim_industry_code_dic[dim] = {}
            else:
                with open(dim_data_dic_path, "rb") as f:
                    self.dim_industry_code_dic[dim] = pickle.load(f)
                f.close()

        super().__init__(user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load)

        for dim in dims_to_load:
            dim_data_dic_path = os.path.join(WORK_BASE_DIR, "{}/{}/dim/".format(user_name, project_name)
                                             + "{}_dic.bin".format(dim))
            with open(dim_data_dic_path, "wb") as f:
                pickle.dump(self.dim_industry_code_dic[dim], f)
            f.close()

    def _load_one_date_data(self, universe, path_templates, current_date, date_data, dims):
        if universe.stock_secu_code_number:
            file_path = path_templates[0].format(current_date.year, current_date.month, current_date.day)
            with open(file_path, "r", encoding='utf8', errors='ignore') as f:
                
                self._read_one_file(f, date_data, dims)
        pass

    def _read_one_row(self, date_data, line_list, dim_index, dims):
        secu_code = line_list[0]
        for dim in dims:
            if dim not in dim_index:
                continue
            date_data_dim_secu_code = self.dim_definitions[dim].value.parser(line_list[dim_index[dim]])
            if self.dim_industry_code_dic[dim]. __contains__(date_data_dim_secu_code):
                date_data[dim][secu_code] = self.dim_industry_code_dic[dim][date_data_dim_secu_code]
            else:
                self.dim_industry_code_dic[dim][date_data_dim_secu_code] = len(self.dim_industry_code_dic[dim])
                date_data[dim][secu_code] = self.dim_industry_code_dic[dim][date_data_dim_secu_code]
        pass

    def _all_dim_definitions(self):
        return {
            "SWF": DataType.UInt32, "SWS": DataType.UInt32, "SWT": DataType.UInt32, "SW2014F": DataType.UInt32,
            "SW2014S": DataType.UInt32, "SW2014T": DataType.UInt32, "ZXF": DataType.UInt32, "ZXS": DataType.UInt32,
            "ZXT": DataType.UInt32,
        }

class IndustryCapDimDataLoader(AbstractDimDataLoader):
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, required_dims=["ZXF",'TOTALSHARES'], new_dims=["INDUSCAP"]):
        super().__init__(user_name, project_name, data_loader_name, universe, version_number, required_dims, new_dims)

    def _dim_definitions(self):
        return{"INDUSCAP": DataType.Float64}

    
    def _calculate_one_day(self, required_dim_datas, dim_datas, di, current_date):
        induscap_dim_data = {}
        current_date = self.universe.date_list[di]
        
        
        
        if di > 0:
            # 获取当天dim_data
            indus_data = required_dim_datas['ZXF'].data[di][1]
            cap_data = required_dim_datas['TOTALSHARES'].data[di][1]
            
            # 对齐数据
            indus_array = np_nan_array(shape = (len(self.universe.secu_code_list),1),dtype = 'float64')
            cap_array = np_nan_array(shape = (len(self.universe.secu_code_list),1),dtype = 'float64')
            secu_code_array = np_nan_array(shape = (len(self.universe.secu_code_list),1),dtype = str)
            tmp_secu_code_list = []
            for i,secu_code in enumerate(self.universe.secu_code_list):
                tmp_secu_code_list.append(secu_code)
                if secu_code not in indus_data.keys() or secu_code not in cap_data.keys():
                    induscap_dim_data[secu_code] = 0
                    continue
                indus_array[i] = indus_data[secu_code]
                cap_array[i] = cap_data[secu_code]
                
            
            unique_indus = np.unique(indus_array)
            unique_indus = unique_indus[~np.isnan(unique_indus)]
            i_data = {}
            for i_indus in unique_indus:
                
                i_indus_valid = indus_array == i_indus
                i_indus_cap = np.nansum(cap_array[i_indus_valid])
                i_indus_secu = np.array(tmp_secu_code_list)[i_indus_valid.ravel()]
                for i_secu_code in i_indus_secu:
                    induscap_dim_data[i_secu_code] = i_indus_cap
            
            pass
        
        else:
            for secu_code in self.universe.secu_code_list:
                induscap_dim_data[secu_code] = 0
        dim_datas['INDUSCAP'].append_date_data(current_date, induscap_dim_data)
        

if __name__ == "__main__":
    user_name = "hqu"
    project_name = "induscap"
    required_data_sources = ["Stock", "Index"]
    start_date = date(2012, 1, 1)
    end_date = date(2012, 6, 30)
    back_days = 60
    end_days = 0
    universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date, back_days, end_days)

    universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)
    stock_path_template = os.path.join(DATA_BASE_DIR, "cooked/BaseData/{}/{}/{}/BASEDATA_new.txt")
    index_path_template = os.path.join(DATA_BASE_DIR, "raw/WIND/IndexQuote/{}/{}/{}/AINDEXEODPRICES.txt")
    dims_to_load = ["TOTALSHARES","TOTALFLOATSHARES"]
    version_number = ProjectData.read_version_number(user_name, project_name)
    ProjectData.save_version_number(user_name, project_name, version_number + 1)
    
    another_base_data_loader = BaseDataLoader(user_name, project_name,"bdl", universe, version_number,
                                                     [stock_path_template, index_path_template], dims_to_load,dim_to_new_name={})
    industry = IndustryDataLoader(user_name, project_name, '', universe, version_number
	, ['S:/data/cooked/Industry/{}/{}/{}/ASHAREINDUSTRIESCLASSCITICS.txt']
	, ['ZXS','ZXF'])
    
    t0 = time.process_time()
    sample_ip_data_loader = IndustryCapDimDataLoader(user_name, project_name, "", universe, version_number)
    sample_ip_data_loader.do_load()
    t1 = time.process_time()
    Logger.info("", "{} seconds".format(t1 - t0))