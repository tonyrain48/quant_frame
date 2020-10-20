#Created by sfu

from quant.data_loader import *
from quant.universe import *
from quant.signature import *
from quant.data import *
from quant.public_dl.dl_combo import *
from quant.helpers import Logger
import os, io

class IndexWeightDataLoader(AbstractRawDataLoader):
    """
    path_template: [index_weight_path_template]
    """       
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, dim_to_new_name={}, fill_missing_file=False):
        print(dim_to_new_name)
        self.dim_to_new_name = dim_to_new_name
        self.fill_missing_file = fill_missing_file
        super().__init__(user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, dim_to_new_name)

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
            # fix the bug of missing files
            # fake an TextIOWrapper, f
            if self.fill_missing_file and (not os.path.exists(file_path)):
                output = io.BytesIO()
                f = io.TextIOWrapper(output, line_buffering=True)
                f.write('|'.join(['SECU_CODE', 'ENDDATE', 'WEIGHT']))
            else:
                f = open(file_path, "r")
            self._read_one_file(f, date_data, dims)
            f.close()
            # end
        pass

    def _all_dim_definitions(self):
        return {"WEIGHT":DataType.Float64,'I_WEIGHT':DataType.Float64} #if not self.dim_to_new_name else {self.dim_to_new_name["WEIGHT"]:DataType.Float64}
    
    pass


class IndexWeightDimDataLoader(AbstractDimDataLoader):
    def _dim_definitions(self):
        """
        :return: {dim_name: data_type}, which describe the dim definition that describe the dim to load
        """
        return {self.new_dims[0]: DataType.Float64, self.new_dims[1]: DataType.Bool}

    def _calculate_one_day(self, required_dim_datas, dim_datas, di, current_date):
        """
        Fill dim_datas for current_date. Need to be implemented by sub-class
        :param required_dim_datas: {dim_name: dim_data} that are the required dimension
        :param dim_datas: {dim_name: dim_data} that are the data to be loaded
        """
        date_dim_data = {}
        date_pool_data = {}
        num_sec_in_index = 0
        for secu_code in self.universe.secu_code_list:
            index_weight = required_dim_datas[self.required_dims[0]].data[di][1][secu_code] if secu_code in required_dim_datas[self.required_dims[0]].data[di][1] else None
            date_dim_data[secu_code] = index_weight
            if (index_weight is not None):
                date_pool_data[secu_code] = True
                num_sec_in_index = num_sec_in_index + 1
            else:
                date_pool_data[secu_code] = False
        dim_datas[self.new_dims[0]].append_date_data(current_date, date_dim_data)
        dim_datas[self.new_dims[1]].append_date_data(current_date, date_pool_data)
        Logger.debug(self.log_prefix, "IndexWeight updated {} instruments in {}".format(num_sec_in_index, current_date))


if __name__ == "__main__":
    t0 = time.process_time()

    # Project set up
    user_name = "hqu"
    project_name = "index_wgt_test"
    required_data_sources = ["Stock","Index"]
    start_date = date(2018, 9, 21)
    end_date = date(2018, 9, 28)
    back_days = 1
    end_days = 0

    # Generate universe
    universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date,
                                           back_days, end_days)
    universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)

    HS300_path_template = os.path.join(DATA_BASE_DIR,
                                                    "raw/WIND/IndexWeight/{}/{}/{}/AINDEXHS300CLOSEWEIGHT.txt")
    CSI500_path_template = os.path.join(DATA_BASE_DIR,"raw/WIND/IndexWeight/{}/{}/{}/AINDEXCSI500WEIGHT.txt")
    dims_to_load = ["WEIGHT"]

    # Get and update version number
    version_number = ProjectData.read_version_number(user_name, project_name)
    ProjectData.save_version_number(user_name, project_name, version_number + 1)

    HS300_weight_data_loader = IndexWeightDataLoader(user_name, project_name, universe, version_number,
                                              [HS300_path_template], dims_to_load)

    HS300_dim_data_loader = IndexWeightDimDataLoader(user_name, project_name, universe, version_number, \
                                                            required_dims=["WEIGHT"], new_dims=["HS300_WEIGHT","HS300_VALID"])

    HS300_dim_data_loader.do_load()

    CSI500_weight_data_loader = IndexWeightDataLoader(user_name, project_name, universe, version_number,
                                                    [CSI500_path_template], dims_to_load)
           
    CSI500_dim_data_loader = IndexWeightDimDataLoader(user_name, project_name, universe, version_number, \
                                                                     required_dims=["WEIGHT"], new_dims=["CSI500_WEIGHT","CSI500_VALID"])
                                                    
    CSI500_dim_data_loader.do_load()
    
    combo_instrument_pool_data_loader = ComboInstrumentPool(user_name, project_name, universe, version_number, required_dims = ["HS300_VALID","CSI500_VALID"], \
                                                                        new_dims = ["COMBO_INSTRUMENT_POOL"], combo_rules = "union", combo_list = ["Y","Y"])
    combo_instrument_pool_data_loader.do_load()

    t1 = time.process_time()
    Logger.info("", "{} seconds".format(t1 - t0))
