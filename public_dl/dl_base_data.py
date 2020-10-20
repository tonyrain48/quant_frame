from quant.data_loader import AbstractRawDataLoader
from quant.definitions import DataType

# 基础数据整理
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
            with open(file_path, "r") as f:
                self._read_one_file(f, date_data, dims)
        if universe.index_secu_code_list:
            file_path = path_templates[1].format(current_date.year, current_date.month, current_date.day)
            with open(file_path, "r") as f:
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


if __name__ == "__main__":
    import time
    import os.path
    from datetime import date
    from quant.universe_generator import UniverseGenerator
    from quant.universe import Universe
    from quant.constants import DATA_BASE_DIR, WORK_BASE_DIR, COLUMN_DELIMITER, DATE_FORMAT_STRING
    from quant.data import ProjectData, DimData, HuianData
    from quant.data_loader import SampleDimDataLoader

    t0 = time.process_time()
    user_name = "jinxing"
    project_name = "public_data_test"
    required_data_sources = ["Stock", "Index"]
    start_date = date(2018, 9, 21)
    end_date = date(2018, 9, 26)
    back_days = 1
    end_days = 0
    universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date, back_days, end_days)

    universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)

    stock_path_template = os.path.join(DATA_BASE_DIR, "cooked/BaseData/{}/{}/{}/BASEDATA.txt")
    index_path_template = os.path.join(DATA_BASE_DIR, "raw/WIND/IndexQuote/{}/{}/{}/AINDEXEODPRICES.txt")
    dims_to_load = ["PREVCLOSEPRICE", "OPENPRICE", "HIGHPRICE", "LOWPRICE", "CLOSEPRICE", "SECUNAME", "BASESHARES", "S_DQ_PRECLOSE"]
    dim_to_new_name = {"PREVCLOSEPRICE": "JINXING_PREVLOW"}
    version_number = ProjectData.read_version_number(user_name, project_name)
    ProjectData.save_version_number(user_name, project_name, version_number + 1)

    public_dim_path_template = {
        "HIGHPRICE": "/work/public_data/dim/{}.bin",
        "AVERAGE_HIGH_LOW": "/work/public_data/dim/{}.bin"
    }
    base_data_loader = BaseDataLoader(user_name, project_name, "abdl", universe, version_number,
                                      [stock_path_template, index_path_template], dims_to_load, dim_to_new_name,
                                      public_dim_path_template=public_dim_path_template, use_public_data=True)
    sample_dim_data_loader = SampleDimDataLoader(user_name, project_name, "sampledimdl", universe, version_number,
                                                 required_dims=["HIGHPRICE", "LOWPRICE"], new_dims=["AVERAGE_HIGH_LOW"],
                                                 public_dim_path_template=public_dim_path_template, use_public_data=True)
    sample_dim_data_loader.do_load()
    t1 = time.process_time()
    print("", "{} seconds".format(t1 - t0))
