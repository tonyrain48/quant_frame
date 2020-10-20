# 现金流量表数据整理

from quant.data_loader import AbstractRawDataLoader
from quant.definitions import DataType
from quant.signature import Signature
import pickle


class MoneyFlowDataLoader(AbstractRawDataLoader):
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
        pass

    def _all_dim_definitions(self):
        return {
                "S_INFO_WINDCODE": DataType.Str, "TRADE_DT": DataType.Str, "BUY_VALUE_EXLARGE_ORDER": DataType.Float64, 
                "SELL_VALUE_EXLARGE_ORDER": DataType.Float64, "BUY_VALUE_LARGE_ORDER": DataType.Float64, 
                "SELL_VALUE_LARGE_ORDER": DataType.Float64, "BUY_VALUE_MED_ORDER": DataType.Float64, 
                "SELL_VALUE_MED_ORDER": DataType.Float64, "BUY_VALUE_SMALL_ORDER": DataType.Float64, 
                "SELL_VALUE_SMALL_ORDER": DataType.Float64, "BUY_VOLUME_EXLARGE_ORDER": DataType.Float64, 
                "SELL_VOLUME_EXLARGE_ORDER": DataType.Float64, "BUY_VOLUME_LARGE_ORDER": DataType.Float64,
                "SELL_VOLUME_LARGE_ORDER": DataType.Float64, "BUY_VOLUME_MED_ORDER": DataType.Float64, 
                "SELL_VOLUME_MED_ORDER": DataType.Float64, "BUY_VOLUME_SMALL_ORDER": DataType.Float64, 
                "SELL_VOLUME_SMALL_ORDER": DataType.Float64, "TRADES_COUNT": DataType.Float64, "BUY_TRADES_EXLARGE_ORDER": DataType.Float64, 
                "SELL_TRADES_EXLARGE_ORDER": DataType.Float64, "BUY_TRADES_LARGE_ORDER": DataType.Float64, "SELL_TRADES_LARGE_ORDER": DataType.Float64, 
                "BUY_TRADES_MED_ORDER": DataType.Float64, "SELL_TRADES_MED_ORDER": DataType.Float64, "BUY_TRADES_SMALL_ORDER": DataType.Float64, 
                "SELL_TRADES_SMALL_ORDER": DataType.Float64, "VOLUME_DIFF_SMALL_TRADER": DataType.Float64, "VOLUME_DIFF_SMALL_TRADER_ACT": DataType.Float64,
                "VOLUME_DIFF_MED_TRADER": DataType.Float64, "VOLUME_DIFF_MED_TRADER_ACT": DataType.Float64, "VOLUME_DIFF_LARGE_TRADER": DataType.Float64, 
                "VOLUME_DIFF_LARGE_TRADER_ACT": DataType.Float64, "VOLUME_DIFF_INSTITUTE": DataType.Float64, "VOLUME_DIFF_INSTITUTE_ACT": DataType.Float64,
                "VALUE_DIFF_SMALL_TRADER": DataType.Float64, "VALUE_DIFF_SMALL_TRADER_ACT": DataType.Float64, "VALUE_DIFF_MED_TRADER": DataType.Float64,
                "VALUE_DIFF_MED_TRADER_ACT": DataType.Float64, "VALUE_DIFF_LARGE_TRADER": DataType.Float64, "VALUE_DIFF_LARGE_TRADER_ACT": DataType.Float64, 
                "VALUE_DIFF_INSTITUTE": DataType.Float64, "VALUE_DIFF_INSTITUTE_ACT": DataType.Float64, "S_MFD_INFLOWVOLUME": DataType.Float64,
                "NET_INFLOW_RATE_VOLUME": DataType.Float64, "S_MFD_INFLOW_OPENVOLUME": DataType.Float64, "OPEN_NET_INFLOW_RATE_VOLUME": DataType.Float64,
                "S_MFD_INFLOW_CLOSEVOLUME": DataType.Float64, "CLOSE_NET_INFLOW_RATE_VOLUME": DataType.Float64, "S_MFD_INFLOW": DataType.Float64, 
                "NET_INFLOW_RATE_VALUE": DataType.Float64, "S_MFD_INFLOW_OPEN": DataType.Float64, "OPEN_NET_INFLOW_RATE_VALUE": DataType.Float64, 
                "S_MFD_INFLOW_CLOSE": DataType.Float64, "CLOSE_NET_INFLOW_RATE_VALUE": DataType.Float64, "TOT_VOLUME_BID": DataType.Float64, 
                "TOT_VOLUME_ASK": DataType.Float64, "MONEYFLOW_PCT_VOLUME": DataType.Float64, "OPEN_MONEYFLOW_PCT_VOLUME": DataType.Float64, 
                "CLOSE_MONEYFLOW_PCT_VOLUME": DataType.Float64, "MONEYFLOW_PCT_VALUE": DataType.Float64, "OPEN_MONEYFLOW_PCT_VALUE": DataType.Float64,
                "CLOSE_MONEYFLOW_PCT_VALUE": DataType.Float64, "S_MFD_INFLOWVOLUME_LARGE_ORDER": DataType.Float64, "NET_INFLOW_RATE_VOLUME_L": DataType.Float64,
                "S_MFD_INFLOW_LARGE_ORDER": DataType.Float64, "NET_INFLOW_RATE_VALUE_L": DataType.Float64, "MONEYFLOW_PCT_VOLUME_L": DataType.Float64, 
                "MONEYFLOW_PCT_VALUE_L": DataType.Float64, "S_MFD_INFLOW_OPENVOLUME_L": DataType.Float64, "OPEN_NET_INFLOW_RATE_VOLUME_L": DataType.Float64,
                "S_MFD_INFLOW_OPEN_LARGE_ORDER": DataType.Float64, "OPEN_NET_INFLOW_RATE_VALUE_L": DataType.Float64, "OPEN_MONEYFLOW_PCT_VOLUME_L": DataType.Float64, 
                "OPEN_MONEYFLOW_PCT_VALUE_L": DataType.Float64, "S_MFD_INFLOW_CLOSEVOLUME_L": DataType.Float64, "CLOSE_NET_INFLOW_RATE_VOLUME_L": DataType.Float64, 
                "S_MFD_INFLOW_CLOSE_LARGE_ORDER": DataType.Float64, "CLOSE_NET_INFLOW_RATE_VALU_L": DataType.Float64, "CLOSE_MONEYFLOW_PCT_VOLUME_L": DataType.Float64,
                "CLOSE_MONEYFLOW_PCT_VALUE_L": DataType.Float64, "BUY_VALUE_EXLARGE_ORDER_ACT": DataType.Float64, "SELL_VALUE_EXLARGE_ORDER_ACT": DataType.Float64,
                "BUY_VALUE_LARGE_ORDER_ACT": DataType.Float64, "SELL_VALUE_LARGE_ORDER_ACT": DataType.Float64, "BUY_VALUE_MED_ORDER_ACT": DataType.Float64,
                "SELL_VALUE_MED_ORDER_ACT": DataType.Float64, "BUY_VALUE_SMALL_ORDER_ACT": DataType.Float64, "SELL_VALUE_SMALL_ORDER_ACT": DataType.Float64,
                "BUY_VOLUME_EXLARGE_ORDER_ACT": DataType.Float64, "SELL_VOLUME_EXLARGE_ORDER_ACT": DataType.Float64, "BUY_VOLUME_LARGE_ORDER_ACT": DataType.Float64,
                "SELL_VOLUME_LARGE_ORDER_ACT": DataType.Float64, "BUY_VOLUME_MED_ORDER_ACT": DataType.Float64, "SELL_VOLUME_MED_ORDER_ACT": DataType.Float64, 
                "BUY_VOLUME_SMALL_ORDER_ACT": DataType.Float64, "SELL_VOLUME_SMALL_ORDER_ACT": DataType.Float64, "OPDATE": DataType.Str, "OPMODE": DataType.Str
                }
        
class MoneyFlowDataLoaderSignature(Signature):
    def __init__(self, start_date, end_date, back_days, end_days, stock_path_template, required_dims):
        super().__init__(start_date, end_date, back_days, end_days)
        self.stock_path_template = stock_path_template
        self.required_dims = set(required_dims)

    def check(self, signature):
        return super().check(signature) \
               and self.stock_path_template == signature.stock_path_template \
               and self.required_dims == signature.required_dims

    def _write_signature_to_file(self, f):
        super()._write_signature_to_file(f)
        pickle.dump(self.stock_path_template, f)
        pickle.dump(self.required_dims, f)

    def _read_signature_from_file(self, f):
        super()._read_signature_from_file(f)
        self.stock_path_template = pickle.load(f)
        self.required_dims = pickle.load(f)

    @staticmethod
    def new_base_data_signature_from_file(file_path):
        signature = MoneyFlowDataLoaderSignature(None, None, 0, 0, "", "", [])
        signature.load_signature(file_path)
        return signature


if __name__ == "__main__":
    import time
    import os.path
    from datetime import date
    from quant.universe_generator import UniverseGenerator
    from quant.universe import Universe
    from quant.constants import DATA_BASE_DIR, WORK_BASE_DIR, COLUMN_DELIMITER, DATE_FORMAT_STRING
    from quant.data import ProjectData, DimData, HuianData
    from quant.data_loader import BaseDataLoader, SampleDimDataLoader, SampleInstrumentPoolDataLoader

    t0 = time.process_time()
    
    # Generate universe
    user_name = "ypan"
    project_name = "moneyflow_test"
    required_data_sources = ["Stock", "Index"]
    start_date = date(2006, 12, 6)
    end_date = date(2019, 4, 24)
    back_days = 0
    end_days = 0
    
    # Generate universe
    universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date, back_days, end_days)
    universe_path = os.path.join(WORK_BASE_DIR, '{}/{}/universe/universe.bin'.format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)
    
    # Get and update version number
    version_number = ProjectData.read_version_number(user_name, project_name)
    ProjectData.save_version_number(user_name, project_name, version_number + 1)
    
    # BaseData setup
    stock_path_template = os.path.join(DATA_BASE_DIR, 'raw/WIND/MoneyFlow/{}/{}/{}/ASHAREMONEYFLOW.txt')
    dims_to_load = ['BUY_VALUE_EXLARGE_ORDER','SELL_VALUE_EXLARGE_ORDER']
    
    moneyflow_data_loader = MoneyFlowDataLoader(user_name, project_name, '', universe, version_number,[stock_path_template], dims_to_load)
    
    t1 = time.process_time()
    print("", "{} seconds".format(t1 - t0))
