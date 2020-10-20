

from quant.definitions import DataType
from quant.data_loader import AbstractDimDataLoader
from quant.helpers import Logger
from quant.signature import Signature
from quant.helpers import CMD
import os
from quant.data import DimData


class AdjFactorDimDataLoader(AbstractDimDataLoader):
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, required_dims=["CLOSEPRICE","SPLIT","ACTUALPLARATIO","DIVIDEND","PLAPRICE"], new_dims=["ADJ_FACTOR","CUM_ADJ_FACTOR"]):
        super().__init__(user_name,project_name, data_loader_name, universe,version_number,required_dims,new_dims)

    def _dim_definitions(self):
        return{"ADJ_FACTOR": DataType.Float64,"CUM_ADJ_FACTOR":DataType. Float64}

    def _calculate_one_day (self, required_dim_datas, dim_datas, di, current_date):
        date_dim_data_1 = {}
        date_dim_data_2 = {}
        if di > 0:
            for secu_code in self.universe.secu_code_list:
                prev_close_price=required_dim_datas["CLOSEPRICE"].data[di-1][1][secu_code] if secu_code in required_dim_datas["CLOSEPRICE"].data[di][1] and  \
                                                                                              secu_code in required_dim_datas["CLOSEPRICE"].data[di-1][1] else 1
                if not prev_close_price:
                    prev_close_price=1
                dividend=required_dim_datas["DIVIDEND"].data[di][1][secu_code] if secu_code in required_dim_datas["DIVIDEND"].data[di][1] else 0
                split=required_dim_datas["SPLIT"].data[di][1][secu_code] if secu_code in required_dim_datas["SPLIT"].data[di][1] else 1
                plaratio=required_dim_datas["ACTUALPLARATIO"].data[di][1][secu_code] if secu_code in required_dim_datas["ACTUALPLARATIO"].data[di][1] else 0
                plaprice=required_dim_datas["PLAPRICE"].data[di][1][secu_code] if secu_code in required_dim_datas["PLAPRICE"].data[di][1] else 0
                date_dim_data_1[secu_code] = ((split + plaratio) * prev_close_price) / (prev_close_price - dividend + plaratio * plaprice)
                date_dim_data_2[secu_code] = dim_datas["CUM_ADJ_FACTOR"].data[di - 1][1][secu_code] * ((split + plaratio) * prev_close_price) / (prev_close_price - dividend + plaratio * plaprice)
        else:
            for secu_code in self.universe.secu_code_list:
                date_dim_data_1[secu_code]=1
                date_dim_data_2[secu_code]=1
        dim_datas["ADJ_FACTOR"].append_date_data(current_date, date_dim_data_1)
        dim_datas["CUM_ADJ_FACTOR"].append_date_data(current_date,date_dim_data_2)

    def do_load(self):
        signature_path = self._signature_path_name(self.user_name, self.project_name, self.data_loader_name)
        universe_path = self._universe_path_name(self.user_name, self.project_name)
        signature = Signature(self.universe.start_date, self.universe.end_date, self.universe.back_days, self.universe.end_days)
        old_signature = Signature.new_signature_from_file(signature_path)
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

