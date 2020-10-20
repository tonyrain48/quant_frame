
from quant.definitions import DataType
from quant.data_loader import AbstractDimDataLoader

class HardPoolDataLoader(AbstractDimDataLoader):
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, required_dims=[], new_dims = ["HARDPOOL"], required_list = []):
        super().__init__(user_name, project_name, data_loader_name, universe, version_number, required_dims, new_dims)

        self.required_list = required_list

    def _dim_definitions(self):
        return {self.new_dims[0]: DataType.Bool}

    def _calculate_one_day(self, required_list, dim_datas, di, current_date):
        date_dim_data = {}
        for ii, secu_code in enumerate(self.universe.secu_code_list):
            if secu_code in self.required_list:
                date_dim_data[secu_code] = True
            else:
                date_dim_data[secu_code] = False
        dim_datas[self.new_dims[0]].append_date_data(current_date, date_dim_data)

