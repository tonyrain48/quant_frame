from quant.data_loader import AbstractRawDataLoader
from quant.alpha import AbstractAlphaEngine
from quant.definitions import DataType
from quant.data import np_nan_array

# 基本面数据整理
class FundamentalDataLoader(AbstractRawDataLoader):
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, dim_to_new_name={}):
        #self.total_assets_dim = "TOTALASSETS"
        #self.total_she_dim = "TOTALSHAREHOLDEREQUITY"
        #self.long_term_loan_dim = "LONGTERMLOAN"
        #self.end_date_dim = "ENDDATE"
        self.internal_dims = ["TOTALASSETS", "TOTALSHAREHOLDEREQUITY", "LONGTERMLOAN", "ENDDATE"]
        self.if_adjusted = "IFADJUSTED"
        self.if_merged = "IFMERGED"
        self.accounting_standards = "ACCOUNTINGSTANDARDS"
        self.enterprise_type = "ENTERPRISETYPE"
        super().__init__(user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, dim_to_new_name)

    def _load_one_date_data(self, universe, path_templates, current_date, date_data, dims):
        """
        Only one item in path_templates
        """
        if universe.stock_secu_code_number:
            file_path = path_templates[0].format(current_date.year, current_date.month, current_date.day)
            with open(file_path, "r") as f:
                self._read_one_file(f, date_data, dims)
        pass

    def _read_one_row(self, date_data, line_list, dim_index, dims):
        secu_code = line_list[0]
        for dim in dims:
            if secu_code not in date_data[dim]:
                date_data[dim][secu_code] = {}
            key = self.construct_key(line_list, dim_index)
            value = self.construct_value(line_list, dim_index)
            #print(key, value)
            date_data[dim][secu_code][key] = value
        pass

    def construct_key(self, line_list, dim_index):
        return "|".join([line_list[dim_index[self.if_adjusted]], line_list[dim_index[self.if_merged]], line_list[dim_index[self.accounting_standards]], line_list[dim_index[self.enterprise_type]]])

    def construct_value(self, line_list, dim_index):
        value = {}
        for dim in self.internal_dims:
            if dim not in dim_index:
                continue
            #print(line_list[dim_index[dim]])
            value[dim] = self.dim_definitions[dim].value.parser(line_list[dim_index[dim]])
        return value

    def _all_dim_definitions(self):
        return {
            "FUNDAMENTALDATA": DataType.Custom,
            "TOTALASSETS": DataType.Float64, "TOTALSHAREHOLDEREQUITY": DataType.Float64, "LONGTERMLOAN": DataType.Float64,
            "INFOPUBLDATE": DataType.Date, "ENDDATE": DataType.Date, "UPDATETIME": DataType.Date
        }

