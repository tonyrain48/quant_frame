from quant.data_loader import AbstractDimDataLoader
from quant.data import DataType

# 股票池整理
class IndexPoolDataLoader(AbstractDimDataLoader):
    def _dim_definitions(self):
        """
        :return: {dim_name: data_type}, which describe the dim definition that describe the dim to load
        """
        return {"INDEX_POOL": DataType.Bool}

    def _calculate_one_day(self, required_dim_datas, dim_datas, di, current_date):
        """
        Fill dim_datas for current_date. Need to be implemented by sub-class
        :param required_dim_datas: {dim_name: dim_data} that are the required dimension
        :param dim_datas: {dim_name: dim_data} that are the data to be loaded
        """
        date_dim_data = {}
        for secu_code in self.universe.secu_code_list:
            index_price = required_dim_datas["S_DQ_CLOSE"].data[di][1][secu_code] if secu_code in required_dim_datas["S_DQ_CLOSE"].data[di][1] else None
            date_dim_data[secu_code] = True if (index_price is not None) else False
        dim_datas["INDEX_POOL"].append_date_data(current_date, date_dim_data)