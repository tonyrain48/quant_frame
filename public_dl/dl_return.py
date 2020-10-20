# 收益率数据整理
from quant.definitions import DataType
from quant.data_loader import AbstractDimDataLoader
import math


PRECISION = 1E-10
class ReturnDimDataLoader (AbstractDimDataLoader):
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, required_dims=["HIGHPRICE", "LOWPRICE", "CLOSEPRICE", "ADJ_FACTOR"], new_dims=["RETURN", "UP_LIMIT", "DOWN_LIMIT"]):
        super().__init__(user_name, project_name, data_loader_name, universe, version_number, required_dims, new_dims)

    def _dim_definitions(self):
        return{"RETURN": DataType.Float64, "UP_LIMIT": DataType.Bool, "DOWN_LIMIT": DataType.Bool}

    def _calculate_one_day (self, required_dim_datas, dim_datas, di, current_date):
        return_dim_data = {}
        up_limit_dim_data = {}
        down_limit_dim_data = {}
        current_date = self.universe.date_list[di]
        number_to_verify = 10
        nnn = 0
        if di > 0:
            for secu_code in self.universe.secu_code_list:
                close = required_dim_datas["CLOSEPRICE"].data[di][1][secu_code] if secu_code in required_dim_datas["CLOSEPRICE"].data[di][1] and \
                      secu_code in required_dim_datas["CLOSEPRICE"].data[di-1][1] else None
                if not close:
                    return_dim_data[secu_code] = 0
                    continue
                prev_close = required_dim_datas["CLOSEPRICE"].data[di-1][1][secu_code] if secu_code in required_dim_datas["CLOSEPRICE"].data[di-1][1] and \
                           secu_code in required_dim_datas["CLOSEPRICE"].data[di][1] else None
                if not prev_close:
                    return_dim_data[secu_code] = 0
                    up_limit_dim_data[secu_code] = False
                    down_limit_dim_data[secu_code] = False
                    continue
                return_rate = close/prev_close*required_dim_datas["ADJ_FACTOR"].data[di][1][secu_code] - 1
                return_dim_data[secu_code] = return_rate
                high_price = required_dim_datas["HIGHPRICE"].data[di][1][secu_code]
                low_price = required_dim_datas["LOWPRICE"].data[di][1][secu_code]
                up_limit_dim_data[secu_code] = (return_rate > 0.09  and math.isclose(high_price, low_price, rel_tol=PRECISION)) if high_price is not None and low_price is not None else False
                down_limit_dim_data[secu_code] = (return_rate < -0.09  and math.isclose(high_price, low_price, rel_tol=PRECISION)) if high_price is not None and low_price is not None else False
        else:
            for secu_code in self.universe.secu_code_list:
                return_dim_data[secu_code] = 0
                up_limit_dim_data[secu_code] = False
                down_limit_dim_data[secu_code] = False
        dim_datas["RETURN"].append_date_data(current_date, return_dim_data)
        dim_datas["UP_LIMIT"].append_date_data(current_date, up_limit_dim_data)
        dim_datas["DOWN_LIMIT"].append_date_data(current_date, down_limit_dim_data)

