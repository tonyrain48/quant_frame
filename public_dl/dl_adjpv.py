
import sys
import numpy as np
from collections import deque
from quant.alpha import UpdatableData
from quant.data import np_nan_array
from quant.alpha import AbstractAlphaEngine


class AdjPriceVolume(UpdatableData):
    def __init__(self, universe, required_dims, window_days):
        super().__init__(universe)
        self.data = {}
        self.adj_di = -1
        self.universe = universe
        self.window_days = window_days
        self.dims_price = list(set(required_dims).intersection(set(["OPENPRICE", "CLOSEPRICE", "HIGHPRICE", "LOWPRICE"])))
        self.dims_volume = list(set(required_dims).intersection(set(["TURNOVERVOLUME", "FREEFLOATSHARES"])))
        self.dims = self.dims_price + self.dims_volume
        self.index = 0

    def update(self, di, data):
        if di <= self.window_days:
            print("adjpv: di less than window_days")
            sys.exit(0)

        if self.data:
            if (di - self.adj_di) == 1:
                self.data_update(di, data)
                self.adj_di = di
            else:
                print(str(di) + " : di update wrong")
                sys.exit(0)
        else:
            if self.adj_di == -1:
                self.data_init(di, data)
                self.adj_di = di
            else:
                print(str(di) + " : di init wrong")
                sys.exit(0)
  
    def data_init(self, di, data):
        for dim in self.dims:
            dim_deque = deque(maxlen=self.window_days + 1)
            for i in range(self.window_days + 1):
                dim_deque.append(np_nan_array(shape=data["CLOSEPRICE"].data[di].shape, dtype="float64")) 
            self.data["ADJ_" + dim] = dim_deque
        for i in range(self.window_days + 1):
            data_di = di - self.window_days + i
            self.data_update(data_di, data) 
    
    def data_update(self, di, data):
        for dim in self.dims:
            self.data["ADJ_" + dim].append(data[dim].data[di].copy())
        adj_factor = data["ADJ_FACTOR"].data[di]
        #adj_valid = [i for i,fi in enumerate(adj_factor) if all([fi == fi, np.abs(fi - 1.0) >= 1.0E-10])]
        adj_valid = np.argwhere(np.multiply(~np.isnan(adj_factor), np.abs(adj_factor - 1.0) >= 1.0E-10))
        adj_valid = adj_valid.reshape(len(adj_valid))
        self.index = self.index + len(adj_valid)
        for ii in adj_valid:
            for j in range(self.window_days):
                for dim in self.dims_price:
                    self.data["ADJ_" + dim][j][ii] = self.data["ADJ_" + dim][j][ii] / adj_factor[ii]
                for dim in self.dims_volume:
                    self.data["ADJ_" + dim][j][ii] = self.data["ADJ_" + dim][j][ii] * adj_factor[ii]  
       
    def get(self, dim, di):
        no_deque = self.window_days - (self.adj_di - di) 
        if (no_deque < 0) or (no_deque > self.window_days):
            print("")
            sys.exit(0)
        else:
            return self.data[dim][no_deque] 

class AlphaNDR(AbstractAlphaEngine):
    def __init__(self, user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims, ndr, delay,  need_to_dump=False):
        super().__init__(user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims, need_to_dump)
        self.ndr = ndr
        self.delay = delay
        self.adjpv = AdjPriceVolume(self.universe, self.required_dims, 5)
    
    def do_calculate_one_day_alpha(self, instrument_pool_data, di, data):
        self.adjpv.update(di - self.delay, data)
        close_di_delay = self.adjpv.get("ADJ_CLOSEPRICE", di - self.delay)
        close_di_ndr_delay = self.adjpv.get("ADJ_CLOSEPRICE", di - self.delay - self.ndr)
        valid = instrument_pool_data.data[di - self.delay]
        close = data["CLOSEPRICE"].data
        res = np_nan_array(shape=close[di].shape, dtype="float64")
        res[valid] = 1.0 - close_di_delay[valid] / close_di_ndr_delay[valid]
        return res
    
    def _get_intrinsic_required_dims(self):
        return ["CLOSEPRICE", "ADJ_FACTOR"]
            
