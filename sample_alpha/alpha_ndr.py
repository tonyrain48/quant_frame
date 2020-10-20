from quant.data import np_nan_array
from quant.alpha import AbstractAlphaEngine

class AlphaNDR(AbstractAlphaEngine):
    def __init__(self, user_name, project_name, alpha_name, universe, instrument_pool, days, op_list=[], required_dims=[] , need_to_dump = False, delay = 1):
        super().__init__(user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims, need_to_dump) 
        self.days = days
        self.delay= delay
        pass

    def _get_intrinsic_required_dims(self):
        return ["CLOSEPRICE", "CUM_ADJ_FACTOR"]

    def do_calculate_one_day_alpha(self, instrument_pool_data, di, data):
        delay = self.delay
        valid = instrument_pool_data.data[di-delay]
        alpha = np_nan_array(shape=valid.shape, dtype="float64")

        close = data["CLOSEPRICE"].data
        cumadjfactor = data["CUM_ADJ_FACTOR"].data

        alpha[valid] = 1.0 - close[di-delay][valid] / close[di-delay-self.days][valid] \
		* cumadjfactor[di-delay][valid] / cumadjfactor[di-delay-self.days][valid]

        return alpha