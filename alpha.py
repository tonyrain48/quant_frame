from quant.data import np_nan_array


class AbstractAlphaEngine(object):
    def __init__(self, user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims=[], need_to_dump=False):
        """
        should add a param delay here. Not using default value, because we want each research to be aware of this fact.
        instrument_pool: 股票池的名称 
        """
        self.user_name = user_name
        self.project_name = project_name
        self.alpha_name = alpha_name
        self.universe = universe
        self.required_dims = required_dims
        self.instrument_pool = instrument_pool
        self.need_to_dump = need_to_dump
        self.op_list = op_list
        self._setup()
        pass

    def _setup(self):
        self.required_dims.extend(self._get_intrinsic_required_dims())
        pass

    def calculate_one_day_alpha(self, instrument_pool_data, di, data):
        raw_alpha = self.do_calculate_one_day_alpha(instrument_pool_data, di, data)
        final_alpha = raw_alpha
        for op in self.op_list:
            final_alpha = op.do_op(final_alpha, instrument_pool_data, di, data)
        return final_alpha

    def do_calculate_one_day_alpha(self, instrument_pool_data, di, data):
        """
        :param di:
        :param data: {dim: tensor_dim_data}
        :param instrument_pool_data:
        :return: the alpha data in for this date
        """
        pass

    def _get_intrinsic_required_dims(self):
        return []


class UpdatableData(object):
    def __init__(self, universe):
        self.universe = universe
        pass

    def update(self, daily_data, daily_instrument_pool_data):
        pass

    def get_data(self, ii):
        pass


class SampleAlpha1(AbstractAlphaEngine):
    def __init__(self, user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims, delay=1, need_to_dump=False):
        # some stuff you like
        super().__init__(user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims, need_to_dump)

    def do_calculate_one_day_alpha(self, instrument_pool_data, di, data):
        res = np_nan_array(shape=data["HIGHPRICE"].data[di].shape, dtype="float64")
        res[instrument_pool_data.data[di]] = data["HIGHPRICE"].data[di][instrument_pool_data.data[di]] + 0.01
        return res

    def _get_intrinsic_required_dims(self):
        return ["HIGHPRICE"]


class SampleAlpha2(AbstractAlphaEngine):
    def __init__(self, user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims, delay=1, need_to_dump=False):
        # some stuff you like
        super().__init__(user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims, need_to_dump)

    def _setup(self):
        self.required_dims = ["LOWPRICE"]

    def do_calculate_one_day_alpha(self, instrument_pool_data, di, data):
        res = np_nan_array(shape=data["LOWPRICE"].data[di].shape, dtype="float64")
        res[instrument_pool_data.data[di]] = data["LOWPRICE"].data[di][instrument_pool_data.data[di]] / 2
        return res
