# OP  class
class Operation(object):
    def __init__(self, required_dims=[]):
        self.required_dims = required_dims
        self._setup()
        pass

    def do_op(self, daily_operation_data, instrument_pool_data, di, data):
        pass

    def _setup(self):
        self.required_dims.extend(self._get_intrinsic_required_dims())
        pass

    def _get_intrinsic_required_dims(self):
        return []


class SampleNewOp(Operation):
    def __init__(self, multiplier, required_dims=[]):
        self.multiplier = multiplier
        super().__init__(required_dims)
        pass

    def do_op(self, daily_alpha_data, instrument_pool_data, di, data):
        return daily_alpha_data * self.multiplier

