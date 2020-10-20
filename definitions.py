from enum import Enum, unique
from quant.helpers import date_parser, float_parser, int_parser, str_parser, bool_parser



# 定义数据格式
@unique
class DataType(Enum):
    class Def(object):
        def __init__(self, name, is_numpy, parser):
            self.name = name
            self.is_numpy = is_numpy # 是否处理成矩阵格式
            self.parser = parser

    Float64 = Def('float64', True, float_parser)
    UInt32 = Def('uint32', True, int_parser)
    UInt64 = Def('uint64', True, int_parser)
    Int32 = Def('int32', True, int_parser)
    Int64 = Def('int64', True, int_parser)
    Bool = Def('bool_', True, bool_parser)
    Str = Def('str', False, str_parser)
    Date = Def('date', False, date_parser)
    Custom = Def('custom', False, None)
    NoneType = Def('none', False, None)
    NAME_TO_TYPE = {
        'float64': Float64,
        'uint32': UInt32,
        'uint64': UInt64,
        'int32': Int32,
        'int64': Int64,
        'bool_': Bool,
        'str': Str,
        'date': Date,
        'custom': Custom,
        'none': NoneType
    }
