from quant.constants import LOG_LEVEL, LOG_LEVEL_TO_PREFIX
from datetime import datetime, date
import numpy as np
import pandas as pd

DATE_HOME_STRING_FORMAT = "%Y-%m-%d %H:%M:%S"

def debug_print(a, header=''):
    apos=[x for x in a if x > 0]
    aneg=[x for x in a if x < 0]
    if header:
        print(header)
    print('type: {}, len: {}'.format(type(a),len(a)))
    print('count: {}, sum: {}, \ncount_pos: {}, sum_pos: {}\ncount_neg: {}, sum_neg: {}'.format(len(a),np.nansum(np.array(a)),len(apos),sum(apos),len(aneg),sum(aneg)))
    return


def nan_rank(x):
    res = np.array(pd.DataFrame(x).rank(ascending=True)[0].tolist())
    return res


def nan_cov(x, y):
    if len(x) != len(y):
        print('Dim inconsistency found while calculating nan_cov !!!')
        raise ValueError
    cov_valid = np.all([~np.isnan(x),~np.isnan(y)],axis=0)
    z = np.array([x[cov_valid], y[cov_valid]])
    z[0] -= np.mean(z[0])#one demean is enough
    cov = np.dot(z[0],z[1])/np.sum(cov_valid)
    corr = cov/np.std(z[0])/np.std(z[1])
    beta = cov/np.var(z[0])
    return cov,corr,beta
#    return {'cov':cov,'corr':corr,'beta':beta}


def nan_neutral(x, y, rank_first=False):
    # 返回 y对x回归的残差
    if rank_first:
        x = nan_rank(x)
        y = nan_rank(y)
    cov,corr,beta = nan_cov(x,y)
#    corr = nan_cov(x,y)['corr']
#    beta = nan_cov(x,y)['beta']
    res = y - beta * x
#    print('corr = {}, beta = {}'.format(corr,beta))
    return res


def parse_dim_index(line_list):
    res = {}
    for dim_index, dim_name in enumerate(line_list):
        res[dim_name] = dim_index
    return res


def date_parser(item):
    if item == "None":
        return None
    return datetime.strptime(item, DATE_HOME_STRING_FORMAT).date()


def float_parser(item):
    if item == "None":
        return None
    return float(item)


def int_parser(item):
    if item == "None":
        return None
    return int(item)


def bool_parser(item):
    if item == "None":
        return None
    return bool(item)


def str_parser(item):
    if item == "None":
        return None
    return item


def transform_one_hot(labels):
    '''
    只支持labels为连续的数字
    '''
    n_labels = np.max(labels) + 1
    one_hot = np.eye(n_labels)[labels]
    return one_hot


class Logger(object):
    @staticmethod
    def log(level, s):
        if LOG_LEVEL >= level:
            print(LOG_LEVEL_TO_PREFIX[level] + s)

    @staticmethod
    def info(prefix, s):
        Logger.log(1, prefix + " " + s)

    @staticmethod
    def debug(prefix, s):
        Logger.log(2, prefix + " " + s)

    @staticmethod
    def warn(prefix, s):
        Logger.log(3, prefix + " " + s)

class CMD(object):
    @staticmethod
    def proceedWhenY(prefix, hint):
        while True:
            user_input = input(prefix + hint + " Y/N: ")
            if user_input in ['Y', 'y']:
                return True
            if user_input in ['N', 'n']:
                return False
