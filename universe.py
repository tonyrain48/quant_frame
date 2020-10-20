import os
import os.path
import pickle


class Universe(object):

    def __init__(self, start_date, end_date, back_days, end_days, date_list, stock_secu_code_number, index_secu_code_list):
        """
        :param start_date: 
        :param end_date: 
        :param back_days: just for dataloaders to detect signature
        :param end_days: just for dataloaders to detect signature
        :param date_list: [date1, date2, ..., daten], a list of dates that represent all the trading days in this universe
        :param stock_secu_code_number: {secu_code: 出现的次数} for stockshares
        :param index_secu_code_list: [index_secu_code1, index_secu_code2, ..., index_secu_coden], a list of indexes' secu_code
        """
        self.start_date = start_date
        self.end_date = end_date
        self.back_days = back_days
        self.end_days = end_days
        self.date_list = date_list
        self.stock_secu_code_number = stock_secu_code_number
        self.index_secu_code_list = index_secu_code_list

        self.start_di = back_days
        self.end_di = len(date_list) - 1 - end_days

        self.secu_code_list = []
        stock_secu_code_list = []
        
        # secu_code_list：在universe中出现过的股票代码和指数代码
        # stock
        for k, v in stock_secu_code_number.items():
            if v > 0:
                stock_secu_code_list.append(k)
        stock_secu_code_list.sort()
        self.secu_code_list.extend(stock_secu_code_list)
        # index
        self.secu_code_list.extend(index_secu_code_list)
        
        # 生成与secu_code_list相对应的ii
        self.secu_code_to_source = {}  # key:股票代码  values:stock / index
        self.secu_code_to_index = {}   # key:股票代码  values: 索引（与secu_code_list对应）
        self.index_to_secu_code = {}   # key:索引     values: 股票代码
        for secu_code in stock_secu_code_list:
            self.secu_code_to_source[secu_code] = "Stock"
        for secu_code in index_secu_code_list:
            self.secu_code_to_source[secu_code] = "Index"
        for code_index, secu_code in enumerate(self.secu_code_list):
            self.secu_code_to_index[secu_code] = code_index
            self.index_to_secu_code[code_index] = secu_code
        
        # 生成与date_list相对应的di
        self.date_to_di = {}   # key: 日期  values:索引（与date_list对应）
        for di, current_date in enumerate(self.date_list):
            self.date_to_di[current_date] = di

    def save_universe(self, file_path):
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        with open(file_path, "wb") as f:
            self._write_universe_to_file(f)

    def _write_universe_to_file(self, f):
        pickle.dump(self.start_date, f)
        pickle.dump(self.end_date, f)
        pickle.dump(self.back_days, f)
        pickle.dump(self.end_days, f)
        pickle.dump(self.start_di, f)
        pickle.dump(self.end_di, f)
        pickle.dump(self.date_list, f)
        pickle.dump(self.stock_secu_code_number, f)
        pickle.dump(self.index_secu_code_list, f)
        pickle.dump(self.secu_code_list, f)
        pickle.dump(self.secu_code_to_source, f)
        pickle.dump(self.secu_code_to_index, f)
        pickle.dump(self.index_to_secu_code, f)
        pickle.dump(self.date_to_di, f)

    def load_universe(self, file_path):
        if not os.path.exists(file_path):
            return
        with open(file_path, "rb") as f:
            self._read_universe_from_file(f)

    def _read_universe_from_file(self, f):
        self.start_date = pickle.load(f)
        self.end_date = pickle.load(f)
        self.back_days = pickle.load(f)
        self.end_days = pickle.load(f)
        self.start_di = pickle.load(f)
        self.end_di = pickle.load(f)
        self.date_list = pickle.load(f)
        self.stock_secu_code_number = pickle.load(f)
        self.index_secu_code_list = pickle.load(f)
        self.secu_code_list = pickle.load(f)
        self.secu_code_to_source = pickle.load(f)
        self.secu_code_to_index = pickle.load(f)
        self.index_to_secu_code = pickle.load(f)
        self.date_to_di = pickle.load(f)

    @staticmethod
    def new_universe_from_file(file_path):
        universe = Universe(None, None, 0, 0, [], {}, [])
        universe.load_universe(file_path)
        return universe
