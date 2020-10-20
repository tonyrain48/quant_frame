import sys
sys.path.append(r'D:\ms_intern\quant_frame\quant')
import os.path
import time
from datetime import date, timedelta
from constants import DATA_BASE_DIR, WORK_BASE_DIR
from signature import Signature, UniverseSignature
from universe import Universe


class UniverseGenerator(object):
    def __init__(self, user_name, project_name, required_data_sources, start_date, end_date, back_days, end_days):
        """
        实例化UniverseGenerator类之后，会生成universe.bin和universe_signature.bin
        -----------------------------------------------
        :param user_name: project owner
        :param project_name: name of the project. universe will be project-wise
        :param required_data_sources: a list of strings that represent data source,
            from ["Stock", "Index"]
        :param start_date: start date of the project
        :param end_date: end date of the project
        :param back_days: back days guard interval for the date dimension
        :param end_days: end days guard interval for the date dimension
        """
        self.required_data_sources = set(required_data_sources) # ["Stock", "Index"]
        self.start_date = start_date
        self.end_date = end_date
        self.back_days = back_days
        self.end_days = end_days
        self.stock_secu_code_number = {}  # key为股票代码，values为date_list中出现的天数
        self.index_secu_code_list = []    # 指数代码列表
        
        # 生成path 用于保存本次生成的universe和universe_signature文件
        signature_path = os.path.join(WORK_BASE_DIR, "{}/{}/signature/universe_signature.bin".format(user_name, project_name))
        universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
        
        # 生成当前的signature信息，并读取之前的old_signature
        universe_signature = UniverseSignature(self.start_date, self.end_date, self.back_days, self.end_days, self.required_data_sources)
        old_universe_signature = UniverseSignature.new_universe_signature_from_file(signature_path)
        
        # 查看signature是否存在 以及 新的signature信息是否改变 如果已经存在并且未改变，则不重新生成
        if os.path.exists(universe_path) and universe_signature.check(old_universe_signature):
            self.universe = Universe.new_universe_from_file(universe_path)  #读取旧的universe作为当前的universe
            return
        
        # 如果文件不存在 或者 signature 变化了 则重新生成
        # A股基本资料path
        self.share_description_path_template = os.path.join(DATA_BASE_DIR, "raw/WIND/BaseData/{}/{}/{}/ASHAREDESCRIPTION.txt")
        # 创建universe
        self._create_universe(universe_path)
        
        universe = Universe(self.start_date, self.end_date, self.back_days, self.end_days, self.date_list, self.stock_secu_code_number, self.index_secu_code_list)
        universe.save_universe(universe_path)
        universe_signature.save_signature(signature_path)
        self.universe = universe
        pass

    def _create_universe(self, universe_path):
        """
        The instrument index (ii) are created universally.
        There is a priority on making iis:
            1. Stock
            2. Index
            3. TBD
        """
        self._create_dis() # 生成date_list
        self.secu_code_sources = []
        if "Stock" in self.required_data_sources:
            self.secu_code_sources.append("Stock")
            if os.path.exists(universe_path):
                old_universe = Universe.new_universe_from_file(universe_path)
                self._create_stock_iis_from_old_universe(old_universe)
            else:
                self._create_stock_iis()
        if "Index" in self.required_data_sources:
            self.secu_code_sources.append("Index")
            self._create_index_iis()
        pass

    def _create_dis(self):
        '''
        根据start_date和end_date生成相应的date_list
        '''
        share_calendar_path = os.path.join(DATA_BASE_DIR, "raw/WIND/ASHARECALENDAR.txt")
        full_date_list = []
        true_start_date = None
        true_end_date = None
        end_days_count = 0
        print(share_calendar_path)
        with open(share_calendar_path, "r") as f:
            last_line = None
            for line_index, line in enumerate(f):
                if line_index == 0:
                    continue
                current_date = date(int(line[0:4]), int(line[4:6]), int(line[6:8]))
                full_date_list.append(current_date)
                if true_start_date is None and current_date >= self.start_date: # 找到开始日期
                    true_start_date = current_date
                if true_end_date is not None and current_date >= self.end_date: 
                    if end_days_count >= self.end_days:
                        break
                    else:
                        end_days_count = end_days_count + 1
                elif true_end_date is None and current_date >= self.end_date:   # 找到结束日期
                    if current_date == self.end_date:
                        true_end_date = current_date
                    else:
                        if last_line is None:
                            raise Exception("last_line should not be None. Please check the start date")
                        true_end_date = date(int(last_line[0:4]), int(last_line[4:6]), int(last_line[6:8]))
                        end_days_count = 1
                last_line = line
        # 切出符合要求的date_list
        start_index = full_date_list.index(true_start_date) - self.back_days
        end_index = full_date_list.index(true_end_date) + self.end_days
        self.date_list = full_date_list[start_index:end_index + 1]

    def _create_stock_iis(self):
        # stock_secu_code_number 为字典格式，key为股票代码，values为date_list中出现的天数
        for d in self.date_list:
            share_description_file_path = self.share_description_path_template.format(str(d.year), str(d.month), str(d.day))
            with open(share_description_file_path,encoding='utf-8') as f:
                for line_index, line in enumerate(f):
                    if line_index == 0:
                        continue
                    secu_code = line.strip().split("|")[0]
                    if secu_code in self.stock_secu_code_number:
                        self.stock_secu_code_number[secu_code] = self.stock_secu_code_number[secu_code] + 1
                    else:
                        self.stock_secu_code_number[secu_code] = 1

    def _create_stock_iis_from_old_universe(self, old_universe):
        self.stock_secu_code_number = dict(old_universe.stock_secu_code_number)
        date_set = set(self.date_list)
        old_date_set = set(old_universe.date_list)
        new_date_list = list(date_set - old_date_set)
        invalid_date_list = list(old_date_set - date_set)
        new_date_list.sort()
        invalid_date_list.sort()

        for d in new_date_list:
            share_description_file_path = self.share_description_path_template.format(str(d.year), str(d.month), str(d.day))
            with open(share_description_file_path,encoding='utf-8') as f:
                for line_index, line in enumerate(f):
                    if line_index == 0:
                        continue
                    secu_code = line.strip().split("|")[0]
                    if secu_code in self.stock_secu_code_number:
                        self.stock_secu_code_number[secu_code] = self.stock_secu_code_number[secu_code] + 1
                    else:
                        self.stock_secu_code_number[secu_code] = 1

        for d in invalid_date_list:
            share_description_file_path = self.share_description_path_template.format(str(d.year), str(d.month), str(d.day))
            with open(share_description_file_path,encoding='utf-8') as f:
                for line_index, line in enumerate(f):
                    if line_index == 0:
                        continue
                    secu_code = line.strip().split("|")[0]
                    if secu_code in self.stock_secu_code_number:
                        self.stock_secu_code_number[secu_code] = self.stock_secu_code_number[secu_code] - 1
                    else:
                        self.stock_secu_code_number[secu_code] = -1

    def _create_index_iis(self):
        index_description_path = os.path.join(DATA_BASE_DIR, "raw/WIND/IndexQuote/AINDEXDESCRIPTION_COOKED.txt")
        with open(index_description_path,encoding='utf-8') as f:
            for line_index, line in enumerate(f):
                if line_index == 0:
                    continue
                self.index_secu_code_list.append(line.strip().split("|")[0])
        self.index_secu_code_list.sort()


if __name__ == "__main__":
    t0 = time.process_time()
    user_name = "hqu"
    project_name = "universe_test"
    required_data_sources = ["Stock", "Index"]
    start_date = date(2018, 9, 21)
    end_date = date(2018, 9, 27)
    back_days = 1
    end_days = 0
    universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date, back_days, end_days)
    t1 = time.process_time()
    print("{} seconds".format(t1 - t0))
