import os
import pickle


class Signature(object):
    """
    The overall signature would be (dis + iis + data).
    dis + iis is covered by the universe signature
    data is covered by the data's version number
    """
    
    def __init__(self, start_date, end_date, back_days, end_days):
        self.start_date = start_date
        self.end_date = end_date
        self.back_days = back_days
        self.end_days = end_days
    
    def check_date(self, other_signature):
        return self.start_date == other_signature.start_date \
               and self.end_date == other_signature.end_date \
               and self.back_days == other_signature.back_days \
               and self.end_days == other_signature.end_days

    def check_outside_date(self, other_signature):
        return True

    def check(self, other_signature):
        '''
        检查日期点是否变化，如果所有日期都没变，则返回True 否则返回False
        '''
        return self.start_date == other_signature.start_date \
               and self.end_date == other_signature.end_date \
               and self.back_days == other_signature.back_days \
               and self.end_days == other_signature.end_days

    def save_signature(self, file_path):
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        with open(file_path, "wb") as f:
            self._write_signature_to_file(f)

    def _write_signature_to_file(self, f):
        pickle.dump(self.start_date, f)
        pickle.dump(self.end_date, f)
        pickle.dump(self.back_days, f)
        pickle.dump(self.end_days, f)

    def load_signature(self, file_path):
        if not os.path.exists(file_path):
            return
        with open(file_path, "rb") as f:
            self._read_signature_from_file(f)

    def _read_signature_from_file(self, f):
        self.start_date = pickle.load(f)
        self.end_date = pickle.load(f)
        self.back_days = pickle.load(f)
        self.end_days = pickle.load(f)

    @staticmethod
    def new_signature_from_file(file_path):
        signature = Signature(None, None, 0, 0)
        signature.load_signature(file_path)
        return signature


class UniverseSignature(Signature):
    '''
    UnierseSignature: dict
    values: start_date
            end_date
            back_Days
            end_days
    '''
    def __init__(self, start_date, end_date, back_days, end_days, secu_code_sources):
        super().__init__(start_date, end_date, back_days, end_days)
        self.secu_code_sources = set(secu_code_sources)
        #   ["Stock", "Index"]

    def check(self, signature):
        return super().check(signature) \
               and self.secu_code_sources == signature.secu_code_sources

    def _write_signature_to_file(self, f):
        super()._write_signature_to_file(f)
        pickle.dump(self.secu_code_sources, f)

    def _read_signature_from_file(self, f):
        super()._read_signature_from_file(f)
        self.secu_code_sources = pickle.load(f)

    @staticmethod
    def new_universe_signature_from_file(file_path):
        # 新创建一个signature实例
        signature = UniverseSignature(None, None, 0, 0, [])
        # 加载原来的signature信息
        signature.load_signature(file_path)
        return signature


class DimDataLoaderSignature(Signature):
    def __init__(self, start_date, end_date, back_days, end_days, required_dim_to_path_template):
        super().__init__(start_date, end_date, back_days, end_days)
        self.required_dim_to_path_template = required_dim_to_path_template

    def check(self, signature):
        return super().check(signature) \
               and self.required_dim_to_path_template == signature.required_dim_to_path_template

    def check_outside_date(self, other_signature):
        return super().check_outside_date(other_signature) \
               and self.required_dim_to_path_template == other_signature.required_dim_to_path_template

    def _write_signature_to_file(self, f):
        super()._write_signature_to_file(f)
        pickle.dump(self.required_dim_to_path_template, f)

    def _read_signature_from_file(self, f):
        super()._read_signature_from_file(f)
        self.required_dim_to_path_template = pickle.load(f)

    @staticmethod
    def new_dim_data_signature_from_file(file_path):
        signature = DimDataLoaderSignature(None, None, 0, 0, {})
        signature.load_signature(file_path)
        return signature


class RawDataLoaderSignature(Signature):
    '''
    RawDataLoaderSignature: dict
    
    
    '''
    
    
    def __init__(self, start_date, end_date, back_days, end_days, path_templates, required_dims, dim_to_new_name):
        super().__init__(start_date, end_date, back_days, end_days)
        self.path_templates = set(path_templates)
        self.required_dims = set(required_dims)
        self.dim_to_new_name = dim_to_new_name  # {}

    def check(self, signature):
        return super().check(signature) \
               and self.path_templates == signature.path_templates \
               and self.required_dims == signature.required_dims \
               and self.dim_to_new_name == signature.dim_to_new_name

    def check_outside_date(self, other_signature):
        # 检查时间范围是否超过原来的范围
        return super().check_outside_date(other_signature) \
               and self.path_templates == other_signature.path_templates \
               and self.required_dims == other_signature.required_dims \
               and self.dim_to_new_name == other_signature.dim_to_new_name

    def _write_signature_to_file(self, f):
        super()._write_signature_to_file(f)
        pickle.dump(self.path_templates, f)
        pickle.dump(self.required_dims, f)
        pickle.dump(self.dim_to_new_name, f)

    def _read_signature_from_file(self, f):
        super()._read_signature_from_file(f)
        self.path_templates = pickle.load(f)
        self.required_dims = pickle.load(f)
        self.dim_to_new_name = pickle.load(f)

    @staticmethod
    def new_raw_data_signature_from_file(file_path):
        signature = RawDataLoaderSignature(None, None, 0, 0, [], [], {})
        signature.load_signature(file_path)
        return signature


class BaseDataLoaderSignature(Signature):
    def __init__(self, start_date, end_date, back_days, end_days, stock_path_template, index_path_template, required_dims):
        super().__init__(start_date, end_date, back_days, end_days)
        self.stock_path_template = stock_path_template
        self.index_path_template = index_path_template
        self.required_dims = set(required_dims)

    def check(self, signature):
        return super().check(signature) \
               and self.stock_path_template == signature.stock_path_template \
               and self.index_path_template == signature.index_path_template \
               and self.required_dims == signature.required_dims

    def _write_signature_to_file(self, f):
        super()._write_signature_to_file(f)
        pickle.dump(self.stock_path_template, f)
        pickle.dump(self.index_path_template, f)
        pickle.dump(self.required_dims, f)

    def _read_signature_from_file(self, f):
        super()._read_signature_from_file(f)
        self.stock_path_template = pickle.load(f)
        self.index_path_template = pickle.load(f)
        self.required_dims = pickle.load(f)

    @staticmethod
    def new_base_data_signature_from_file(file_path):
        signature = BaseDataLoaderSignature(None, None, 0, 0, "", "", [])
        signature.load_signature(file_path)
        return signature


class RawWindDataProcessSignature(Signature):
    '''
    RawWindDataProcessSignature: dict
    处理完之后的数据名称  done_data_names []
    处理完成之后的数据的进程  done_files {data1:[[历史数据地址],[更新数据地址]],data2:[...] ......}
    '''
    
    def __init__(self, start_date, path_templates, raw_data_to_load, done_files):
        self.start_date = start_date
        self.path_templates = path_templates
        self.raw_data_to_load = set(raw_data_to_load)
        self.done_files = done_files  # {}

    def check(self, signature):
        return  self.path_templates == signature.path_templates \
               and self.raw_data_to_load == signature.raw_data_to_load \
               and self.done_files == signature.done_files
               
    def check_update(self, other_signature):
            
        update_data_path_dict = {}
        for data_name,path_list in self.done_files.items():
            history_path = path_list[0]
            update_path = path_list[1]
            
            if other_signature.done_files is None:
                update_data_path_dict[data_name] = [history_path, update_path]
                
            # 如果有新数据 则全部更新
            elif data_name not in other_signature.raw_data_to_load:
                need_load_history_path = history_path
                need_load_update_path = update_path
                update_data_path_dict[data_name] = [need_load_history_path, need_load_update_path]
            # 如果更新部分，则提取更新部分的path
            else:
                need_load_history_path = list(set(history_path).difference(set(other_signature.done_files[data_name][0])))
                need_load_update_path = list(set(update_path).difference(set(other_signature.done_files[data_name][1])))
                if need_load_history_path and need_load_update_path:
                    update_data_path_dict[data_name] = [need_load_history_path, need_load_update_path]
                else:
                    return None
        return update_data_path_dict


    def _write_signature_to_file(self, f):
        pickle.dump(self.start_date, f)
        pickle.dump(self.path_templates, f)
        pickle.dump(self.raw_data_to_load, f)
        pickle.dump(self.done_files, f)

    def _read_signature_from_file(self, f):
        self.start_date = pickle.load(f)
        self.path_templates = pickle.load(f)
        self.raw_data_to_load = pickle.load(f)
        self.done_files = pickle.load(f)
        
    @staticmethod
    def new_raw_data_signature_from_file(file_path):
        signature = RawWindDataProcessSignature(None, None, [], None)
        signature.load_signature(file_path)
        return signature