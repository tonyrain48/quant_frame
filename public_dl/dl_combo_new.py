
import pickle
from quant.data_loader import *
from quant.universe import *
from quant.signature import *
from quant.data import *
from quant.public_dl.dl_indexwgt import *
from quant.helpers import Logger

class ComboInstrumentPool(AbstractDimDataLoader):

    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, required_dims, new_dims, combo_rules, combo_list = [True]):
        super().__init__(user_name, project_name, data_loader_name, universe, version_number, required_dims, new_dims)
        self.combo_rules = combo_rules
        self.combo_list = [True for x in required_dims] if combo_list == [True] else combo_list

    def _dim_definitions(self):
        """
        :return: {dim_name: data_type}, which describe the dim definition that describe the dim to load
        """
        return {self.new_dims[0]: DataType.Bool}

    def do_load(self):
        signature_path = self._signature_path_name(self.user_name, self.project_name, self.data_loader_name)
        universe_path = self._universe_path_name(self.user_name,self.project_name)
        signature = ComboSignature(self.universe.start_date, self.universe.end_date, self.universe.back_days, self.universe.end_days, \
                                   self.combo_rules,self.combo_list)
        old_signature = ComboSignature.new_combo_data_signature_from_file(signature_path)

        self.dim_definitions = self._dim_definitions()
        if not self._check_dependent_data_version(self._dependent_data_paths(), self.version_number):
            Logger.info(self.log_prefix, "Dependent data has changed. Need to reload all")
            calulate_all = CMD.proceedWhenY(self.log_prefix, "Detect newly updated data, recalculate or not")
            if calulate_all:
                self._load_all()
                signature.save_signature(signature_path)
            else:
                Logger.info(self.log_prefix, "Manually skip reload")
            return
        elif not self._check_dim_data_existance(self.new_dims):
            Logger.info(self.log_prefix, "Unloaded dim detected not created correctly. Need to load all")
            self._load_all()
            signature.save_signature(signature_path)
            return
        elif signature.check(old_signature):
            Logger.info(self.log_prefix, "No change. Skip reload")
            return
        elif not self._check_parameter(signature,old_signature):
            Logger.info(self.log_prefix,"Some parameters change:Try to load all")
            self._load_all()
            self.universe.save_universe(universe_path)
            signature.save_signature(signature_path)
            return
        elif not self._check_dim_data_range(self.new_dims, self.universe):
            Logger.info(self.log_prefix, "Date list changed. Try to load partial")
            head_date_list, tail_date_list = self._get_head_and_tail_list()
            self._load_partial(head_date_list, tail_date_list)
            signature.save_signature(signature_path)
            return
        else:
            Logger.info(self.log_prefix, "Date range already covered. Skip reload")
            signature.save_signature(signature_path)
            return

    def _check_parameter(self,signature,old_signature):
        return signature.combo_rules == old_signature.combo_rules  \
               and signature.combo_list == old_signature.combo_list

    def _calculate_one_day(self, required_dim_datas, dim_datas, di, current_date):
        """
        Fill dim_datas for current_date. Need to be implemented by sub-class
        :param required_dim_datas: {dim_name: dim_data} that are the required dimension
        :param dim_datas: {dim_name: dim_data} that are the data to be loaded
        """
        date_dim_data = {}
        num_sec_true = 0
        for secu_code in self.universe.secu_code_list:
            if self.combo_rules == "intersection":
                for i in range(len(self.combo_list)):
                    if i == 0:
                        if secu_code in required_dim_datas[self.required_dims[i]].data[di][1]:
                            judge = required_dim_datas[self.required_dims[i]].data[di][1][secu_code] if self.combo_list[i]  \
                                else (not required_dim_datas[self.required_dims[i]].data[di][1][secu_code])
                        else:
                            judge = False if self.combo_list[i] else True
                    else:
                        if secu_code in required_dim_datas[self.required_dims[i]].data[di][1]:
                            tmp_judge = required_dim_datas[self.required_dims[i]].data[di][1][secu_code] \
                                    if self.combo_list[i] else (not required_dim_datas[self.required_dims[i]].data[di][1][secu_code])
                            judge = judge and tmp_judge
                        else:
                            tmp_judge = False if self.combo_list[i] else True
                            judge = judge and tmp_judge
                date_dim_data[secu_code] = judge
                if judge == True:
                    num_sec_true = num_sec_true + 1

            elif self.combo_rules == "union":
                for i in range(len(self.combo_list)):
                    if i == 0:
                        if secu_code in required_dim_datas[self.required_dims[i]].data[di][1]:
                            judge = required_dim_datas[self.required_dims[i]].data[di][1][secu_code] if self.combo_list[i] \
                                else (not required_dim_datas[self.required_dims[i]].data[di][1][secu_code])
                        else:
                            judge = False if self.combo_list[i] else True
                    else:
                        if secu_code in required_dim_datas[self.required_dims[i]].data[di][1]:
                            tmp_judge = required_dim_datas[self.required_dims[i]].data[di][1][secu_code] \
                                    if self.combo_list[i] else (not required_dim_datas[self.required_dims[i]].data[di][1][secu_code])
                            judge = judge or tmp_judge
                        else:
                            tmp_judge = False if self.combo_list[i] else True
                            judge = judge or tmp_judge
                date_dim_data[secu_code] = judge
                if judge == True:
                    num_sec_true = num_sec_true + 1

        dim_datas[self.new_dims[0]].append_date_data(current_date, date_dim_data)
        Logger.debug(self.log_prefix, "Combo updated {} instruments in {}".format(num_sec_true, current_date))


class ComboSignature(Signature):
    def __init__(self,start_date,end_date,back_days,end_days,combo_rules,combo_list):
        super().__init__(start_date,end_date,back_days,end_days)
        self.combo_rules = combo_rules
        self.combo_list = combo_list

    def check(self,signature):
        return super().check(signature) \
                and self.combo_rules == signature.combo_rules \
                and self.combo_list == signature.combo_list

    def _write_signature_to_file(self, f):
        super()._write_signature_to_file(f)
        pickle.dump(self.combo_rules,f)
        pickle.dump(self.combo_list, f)

    def _read_signature_from_file(self, f):
        super()._read_signature_from_file(f)
        self.combo_rules = pickle.load(f)
        self.combo_list = pickle.load(f)

    @staticmethod
    def new_combo_data_signature_from_file(file_path):
        signature = ComboSignature(None,None,0,0,"",[])
        print(file_path)
        signature.load_signature((file_path))
        return signature

if __name__ == "__main__":
    t0 = time.process_time()

    # Project set up
    user_name = "sfu"
    project_name = "dl_combo_test"
    required_data_sources = ["Stock","Index"]
    start_date = date(2018, 9, 21)
    end_date = date(2018, 9, 28)
    back_days = 1
    end_days = 0

    # Generate universe
    universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date,
                                           back_days, end_days)
    universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)

    HS300_path_template = os.path.join(DATA_BASE_DIR,
                                                    "raw/WIND/IndexWeight/{}/{}/{}/AINDEXHS300CLOSEWEIGHT.txt")
    CSI500_path_template = os.path.join(DATA_BASE_DIR,"raw/WIND/IndexWeight/{}/{}/{}/AINDEXCSI500WEIGHT.txt")
    dims_to_load = ["WEIGHT"]

    # Get and update version number
    version_number = ProjectData.read_version_number(user_name, project_name)
    ProjectData.save_version_number(user_name, project_name, version_number + 1)

    HS300_weight_data_loader = IndexWeightDataLoader(user_name, project_name, universe, version_number,
                                              [HS300_path_template], dims_to_load)

    HS300_dim_data_loader = IndexWeightDimDataLoader(user_name, project_name, universe, version_number, \
                                                            required_dims=["WEIGHT"], new_dims=["HS300_WEIGHT","HS300_VALID"])

    HS300_dim_data_loader.do_load()

    CSI500_weight_data_loader = IndexWeightDataLoader(user_name, project_name, universe, version_number,
                                                    [CSI500_path_template], dims_to_load)

    CSI500_dim_data_loader = IndexWeightDimDataLoader(user_name, project_name, universe, version_number, \
                                                                     required_dims=["WEIGHT"], new_dims=["CSI500_WEIGHT","CSI500_VALID"])

    CSI500_dim_data_loader.do_load()

    combo_instrument_pool_data_loader = ComboInstrumentPool(user_name, project_name, universe, version_number, required_dims = ["CSI500_VALID","HS300_VALID"], \
                                                                        new_dims = ["COMBO_INSTRUMENT_POOL"], combo_rules = "intersection", combo_list = [True,True])
    combo_instrument_pool_data_loader.do_load()

    t1 = time.process_time()
    Logger.info("", "{} seconds".format(t1 - t0))
