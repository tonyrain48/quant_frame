from quant.alpha import AbstractAlphaEngine, UpdatableData
from quant.data import np_nan_array
# fundamental sample

class FundamentalUpdatableData(UpdatableData):
    def __init__(self, universe):
        super().__init__(universe)
        # {ii: {key: [list of records]}}
        # key: ifadjusted|ifmerged|accountingstandards|enterprisetype
        self.record_data = {} 
        # {ii: {merge_type: {year: {month(like 3 or 6 or 9 or 12): data}}}}
        # merge_type is 1 or 2
        self.updated_data = {} 
        pass

    def update(self, daily_data, daily_instrument_pool_data):
        #print(daily_data)
        for ii in range(len(daily_data)):
            if ii not in self.record_data:
                self.record_data[ii] = {}
            if ii not in self.updated_data:
                self.updated_data[ii] = {}
            if not daily_data[ii]:
                continue
            data_to_update = daily_data[ii]
            #print(data_to_update)
            for k, v_list in data_to_update.items():
                # record its own type
                if k not in self.record_data[ii]:
                    self.record_data[ii][k] = []
                self.record_data[ii][k].append(v_list)

                for v in v_list:
                    # update and record Q1, Q2, Q3, Q4
                    end_date = v["ENDDATE"]
                    merge_type = self.get_merge_type(k)
                    if end_date is None:
                        raise Exception("Fundamental End date is None!!")
                    if merge_type not in self.updated_data[ii]:
                        self.updated_data[ii][merge_type] = {}
                    if end_date.year not in self.updated_data[ii][merge_type]:
                        self.updated_data[ii][merge_type][end_date.year] = {}
                    if end_date.month not in self.updated_data[ii][merge_type][end_date.year]:
                        self.updated_data[ii][merge_type][end_date.year][end_date.month] = v
                    else:
                        self.updated_data[ii][merge_type][end_date.year][end_date.month].update(v)
            #print("RECORD ii {}, {}".format(ii, self.record_data[ii]))
            #print("UPDATE ii {}, {}".format(ii, self.updated_data[ii]))

    def get_merge_type(self, key):
        return key.split("|")[1]

    def get_adjust_type(self, key):
        return key.split("|")[1]

    def get(self, ii):
        return self.updated_data[ii]


class FundamentalAlpha1(AbstractAlphaEngine):
    def __init__(self, user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims, t=2, need_to_dump=False, delay=1):
        self.t = t
        self.delay = delay
        self.fd_data = FundamentalUpdatableData(universe)
        super().__init__(user_name, project_name, alpha_name, universe, instrument_pool, op_list, required_dims, need_to_dump)

    def do_calculate_one_day_alpha(self, instrument_pool_data, di, data):
        print("DI START --------------- {} --------------".format(di))
        res = np_nan_array(shape=data["CLOSEPRICE"].data[di].shape, dtype="float64")
        if di == self.universe.start_di:
            di_to_update = 0
            while di_to_update <= self.universe.start_di - self.delay:
                print("BACKFILL DI {}, DATE {}".format(di_to_update, self.universe.date_list[di_to_update]))
                self.fd_data.update(data["FUNDAMENTALDATA"].data[di_to_update], instrument_pool_data.data[di_to_update])
                di_to_update = di_to_update + 1
        elif di < self.delay:
            return res
        else:
            self.fd_data.update(data["FUNDAMENTALDATA"].data[di - self.delay], instrument_pool_data.data[di - self.delay])
        today = self.universe.date_list[di]
        year = today.year
        month = today.month
        for ii in range(len(self.universe.secu_code_list)):
            #print("di: {}, ii : {}, date: {}, secu_code: {}".format(di, ii, today, self.universe.secu_code_list[ii]))
            d = self.fd_data.get(ii)
            last_year_a = self.get_year_a(d, year - 1)
            t_year_a = self.get_year_a(d, year - self.t)
            if last_year_a and t_year_a:
                res[ii] = last_year_a / t_year_a
                #print("    ALPHA ii {}, res[ii] {}".format(ii, res[ii]))
            else:
                res[ii] = 1.0
        print("ALPHA di {}, {}, res {}\n".format(di, today, res))
        return res

    def get_year_a(self, d, year):
        if '1' not in d:
            return None
        if year not in d['1']:
            return None
        if 12 not in d['1'][year]:
            return None
        # sample alpha: use merge_type = '1'
        #print("    d data year {} : {}\n   {} ".format(year, d, d['1'][year][12]))
        long_term_loan = d['1'][year][12]["LONGTERMLOAN"] if "LONGTERMLOAN" in d['1'][year][12] else None
        total_assets = d['1'][year][12]["TOTALASSETS"] if "TOTALASSETS" in d['1'][year][12] else None
        if long_term_loan is None or total_assets is None:
            return None
        else:
            return long_term_loan / total_assets

    def _get_intrinsic_required_dims(self):
        return ["FUNDAMENTALDATA"]

