import time
from datetime import date
import os.path
from quant.constants import WORK_BASE_DIR, DATA_BASE_DIR
from quant.universe import Universe
from quant.universe_generator import UniverseGenerator
from quant.alpha import SampleAlpha1, SampleAlpha2
from quant.data import ProjectData
from quant.data_loader import BaseDataLoader, SampleDimDataLoader, SampleInstrumentPoolDataLoader, OtherBaseDataLoader
from quant.backtest import BackTestEngine
from quant.helpers import Logger
from quant.trade_and_stats import TradeAndStatsSimple, TradeAndStatsPure

from quant.public_dl.dl_adjfactor import AdjFactorDimDataLoader
from quant.public_dl.dl_adv import AdjVolumeDimDataLoader
#from quant.public_dl.dl_barra import *
from quant.public_dl.dl_combo import ComboInstrumentPool
from quant.public_dl.dl_indexwgt2 import IndexWeightDataLoader, IndexWeightDimDataLoader
from quant.public_dl.dl_industry import IndustryDataLoader, IndustryCapDimDataLoader
from quant.public_dl.dl_return import ReturnDimDataLoader
from quant.public_dl.dl_topliquid import TopLiquidDataLoader

from quant.public_op.op_power import OpPower
from quant.public_op.op_decay import OpDecay
from quant.public_op.op_neutral import OpNeutral
from quant.public_op.op_neutral_new import OpNeutralNew
from quant.public_op.op_section import OpSection
from quant.public_op.op_omega import OpOmega, OpDimOmega
from quant.sample_alpha.alpha_ndr import AlphaNDR
from quant.sample_alpha.alpha_total_volatility import AlphaTotalVol
from quant.sample_alpha.alpha_beta import AlphaBeta

if __name__ == '__main__':
    t0 = time.process_time()

    # Project set up
    user_name = 'hqu'
    project_name = 'NewTest'
    required_data_sources = ['Stock','Index']

    start_date = date(2007, 1, 1)
    end_date = date(2020,1, 1)
    back_days = 100
    end_days = 0

    # Generate universe
    universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date, back_days, end_days)
    universe_path = os.path.join(WORK_BASE_DIR, '{}/{}/universe/universe.bin'.format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)

    # Get and update version number
    version_number = ProjectData.read_version_number(user_name, project_name)
    ProjectData.save_version_number(user_name, project_name, version_number + 1)

    # BaseData setup
    stock_path_template = os.path.join(DATA_BASE_DIR, 'cooked/BaseData/{}/{}/{}/BASEDATA_new.txt')
    index_path_template = os.path.join(DATA_BASE_DIR, 'raw/WIND/IndexQuote/{}/{}/{}/AINDEXEODPRICES.txt')
    dims_to_load = ['CLOSEPRICE', 'S_DQ_CLOSE', 'SPLIT', 'DIVIDEND', 'ACTUALPLARATIO', 'PLAPRICE', 
                    'TURNOVERVOLUME', 'TRADESTATE', 'TURNOVERVALUE', 'TOTALSHARES','HIGHPRICE','LOWPRICE','OPENPRICE'
                    ,'LISTEDDATE']
    base_data_loader = BaseDataLoader(user_name, project_name, '', universe, version_number,
                                              [stock_path_template,index_path_template], dims_to_load)
    
    balance_sheet_basedata_path_template = os.path.join(DATA_BASE_DIR, 'raw/WIND/BaseData/{}/{}/{}/ASHAREBALANCESHEET.txt')
    dims_to_load = ['TOT_ASSETS']
    base_data_loader = OtherBaseDataLoader(user_name, project_name, '', universe, version_number,
                                              balance_sheet_basedata_path_template, dims_to_load)
    
    
    adjfactor = AdjFactorDimDataLoader(user_name, project_name, '',universe, version_number)
    adjfactor.do_load()
    adv5 = AdjVolumeDimDataLoader(user_name, project_name, '', universe, version_number, days = 5)
    adv5.do_load()
    adv60 = AdjVolumeDimDataLoader(user_name, project_name, '', universe, version_number, days = 60)
    adv60.do_load()
    dlreturn = ReturnDimDataLoader(user_name, project_name, '', universe, version_number)
    dlreturn.do_load()
    top300 = TopLiquidDataLoader(user_name, project_name, 'TOP300',universe, version_number, [], ['TOP300'], WindowDays = 60, Lookback = 40, Passrate = 100, Sticky = 40, Delta = 0.1, top = '300')
    top300.do_load()
    top1500 = TopLiquidDataLoader(user_name, project_name, 'TOP1500',universe, version_number, [], ['TOP1500'], WindowDays = 60, Lookback = 40, Passrate = 100, Sticky = 40, Delta = 0.1, top = '1500')
    top1500.do_load()
    allinst = TopLiquidDataLoader(user_name, project_name, 'ALL',universe, version_number, [], ['ALL'], WindowDays = 60, Lookback = 40, Passrate = 100, Sticky = 40, Delta = 0.1, top = 'ALL')
    allinst.do_load()

    HS300Wgt = IndexWeightDataLoader(user_name, project_name, 'HS300', universe, version_number
 	, ['S:/data/cooked/IndexWeight/{}/{}/{}/AINDEXHS300CLOSEWEIGHT.txt']
 	, ['I_WEIGHT'], {'I_WEIGHT':'WEIGHT300'})
    
    HS300Vld = IndexWeightDimDataLoader(user_name, project_name, 'HS300', universe, version_number
	, required_dims=['WEIGHT300'], new_dims=['HS300_WEIGHT','HS300_VALID'])
    HS300Vld.do_load()

    ZZ500Wgt = IndexWeightDataLoader(user_name, project_name, 'ZZ500', universe, version_number
 	, ['S:/data/cooked/IndexWeight/{}/{}/{}/SA_TRADABLESHARE_CSI500.txt']
 	, ['WEIGHT'], {'WEIGHT':'WEIGHT500'})

    ZZ500Vld = IndexWeightDimDataLoader(user_name, project_name, 'ZZ500', universe, version_number
	, required_dims=['WEIGHT500'], new_dims=['ZZ500_WEIGHT','ZZ500_VALID'])
    ZZ500Vld.do_load()

    COMBO1500 = ComboInstrumentPool(user_name, project_name, 'COMBO1500', universe, version_number, ['TOP1500','HS300_VALID'], ['COMBO1500'], 'union', [True,True])
    COMBO1500.do_load()
    industry = IndustryDataLoader(user_name, project_name, '', universe, version_number
	, ['S:/data/cooked/Industry/{}/{}/{}/ASHAREINDUSTRIESCLASSCITICS.txt']
	, ['ZXS','ZXF'])
    
    dlreturn = IndustryCapDimDataLoader(user_name, project_name, '', universe, version_number)
    dlreturn.do_load()
    

# BackTest setup, alphas, trade, stats
    size = 2E8
    multiple = 100.0
    trade_cost = 0.0
    ndecay = 5
    vdecay = [ndecay-i for i in range(ndecay)]
    version = ''

    alpha1 = AlphaNDR(user_name, project_name, 'alpha_5DR_HS300_VALID'+version, universe, 'HS300_VALID', 5, [
	OpPower(rank_type=1 , power=1.5)
	, OpDecay(ndecay)
	, OpNeutral('ZXS')
	], required_dims = ['ZXS'], need_to_dump = False)




    test_alphas = [alpha1]

    trade_and_stats_engine = TradeAndStatsSimple(user_name, project_name, universe, '')
    # trade_and_stats_engine = TradeAndStatsPure(user_name, project_name, universe, '')
    
    backtest_engine = BackTestEngine(user_name, project_name, universe, size, multiple, trade_cost, test_alphas, trade_and_stats_engine)
    backtest_engine.run_test()

    t1 = time.process_time()
    Logger.info('', '{} seconds'.format(t1 - t0))
