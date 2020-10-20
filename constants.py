import os
import configparser

# 读取配置文件
class ConfigParser(object):

    def __init__(self):
        config_file = os.path.dirname(os.path.realpath(__file__)) + '/config.ini'
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def get(self, section=None, key=None):
        return self.config.get(section, key)

    def get_int(self, section=None, key=None):
        return self.config.getint(section, key)


LOG_LEVEL_NAME_TO_VALUE = {"info": 1, "debug": 2, "warn": 3}
LOG_LEVEL_TO_PREFIX = {1: "[INFO] ", 2: "[DEBUG] ", 3: "[WARNING] "}

config = ConfigParser()
config_path_section = "path"
config_date_section = "date"
config_user_section = "user"
config_log_section = "log"

DATA_BASE_DIR = config.get(config_path_section, "data_base_dir")
WORK_BASE_DIR = config.get(config_path_section, "work_base_dir")
LOG_LEVEL_NAME = config.get(config_log_section, "log_level")
LOG_LEVEL = LOG_LEVEL_NAME_TO_VALUE.get(LOG_LEVEL_NAME)

COLUMN_DELIMITER = "|"
#DATE_FORMAT_STRING = "%Y-%m-%d"
DATE_FORMAT_STRING = "%Y%m%d"
DATE_FORMAT_STRING2 = "%Y%m%d"


COMMON_FACTOR_FOR_MSCI = ['CNE5S_BETA', 'CNE5S_BTOP','CNE5S_SIZE','CNE5S_SIZENL','CNE5S_LEVERAGE','CNE5S_LIQUIDTY','CNE5S_GROWTH','CNE5S_RESVOL','CNE5S_MOMENTUM',
                          'CNE5S_EARNYILD', 'CNE5S_COUNTRY','CNE5S_AERODEF','CNE5S_AIRLINE','CNE5S_AUTO','CNE5S_BANKS','CNE5S_BEV', 'CNE5S_BLDPROD', 'CNE5S_CHEM',
                          'CNE5S_CNSTENG', 'CNE5S_COMSERV', 'CNE5S_CONMAT','CNE5S_CONSSERV', 'CNE5S_DVFININS', 'CNE5S_ELECEQP', 'CNE5S_ENERGY', 'CNE5S_FOODPROD',
                          'CNE5S_HDWRSEMI', 'CNE5S_HEALTH', 'CNE5S_HOUSEDUR','CNE5S_INDCONG','CNE5S_LEISLUX','CNE5S_MACH','CNE5S_MARINE','CNE5S_MATERIAL','CNE5S_MEDIA',
                          'CNE5S_MTLMIN','CNE5S_PERSPRD','CNE5S_RDRLTRAN','CNE5S_REALEST','CNE5S_RETAIL','CNE5S_SOFTWARE','CNE5S_TRDDIST','CNE5S_UTILITIE']
COV_FACTOR_FOR_MSCI = COMMON_FACTOR_FOR_MSCI
COMMON_FACTOR_FOR_TL = ['BETA', 'BTOP', 'SIZE', 'SIZENL', 'LEVERAGE', 'LIQUIDTY', 'GROWTH', 'RESVOL', 'MOMENTUM','EARNYILD', 'COUNTRY', 'AERODEF', 'AGRIFOREST','AUTO','BANK',
                      'BUILDDECO', 'CHEM', 'COMMETRADE', 'COMPUTER', 'CONGLOMERATES','CONMAT','ELECEQP', 'ELECTRONICS', 'FOODBEVER', 'HEALTH', 'HOUSEAPP','IRONSTEEL',
                      'LEISERVICE', 'LIGHTINDUS','MACHIEQUIP','MEDIA','MINING','NONBANKFINAN','NONFERMETAL','REALESTATE', 'TELECOM','TEXTILE','TRANSPORTATION', 'UTILITIES']
COV_FACTOR_FOR_TL = ['BETA', 'BTOP','SIZE','SIZENL','LEVERAGE','LIQUIDTY','GROWTH','RESVOL','MOMENTUM','EARNYILD','COUNTRY','AERODEF','AgriForest','Auto','Bank','BuildDeco',
                     'CHEM', 'CommeTrade', 'Computer','Conglomerates','CONMAT','ELECEQP', 'Electronics', 'FoodBever', 'Health', 'HouseApp','IronSteel','LeiService',
                     'LightIndus','MachiEquip','Media','Mining','NonBankFinan','NonFerMetal','RealEstate','Telecom','Textile','Transportation', 'Utilities']

TITLE_FOR_ATT = ['Date', 'Total_Size', 'Total_PnL', 'Factor_PnL', 'Specific_PnL']
TITLE_FOR_EXP = ['Date', 'Total_Size', 'Total_Risk', 'Factor_Risk', 'Specific_Risk']
