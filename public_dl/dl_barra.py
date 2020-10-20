
from quant.data_loader import AbstractRawDataLoader 
from quant.definitions import DataType
from quant.helpers import parse_dim_index
from quant.constants import COLUMN_DELIMITER

## barra common columns
datayes_common_columns = {
            'BETA':DataType.Float64,
            'MOMENTUM':DataType.Float64,
            'SIZE':DataType.Float64,
            'EARNYILD':DataType.Float64,
            'RESVOL':DataType.Float64,
            'GROWTH':DataType.Float64,
            'BTOP':DataType.Float64,
            'LEVERAGE':DataType.Float64,
            'LIQUIDTY':DataType.Float64,
            'SIZENL':DataType.Float64,
            'BANK':DataType.Float64,
            'REALESTATE':DataType.Float64,
            'HEALTH':DataType.Float64,
            'TRANSPORTATION':DataType.Float64,
            'MINING':DataType.Float64,
            'NONFERMETAL':DataType.Float64,
            'HOUSEAPP':DataType.Float64,
            'LEISERVICE':DataType.Float64,
            'MACHIEQUIP':DataType.Float64,
            'BUILDDECO':DataType.Float64,
            'COMMETRADE':DataType.Float64,
            'CONMAT':DataType.Float64,
            'AUTO':DataType.Float64,
            'TEXTILE':DataType.Float64,
            'FOODBEVER':DataType.Float64,
            'ELECTRONICS':DataType.Float64,
            'COMPUTER':DataType.Float64,
            'LIGHTINDUS':DataType.Float64,
            'UTILITIES':DataType.Float64,
            'TELECOM':DataType.Float64,
            'AGRIFOREST':DataType.Float64,
            'CHEM':DataType.Float64,
            'MEDIA':DataType.Float64,
            'IRONSTEEL':DataType.Float64,
            'NONBANKFINAN':DataType.Float64,
            'ELECEQP':DataType.Float64,
            'AERODEF':DataType.Float64,
            'CONGLOMERATES':DataType.Float64,
            'COUNTRY':DataType.Float64,
        
}
datayes_srisk_columns = {'SRISK':DataType.Float64}
### msci
msci_common_columns = {
'CNE5S_BETA':DataType.Float64,
'CNE5S_BTOP':DataType.Float64,
'CNE5S_SIZE':DataType.Float64,
'CNE5S_SIZENL':DataType.Float64,
'CNE5S_LEVERAGE':DataType.Float64,
'CNE5S_LIQUIDTY':DataType.Float64,
'CNE5S_GROWTH':DataType.Float64,
'CNE5S_RESVOL':DataType.Float64,
'CNE5S_MOMENTUM':DataType.Float64,
'CNE5S_EARNYILD':DataType.Float64,
'CNE5S_AERODEF':DataType.Float64,
'CNE5S_AIRLINE':DataType.Float64,
'CNE5S_AUTO':DataType.Float64,
'CNE5S_BANKS':DataType.Float64,
'CNE5S_BEV':DataType.Float64,
'CNE5S_BLDPROD':DataType.Float64,
'CNE5S_CHEM':DataType.Float64,
'CNE5S_CNSTENG':DataType.Float64,
'CNE5S_COMSERV':DataType.Float64,
'CNE5S_CONMAT':DataType.Float64,
'CNE5S_CONSSERV':DataType.Float64,
'CNE5S_COUNTRY':DataType.Float64,
'CNE5S_DVFININS':DataType.Float64,
'CNE5S_ELECEQP':DataType.Float64,
'CNE5S_ENERGY':DataType.Float64,
'CNE5S_FOODPROD':DataType.Float64,
'CNE5S_HDWRSEMI':DataType.Float64,
'CNE5S_HEALTH':DataType.Float64,
'CNE5S_HOUSEDUR':DataType.Float64,
'CNE5S_INDCONG':DataType.Float64,
'CNE5S_LEISLUX':DataType.Float64,
'CNE5S_MACH':DataType.Float64,
'CNE5S_MARINE':DataType.Float64,
'CNE5S_MATERIAL':DataType.Float64,
'CNE5S_MEDIA':DataType.Float64,
'CNE5S_MTLMIN':DataType.Float64,
'CNE5S_PERSPRD':DataType.Float64,
'CNE5S_RDRLTRAN':DataType.Float64,
'CNE5S_REALEST':DataType.Float64,
'CNE5S_RETAIL':DataType.Float64,
'CNE5S_SOFTWARE':DataType.Float64,
'CNE5S_TRDDIST':DataType.Float64,
'CNE5S_UTILITIE':DataType.Float64,
}
msci_srisk_columns = {'UnadjSpecRisk%':DataType.Float64}
default_dim_to_new_name = lambda k:dict(zip(k, [item + '_COV' for item in k]))
msci_freturn_columns = {'DlyReturn':DataType.Float64}
datayes_freturn_columns = {'DlyReturn':DataType.Float64}

class ExposureDataLoader(AbstractRawDataLoader):
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, \
                dim_to_new_name={}, data_from = 'datayes'):
        self.data_from = data_from
        AbstractRawDataLoader.__init__(self, user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, dim_to_new_name)

    def _load_one_date_data(self, universe, path_templates, current_date, date_data, dims):
        if universe.stock_secu_code_number:
            file_path = path_templates[0].format(current_date.year, current_date.month, current_date.day)
            with open(file_path, "r") as f:
                self._read_one_file(f, date_data, dims)

    def _all_dim_definitions(self):
        if self.data_from.startswith('d') or self.data_from.startswith('D'):
            return datayes_common_columns 
        if self.data_from.startswith('m') or  self.data_from.startswith('M'):
            return msci_common_columns

 
class SriskDataLoader(ExposureDataLoader):
    def _all_dim_definitions(self):
        if self.data_from.startswith('d') or self.data_from.startswith('D'):
            return datayes_srisk_columns
        if self.data_from.startswith('m') or self.data_from.startswith('M'):   
            return msci_srisk_columns


class CovarianceDataLoader(AbstractRawDataLoader):
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, \
                dim_to_new_name={}, data_from = 'datayes'):
        #dims_to_load = self._all_dim_definitions().keys()
        self.data_from = data_from
        if not dim_to_new_name:
            dim_to_new_name = default_dim_to_new_name(dims_to_load)
        AbstractRawDataLoader.__init__(self, user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, dim_to_new_name)

    def _all_dim_definitions(self):
        if self.data_from.startswith('d') or self.data_from.startswith('D'):
            return datayes_common_columns
        if self.data_from.startswith('m') or  self.data_from.startswith('M'):
            return msci_common_columns

    def _load_one_date_data(self, universe, path_templates, current_date, date_data, dims):
        file_path = path_templates[0].format(current_date.year, current_date.month, current_date.day)
        with open(file_path, "r") as f:
            self._read_one_file(f, date_data, dims)

    def _read_one_row(self, date_data, line_list, dim_index, dims):
        ## factor name
        if self.data_from.startswith('d') or self.data_from.startswith('D'):
            factor = line_list[3]
            for dim in dims:
                if dim not in dim_index:
                    continue
                date_data[dim][factor] = self.dim_definitions[dim].value.parser(line_list[dim_index[dim]])   
        if self.data_from.startswith('m') or  self.data_from.startswith('M'):
            factor = line_list[0]
            for dim in dims:
                if dim not in dim_index:
                    continue
                date_data[dim][factor] = self.dim_definitions[dim].value.parser(line_list[dim_index[dim]])

class FactorReturnDataLoader(AbstractRawDataLoader):
    def __init__(self, user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, \
                dim_to_new_name={}, data_from = 'DATAYES'):
        #dims_to_load = self._all_dim_definitions().keys()
        self.data_from = data_from
        AbstractRawDataLoader.__init__(self, user_name, project_name, data_loader_name, universe, version_number, path_templates, dims_to_load, dim_to_new_name)

    def _all_dim_definitions(self):
        if self.data_from.startswith('d') or self.data_from.startswith('D'):
            return datayes_freturn_columns
        if self.data_from.startswith('m') or self.data_from.startswith('M'):
            return msci_freturn_columns

    def _load_one_date_data(self, universe, path_templates, current_date, date_data, dims):
        file_path = path_templates[0].format(current_date.year, current_date.month, current_date.day)
        with open(file_path, "r") as f:
            self._read_one_file(f, date_data, dims)

    def _read_one_row(self, date_data, line_list, dim_index, dims):
        ## factor name
        factor = line_list[0]
        for dim in dims:
            if dim not in dim_index:
                continue
            date_data[dim][factor] = self.dim_definitions[dim].value.parser(line_list[dim_index[dim]])


if __name__ == '__main__':
    from datetime import date
    from quant.universe_generator import UniverseGenerator
    import os, time
    from quant.universe import Universe
    from quant.constants import DATA_BASE_DIR, WORK_BASE_DIR, COLUMN_DELIMITER, DATE_FORMAT_STRING
    from quant.data import ProjectData, DimData, HuianData
    from quant.definitions import DataType
    user_name = "yzhou"
    project_name = "test1201"
    required_data_sources = ["Stock"]
    start_date = date(2018, 11, 2)
    end_date = date(2018, 11, 2)
    back_days = 0
    end_days = 0
    universe_generator = UniverseGenerator(user_name, project_name, required_data_sources, start_date, end_date, back_days, end_days)
    universe_path = os.path.join(WORK_BASE_DIR, "{}/{}/universe/universe.bin".format(user_name, project_name))
    universe = Universe.new_universe_from_file(universe_path)
    stock_path_template = os.path.join(DATA_BASE_DIR, "raw/DATAYES/RiskModel/{}/{}/{}/DY1D_EXPOSURE.txt")

    dims_to_load = ['BETA', 'SIZE']
    version_number = ProjectData.read_version_number(user_name, project_name)
    ProjectData.save_version_number(user_name, project_name, version_number + 1)
    path3 = os.path.join(DATA_BASE_DIR, "raw/DATAYES/RiskModel/{}/{}/{}/DY1D_FACTOR_RET.txt")
    
    barra_risk = FactorReturnDataLoader(user_name, project_name, 'tl_freturn', universe, version_number, [path3], dims_to_load, data_from = 'DATAYES')

