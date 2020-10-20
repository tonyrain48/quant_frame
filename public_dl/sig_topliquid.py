import pickle
from quant.signature import Signature

# 流动性股票池 signature
class TopLiquidSignature(Signature):
    def __init__(self, start_date, end_date, back_days, end_days, requiredpara,option,optionpara):
        super().__init__(start_date, end_date, back_days, end_days)
        self.requiredpara = requiredpara
        self.option = option
        self.optionpara = optionpara

    def check(self, signature):
        if self.option == signature.option == False:
            return super().check(signature) \
                   and self.requiredpara == signature.requiredpara \
                   and self.option == signature.option
        else:
            return super().check(signature) \
                   and self.requiredpara == signature.requiredpara \
                   and self.option == signature.option \
                   and self.optionpara == signature.optionpara

    def _write_signature_to_file(self, f):
        super()._write_signature_to_file(f)
        pickle.dump(self.requiredpara, f)
        pickle.dump(self.option, f)
        pickle.dump(self.optionpara, f)

    def _read_signature_from_file(self, f):
        super()._read_signature_from_file(f)
        self.requiredpara = pickle.load(f)
        self.option = pickle.load(f)
        self.optionpara = pickle.load(f)

    @staticmethod
    def new_topliquid_data_signature_from_file(file_path):
        signature = TopLiquidSignature(None, None, 0, 0, [], None, [])
        signature.load_signature(file_path)
        return signature
