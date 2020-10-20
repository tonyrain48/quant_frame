# Created by yzhou
import numpy as np
from collections import deque
from quant.operation import Operation 
from quant.data import np_nan_array

class OpDecay(Operation):
    def __init__(self, decay_weight,  mode = 'wa'):
        '''
        :param back_days: initial di
        :param decay_weight: can be a list or int or float
          1)  list. e.g [3,2,1] means the weights are [3/(3+2+1), 2/(3+2+1), 1/()]
          2)  int. e.g 3 means the weights are [3/(3+2+1), 2/(), 1/()] when mode == 'wa'
                       3 means the span = 2/(3+1) when mode == 'ema'
          3) float &&  belongs to (0,1), which means the param is the coefficient of ema 
        :param mode: 'wa' or 'ema'. Default is 'wa' 
        '''
        self.mode = mode
        self.decay_weight = decay_weight
        self._alpha_list = []
        self._alpha_deque = deque()
        self._ema = None
    def _weighted_alpha(self, alpha_list, weight_list):
        new_alpha = []
        for i in range(len(alpha_list)):
            if not new_alpha:
                new_alpha = [alpha * weight_list[i] for alpha in alpha_list[i]]
            else:
                new_alpha = [new_alpha[j] + alpha_list[i][j] * weight_list[i] for j in range(len(alpha_list[0]))]
        return new_alpha

    def do_op(self, daily_alpha_data, instrument_pool_data, di, data):
        '''
        :param input_alpha: a list of alpha data
        :param di: current di
        '''

        if self.mode == 'wa':
            return np.array(self._wa_decay(daily_alpha_data, self.decay_weight, di), dtype = np.float32)
        elif self.mode in ['ema', 'EMA']:
            return np.array(self._ema_decay(daily_alpha_data, self.decay_weight, di), dtype = np.float32)
        
    def _wa_decay(self, input_alpha, decay_weight, di):
        if type(decay_weight) == type([]):
            weight_sum = sum(decay_weight)*1.0
            if weight_sum == 0:
                print('The sum of weight cannot be 0!')
                return None
            factors = [item/weight_sum for item in decay_weight]
            count = len(decay_weight)
            ## the key word global only comes into effect in this module
            if len(self._alpha_deque) < count:
                self._alpha_deque.appendleft(input_alpha)
                
            else:
                self._alpha_deque.pop()
                self._alpha_deque.appendleft(input_alpha)
            new_alpha = self._weighted_alpha(self._alpha_deque, factors)
            #print(self._alpha_deque[0][:10])
            #if len(self._alpha_deque)>1:
            #    print(self._alpha_deque[1][:10])
            return new_alpha

        elif type(decay_weight) == int:
            weight_sum = sum(range(1, decay_weight+1))*1.0
            factors = [item / weight_sum for item in range(decay_weight, 0, -1)]
            count = len(factors)
            if not self._alpha_list:
                
                alpha_deque = deque()
                alpha_deque.append(input_alpha)
                self._alpha_list.append(alpha_deque)
                new_alpha = [alpha*factors[0] for alpha in input_alpha]
                equal_alpha = [alpha for alpha in input_alpha]
                self._alpha_list.append(new_alpha)
                self._alpha_list.append(equal_alpha)
                return new_alpha
            if len(self._alpha_list[0]) < count:
                self._alpha_list[1] = [self._alpha_list[1][i] + input_alpha[i]*factors[0] - self._alpha_list[2][i]*factors[-1] \
                                  for i in range(len(input_alpha))]
                self._alpha_list[2] = [self._alpha_list[2][i] + input_alpha[i] for i in range(len(input_alpha))]
                self._alpha_list[0].appendleft(input_alpha)
            else:
                self._alpha_list[1] = [self._alpha_list[1][i] + input_alpha[i]*factors[0] - self._alpha_list[2][i]*factors[-1] \
                                  for i in range(len(input_alpha))]
                self._alpha_list[2] = [self._alpha_list[2][i] + input_alpha[i] - self._alpha_list[0][-1][i] for i in range(len(input_alpha))]
                self._alpha_list[0].pop()
                self._alpha_list[0].appendleft(input_alpha)
            new_alpha = self._alpha_list[1]
            return new_alpha
    def _ema_decay(self, input_alpha, decay_weight, di):
                
        beta = 2/(decay_weight+1) if type(decay_weight) == int else decay_weight
        global _ema
        if not self._ema :
            self._ema = input_alpha
            return input_alpha
        self._ema = [self._ema[i]*(1-beta) + input_alpha[i]*beta for i in range(len(self._ema))]
        new_alpha = self._ema.copy()
        return _ema
        
        

if __name__ == '__main__':
    nrow, ncol = [10, 5]
    #data = np.random.uniform(0, 1, size = (nrow, ncol))
    data = np.zeros(shape= (nrow, ncol))
    op1 = OpDecay(5)
    op2 = OpDecay([5, 4, 3, 2, 1])
    tmp = (data[0]+1).tolist()
    for i in range(nrow):
        #tmp = (data[0]+i+1).tolist()
        #data = (np.zeros(shape = (1,5))[0]+i+1).tolist()
        print(tmp)
        print('------new_alpha1 ')
        print(op1.do_op(tmp,None, i, None))
        print('------new_alpha2 ')
        print(op2.do_op(tmp, None, i, None))
        print('\n')
        tmp = op1.do_op(tmp,None, i, None)
    """
    op1 = OpDecay(2)
    op2 = OpDecay([2, 1])
    for i in range(3,nrow+3):
        tmp = (data[0]+i+1-3).tolist()
        #data = (np.zeros(shape = (1,5))[0]+i+1).tolist()
        print(tmp)
        print('------new_alpha1 ')
        print(op1.do_op(tmp, None, i, None))
        print('------new_alpha2 ')
        print(op2.do_op(tmp, None, i, None))
        print('\n')
    """
