# coding:utf-8

import corrlab
import numpy as np
from sklearn.preprocessing import StandardScaler
import matplotlib.pylab as plt
import pandas as pd
from sklearn.linear_model import RidgeCV, Ridge
import gc


class simu(object):
    def __init__(self,train_start, train_end, test_start, test_end, period, lag, target, type = 0, filedir ='/hdd/ctp/day/'):
        self.train_start = train_start
        self.train_end = train_end
        self.test_start = test_start
        self.test_end = test_end
        self.period = period
        self.lag = lag
        self.target = target
        self.type = type
        self.filedir = filedir
        self.corr = corrlab.corrAna(filedir=self.filedir, start_date=self.train_start, end_date=self.train_end, type=self.type)

    def get_train_test(self):
        trainLst = self.corr.generateDayLst()
        testLst = self.corr.generateDayLst(self.test_start, self.test_end)
        train = self.corr.concatdata(dayLst=trainLst)
        test = self.corr.concatdata(dayLst=testLst)
        return train, test

    def get_train_test_volu(self):
        trainLst = self.corr.generateDayLst()
        testLst = self.corr.generateDayLst(self.test_start, self.test_end)
        train_volu, test_volu = self.get_volu(trainLst), self.get_volu(testLst)
        train_volu.columns = [col+'_volu' for col in train_volu.columns.values]
        test_volu.columns = [col + '_volu' for col in test_volu.columns.values]
        return train_volu, test_volu

    def get_volu(self, dayLst, filterLst='major', split=2):
        '''load multidays and filter and concat together
        split means split one second into how many parts, choose from [2,4]'''
        if len(dayLst) == 1:
            symbolKey = dayLst[0]
        else:
            symbolKey = dayLst[0] + '-' + dayLst[-1]
        temp = self.corr.loaddata(day=dayLst[0], split=split)
        if filterLst == 'major':
            major = self.corr.findMostInType(temp)
            self.corr.recordSymbol(symbolKey, major)
            filterLst = major.values()
        res = self.corr.filtervolu(temp, lst=filterLst, threshold=1000, volu='ask_volume')
        del temp;
        gc.collect()
        if len(dayLst) > 1:
            for day in dayLst[1:]:
                temp = self.corr.loaddata(day=day, split=split)
                major = self.corr.findMostInType(temp)
                filterLst = major.values()
                self.corr.recordSymbol(symbolKey, major)
                res0 = self.corr.filtervolu(temp, lst=filterLst, threshold=1000, volu='ask_volume')
                res = pd.concat([res, res0])
                del temp, res0
                gc.collect()
        return res

    def shift(self, train, test,lag = None):
        '''have a default lag when initialize but can change lag value by appointed here without initialize again'''
        train_align_base = self.corr.get_align_base(train)
        test_align_base = self.corr.get_align_base(test)
        if lag:
            # y要先进行lag并对齐和sample
            shifted_train = self.corr.shift_align(train, self.target, lag=lag, align_base=train_align_base)
            # 对test进行lag
            shifted_test = self.corr.shift_align(test, target=self.target, lag=lag, align_base=test_align_base)
        else:
            # y要先进行lag并对齐和sample
            shifted_train = self.corr.shift_align(train, self.target, lag=self.lag, align_base=train_align_base)
            # 对test进行lag
            shifted_test = self.corr.shift_align(test, target=self.target, lag=self.lag, align_base=test_align_base)
        shifted_train.fillna(method = 'ffill', inplace = True)
        shifted_test.fillna(method = 'ffill', inplace = True)
        return shifted_train,shifted_test

    def sample(self, train, test, period = None):
        if period != None:
            sampled_train = self.corr.sampledata(train, period=period)
            sampled_test = self.corr.sampledata(test, period=period)
        else:
            sampled_train = self.corr.sampledata(train, period=self.period)
            sampled_test = self.corr.sampledata(test, period=self.period)
        sampled_train.dropna(how = 'all',axis = 0, inplace=True)
        sampled_test.dropna(how = 'all',axis = 0, inplace=True)
        sampled_train.fillna(method='ffill', inplace=True)
        sampled_test.fillna(method='ffill', inplace=True)
        return sampled_train, sampled_test

    def filterSymbol(self, data, target, abs_or_not = True, threshold = 0.1):
        '''get symbols that has a corr larger than threshold'''
        if abs_or_not:
            use_col = data.corr()[abs(data.corr()[target])>threshold].index.values
        else:
            use_col = data.corr()[data.corr()[target]>threshold].index.values
        use_col = list(use_col)
        use_col.remove(target)
        return use_col

    def largestsymbol(self, df, k):
        df_symbol = df.corr().nlargest(k, self.target)[self.target].index.values
        return df_symbol

    def calssConvert(self, lst):
        flag = [1 if i > 0 else 0 for i in lst]
        return flag

    def accu(self, lst1, lst2):
        return np.mean(np.array(lst1) == np.array(lst2))

    def contrast(self, pred, origin, start, end, coef = 1, figsize = (10,7)):
        if (end - start) >= 100:
            style = '-'
        else:
            style = '-o'
        plt.subplots(figsize = figsize)
        plt.plot(origin[start:end], style, label='unshifted')
        plt.plot(pred[start:end]*coef, style, label='pred')
        plt.plot([0 if i == 0 else 0 for i in pred[start:end]])
        plt.legend()
        plt.show()

    def contrast_sca(self, pred, origin, start, end, coef = 1, figsize = (10,7)):
    	origin_no_zero = np.array(origin) != 0
    	pred = np.array(pred)[origin_no_zero] 
    	origin = np.array(origin)[origin_no_zero]
    	pred_equal = [pred[i] * coef if pred[i]*origin[i] >=0 else np.nan for i in range(len(pred))] 
    	pred_unequal = [pred[i] *coef if pred[i]*origin[i] <0 else np.nan for i in range(len(pred))]
    	
    	origin_equal = [origin[i] if pred[i]*origin[i] >=0 else np.nan for i in range(len(pred))]
    	origin_unequal = [origin[i] if pred[i]*origin[i] <0 else np.nan for i in range(len(pred))]
    
    	plt.subplots(figsize = figsize)
    	plt.plot(origin[start:end], label='unshifted')
        plt.plot(pred[start:end]*coef, label='pred')
    	plt.plot(origin_equal[start:end],'ro')
    	plt.plot(origin_unequal[start:end],'bo')
    	plt.plot(pred_equal[start:end],'ro')
    	plt.plot(pred_unequal[start:end],'bo')
    	plt.plot(np.zeros((end-start)))
    	plt.title('red point for same symbol, blue for different.',fontsize = 15)
    	plt.legend(loc = 'best')
    	plt.show()
    	

    def countNozero(self, data):
        '''return number of != 0 of each symbol'''
        dic = {}
        for col in data.columns.values:
            dic[col] = sum(data[col] != 0)
        return dic

    def filternum(self,data, threshold = 0.5):
        dic = self.countNozero(data)
        tar = dic[self.target]
        flag = [True if value/tar*1.0 > threshold else False for value in dic.values()]
        return np.array(dic.keys())[flag]

    def plot(self,data, var, start = 0, end = 10000):
        if end >= 100:
            style = '-'
        else:
            style = '-o'
        plt.plot(data[self.target][start:end], style, label='ru')
        plt.plot(data[var][start:end], style, label=var)
        plt.legend()
        plt.show()
