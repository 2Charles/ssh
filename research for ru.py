#coding:utf-8

import research
import corrlab
import matplotlib.pylab as plt
import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge,RidgeCV
from sklearn.linear_model import Lasso, LassoCV
from sklearn.linear_model import ElasticNet, ElasticNetCV

train_start, train_end = '20171114', '20171114'
test_start, test_end = '20171115', '20171115'
period, lag, target = '1s', '1s', 'ru0'
type = 0

rs = research.simu(train_start, train_end,test_start, test_end, period, lag, target, type = type, filedir ='/home/charles/python/intern/data/')

train, test = rs.get_train_test()
train_volu, test_volu = rs.get_train_test_volu()
train_volu, test_volu = rs.get_train_test_volu()

# train_volu, test_volu = rs.get_train_test_volu()
sampled_train, sampled_test = rs.sample(train, test)
shifted_train, shifted_test = rs.shift(sampled_train, sampled_test)
shifted_train.fillna(method = 'ffill', inplace = True)
shifted_test.fillna(method = 'ffill', inplace = True)

sampled_train_volu, sampled_test_volu = rs.sample(train_volu, test_volu)
shifted_train_volu, shifted_test_volu = rs.shift(sampled_train_volu, sampled_test_volu)
shifted_train_volu.fillna(method = 'ffill', inplace = True)
shifted_test_volu.fillna(method='ffill',inplace = True)


train_col = rs.filterSymbol(shifted_train,target, threshold=0, abs_or_not = True)
train_volu_col = [col+'_volu' for col in train_col]

combine = pd.concat([shifted_train, shifted_train_volu],axis = 1)   #combne return data and volu data
combine_col = rs.filterSymbol(combine, target,threshold=0)


lso = LassoCV(normalize=True,max_iter=5000,eps = 0.001,n_alphas=200,cv=10,n_jobs=-1)
lso.fit(combine[combine_col], combine[target])
lso_score = lso.score(combine[combine_col], combine[target])
print 'lso score is:', lso_score
print 'lso alpha is:', lso.alpha_
lso_train_pred = lso.predict(combine[combine_col])
lso_train_pred_class = rs.calssConvert(lso_train_pred)
flag = (sampled_train[target] != 0).values
train_class = rs.calssConvert(sampled_train[target].values)
lso_train_accu = np.mean(np.array(train_class)[flag] == np.array(lso_train_pred_class)[flag])
print 'class accuarcy is:',lso_train_accu

# rs.contrast(np.array(lso_train_pred)[flag], sampled_train[target].values[flag],0,30,coef=2)


elncv = ElasticNetCV(l1_ratio = 1, eps = 1e-3, max_iter=5000,n_alphas = 300, alphas = [0.002,0.001], normalize= True, cv  = 10, n_jobs = -1)
elncv.fit(combine[combine_col], combine[target])
eln_socre = elncv.score(combine[combine_col], combine[target])
print 'ElasticNet score is:', eln_socre
print 'ElasticNet alpha is:', elncv.alpha_
elncv_train_pred = elncv.predict(combine[combine_col])
flag = (sampled_train[target] != 0).values
elncv_pred_class = rs.calssConvert(elncv_train_pred)
elncv_train_accu = np.mean(np.array(train_class)[flag] == np.array(elncv_pred_class)[flag])
print 'class accuarcy is:',elncv_train_accu