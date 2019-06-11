# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
对时间序列的DataFrame格式转换
'''

import pandas as pd
import numpy as np
from datetime import datetime

class DateFormatDf:
    def __init__(self):
        pass

    def getStrToDate(self,tempDf,flag=1):
        '''
        flag=1:'2019-01-01'
        flag=2:'20190101'
        :param tempDf:
        :param flag:
        :return:
        '''
        dateList = tempDf.index.tolist()
        if flag==1:
            dateNewList = [datetime.strptime(dateStr,"%Y-%m-%d") for dateStr in dateList]
        elif flag==2:
            dateTempList = [dateStr[:4]+'-'+dateStr[4:6]+'-'+dateStr[6:] for dateStr in dateList]
            dateNewList = [datetime.strptime(dateStr, "%Y-%m-%d") for dateStr in dateTempList]
        resultDf = pd.DataFrame(tempDf.values,index=dateNewList,columns=tempDf.columns)
        return resultDf

if __name__=='__main__':
    DateFormatDfDemo = DateFormatDf()
    DateFormatDfDemo.getStrToDate()