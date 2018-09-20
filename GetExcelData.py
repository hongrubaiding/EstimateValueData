# -- coding: utf-8 --
'''
    解析估值表数据
'''

import os
import pandas as pd
from datetime import datetime,date
import xlrd

class GetExcelData:
    def __init__(self,fileTotalPath):
        self.fileTotalPath = fileTotalPath          #估值表文件夹

    # 获取excel数据
    def getData(self):
        # ctype： 0 empty,1 string, 2 number, 3 date, 4 boolean, 5 error
        totalExcelNameList = os.listdir(self.fileTotalPath)
        dicProduct = {}  # 按日期整理的持仓数据

        dicName = {}  # 数据字段对应英文名称
        dicName[u'单位净值'] = 'netValue'
        dicName[u'累计单位净值'] = 'accNetValue'
        dicName[u'日净值增长率'] = 'netReturn'
        dicName[u'累计净值增长率'] = 'accNetReturn'
        dicName[u'实现收益'] = 'eargeMoney'
        dicName[u'本期净值增长率'] = 'thisNetReturn'
        dicName[u'流通股票投资合计'] = 'stockRate'

        dicName['1002'] = 'cashRate'
        dicName['1031'] = 'ensureMoneyRate'
        dicName[u'其中股票投资'] = 'securityRate'
        dicName[u'其中基金投资'] = 'fundRate'
        dicName['1202'] = 'antiSaleRate'
        dicName['1203'] = 'receivableSeRate'  # 应收股利
        dicName['1204'] = 'receivableIrRate'  # 应收利息
        dicName['3003'] = 'securityCalcRate'  # 证券清算款

        dicNetAsset = {}  # 资产及净值类数据
        dicAssetType = {}  # 资产及其种类比例数据

        for excelName in totalExcelNameList:
            upDate = excelName[-12:-4]
            upDate = upDate[:4]+'-'+upDate[4:6]+'-'+upDate[6:]
            data = xlrd.open_workbook(self.fileTotalPath + '\\' + excelName)
            table = data.sheet_by_index(0)

            dicNameCode = {}
            for rowNum in range(table.nrows):
                judgeStr = table.cell(rowNum, 0).value
                if judgeStr[:4] =='1102' and judgeStr[-2:] in ['SH','SZ']:  # 股票持仓数据
                    dicNameCode[judgeStr[-9:]] = table.row_values(rowNum)
                elif judgeStr in dicName.keys():  # 资产及净值类数据
                    dicNetAsset[dicName[judgeStr]] = dicNetAsset.get(dicName[judgeStr], {})
                    dicNetAsset[dicName[judgeStr]][upDate] = {}

                    if judgeStr not in ['流通股票投资合计', '1203', '1002', '1031', '其中股票投资', '其中基金投资', '1202', '3003', '1204']:
                        temp = table.cell(rowNum, 1)
                    else:
                        temp = table.cell(rowNum, 10)
                    try:
                        dicNetAsset[dicName[judgeStr]][upDate] = float(temp.value)
                    except:
                        if temp.ctype == 1:
                            if temp.value.find('%') != -1:
                                dicNetAsset[dicName[judgeStr]][upDate] = float(temp.value[:-1]) / 100
                            else:
                                temp = temp.value.replace(',', '')
                                dicNetAsset[dicName[judgeStr]][upDate] = float(temp)
            tempDf = pd.DataFrame(dicNameCode, index=table.row_values(4)).T
            dicProduct[upDate] = tempDf

        netAssetDf = pd.DataFrame(dicNetAsset)
        start_date = [datetime.strptime(datestr, "%Y-%m-%d").date() for datestr in
                      netAssetDf.index.tolist()]
        netAssetDf.index = start_date
        netAssetDf.index.name = 'update'
        # tempDf = netAssetDf[
        #     ['cashRate', 'ensureMoneyRate','receivableSeRate', 'antiSaleRate', 'securityRate', 'fundRate', 'receivableIrRate', 'securityCalcRate']].fillna(0)
        tempDf = netAssetDf[
            ['cashRate', 'ensureMoneyRate', 'antiSaleRate', 'securityRate', 'fundRate']].fillna(0)
        netAssetDf['otherRate'] = 1 - tempDf.sum(axis=1)
        # writer = pd.ExcelWriter('tempResult.xlsx')
        # netAssetDf.to_excel(writer)
        # writer.save()
        return netAssetDf, dicProduct

if __name__=='__main__':
    fileTotalPath = os.getcwd() + r'\乐道4估值表'  # 估值表文件夹路径
    GetExcelDataDemo = GetExcelData(fileTotalPath=fileTotalPath)
    GetExcelDataDemo.getData()
