# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com
'''
    估值表数据字典，用于解析不同估值表格式后，对应到本地mysql数据库的字段
'''

class DataDic:
    def __init__(self):
        pass

    def getDataDic(self,nameList=[]):
        dicResult = {}
        dicTotalResult = self.getTotalDataDic()
        for nameStr in nameList:
            dicResult[nameStr] = dicTotalResult[nameStr]
        return dicResult

    def getTotalDataDic(self):
        dicResult = {}
        dicResult['基金代码'] = 'fund_code'
        dicResult['基金名称'] = 'fund_name'
        dicResult['单位净值']='net_value'
        dicResult['基金单位净值'] = 'net_value'

        dicResult['昨日单位净值'] = 'pre_net_value'
        dicResult['累计单位净值'] = 'acc_net_value'
        dicResult['累计净值'] = 'acc_net_value'
        dicResult['日净值增长率'] = 'rate_net_value'
        dicResult['净值日增长率(比)'] = 'rate_net_value'

        dicResult['数据日期'] = 'update_time'

        dicResult['科目代码'] = 'style_code'
        dicResult['科目名称'] = 'style_name'
        dicResult['成本'] = 'cost'
        dicResult['成本占比'] = 'cost_rate'
        dicResult['成本占净值比'] = 'cost_rate'


        dicResult['市值'] = 'market_value'
        dicResult['市值占比'] = 'market_value_rate'
        dicResult['市值占净值比'] = 'market_value_rate'


        dicResult['数量'] = 'quantity'
        dicResult['单位成本'] = 'unit_cost'
        dicResult['行情'] = 'close_price'
        dicResult['市价'] = 'close_price'
        dicResult['估值增值'] = 'estimate_change'
        dicResult['停牌信息'] = 'trade_flag'

        return dicResult

if __name__=='__main__':
    DataDicDemo = DataDic()
    DataDic.getTotalDataDic()