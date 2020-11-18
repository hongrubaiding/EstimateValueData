# -- coding: utf-8 --



'''
004253.OF 国泰黄金ETF，投资金额20444.18，成本价1.3430；
001023.OF 华夏亚债中国C，投资金额14058.63，成本价1.2560；
005658.OF 华夏沪深300ETFC，投资金额5615.57，成本价1.3580；
005659.OF 南方恒生ETFC，投资金额7068.28，成本价1.0367；
004253.OF 标普500ETF，投资金额13560，成本价2.3594；
'''


from datetime import datetime,timedelta

class YunFeiCalc:
    def __init__(self):
        pass

    def calc_main(self):
        au9999=1.4680
        asset1= 20444.18*(au9999/1.3430-1)

        bond = 1.1290
        asset2 = 561.32+(14058.63*(1.2600/1.2560)-561.32) * (bond / 1.2560 - 1)

        hs300 = 1.6341
        asset3 = 5615.57 * (hs300 / 1.3580 - 1)

        bp500 = 2.6630
        asset4 = 13560 * (bp500 / 2.3594 - 1)

        hsetf = 1.0160
        asset5 =7068.28*(hsetf/1.0367-1)

        total_earn = asset1+asset2+asset3+asset4+asset5
        print("排除货币基金，共盈利：%s"%total_earn)

        jgday = (datetime.today()-datetime.strptime("2020-03-05","%Y-%m-%d")).days
        print("货基建仓天数%s"%jgday)

        hbjj = 40000*0.0262*jgday/365
        print("货基盈利%s"%hbjj)

        print("总共盈利%s"%(total_earn+hbjj))


if __name__=='__main__':
    YunFeiCalcDemo = YunFeiCalc()
    YunFeiCalcDemo.calc_main()