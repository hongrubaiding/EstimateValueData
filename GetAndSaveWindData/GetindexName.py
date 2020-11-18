# -- coding: utf-8 --

import pandas as pd


from WindPy import w

class GetindeName:
    def __init__(self):
        pass

    def get_data(self):
        w.start()
        df1 = pd.read_excel("行业指数ETF概况.xlsx",sheet_name='Sheet1',index_col=0)
        df2 = pd.read_excel("策略指数ETF概况.xlsx", sheet_name='Sheet1', index_col=0)
        df3 = pd.read_excel("主题指数ETF概况.xlsx", sheet_name='Sheet1', index_col=0)
        df4 = pd.read_excel("规模指数ETF概况.xlsx", sheet_name='Sheet1', index_col=0)
        df5 = pd.read_excel("风格指数ETF概况.xlsx", sheet_name='Sheet1', index_col=0)

        # index_code_list = df1.index.tolist()+df2.index.tolist()+df3.index.tolist()+df4.index.tolist()+df5.index.tolist()
        name_list = ['行业指数ETF概况','策略指数ETF概况','主题指数ETF概况','规模指数ETF概况','风格指数ETF概况']
        df_list = [df1,df2,df3,df4,df5]
        for name in name_list:
            df = df_list[name_list.index(name)]
            aa = w.wss(df.index.tolist(), "sec_name")
            tempdf1 = pd.DataFrame(aa.Data, columns=aa.Codes, index=aa.Fields).T
            result = pd.concat([df1,tempdf1],axis=1,sort=True)
            result.to_excel("%s.xlsx"%name)
            break

if __name__=='__main__':
    GetindeNameDemo = GetindeName()
    GetindeNameDemo.get_data()