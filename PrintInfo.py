# -- coding: utf-8 --

'''
    日志信息打印
'''

from datetime import datetime

class PrintInfo:
    def __init__(self):
        pass

    def PrintLog(self,infostr,otherInfo=''):
        currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(otherInfo,str):
            if not otherInfo:
                print(currentTime + '[INFO]: '+infostr)
            else:
                print(currentTime+ '[INFO]: '+infostr,otherInfo)
        else:
            print(currentTime + '[INFO]: ' + infostr, otherInfo)


if __name__ == '__main__':
    PrintInfoDemo = PrintInfo()
    PrintInfoDemo.PrintLog('日期信息打印测试')
