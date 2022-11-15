import time
import configparser
import dbf

class XHPT():
    """docstring for XHPT."""
    def __init__(self, runDate, moniterDir):
        self._moniterDir=moniterDir
        self._runDate=runDate
        self._readConfig()
        self._orderList=[]
        self._cancelList=[]
        self._pendingList=[]

    def _readConfig(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self._zcdybh=config['DEFAULT']['ZCDYBH']
        self._zhbh=config['DEFAULT']['ZHBH']
        self._counter=int(config['DEFAULT']['COUNTER'])

    def _writeConfig(self):
        config = configparser.ConfigParser()
        config['DEFAULT']={'ZCDYBH':self._zcdybh, 'ZHBH':self._zhbh, 'COUNTER':str(self._counter)}
        with open('config.ini', 'w') as configfile:
            config.write(configfile)

    def _writeDBF(self, flag):
        if flag=='WT':
            table=dbf.Table(filename=f'{self._moniterDir}\\XHPT_WT{self._runDate}.dbf', codepage='cp936')
            table.open(mode=dbf.READ_WRITE)
            for _order in self._orderList:
                table.append(_order)
        elif flag=='CD':
            table=dbf.Table(filename=f'{self._moniterDir}\\XHPT_CD{self._runDate}.dbf', codepage='cp936')
            table.open(mode=dbf.READ_WRITE)
            for _cancel in self._cancelList:
                table.append(_cancel)
        
        table.close()

    def _readDBF(self, flag):
        if flag=='WTCX':
            table=dbf.Table(filename=f'{self._moniterDir}\\XHPT_WTCX{self._runDate}.dbf', codepage='cp936')
            table.open(mode=dbf.READ_ONLY)
            self._pendingList=[record for record in table]
        elif flag=='CJCX':
            table=dbf.Table(filename=f'{self._moniterDir}\\XHPT_CJCX{self._runDate}.dbf', codepage='cp936')
            table.open(mode=dbf.READ_ONLY)
            for record in table:
                print(record)

    def order(self, orderNumber, CPBH, JYSC, ZQDM, WTFX, WTJGLX, WTJG, WTSL):
        """
        orderNumber: 委托笔数
        CPBH: 产品编号
        JYSC: 交易市场
        ZQDM: 证券代码
        WTFX: 委托方向, 1买入; 2卖出
        WTJGLX: 委托价格类型
        WTJG: 委托价格
        WTSL: 委托数量
        """
        gddm=None
        if JYSC=='1': # 上交所
            gddm='E000000509'
        elif JYSC=='2': # 深交所
            gddm='0001520507'
        elif JYSC=='j': # 股转系统
            gddm='4520000920'
        elif JYSC=='r': # 北交所
            gddm='4520000920'

        self._orderList=[]
        for i in range(orderNumber):
            self._counter+=1
            self._orderList.append({
                "CPBH": CPBH, 
                "ZCDYBH": self._zcdybh, # 单元编号
                "ZHBH": self._zhbh, # 组合编号
                "GDDM": gddm, 
                "JYSC": JYSC, 
                "ZQDM": ZQDM, 
                "WTFX": WTFX, 
                "WTJGLX": WTJGLX, 
                "WTJG": WTJG, 
                "WTSL": WTSL, 
                "WBZDYXH": self._counter# 自定义编号                
            })
        print(f'下单:{len(self._orderList)}条')
        self._writeDBF(flag='WT')
        self._writeConfig()
        
    def cancel(self, wtxhs):
        '''
        WTXH: 委托序号，用于撤单
        '''
        self._cancelList=[]
        for xh in wtxhs:
            self._cancelList.append({
                'WTXH':xh,
            })
        self._writeDBF(flag='CD')
        
    def queryOrder(self):
        self._readDBF(flag='WTCX')

    def queryTrade(self):
        self._readDBF(flag='CJCX')

    def auto_cancel(self, delay=10):
        time.sleep(delay)
        self.queryOrder()
        # 只有状态为已报(4),部成(6)的委托才能撤单
        wtxhs=[record.WTXH for record in self._pendingList if (record.WTZT=='4' or record.WTZT=='6')]
        # 清空队列
        self._pendingList=[]
        # print(wtxhs)
        if wtxhs:
            # 撤单
            self.cancel(wtxhs)
            print(f'撤单:{len(wtxhs)}条')


if __name__ == "__main__":
    obj=XHPT(runDate='20220712', moniterDir='D:\\MyWork\\MaidanDir')
    # 下单10手, 深交所000001限价
    obj.order(
        orderNumber=10, 
        CPBH='00010009', 
        JYSC='2', 
        ZQDM='000001', 
        WTFX='1', 
        WTJGLX='0', 
        WTJG=14.17, 
        WTSL=100
        )
    # 下委托10s后，未成交撤单
    obj.auto_cancel(delay=10)