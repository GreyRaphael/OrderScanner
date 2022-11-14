from datetime import datetime, timedelta
import time
import csv
import dbf


class OrderScanner:
    def __init__(self, moniterDir):
        self._moniterDir = moniterDir
        self._runDate = time.strftime("%Y%m%d")
        self._orderList = []
        self._cancelList = []
        self._pendingList = []

    def _writeDBF(self, flag):
        if flag == "order":
            table = dbf.Table(
                filename=f"{self._moniterDir}\\OrderAlgo_{self._runDate}.dbf",
                # codepage="cp936",
                codepage="utf8",
            )
            table.open(mode=dbf.READ_WRITE)
            for _order in self._orderList:
                table.append(_order)
        elif flag == "cancel":
            table = dbf.Table(
                filename=f"{self._moniterDir}\\CancelOrderAlgo_{self._runDate}.dbf",
                # codepage="cp936",
                codepage="utf8",
            )
            table.open(mode=dbf.READ_WRITE)
            for _cancel in self._cancelList:
                table.append(_cancel)
        table.close()

    def _readDBF(self, flag):
        if flag == "order":
            table = dbf.Table(
                filename=f"{self._moniterDir}\\ReportOrderAlgo_{self._runDate}.dbf",
                # codepage="cp936",
                codepage="utf8",
            )
            table.open(mode=dbf.READ_ONLY)
            self._pendingList = [record for record in table]

        elif flag == "asset":
            table = dbf.Table(
                filename=f"{self._moniterDir}\\ReportBalance_{self._runDate}.dbf",
                # codepage="cp936",
                codepage="utf8",
            )
            table.open(mode=dbf.READ_ONLY)
            for record in table:
                clientName = record.CLIENTNAME.strip()
                availableBalence = record.ENBALANCE
                print(clientName, availableBalence)
        elif flag == "hold":
            table = dbf.Table(
                filename=f"{self._moniterDir}\\ReportPosition_{self._runDate}.dbf",
                # codepage="cp936",
                codepage="utf8",
            )
            table.open(mode=dbf.READ_ONLY)
            hold_list = [
                {
                    "ClientName": record.CLIENTNAME.strip(),
                    "SECUCODE": record.SYMBOL.strip(),
                    "CurrentVolume": record.CURRENTQ,
                    "AvailableVolume": record.ENABLEQTY,
                }
                for record in table
            ]
            hold_list.sort(key=lambda x: (x['ClientName'],x['SECUCODE']))
            OrderScanner.writeCSV('hold.csv', hold_list)

        table.close()

    def order(self, orderNumber, clientName, code, direction, volume, ordType=101):
        """
        ExternalId, Character, 30, N, 自定义委托编号
        ClientName, Character, 255, Y, 账户名称
        Symbol, Character, 40, Y, 证券代码
        Side, Number, 3, Y, 买卖方向: Buy 1;Sell 2
        OrderQty, Number, 10, Y, 委托数量
        OrdType, Number, 3, Y, 算法类型
        EffTime, Character, 17, Y, 开始时间
        ExpTime, Character, 17, Y, 结束时间
        LimAction, Number, 1, Y, 0-涨跌停后不交易;1-涨跌停后仍交易
        AftAction, Number, 1, Y, 0-时间过期后不交易;1-时间过期后仍交易
        AlgoParam, Character, 255, N, 策略参数
        """
        start_time = datetime.now()
        stop_time = start_time + timedelta(minutes=10)

        self._orderList = []

        for i in range(orderNumber):
            self._orderList.append(
                {
                    "ClientName": clientName,
                    # "ClientName": '私募基金A'.encode('utf8').decode('gbk'),
                    "Symbol": code,
                    "Side": direction,
                    "OrderQty": volume,
                    "OrdType": ordType,
                    "EffTime": start_time.strftime("%Y%m%d%H%M%S000"),
                    # "EffTime": '20221110100000000',
                    "ExpTime": stop_time.strftime("%Y%m%d%H%M%S000"),
                    # "ExpTime": '20221110145700000',
                    "LimAction": 0,
                    "AftAction": 1,
                    # "AlgoParam":'price=9.9'
                }
            )
        print(f"sending code={code}, direction={direction}, vol={volume}")
        self._writeDBF(flag="order")

    def cancel(self, quote_list):
        self._cancelList = []
        for quoteId in quote_list:
            print(f"cancel {quoteId}")
            self._cancelList.append(
                {
                    "QuoteId": quoteId,
                    "CxlType": 1,
                }
            )
        self._writeDBF(flag="cancel")

    def queryOrder(self):
        self._readDBF(flag="order")

    def queryAsset(self):
        self._readDBF(flag="asset")

    def queryHold(self):
        self._readDBF(flag="hold")

    def auto_cancel(self, delay=3):
        time.sleep(delay)
        self.queryOrder()
        # 只有状态为已报(0),部成(1)的委托才能撤单
        quoteId_list = [
            record.QuoteId
            for record in self._pendingList
            if (record.OrdStatus == 0 or record.OrdStatus == 1)
        ]
        # 清空队列
        self._pendingList = []
        # print(quoteId_list)
        if quoteId_list:
            # 撤单
            self.cancel(quoteId_list)
            print(f"撤单:{len(quoteId_list)}条")

    @staticmethod
    def readCSV(filename):
        with open(filename, "r", encoding="utf8") as file:
            reader = csv.DictReader(file)
            return list(reader)

    @staticmethod
    def writeCSV(filename, data_list):
        if len(data_list) == 0:
            print("price list is empty!")
            return
        colNames = data_list[0].keys()
        with open(filename, "w", encoding="utf8", newline="") as file:
            csv_writer = csv.DictWriter(file, fieldnames=colNames)
            csv_writer.writeheader()
            csv_writer.writerows(data_list)

    def batchOrders(self, secucode_list, clientName, direction, volume):
        for secucode in secucode_list:
            self.order(1, clientName, secucode, direction, volume)
            # print(f"sending code={secucode}, direction={direction}, vol={volume}")


if __name__ == "__main__":
    obj = OrderScanner(moniterDir=r"D:\SWAP\ATX\OrderScan")
    clientNames = [
        "shanghaitest1",
        # "shanghaitest2",
    ]
    secucodes = [
        "000001.SZ",
        "300513.SZ",
        "600019.SH",
        "688036.SH",
    ]

    # data_list=OrderScanner.readCSV("sell.csv")
    # data_list=OrderScanner.readCSV("buy.csv")
    # for record in data_list:
    #     secucode=record['SECUCODE']
    #     vol=eval(record['volume'])
    #     obj.order(1, 'shanghaitest1', secucode, 1, vol)

    # for name in clientNames:
    #     obj.batchOrders(secucodes, name, direction=1, volume=1000)

    # obj.batchOrders(secucodes, 1, 200)
    # obj.order(
    #     orderNumber=10,
    #     code="000016.SZ",
    #     direction=1,
    #     volume=100,
    # )
    
    # obj.queryAsset()
    obj.queryHold()
    # obj.queryOrder()
    # obj.auto_cancel()
