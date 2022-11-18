from datetime import datetime, timedelta
import time
import csv
import dbf


class OrderScanner:
    def __init__(self, moniterDir):
        self._moniterDir = moniterDir
        self._runDate = time.strftime("%Y%m%d")

    def _writeDBF(self, file_name, record_list):
        table = dbf.Table(filename=file_name, codepage="utf8")
        table.open(mode=dbf.READ_WRITE)
        for record in record_list:
            table.append(record)
        table.close()

    def _readDBF(self, file_name):
        table = dbf.Table(filename=file_name, codepage="utf8")
        table.open(mode=dbf.READ_ONLY)
        record_list = [record for record in table]
        table.close()
        return record_list

    def order(self, batchSize, clientName, code, direction, volume, ordType=101):
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
        stop_time = start_time + timedelta(minutes=60)

        order_list = [
            {
                "ClientName": clientName,
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
            for i in range(batchSize)
        ]

        print(
            f"sending code={code}, direction={direction}, vol={volume}, batch Size={batchSize}"
        )
        file_name = f"{self._moniterDir}\\OrderAlgo_{self._runDate}.dbf"
        self._writeDBF(file_name, order_list)

    def cancel(self, quote_list):
        cancel_list = []
        for quote_id in quote_list:
            print(f"cancel QuoteId={quote_id}")
            cancel_list.append(
                {
                    "QuoteId": quote_id,
                    "CxlType": 1,
                }
            )

        file_name = f"{self._moniterDir}\\CancelOrderAlgo_{self._runDate}.dbf"
        self._writeDBF(file_name, cancel_list)

    def queryOrder(self):
        filename = f"{self._moniterDir}\\ReportOrderAlgo_{self._runDate}.dbf"
        record_list = self._readDBF(filename)
        print(f"query {len(record_list)} orders")
        return record_list

    def queryAsset(self):
        filename = f"{self._moniterDir}\\ReportBalance_{self._runDate}.dbf"
        record_list = self._readDBF(filename)
        for record in record_list:
            client_name = record.CLIENTNAME.strip()
            available_balence = record.ENBALANCE
            print(f"ClientName={client_name}, AvailableBalence={available_balence}")

    def filterHold(self, hold_dict):
        secucode = hold_dict["SECUCODE"]
        available_vol = hold_dict["AvailableVolume"]
        if secucode.startswith("688") and 0 < available_vol < 200:
            return True
        elif 0 < available_vol < 100:
            return True
        else:
            return False

    def queryHold(self):
        filename = f"{self._moniterDir}\\ReportPosition_{self._runDate}.dbf"
        record_list = self._readDBF(filename)

        hold_list = [
            {
                "ClientName": record.CLIENTNAME.strip(),
                "SECUCODE": record.SYMBOL.strip(),
                "CurrentVolume": record.CURRENTQ,
                "AvailableVolume": record.ENABLEQTY,
            }
            for record in record_list
        ]
        # hold_list=filter(self.filterHold, hold_list) # 筛选零股
        hold_list.sort(key=lambda x: (x["ClientName"], x["SECUCODE"]))
        OrderScanner.writeCSV("output/hold.csv", hold_list)

    def autoCancel(self, delay=3):
        time.sleep(delay)
        record_list = self.queryOrder()
        # 只有状态为已报(0),部成(1)的委托才能撤单
        quoteId_list = [
            record.QuoteId
            for record in record_list
            if (record.OrdStatus == 0 or record.OrdStatus == 1)
        ]
        # print(quoteId_list)
        if quoteId_list:
            self.cancel(quoteId_list)
            print(f"cancel:{len(quoteId_list)} orders")

    @staticmethod
    def readCSV(filename):
        with open(filename, "r", encoding="utf8") as file:
            reader = csv.DictReader(file)
            return list(reader)

    @staticmethod
    def writeCSV(filename, data_list):
        if len(data_list) == 0:
            print("data list is empty!")
            return
        colNames = data_list[0].keys()
        with open(filename, "w", encoding="utf8", newline="") as file:
            csv_writer = csv.DictWriter(file, fieldnames=colNames)
            csv_writer.writeheader()
            csv_writer.writerows(data_list)


if __name__ == "__main__":
    obj = OrderScanner(moniterDir=r"D:\SWAP\ATX\OrderScan")

    dict_list = OrderScanner.readCSV("input/opfile.csv")
    for dict_data in dict_list:
        secucode = dict_data["SECUCODE"]
        direction=int(dict_data['direction'])
        vol = eval(dict_data["volume"])
        obj.order(
            batchSize=1,
            clientName="私募基金A",
            code=secucode,
            direction=direction,  # 1 买入;2 卖出
            volume=vol,
        )

    # obj.autoCancel(delay=10)

    # # query Asset, Hold, Order
    # obj.queryAsset()
    # obj.queryHold()
    # obj.queryOrder()
