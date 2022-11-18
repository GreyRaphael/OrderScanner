import time
import csv
import dbf


class i2OrderScanner:
    def __init__(self, moniterDir):
        self._moniterDir = moniterDir
        self._runDate = time.strftime("%Y%m%d")

    def _writeDBF(self, file_name, record_list):
        table = dbf.Table(filename=file_name, codepage="cp936")
        table.open(mode=dbf.READ_WRITE)
        for record in record_list:
            table.append(record)
        table.close()

    def _readDBF(self, file_name):
        table = dbf.Table(filename=file_name, codepage="cp936")
        table.open(mode=dbf.READ_ONLY)
        record_list = [record for record in table]
        table.close()
        return record_list

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

    def queryOrder(self):
        filename = f"{self._moniterDir}\\XHPT_WTCX{self._runDate}.dbf"
        record_list = self._readDBF(filename)
        data_list = [
            {
                "委托日期时间": f"{record.WTRQ}{record.WTSJ}",
                "委托序号": record.WTXH,
                "外部自定义序号": record.WBZDYXH,
                "产品编号": record.CPBH.strip(),
                "资产单元编号": record.ZCDYBH.strip(),
                "组合编号": record.ZHBH.strip(),
                "股东代码": record.GDDM.strip(),
                "证券代码": record.ZQDM.strip(),
                "委托价格类型": record.WTJGLX,
                "Direction": record.WTFX.strip(),
                "Price": record.WTJG,
                "Volume": record.WTSL,
                "OrderBalence": record.WTJE,
                "预买冻结金额": record.YMDJJE,
                "预卖收入金额": record.YMSRJE,
                "OrderStatus": record.WTZT,
                "委托撤成数量": record.WTCCSL,
                "废单原因": record.FDYY.strip(),
                "交易所申报编号": record.JYSSBBH.strip(),
                # '处理标志':record.CLBZ.strip(),
                # '备用字段':record.BYZD.strip(),
                # '特殊标志':record.TSBS.strip(),
            }
            for record in record_list
        ]
        filename = "output/queryOrder.csv"
        print(f"write {len(data_list)} record to {filename}")
        i2OrderScanner.writeCSV(filename, data_list)

    def queryTrade(self):
        filename = f"{self._moniterDir}\\XHPT_CJCX{self._runDate}.dbf"
        record_list = self._readDBF(filename)
        data_list = [
            {
                "成交日期时间": f"{record.CJRQ}{record.CJSJ}",
                "成交编号": record.CJBH.strip(),
                "成交序号": record.WTXH,
                "外部自定义序号": record.WBZDYXH,
                "产品编号": record.CPBH.strip(),
                "资产单元编号": record.ZCDYBH.strip(),
                "组合编号": record.ZHBH.strip(),
                "股东代码": record.GDDM.strip(),
                "证券代码": record.ZQDM.strip(),
                "Direction": record.WTFX.strip(),
                "Price": record.CJJG,
                "Volume": record.CJSL,
                "TradeBalence": record.CJJE,
                "总费用": record.ZFY,
                "交易所成交编号": record.JYSCJBH.strip(),
                # '处理标志':record.CLBZ.strip(),
                # '备用字段':record.BYZD.strip(),
                # '特殊标志':record.TSBS.strip(),
            }
            for record in record_list
        ]
        filename = "output/queryTrade.csv"
        print(f"write {len(data_list)} record to {filename}")
        i2OrderScanner.writeCSV(filename, data_list)


if __name__ == "__main__":
    obj = i2OrderScanner(moniterDir=r"D:\MyWork\MaidanDir")

    # query Order, Trade
    obj.queryOrder()
    obj.queryTrade()
