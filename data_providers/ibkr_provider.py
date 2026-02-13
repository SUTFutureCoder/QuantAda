import math
from datetime import datetime

import pandas as pd

try:
    from ib_insync import IB, Stock, Forex, Crypto, ContFuture, util
except ImportError:
    print("Warning: 'ib_insync' not installed. IbkrProvider will not work.")
    IB = object  # Mock for class definition

import config
from data_providers.base_provider import BaseDataProvider


class IbkrDataProvider(BaseDataProvider):
    """
    Interactive Brokers (IBKR) 数据源

    特点：
    1. 全球多资产覆盖 (美股, 港股, 外汇, 期货, 期权)。
    2. 数据质量极高，支持分红调整 (ADJUSTED_LAST)。
    3. 需要本地运行 TWS 或 IB Gateway。
    """

    PRIORITY = 40  # 优先级略高于 Tiingo (假设有 IB 账户通常优先用 IB)

    def __init__(self, ib_instance=None):
        """
        初始化 IB 连接
        :param host: TWS/Gateway IP (通常是 127.0.0.1)
        :param port: TWS 默认 7496 文件-全局配置-API-设置-启用套接字客户端&关闭只读API, IB Gateway 默认 4001
        :param client_id: 独立的 Client ID，防止冲突
        """
        self.host = config.IBKR_HOST
        self.port = config.IBKR_PORT
        self.client_id = config.IBKR_CLIENT_ID

        if ib_instance:
            self.ib = ib_instance
            # 否则尝试创建新实例 (用于回测或独立调用)
        elif IB is not object:
            self.ib = IB()
        else:
            self.ib = None

    def _connect(self):
        """确保连接处于活动状态"""
        if not self.ib:
            return False

        if not self.ib.isConnected():
            import time
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    self.ib.connect(self.host, self.port, clientId=self.client_id)
                    return True
                except Exception as e:
                    # 使用 repr 捕获 TimeoutError 字面量
                    err_msg = repr(e)

                    # 只要是超时（TWS拒载抛出的默认异常）或明确的报错，全部执行换号重试
                    if "Timeout" in err_msg or "already in use" in err_msg or "326" in err_msg:
                        self.client_id += 1
                        print(f"[IBKR] 🔄 遇到幽灵占用或握手超时，自动将 clientId 切换为 {self.client_id} 并重试...")
                        time.sleep(1)
                        continue

                    # 如果是 ConnectionRefusedError 等真正的硬核网络错误，直接打印并失败
                    print(f"[IBKR] 真网络硬错误: {err_msg}")
                    return False

            print("[IBKR] ❌ 重试次数耗尽，无法连接到 TWS/Gateway。")
            return False

        return True

    def _parse_contract(self, symbol: str):
        parts = symbol.split('.')

        # 情况 1: 标准 Backtrader 格式 (Type.Ticker.Currency)
        if len(parts) == 3:
            sec_type, ticker, currency = parts
            if sec_type == 'STK':
                return Stock(ticker, 'SMART', currency)
            elif sec_type == 'CASH':
                return Forex(f"{ticker}{currency}")
            elif sec_type == 'CRYPTO':
                return Crypto(ticker, 'PAXOS', currency)

        # 情况 2: 处理两段式 (可能是 Ticker.Exchange 也可能是 Exchange.Ticker)
        if len(parts) == 2:
            p1, p2 = parts

            # A. 识别美股 Ticker.Exchange (如 QQQ.ISLAND)
            # 常用美股主交易所白名单
            us_exchanges = ['ISLAND', 'NASDAQ', 'ARCA', 'NYSE', 'AMEX', 'BATS', 'PINK', 'SMART']
            if p2 in us_exchanges:
                # 关键修正：拆分 symbol 和 primaryExchange
                return Stock(p1, 'SMART', 'USD', primaryExchange=p2)

            # B. 识别港股/A股 Exchange.Ticker (如 SEHK.700)
            if p1 in ['SEHK', 'HK']:
                return Stock(p2, 'SEHK', 'HKD')

        # 情况 3: 默认作为美股 Ticker 处理
        return Stock(symbol, 'SMART', 'USD')

    def _calc_duration(self, start_date, end_date):
        """计算 IB API 需要的 durationStr"""
        if not start_date:
            return "1 Y"  # 默认回溯1年

        start_dt = pd.to_datetime(start_date)
        # 如果没有 end_date，默认为今天
        end_dt = pd.to_datetime(end_date) if end_date else datetime.now()

        delta = end_dt - start_dt
        days = delta.days + 1  # 多取一点buffer

        if days < 365:
            return f"{days} D"
        else:
            years = math.ceil(days / 365)
            return f"{years} Y"

    def get_data(self, symbol, start_date=None, end_date=None, timeframe='Days', compression=1):
        if not self._connect():
            return None

        contract = self._parse_contract(symbol)

        # 1. 尝试标准化合约 (获取准确的 localSymbol, exchange 等)
        # 这一步是可选的，但在实盘中非常重要，可以防止歧义
        try:
            details = self.ib.reqContractDetails(contract)
            if not details:
                print(f"[IBKR] Symbol not found: {symbol}")
                return None
            contract = details[0].contract
            # print(f"[IBKR] Resolved contract: {contract.localSymbol} @ {contract.exchange}")
        except Exception as e:
            print(f"[IBKR] Error resolving contract {symbol}: {e}")
            return None

        # 2. 决定数据类型 (whatToShow) 和 请求参数
        # 默认规则: 股票用 ADJUSTED_LAST，外汇用 MIDPOINT，其他用 TRADES
        what_to_show = 'TRADES'
        if contract.secType == 'STK':
            what_to_show = 'ADJUSTED_LAST'
        elif contract.secType == 'CASH':
            what_to_show = 'MIDPOINT'

        # 3. 处理时间参数
        # ADJUSTED_LAST 不支持指定 endDateTime，必须为空
        req_end_date = ''
        calc_end_date = end_date

        if what_to_show == 'ADJUSTED_LAST':
            # 强制请求截至当前的数据
            req_end_date = ''
            # 既然截至当前，计算 duration 时必须以 'now' 为终点，
            # 否则如果 start_date 是 3 年前，end_date 是 2 年前，
            # 用 end_date 算出的 1 年 duration 从 now 倒推回去，就只包含最近 1 年，完全错过了目标区间。
            calc_end_date = datetime.now()
        else:
            # 其他类型正常处理
            if end_date:
                end_dt = pd.to_datetime(end_date)
                req_end_date = end_dt.strftime('%Y%m%d 23:59:59')
            else:
                req_end_date = ''
                calc_end_date = datetime.now()

        duration_str = self._calc_duration(start_date, calc_end_date)

        # Bar Size 映射
        bar_size = "1 day"
        if timeframe == 'Minutes':
            bar_size = f"{compression} min"
        elif timeframe == 'Weeks':
            bar_size = "1 week"

        print(f"[IBKR] Fetching {contract.symbol} ({duration_str}) [{what_to_show}]...")

        try:
            # 3. 请求历史数据 (阻塞式调用)
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime=req_end_date,  # 动态调整
                durationStr=duration_str,
                barSizeSetting=bar_size,
                whatToShow=what_to_show,  # 动态调整
                useRTH=True,
                formatDate=1
            )

            if not bars:
                print(f"[IBKR] No data returned for {symbol}")
                return None

            # 4. 转换为 DataFrame
            df = util.df(bars)

            if df is None or df.empty:
                return None

            if 'date' in df.columns:
                df.rename(columns={'date': 'datetime'}, inplace=True)

            # 处理 datetime 索引
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)

            # 裁剪日期 (因为 durationStr 可能会取多一点数据)
            if start_date:
                df = df[df.index >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df.index <= pd.to_datetime(end_date)]

            # 确保列存在
            cols = ['open', 'high', 'low', 'close', 'volume']
            existing_cols = [c for c in cols if c in df.columns]
            return df[existing_cols]

        except Exception as e:
            print(f"[IBKR] Error fetching data for {symbol}: {e}")
            return None

    def __del__(self):
        """析构时断开连接，避免僵尸连接"""
        if self.ib and self.ib.isConnected():
            try:
                self.ib.disconnect()
            except:
                pass


if __name__ == '__main__':
    # 单元测试 (需要开启 TWS/Gateway)
    p = IbkrDataProvider()

    print("\n--- Test US Stock ---")
    df = p.get_data("STK.NVDA.USD", start_date="20240101")
    if df is not None:
        print(df.tail())
    else:
        print("Test failed or TWS not running.")