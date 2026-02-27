import math
from datetime import datetime

import pandas as pd

try:
    from ib_insync import IB, Stock, Forex, Crypto, ContFuture, util
except ImportError:
    print("Warning: 'ib_insync' not installed. IbkrProvider will not work.")
    IB = object  # Mock for class definition

import config
from common.ib_symbol_parser import resolve_ib_contract_spec
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
        """确保连接处于活动状态 (带静默降级、防幽灵占用与自愈重建)"""
        # 如果实例丢失，尝试自动重建
        if not self.ib:
            try:
                from ib_insync import IB
                self.ib = IB()
            except ImportError:
                return False

        if self.ib.isConnected():
            return True

        import time
        import logging

        # 给 ib_insync 的原生报错装上“消音器”
        ib_client_logger = logging.getLogger('ib_insync.client')
        original_level = ib_client_logger.level
        ib_client_logger.setLevel(logging.CRITICAL)

        try:
            max_retries = 5  # 减少重试次数，5次足够了
            for attempt in range(max_retries):
                try:
                    self.ib.connect(self.host, self.port, clientId=self.client_id)
                    return True
                except Exception as e:
                    err_msg = repr(e)

                    # A. 遇到占用或握手超时，换号重试
                    if "Timeout" in err_msg or "already in use" in err_msg or "326" in err_msg:
                        self.client_id += 1
                        time.sleep(1)
                        continue

                    # B. 真网络硬错误 (没开 TWS) -> 静默跳过，让给下一个数据源
                    if "ConnectionRefusedError" in err_msg or "1225" in err_msg or "OSError" in err_msg:
                        return False

                    # C. 💥 核心修复：事件循环崩溃 -> 尝试自动重建 IB 实例 (浴火重生)
                    if "Event loop is closed" in err_msg or "RuntimeError" in err_msg:
                        # print(f"[IBKR] 自动修复：事件循环关闭，正在重建实例...")
                        try:
                            from ib_insync import IB
                            self.ib = IB()
                        except:
                            pass
                        time.sleep(1)
                        continue

                    # D. 其他未知异常 -> 打印出来排查！且【绝对不能】再写 self.ib = None 了
                    print(f"[IBKR] 连接遇到异常: {err_msg}")
                    return False

            return False

        finally:
            # 无论成功失败，恢复日志级别
            ib_client_logger.setLevel(original_level)

    def _parse_contract(self, symbol: str):
        spec = resolve_ib_contract_spec(symbol)

        if spec['kind'] == 'forex':
            return Forex(spec['pair'])

        if spec['kind'] == 'crypto':
            return Crypto(spec['symbol'], spec['exchange'], spec['currency'])

        if spec['primary_exchange']:
            return Stock(
                spec['symbol'],
                spec['exchange'],
                spec['currency'],
                primaryExchange=spec['primary_exchange']
            )

        return Stock(spec['symbol'], spec['exchange'], spec['currency'])

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
            # 3. 请求历史数据 (阻塞式调用，带超时与降级兜底)
            try:
                hist_timeout = float(getattr(config, 'IBKR_HIST_TIMEOUT_SEC', 8.0))
            except Exception:
                hist_timeout = 8.0
            hist_timeout = max(1.0, hist_timeout)

            # 外汇不使用 RTH；股票默认用 RTH，避免盘前盘后噪声。
            default_use_rth = False if contract.secType == 'CASH' else True

            request_plan = [(what_to_show, default_use_rth, hist_timeout, 1)]
            if contract.secType == 'STK' and what_to_show == 'ADJUSTED_LAST':
                # ADJUSTED_LAST 在部分账户/时段会超时，先短超时重试两次，再降级 TRADES。
                fallback_timeout = max(1.0, min(hist_timeout, 5.0))
                try:
                    adjusted_retry_times = int(getattr(config, 'IBKR_ADJUSTED_LAST_RETRIES', 2))
                except Exception:
                    adjusted_retry_times = 2
                adjusted_retry_times = max(0, adjusted_retry_times)

                # attempt 编号从 2 开始，1 号已由首次请求占用。
                for idx in range(adjusted_retry_times):
                    request_plan.append(('ADJUSTED_LAST', default_use_rth, fallback_timeout, idx + 2))
                request_plan.append(('TRADES', default_use_rth, fallback_timeout, 1))
                request_plan.append(('TRADES', False, fallback_timeout, 2))

            bars = None
            request_errors = []
            selected_mode = what_to_show

            for mode, use_rth, timeout_sec, attempt in request_plan:
                try:
                    bars = self.ib.reqHistoricalData(
                        contract,
                        endDateTime=req_end_date,  # 动态调整
                        durationStr=duration_str,
                        barSizeSetting=bar_size,
                        whatToShow=mode,
                        useRTH=use_rth,
                        formatDate=1,
                        timeout=timeout_sec
                    )
                    if bars:
                        selected_mode = mode
                        break
                    request_errors.append(f"{mode}#{attempt}/useRTH={use_rth}: empty")
                except Exception as e:
                    request_errors.append(f"{mode}#{attempt}/useRTH={use_rth}: {e}")
                    print(
                        f"[IBKR] Historical request failed for {contract.symbol} "
                        f"[{mode} attempt#{attempt}, useRTH={use_rth}, timeout={timeout_sec:.1f}s]: {e}"
                    )

            if not bars:
                print(f"[IBKR] No data returned for {symbol}")
                if request_errors:
                    print(f"[IBKR] Historical attempts: {' | '.join(request_errors)}")
                return None

            if selected_mode != what_to_show:
                print(f"[IBKR] Historical fallback in use for {contract.symbol}: {selected_mode}")

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
