import re
from typing import Dict, Optional

# 常见交易所提示集合:
# - 显式白名单用于优先支持常见路由
# - 同时配合规则匹配，避免每次新增交易所都要改代码
US_PRIMARY_EXCHANGE_HINTS = {
    'SMART', 'ISLAND', 'NASDAQ', 'ARCA', 'NYSE', 'AMEX', 'BATS', 'PINK',
    'IEX', 'CBOE', 'MEMX', 'EDGX', 'EDGEA', 'BYX', 'BEX', 'NYSENAT'
}

CN_PREFIXES = {'SHSE', 'SZSE'}
HK_PREFIXES = {'SEHK', 'HK'}

# 用于避免把 BTC.USD、EUR.USD 误识别成 Ticker.Exchange
COMMON_CURRENCIES = {
    'USD', 'EUR', 'GBP', 'JPY', 'HKD', 'CNH', 'CNY',
    'AUD', 'CAD', 'CHF', 'NZD', 'SGD'
}

# 交易所代码通常是 2-10 位英文字母 (如 IEX、NASDAQ、NYSENAT)
EXCHANGE_CODE_RE = re.compile(r'^[A-Z]{2,10}$')


def _is_likely_exchange_token(token: str) -> bool:
    token = str(token or '').strip().upper()
    if not token:
        return False
    if token in COMMON_CURRENCIES:
        return False
    return token in US_PRIMARY_EXCHANGE_HINTS or bool(EXCHANGE_CODE_RE.fullmatch(token))


def resolve_ib_contract_spec(symbol: str) -> Dict[str, Optional[str]]:
    """
    将用户输入的 symbol 解析成统一规格，供 Broker 与 DataProvider 复用。
    返回字段:
    - kind: 'stock' | 'forex' | 'crypto'
    - stock: symbol, exchange, currency, primary_exchange
    - forex: pair
    - crypto: symbol, exchange, currency
    """
    raw = str(symbol or '').strip()
    sym = raw.upper()

    # 默认兜底: 直接当作美股 SMART/USD
    default = {
        'kind': 'stock',
        'symbol': sym,
        'exchange': 'SMART',
        'currency': 'USD',
        'primary_exchange': None,
    }

    if not sym:
        return default

    parts = sym.split('.')

    # 1) 标准三段式: STK.AAPL.USD / CASH.EUR.USD / CRYPTO.BTC.USD
    if len(parts) == 3:
        sec_type, p1, p2 = parts
        if sec_type == 'STK':
            return {
                'kind': 'stock',
                'symbol': p1,
                'exchange': 'SMART',
                'currency': p2,
                'primary_exchange': None,
            }
        if sec_type == 'CASH':
            return {'kind': 'forex', 'pair': f'{p1}{p2}'}
        if sec_type == 'CRYPTO':
            return {'kind': 'crypto', 'symbol': p1, 'exchange': 'PAXOS', 'currency': p2}

    # 2) 兼容 A 股/港股前缀
    if len(parts) == 2:
        p1, p2 = parts
        if p1 in CN_PREFIXES:
            # A股走深港/沪港通
            return {
                'kind': 'stock',
                'symbol': p2,
                'exchange': 'SEHK',
                'currency': 'CNH',
                'primary_exchange': None,
            }

        if p1 in HK_PREFIXES:
            hk_code = str(int(p2)) if p2.isdigit() else p2
            return {
                'kind': 'stock',
                'symbol': hk_code,
                'exchange': 'SEHK',
                'currency': 'HKD',
                'primary_exchange': None,
            }

        # 2.1) Exchange.Ticker 形式: NASDAQ.AAPL
        if p1 in US_PRIMARY_EXCHANGE_HINTS:
            return {
                'kind': 'stock',
                'symbol': p2,
                'exchange': 'SMART',
                'currency': 'USD',
                'primary_exchange': None if p1 == 'SMART' else p1,
            }

        # 2.2) Forex 简写: EUR.USD
        if p1 in COMMON_CURRENCIES and p2 in COMMON_CURRENCIES:
            return {'kind': 'forex', 'pair': f'{p1}{p2}'}

        # 2.3) Ticker.Exchange 形式: AAPL.IEX / QQQ.ISLAND / EWJ.SMART
        if _is_likely_exchange_token(p2):
            return {
                'kind': 'stock',
                'symbol': p1,
                'exchange': 'SMART',
                'currency': 'USD',
                'primary_exchange': None if p2 == 'SMART' else p2,
            }

    # 3) 兼容纯数字港股代码
    if sym.isdigit() or (len(sym) == 5 and sym.startswith('0')):
        code = str(int(sym))
        return {
            'kind': 'stock',
            'symbol': code,
            'exchange': 'SEHK',
            'currency': 'HKD',
            'primary_exchange': None,
        }

    return default
