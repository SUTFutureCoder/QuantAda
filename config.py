# --- 交易及框架基础配置 ---
# 股数取整（A股100，IBKR、国际市场或加密货币1）
LOT_SIZE = 100

# 年交易日，如果是加密货币请设置为365
ANNUAL_FACTOR = 252

# 数据缓存路径
DATA_PATH = '.data'

# 缓存数据，常用于离线或数据源不稳定等情况。使用后请使用--refresh或手动删除缓存目录下文件
CACHE_DATA = False

# 是否打印详细交易日志
LOG = True

# 是否打印交易计划
PRINT_PLAN = False

# 保护标的不参与策略计算，优先级低于策略参数。适用于压舱石；尊重客户主观选择实现虚拟多租户分仓；跨越法币-券商通道摩擦，券商内部转换标的，避免银行无法打款。
IGNORED_SYMBOLS = ['SGOV', 'BIL', 'USFR', 'SHY']


# --- 数据源配置 ---
# Tushare API Token
# 请到 https://tushare.pro/user/token 免费注册获取
TUSHARE_TOKEN = 'your_token_here'

# 山西证券Tushare API Token
# 用于演示快速接入数据源
SXSC_TUSHARE_TOKEN = 'your_token_here'

# 掘金API Token 格式： TOKEN[|HOST:PORT]
GM_TOKEN = 'your_token_here|host:port'

# Tiingo API Token
TIINGO_TOKEN = 'your_token_here'


# --- 报警与监控配置 ---
ALARMS_ENABLED = False

# 钉钉机器人 Webhook
# 格式: 'https://oapi.dingtalk.com/robot/send?access_token=xxxx'
DINGTALK_WEBHOOK = ''

# 企业微信机器人 Webhook
# 格式: 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
WECOM_WEBHOOK = ''

# 报警级别过滤: INFO, WARNING, ERROR, CRITICAL
ALARM_LEVEL = 'INFO'


# --- 数据库记录配置 ---
# 是否开启数据库记录
DB_ENABLED = False

# 数据库连接字符串
# 格式: dialect+driver://username:password@host:port/database
# 示例 (MySQL): 'mysql+pymysql://root:123456@localhost:3306/quantada_db'
# 示例 (SQLite): 'sqlite:///quantada_logs.db'
DB_URL = 'mysql+pymysql://root:yourpassword@localhost:3306/quant'


# --- 机器学习优化器配置 ---
# 参数优化实时看板端口
OPTUNA_DASHBOARD_PORT = 8090


# --- IBKR配置 ---
IBKR_HOST = '127.0.0.1'
# 文件-全局配置-API-设置-启用套接字客户端&关闭只读API, 实盘与模拟端口不一样，IB Gateway默认4001
IBKR_PORT = 7497
IBKR_CLIENT_ID = 999


# --- 框架 → 实盘/仿真连接通道配置 ---
# BROKER_ENVIRONMENTS 的 Key 必须与 adapters 下的文件名一致 (如 gm_broker.py -> "gm_broker")
BROKER_ENVIRONMENTS = {
    "gm_broker": {
        'sim': {
            'strategy_id': 'xxx',
            'token': 'xxx',
            'serv_addr': '127.0.0.1:7001',
            'schedule': '1d:14:45:00'
        },
        'real': {
            'strategy_id': 'xxx',
            'token': 'xxx',
            'serv_addr': '127.0.0.1:7001',
            'schedule': '1d:14:45:00'
        }
    },

    # IB 配置
    "ib_broker": {
        'sim': {  # 模拟盘/Paper Trading
            'schedule': '1d:15:45:00',
            'timezone': 'America/New_York',
            # 实盘特定不卖的长线资产
            # 'ignored_symbols': ['AAPL', 'TSLA']
        },
        'real': {  # 实盘/Docker Gateway
            'schedule': '1d:15:45:00',
            'timezone': 'America/New_York',
            # 实盘特定不卖的长线资产
            # 'ignored_symbols': ['AAPL', 'TSLA']
        }
    }
}