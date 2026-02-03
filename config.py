# --- 交易及框架基础配置 ---
# 股数取整（A股100，IBKR、国际市场或加密货币1）
DEFAULT_LOT_SIZE = 100

# 年交易日，如果是加密货币请设置为365
ANNUAL_FACTOR = 252

# 限价单即时成交的溢价比例 (默认 2%)
# 作用：实盘下单时，以 现价 * (1 ± SLIPPAGE) 发送限价单。
# 目的：既模拟市价单的成交速度，又避免因价格瞬间剧烈波动造成的资金透支或高位接盘。
LIVE_LIMIT_ORDER_SLIPPAGE = 0.02

# 数据缓存路径
DATA_PATH = '.data'

# 缓存数据
CACHE_DATA = False

# 是否打印详细交易日志
LOG = True


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
            'host': '127.0.0.1',
            'port': 7497,  # TWS Paper默认端口
            'client_id': 1,
            'account': 'DU12345'  # 可选
        },
        'real': {  # 实盘/Docker Gateway
            'host': '127.0.0.1',
            'port': 4001,  # Docker Gateway 通常暴露 4001/4002
            'client_id': 99,  # 实盘建议用不同的ID
        }
    }
}