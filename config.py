# --- 交易及框架基础配置 ---
# 股数取整（A股100，IBKR、国际市场或加密货币1）
DEFAULT_LOT_SIZE = 100

# 年交易日，如果是加密货币请设置为365
ANNUAL_FACTOR = 252

# 数据缓存路径
DATA_PATH = '.data'

# 缓存数据
CACHE_DATA = False

# 是否打印详细交易日志
LOG = True


# --- 数据源配置 ---
# Tushare API Token
# 请到 https://tushare.pro/user/token 免费注册获取
TUSHARE_TOKEN = 'your_tushare_token_here'

# 山西证券Tushare API Token
# 用于演示快速接入数据源
SXSC_TUSHARE_TOKEN = 'your_sxsc_tushare_token_here'

# 掘金API Token
GM_TOKEN = 'your_gm_token_here'


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
