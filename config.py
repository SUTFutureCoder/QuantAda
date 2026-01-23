# 股数取整（A股100，IBKR、国际市场或加密货币1）
DEFAULT_LOT_SIZE = 100

# 数据缓存路径
DATA_PATH = 'data'

# 缓存数据
CACHE_DATA = False

# 是否打印详细交易日志
LOG = True

# Tushare API Token
# 请到 https://tushare.pro/user/token 免费注册获取
TUSHARE_TOKEN = 'your_tushare_token_here'

# 山西证券Tushare API Token
# 用于演示快速接入数据源
SXSC_TUSHARE_TOKEN = 'your_sxsc_tushare_token_here'

# 是否开启数据库记录
DB_ENABLED = True

# 数据库连接字符串
# 格式: dialect+driver://username:password@host:port/database
# 示例 (MySQL): 'mysql+pymysql://root:123456@localhost:3306/quantada_db'
# 示例 (SQLite): 'sqlite:///quantada_logs.db'
DB_URL = 'mysql+pymysql://root:yourpassword@localhost:3306/quant'