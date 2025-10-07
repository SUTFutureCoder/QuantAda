import pandas as pd

try:
    from sqlalchemy import create_engine, text
except ImportError:
    create_engine = None


class MysqlExtraProvider:
    """
    一个专用于从MySQL获取额外数据的工具类。
    """

    def __init__(self):
        if create_engine is None:
            raise ImportError("'sqlalchemy' is required. Please run: pip install sqlalchemy")
        self.db_config = {
            'host': 'IP',
            'user': 'USER',
            'password': 'PWD',
            'database': 'DATABASE'
        }

        connection_string = f"mysql+mysqlconnector://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}/{self.db_config['database']}"
        self.engine = create_engine(connection_string)

    def query(self, query, index_col=None, parse_dates=None) -> pd.DataFrame:
        try:
            df = pd.read_sql(query, self.engine, index_col=index_col, parse_dates=parse_dates)
            if df.empty:
                return None
            return df

        except Exception as e:
            print(f"SQLAlchemy failed to query data: {e} query: {query}")
            return None

