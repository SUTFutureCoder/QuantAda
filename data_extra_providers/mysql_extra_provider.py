import pandas as pd

try:
    import mysql.connector
except ImportError:
    mysql = None


class MysqlExtraProvider:
    """
    一个专用于从MySQL获取额外数据的工具类。
    """

    def __init__(self):
        if mysql is None:
            raise ImportError("'mysql-connector-python' is required. Please run: pip install mysql-connector-python")
        self.db_config = {
            'host': 'IP',
            'user': 'USER',
            'password': 'PWD',
            'database': 'DATABASE'
        }
        self.table_name = 'TABLENAME'

    def fetch(self, start_date, end_date) -> pd.DataFrame | None:
        """
        获取指定日期范围内的指数数据。
        :param start_date: 开始日期 (datetime.date or str 'YYYY-MM-DD')
        :param end_date: 结束日期 (datetime.date or str 'YYYY-MM-DD')
        :return: 以datetime为索引的DataFrame，或在失败时返回None。
        """
        conn = None
        try:
            query = f"""
                SELECT date, close, degree, asset_rate, avg_return_3, return_day, rw_pb, ds
                FROM {self.table_name}  
                WHERE ds BETWEEN %s AND %s
                ORDER BY ds ASC
            """
            # 格式化日期为 'YYYYMMDD'
            start_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
            end_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
            params = (start_str, end_str)

            conn = mysql.connector.connect(**self.db_config)
            df = pd.read_sql(query, conn, params=params, index_col='ds', parse_dates={'ds': '%Y-%m-%d'})

            if df.empty:
                return None

            df.index.name = 'ds'
            return df

        except Exception as e:
            print(f"MysqlIndexProvider failed to fetch data: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()
