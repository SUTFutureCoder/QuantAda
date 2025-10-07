import pandas as pd

from data_extra_providers.mysql_extra_provider import MysqlExtraProvider


class TemperatureHistoryProvider:

    def fetch(self) -> pd.DataFrame | None:
        """
        获取指定日期范围内的指数数据。
        :return: 以datetime为索引的DataFrame，或在失败时返回None。
        """
        try:
            query = f"""
                   SELECT date, close, degree, asset_rate, avg_return_3, return_day, rw_pb, ds
                   FROM long_term_temperate_history 
                   ORDER BY ds ASC
               """

            # 使用 SQLAlchemy 引擎
            df = MysqlExtraProvider().query(query, index_col='date')
            if df.empty:
                return None
            return df

        except Exception as e:
            print(f"SQLAlchemy failed to fetch data: {e}")
            return None