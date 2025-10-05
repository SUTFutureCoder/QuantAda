import requests
import pandas as pd
import json
import re
from typing import Optional


class HttpExtraProvider:
    """
    一个专用于从HTTP获取额外数据的工具类。
    """

    SAMPLE_URL = ("https://72.push2delay.eastmoney.com/api/qt/clist/get?cb=jQuery112404634676980987873_1758102732377"
                  "&pn=1&pz=200&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&dect=1&wbp2u=|0|0|0|web"
                  "&fid=f3&fs=m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,"
                  "f21,f23,f24,f25,f22,f11,f62,f100,f103,f124,f128,f136,f115,f152&_=1758102732377")

    def __init__(self):
        pass

    def fetch(self) -> Optional[pd.DataFrame]:
        """
        从东方财富接口获取股票数据并解析为DataFrame

        Returns:
            pd.DataFrame | None: 包含股票数据的DataFrame，如果请求失败则返回None
        """
        try:
            # 发送HTTP请求
            response = requests.get(self.SAMPLE_URL, timeout=10)
            response.raise_for_status()  # 如果状态码不是200，抛出异常

            # 提取JSON数据（去除JSONP包装）
            json_str = self._extract_jsonp(response.text)

            # 解析JSON数据
            data_dict = json.loads(json_str)

            # 提取股票数据
            stock_data = data_dict.get('data', {}).get('diff', [])

            if not stock_data:
                print("未找到股票数据")
                return None

            # 转换为DataFrame
            df = pd.DataFrame(stock_data)

            # 添加列名映射（根据字段说明）
            column_mapping = {
                'f1': 'unknown_1',
                'f2': 'current_price',  # 当前价格
                'f3': 'change_percent',  # 涨跌幅
                'f4': 'change_amount',  # 涨跌额
                'f5': 'volume',  # 成交量
                'f6': 'turnover',  # 成交额
                'f7': 'amplitude',  # 振幅
                'f8': 'turnover_rate',  # 换手率
                'f9': 'pe_ratio',  # 市盈率
                'f10': 'pb_ratio',  # 市净率
                'f11': 'unknown_11',
                'f12': 'stock_code',  # 股票代码
                'f13': 'market_type',  # 市场类型
                'f14': 'stock_name',  # 股票名称
                'f15': 'close_price',  # 收盘价
                'f16': 'high_price',  # 最高价
                'f17': 'low_price',  # 最低价
                'f18': 'open_price',  # 开盘价
                'f20': 'total_market_cap',  # 总市值
                'f21': 'circulation_market_cap',  # 流通市值
                'f22': 'unknown_22',
                'f23': 'volume_ratio',  # 量比
                'f24': 'unknown_24',
                'f25': 'unknown_25',
                'f62': 'main_net_inflow',  # 主力净流入
                'f100': 'industry',  # 行业
                'f103': 'concepts',  # 概念
                'f115': 'unknown_115',
                'f124': 'timestamp',  # 时间戳
                'f128': 'unknown_128',
                'f140': 'unknown_140',
                'f141': 'unknown_141',
                'f136': 'unknown_136',
                'f152': 'unknown_152'
            }

            # 重命名列
            df = df.rename(columns=column_mapping)

            print(f"成功获取 {len(df)} 条股票数据")
            return df

        except requests.exceptions.RequestException as e:
            print(f"网络请求失败: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"处理数据时发生错误: {e}")
            return None

    def _extract_jsonp(self, jsonp_str: str) -> str:
        """
        从JSONP字符串中提取JSON数据

        Args:
            jsonp_str: JSONP格式的字符串

        Returns:
            str: 纯JSON字符串
        """
        # 方法1: 使用正则表达式提取JSON部分
        match = re.search(r'\(({.*})\)', jsonp_str)
        if match:
            return match.group(1)

        # 方法2: 如果正则失败，尝试直接去掉回调函数包装
        if jsonp_str.startswith('jQuery') and jsonp_str.endswith(');'):
            # 找到第一个左括号和最后一个右括号（去掉分号）
            start_idx = jsonp_str.find('(') + 1
            end_idx = jsonp_str.rfind(')')
            return jsonp_str[start_idx:end_idx]

        return jsonp_str

    def get_stock_data(self) -> Optional[pd.DataFrame]:
        """获取股票数据的便捷方法"""
        return self.fetch()
