from abc import ABC, abstractmethod

import pandas


class BaseSelector(ABC):
    """
    选股器的抽象基类。
    它的唯一职责是输出一个股票代码列表。
    """
    def __init__(self, data_manager):
        """
        初始化时接收一个DataManager实例，以便在复杂的选股逻辑中获取数据。
        """
        self.data_manager = data_manager

    @abstractmethod
    def run_selection(self) -> list[str] | pandas.DataFrame:
        """
        【核心】执行选股逻辑并返回一个包含标的代码字符串的列表。如果返回DataFrame，需要将标的设置为index。
        """
        pass
