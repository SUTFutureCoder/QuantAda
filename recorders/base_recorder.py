from abc import ABC, abstractmethod

class BaseRecorder(ABC):
    """
    Recorder 抽象基类。
    所有具体的 Recorder (DB, HTTP, File) 都必须继承此类。
    """

    @abstractmethod
    def log_trade(self, dt, symbol, action, price, size, comm, order_ref, cash, value):
        """记录单笔交易"""
        pass

    @abstractmethod
    def finish_execution(self, final_value, total_return, sharpe, max_drawdown, annual_return, trade_count, win_rate):
        """记录回测结束时的最终绩效"""
        pass