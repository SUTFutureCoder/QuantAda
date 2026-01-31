import datetime
import uuid

from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, JSON, ForeignKey, Index, \
    UniqueConstraint
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy_utils import database_exists, create_database

import config
from .base_recorder import BaseRecorder

Base = declarative_base()


# --- 1. 高性能表结构设计 ---

class BacktestExecution(Base):
    """
    策略执行主表
    """
    __tablename__ = 'backtest_executions'

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # 核心唯一索引：策略名
    strategy_name = Column(String(100), nullable=False)

    description = Column(String(255))
    start_time = Column(DateTime, default=datetime.datetime.now)
    updated_time = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    # 配置快照
    backtest_start_date = Column(String(20))
    backtest_end_date = Column(String(20))
    params = Column(JSON)

    # 初始资金与费率
    initial_cash = Column(Float)
    commission_scheme = Column(Float)

    # 结果字段
    final_value = Column(Float, nullable=True)
    total_return = Column(Float, nullable=True)
    annual_return = Column(Float, nullable=True)
    max_drawdown = Column(Float, nullable=True)
    sharpe_ratio = Column(Float, nullable=True)

    # [新增] 统计字段
    trade_count = Column(Integer, nullable=True)  # 交易次数
    win_rate = Column(Float, nullable=True)  # 胜率

    status = Column(String(20), default='FINISHED')

    trades = relationship("BacktestTradeLog", backref="execution", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('strategy_name', 'description', name='uix_strategy_desc'),
    )

class BacktestTradeLog(Base):
    """
    交易流水表 (保持不变)
    """
    __tablename__ = 'backtest_trade_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_id = Column(String(36), ForeignKey('backtest_executions.id'), nullable=False, index=True)

    dt = Column(DateTime, nullable=False)
    symbol = Column(String(50), nullable=False)
    action = Column(String(10), nullable=False)

    price = Column(Float)
    size = Column(Float)
    commission = Column(Float)

    cash_snapshot = Column(Float)
    value_snapshot = Column(Float)

    order_ref = Column(String(50))

    __table_args__ = (
        Index('idx_exec_time', 'execution_id', 'dt'),
    )


# --- 2. 数据库记录器逻辑 ---

class DBRecorder(BaseRecorder):
    def __init__(self, strategy_name, description, params, start_date, end_date, initial_cash, commission):
        if not config.DB_ENABLED:
            self.active = False
            return

        self.active = True
        self.engine = self._init_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        if description is None:
            description = ""

        self.meta_data = {
            "strategy_name": strategy_name,
            "description": description,
            "params": params,
            "start_date": start_date,
            "end_date": end_date,
            "initial_cash": initial_cash,
            "commission": commission,
            "start_time": datetime.datetime.now()
        }

        self.trades_buffer = []
        print(f"--- DB Recorder: Initialized (Buffered mode) for '{strategy_name}' ---")

    def _init_engine(self):
        url = make_url(config.DB_URL)
        if not database_exists(url):
            create_database(url)
        engine = create_engine(config.DB_URL)
        Base.metadata.create_all(engine)
        return engine

    def log_trade(self, dt, symbol, action, price, size, comm, order_ref, cash, value):
        if not self.active: return

        self.trades_buffer.append({
            "dt": dt,
            "symbol": symbol,
            "action": action,
            "price": price,
            "size": size,
            "commission": comm,
            "order_ref": str(order_ref),
            "cash_snapshot": cash,
            "value_snapshot": value
        })

    # 接收 trade_count 和 win_rate
    def finish_execution(self, final_value, total_return, sharpe, max_drawdown, annual_return, trade_count, win_rate):
        if not self.active: return

        print(f"--- DB Recorder: Saving results to DB... ---")
        try:
            name = self.meta_data["strategy_name"]
            desc = self.meta_data["description"]

            execution = self.session.query(BacktestExecution).filter_by(strategy_name=name, description=desc).first()
            if execution:
                self.session.query(BacktestTradeLog).filter_by(execution_id=execution.id).delete(
                    synchronize_session=False)

                execution.start_time = self.meta_data["start_time"]
                execution.updated_time = datetime.datetime.now()
                execution.params = self.meta_data["params"]
                execution.backtest_start_date = self.meta_data["start_date"]
                execution.backtest_end_date = self.meta_data["end_date"]
                execution.initial_cash = self.meta_data["initial_cash"]
                execution.commission_scheme = self.meta_data["commission"]
            else:
                execution = BacktestExecution(
                    strategy_name=name,
                    description=desc,
                    start_time=self.meta_data["start_time"],
                    updated_time=datetime.datetime.now(),
                    params=self.meta_data["params"],
                    backtest_start_date=self.meta_data["start_date"],
                    backtest_end_date=self.meta_data["end_date"],
                    initial_cash=self.meta_data["initial_cash"],
                    commission_scheme=self.meta_data["commission"]
                )
                self.session.add(execution)
                self.session.flush()

            # 2. 写入本次回测的结果数据
            execution.final_value = final_value
            execution.total_return = total_return
            execution.sharpe_ratio = sharpe
            execution.max_drawdown = max_drawdown
            execution.annual_return = annual_return
            # [新增] 写入新增字段
            execution.trade_count = trade_count
            execution.win_rate = win_rate

            execution.status = 'FINISHED'

            # 3. 批量写入 Trade Logs
            trade_objects = [
                BacktestTradeLog(
                    execution_id=execution.id,
                    **trade_data
                ) for trade_data in self.trades_buffer
            ]

            self.session.add_all(trade_objects)
            self.session.commit()
            print(f"--- DB Recorder: Success! Results saved for {name} ---")

        except Exception as e:
            self.session.rollback()
            print(f"DB Finish Error (Rolled back): {e}")
        finally:
            self.session.close()