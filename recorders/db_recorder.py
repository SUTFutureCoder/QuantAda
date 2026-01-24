import datetime
import uuid

from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, JSON, ForeignKey, Index
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
    strategy_name = Column(String(100), unique=True, nullable=False)

    description = Column(String(255))
    start_time = Column(DateTime, default=datetime.datetime.now)
    updated_time = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    # 配置快照
    backtest_start_date = Column(String(20))
    backtest_end_date = Column(String(20))
    params = Column(JSON)

    # 初始资金与费率 (新增 commission_scheme)
    initial_cash = Column(Float)
    commission_scheme = Column(Float)  # 记录 --commission 参数

    # 结果字段
    final_value = Column(Float, nullable=True)
    total_return = Column(Float, nullable=True)
    annual_return = Column(Float, nullable=True)
    max_drawdown = Column(Float, nullable=True)
    sharpe_ratio = Column(Float, nullable=True)

    status = Column(String(20), default='RUNNING')

    trades = relationship("BacktestTradeLog", backref="execution", cascade="all, delete-orphan")


class BacktestTradeLog(Base):
    """
    交易流水表
    """
    __tablename__ = 'backtest_trade_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)

    execution_id = Column(String(36), ForeignKey('backtest_executions.id'), nullable=False, index=True)

    dt = Column(DateTime, nullable=False)
    symbol = Column(String(50), nullable=False)
    action = Column(String(10), nullable=False)  # BUY / SELL

    price = Column(Float)  # 决策价格 (通常是当前Close)
    size = Column(Float)  # 决策数量
    commission = Column(Float)  # 预估手续费

    # 资金快照
    cash_snapshot = Column(Float)
    value_snapshot = Column(Float)

    order_ref = Column(String(50))

    __table_args__ = (
        Index('idx_exec_time', 'execution_id', 'dt'),
    )


# --- 2. 数据库记录器逻辑 ---

class DBRecorder(BaseRecorder):
    # 修改 init 增加 commission
    def __init__(self, strategy_name, description, params, start_date, end_date, initial_cash, commission):
        if not config.DB_ENABLED:
            self.active = False
            return

        self.active = True
        self.engine = self._init_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        self.execution_id = self._initialize_execution_record(
            strategy_name, description, params, start_date, end_date, initial_cash, commission
        )
        print(f"--- DB Recorder: Overwriting record for '{strategy_name}' (ID: {self.execution_id}) ---")

    def _init_engine(self):
        url = make_url(config.DB_URL)
        if not database_exists(url):
            create_database(url)
        engine = create_engine(config.DB_URL)
        Base.metadata.create_all(engine)
        return engine

    # 修改初始化逻辑增加 commission
    def _initialize_execution_record(self, name, desc, params, start, end, cash, comm):
        try:
            execution = self.session.query(BacktestExecution).filter_by(strategy_name=name).first()

            if execution:
                # 复用与清洗
                exec_id = execution.id
                self.session.query(BacktestTradeLog).filter_by(execution_id=exec_id).delete(synchronize_session=False)

                execution.status = 'RUNNING'
                execution.description = desc
                execution.start_time = datetime.datetime.now()
                execution.params = params
                execution.backtest_start_date = start
                execution.backtest_end_date = end
                execution.initial_cash = cash
                execution.commission_scheme = comm  # 更新费率

                execution.final_value = None
                execution.total_return = None
                execution.annual_return = None
                execution.max_drawdown = None
                execution.sharpe_ratio = None

                self.execution = execution

            else:
                # 新建
                self.execution = BacktestExecution(
                    strategy_name=name,
                    description=desc,
                    params=params,
                    backtest_start_date=start,
                    backtest_end_date=end,
                    initial_cash=cash,
                    commission_scheme=comm  # 记录费率
                )
                self.session.add(self.execution)
                self.session.flush()
                exec_id = self.execution.id

            self.session.commit()
            return exec_id

        except Exception as e:
            self.session.rollback()
            print(f"DB Init Error: {e}")
            raise e

    def log_trade(self, dt, symbol, action, price, size, comm, order_ref, cash, value):
        if not self.active: return

        log = BacktestTradeLog(
            execution_id=self.execution_id,
            dt=dt,
            symbol=symbol,
            action=action,
            price=price,
            size=size,
            commission=comm,
            order_ref=str(order_ref),
            cash_snapshot=cash,
            value_snapshot=value
        )
        self.session.add(log)
        self.session.commit()

    def finish_execution(self, final_value, total_return, sharpe, max_drawdown, annual_return):
        if not self.active: return
        try:
            self.execution.final_value = final_value
            self.execution.total_return = total_return
            self.execution.sharpe_ratio = sharpe
            self.execution.max_drawdown = max_drawdown
            self.execution.annual_return = annual_return
            self.execution.status = 'FINISHED'
            self.session.commit()
            print(f"--- DB Recorder: Results saved for {self.execution.strategy_name} ---")
        except Exception as e:
            print(f"DB Finish Error: {e}")
            self.session.rollback()
        finally:
            self.session.close()