import datetime
import uuid
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Text, JSON, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import database_exists, create_database
from .base_recorder import BaseRecorder

import config

Base = declarative_base()


class BacktestExecution(Base):
    """回测执行记录主表"""
    __tablename__ = 'backtest_executions'

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    strategy_name = Column(String(100), index=True)
    description = Column(String(255))  # 对应 desc 参数
    start_time = Column(DateTime, default=datetime.datetime.now)  # 实际执行时间

    # 回测配置
    backtest_start_date = Column(String(20))
    backtest_end_date = Column(String(20))
    params = Column(JSON)  # 策略参数快照

    # 绩效结果
    initial_cash = Column(Float)
    final_value = Column(Float)
    total_return = Column(Float)
    annual_return = Column(Float)
    max_drawdown = Column(Float)
    sharpe_ratio = Column(Float)

    status = Column(String(20), default='RUNNING')  # RUNNING, FINISHED, FAILED


class BacktestTradeLog(Base):
    """回测交易流水表"""
    __tablename__ = 'backtest_trade_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_id = Column(String(36), index=True)  # 关联主表

    dt = Column(DateTime, nullable=False)  # 交易发生的 K 线时间
    symbol = Column(String(50), nullable=False)
    action = Column(String(10), nullable=False)  # BUY / SELL

    price = Column(Float)
    size = Column(Float)
    commission = Column(Float)

    # 快照数据
    cash_snapshot = Column(Float)  # 交易后的现金
    value_snapshot = Column(Float)  # 交易后的账户总值

    # 辅助字段，用于去重 (Backtrader 内部 Order Ref)
    order_ref = Column(String(50))

    # 1. 创建唯一索引，满足"执行流水表创建唯一索引"的需求
    # 联合索引确保同一条回测记录下的同一个订单操作是唯一的
    __table_args__ = (
        UniqueConstraint('execution_id', 'dt', 'symbol', 'order_ref', 'action', name='uq_exec_trade'),
    )


class DBRecorder(BaseRecorder):
    def __init__(self, strategy_name, description, params, start_date, end_date, initial_cash):
        if not config.DB_ENABLED:
            self.active = False
            return

        self.active = True
        self.engine = self._init_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        # 初始化执行记录
        self.execution = BacktestExecution(
            strategy_name=strategy_name,
            description=description,
            params=params,
            backtest_start_date=start_date,
            backtest_end_date=end_date,
            initial_cash=initial_cash
        )
        self.session.add(self.execution)
        self.session.commit()
        self.execution_id = self.execution.id
        print(f"--- DB Recording Started: ID {self.execution_id} ---")

    def _init_engine(self):
        """初始化数据库连接，不存在则创建库和表"""
        url = make_url(config.DB_URL)

        # 3. 当不存在这张表时，请尝试创建库表
        # 使用 sqlalchemy_utils 检查并创建数据库
        if not database_exists(url):
            print(f"Database {url.database} does not exist. Creating...")
            create_database(url)

        engine = create_engine(config.DB_URL)
        Base.metadata.create_all(engine)  # 创建表
        return engine

    def log_trade(self, dt, symbol, action, price, size, comm, order_ref, cash, value):
        """
        记录交易流水
        1. 满足"insert on update，允许重复执行"的需求
        使用 session.merge (Upsert) 逻辑
        """
        if not self.active: return

        # 构建记录对象
        log_entry = BacktestTradeLog(
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

        try:
            # merge 会根据主键或唯一索引判断：存在则更新，不存在则插入
            # 这里虽然 execution_id 每次运行通常不同，但如果用户手动指定 ID 或逻辑重试
            # merge 能保证幂等性
            self.session.merge(log_entry)
            self.session.commit()
        except SQLAlchemyError as e:
            print(f"DB Log Error: {e}")
            self.session.rollback()

    def finish_execution(self, final_value, total_return, sharpe, max_drawdown, annual_return):
        """5. 执行完毕后，将最终的绩效数据填写到执行记录表中"""
        if not self.active: return

        try:
            self.execution.final_value = final_value
            self.execution.total_return = total_return
            self.execution.sharpe_ratio = sharpe
            self.execution.max_drawdown = max_drawdown
            self.execution.annual_return = annual_return
            self.execution.status = 'FINISHED'
            self.session.commit()
            print("--- DB Recording Finished ---")
        except Exception as e:
            print(f"DB Update Error: {e}")
        finally:
            self.session.close()