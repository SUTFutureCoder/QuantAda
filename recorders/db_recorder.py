import datetime
import uuid

from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, JSON, ForeignKey, Index, UniqueConstraint
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy_utils import database_exists, create_database

import config
from .base_recorder import BaseRecorder

Base = declarative_base()


# --- 1. 高性能表结构设计 (保持不变) ---

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

    status = Column(String(20), default='FINISHED')  # 默认为 FINISHED，因为我们只保存完成的

    trades = relationship("BacktestTradeLog", backref="execution", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('strategy_name', 'description', name='uix_strategy_desc'),
    )

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

    price = Column(Float)
    size = Column(Float)
    commission = Column(Float)

    # 资金快照
    cash_snapshot = Column(Float)
    value_snapshot = Column(Float)

    order_ref = Column(String(50))

    __table_args__ = (
        Index('idx_exec_time', 'execution_id', 'dt'),
    )


# --- 2. 数据库记录器逻辑 (核心修改) ---

class DBRecorder(BaseRecorder):
    def __init__(self, strategy_name, description, params, start_date, end_date, initial_cash, commission):
        if not config.DB_ENABLED:
            self.active = False
            return

        self.active = True
        self.engine = self._init_engine()
        self.Session = sessionmaker(bind=self.engine)
        # 注意：这里我们只初始化连接，但不进行任何写操作
        self.session = self.Session()

        if description is None:
            description = ""

        # --- 变更点 1: 将所有元数据暂存到内存 self.meta_data ---
        self.meta_data = {
            "strategy_name": strategy_name,
            "description": description,
            "params": params,
            "start_date": start_date,
            "end_date": end_date,
            "initial_cash": initial_cash,
            "commission": commission,
            "start_time": datetime.datetime.now()  # 记录开始时间
        }

        # --- 变更点 2: 交易记录暂存到内存列表 ---
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

        # --- 变更点 3: 只记入内存 buffer，不写库 ---
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

    def finish_execution(self, final_value, total_return, sharpe, max_drawdown, annual_return):
        if not self.active: return

        print(f"--- DB Recorder: Saving results to DB... ---")
        try:
            # --- 变更点 4: 在结束时一次性开启事务 ---

            name = self.meta_data["strategy_name"]
            desc = self.meta_data["description"]

            execution = self.session.query(BacktestExecution).filter_by(strategy_name=name, description=desc).first()
            if execution:
                # 如果存在，复用 ID，清空旧的 trade logs
                # 注意：此时因为还没有 commit，如果下面报错 rollback，这些删除操作也会撤销
                self.session.query(BacktestTradeLog).filter_by(execution_id=execution.id).delete(
                    synchronize_session=False)

                # 更新元数据
                execution.start_time = self.meta_data["start_time"]
                execution.updated_time = datetime.datetime.now()
                execution.params = self.meta_data["params"]
                execution.backtest_start_date = self.meta_data["start_date"]
                execution.backtest_end_date = self.meta_data["end_date"]
                execution.initial_cash = self.meta_data["initial_cash"]
                execution.commission_scheme = self.meta_data["commission"]
            else:
                # 如果不存在，创建新的
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
                # Flush 以便获取 generated ID (如果需要的话，虽然 UUID 通常在 python 端生成)
                self.session.flush()

            # 2. 写入本次回测的结果数据
            execution.final_value = final_value
            execution.total_return = total_return
            execution.sharpe_ratio = sharpe
            execution.max_drawdown = max_drawdown
            execution.annual_return = annual_return
            execution.status = 'FINISHED'  # 直接标记为完成

            # 3. 批量写入 Trade Logs
            # 使用列表推导式构建对象列表
            trade_objects = [
                BacktestTradeLog(
                    execution_id=execution.id,
                    **trade_data  # 解包字典
                ) for trade_data in self.trades_buffer
            ]

            # 使用 add_all 批量添加，性能更好
            self.session.add_all(trade_objects)

            # 4. 提交事务 (Atomic Commit)
            # 只有到了这一步才会真正修改数据库
            self.session.commit()
            print(f"--- DB Recorder: Success! Results saved for {name} ---")

        except Exception as e:
            # 5. 发生任何错误，回滚事务
            self.session.rollback()
            print(f"DB Finish Error (Rolled back): {e}")
            # 这里可以选择是否抛出异常，如果不抛出则不影响主程序退出
            # raise e
        finally:
            self.session.close()