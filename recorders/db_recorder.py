import datetime
import uuid
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, JSON, ForeignKey, Index
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import database_exists, create_database

from .base_recorder import BaseRecorder
import config

Base = declarative_base()


# --- 1. 高性能表结构设计 ---

class BacktestExecution(Base):
    """
    策略执行主表
    设计重点：strategy_name 是唯一入口，保证同一个策略文件在库里只有一条记录
    """
    __tablename__ = 'backtest_executions'

    # 使用 UUID 作为内部关联键
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # 核心唯一索引：策略名 (文件名)
    # unique=True 保证了不会有重复的策略记录
    strategy_name = Column(String(100), unique=True, nullable=False)

    description = Column(String(255))
    start_time = Column(DateTime, default=datetime.datetime.now)
    updated_time = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    # 配置快照
    backtest_start_date = Column(String(20))
    backtest_end_date = Column(String(20))
    params = Column(JSON)

    # 初始资金
    initial_cash = Column(Float)

    # 结果字段 (初始化时需重置)
    final_value = Column(Float, nullable=True)
    total_return = Column(Float, nullable=True)
    annual_return = Column(Float, nullable=True)
    max_drawdown = Column(Float, nullable=True)
    sharpe_ratio = Column(Float, nullable=True)

    status = Column(String(20), default='RUNNING')  # RUNNING, FINISHED

    # 建立关系，级联删除配置并不是必须的，因为我们会手动管理清除逻辑以获得更高性能
    trades = relationship("BacktestTradeLog", backref="execution", cascade="all, delete-orphan")


class BacktestTradeLog(Base):
    """
    交易流水表
    设计重点：写多读少。取消复杂的联合主键，使用数据库原生自增ID以获得最快写入速度。
    """
    __tablename__ = 'backtest_trade_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 外键关联，index=True 极大加速初始化时的 "DELETE FROM ... WHERE execution_id=..."
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

    # 辅助字段
    order_ref = Column(String(50))

    # 复合索引：方便后续按时间查询某个策略的流水
    __table_args__ = (
        Index('idx_exec_time', 'execution_id', 'dt'),
    )


# --- 2. 数据库记录器逻辑 ---

class DBRecorder(BaseRecorder):
    def __init__(self, strategy_name, description, params, start_date, end_date, initial_cash):
        if not config.DB_ENABLED:
            self.active = False
            return

        self.active = True
        self.engine = self._init_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        # --- 核心逻辑：覆盖更新 (Upsert / Overwrite) ---
        self.execution_id = self._initialize_execution_record(
            strategy_name, description, params, start_date, end_date, initial_cash
        )
        print(f"--- DB Recorder: Overwriting record for '{strategy_name}' (ID: {self.execution_id}) ---")

    def _init_engine(self):
        """自动建库建表"""
        url = make_url(config.DB_URL)
        if not database_exists(url):
            create_database(url)
        engine = create_engine(config.DB_URL)
        Base.metadata.create_all(engine)
        return engine

    def _initialize_execution_record(self, name, desc, params, start, end, cash):
        """
        初始化执行记录：
        1. 检查是否存在同名策略
        2. 存在 -> 清空其旧交易日志，重置状态
        3. 不存在 -> 创建新记录
        """
        try:
            # 1. 查询现有记录
            execution = self.session.query(BacktestExecution).filter_by(strategy_name=name).first()

            if execution:
                # === 存在：复用 ID，清洗数据 ===
                exec_id = execution.id

                # A. 极速清空旧日志 (利用 execution_id 索引)
                # 使用 delete() 方法比逐条删除快得多
                self.session.query(BacktestTradeLog).filter_by(execution_id=exec_id).delete(synchronize_session=False)

                # B. 重置主表状态
                execution.status = 'RUNNING'
                execution.description = desc
                execution.start_time = datetime.datetime.now()
                execution.params = params
                execution.backtest_start_date = start
                execution.backtest_end_date = end
                execution.initial_cash = cash

                # C. 清空旧绩效指标 (防止回测失败后残留旧的高绩效误导)
                execution.final_value = None
                execution.total_return = None
                execution.annual_return = None
                execution.max_drawdown = None
                execution.sharpe_ratio = None

                self.execution = execution  # 保持引用

            else:
                # === 不存在：新建 ===
                self.execution = BacktestExecution(
                    strategy_name=name,
                    description=desc,
                    params=params,
                    backtest_start_date=start,
                    backtest_end_date=end,
                    initial_cash=cash
                )
                self.session.add(self.execution)
                # Flush 以生成 ID
                self.session.flush()
                exec_id = self.execution.id

            self.session.commit()
            return exec_id

        except Exception as e:
            self.session.rollback()
            print(f"DB Init Error: {e}")
            raise e

    def log_trade(self, dt, symbol, action, price, size, comm, order_ref, cash, value):
        """
        高性能写入
        因为已在初始化阶段清空了旧数据，这里只需无脑 Insert，无需 check exists。
        """
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
        # 考虑到回测速度，不需要每条都 commit，可以积攒一定数量
        # 但为了实时性（看到即所得），这里暂保持每次提交。
        # 如果追求极致速度，可改为 self.session.flush()，最后统一 commit
        self.session.flush()

    def finish_execution(self, final_value, total_return, sharpe, max_drawdown, annual_return):
        if not self.active: return

        try:
            # 直接更新持有对象的属性
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