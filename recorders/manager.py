from .base_recorder import BaseRecorder

class RecorderManager(BaseRecorder):
    """
    Recorder 组合管理器。
    对于 Backtester 来说，它只是一个普通的 Recorder。
    但实际上它会将调用分发给内部注册的所有 Recorder。
    """
    def __init__(self, recorders: list[BaseRecorder] = None):
        self.recorders = recorders or []
        self.active = True

    def add_recorder(self, recorder: BaseRecorder):
        if recorder:
            self.recorders.append(recorder)

    def log_trade(self, *args, **kwargs):
        # 遍历所有 Recorder 进行分发
        for r in self.recorders:
            try:
                r.log_trade(*args, **kwargs)
            except Exception as e:
                # 容错：单个 Recorder 失败不应影响主流程和其他 Recorder
                print(f"[{type(r).__name__}] Error logging trade: {e}")

    def finish_execution(self, *args, **kwargs):
        for r in self.recorders:
            try:
                r.finish_execution(*args, **kwargs)
            except Exception as e:
                print(f"[{type(r).__name__}] Error finishing execution: {e}")