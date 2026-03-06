from types import SimpleNamespace

import common.optimizer as optimizer


def _build_args(**overrides):
    base = {
        "metric": "sharpe",
        "opt_params": "{}",
        "train_roll_period": "1Y",
        "test_roll_period": "3M",
        "test_period": None,
        "start_date": "20240101",
        "end_date": "20250331",
        "data_source": "ibkr",
        "selection": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _sample_metrics(start="20250101", end="20250331"):
    return {
        "start_date": start,
        "end_date": end,
        "total_return": 0.10,
        "annual_return": 0.12,
        "sharpe_ratio": 1.45,
        "max_drawdown": 0.08,
        "calmar_ratio": 1.50,
        "total_trades": 20,
        "win_rate": 55.0,
        "profit_factor": 1.90,
        "final_portfolio": 110000.0,
    }


class _DummyOptimizationJob:
    def __init__(self, args, fixed_params, opt_params_def, risk_params, shared_context=None):
        self.args = args
        self.fixed_params = fixed_params
        self.train_range = ("20240101", "20241231")
        self.test_range = ("20250101", "20250331")
        self.target_symbols = ["AAPL"]

    def export_shared_context(self):
        return {
            "strategy_class": object(),
            "risk_control_classes": [],
            "data_manager": object(),
            "target_symbols": ["AAPL"],
            "raw_datas": {},
            "train_datas": {"AAPL": object()},
            "test_datas": {"AAPL": object()},
            "train_range": self.train_range,
            "test_range": self.test_range,
            "window_data_cache": {},
        }

    def _run_recent_3y_backtest(self, params):
        return _sample_metrics(start="20230101", end="20251231")

    def _run_test_set_backtest(self, params, verbose=False):
        return _sample_metrics(start="20250101", end="20250331")

    def run(self):
        return {
            "best_score": "1.2345",
            "best_params": {"lookback": 20},
            "trials_completed": 5,
            "log_file": "dummy.log",
            "recent_backtest": _sample_metrics(start="20230101", end="20251231"),
            "test_backtest": _sample_metrics(start="20250101", end="20250331"),
        }

    @classmethod
    def build_optuna_name_tag(cls, **kwargs):
        return "dummy_name_tag"

    def _launch_dashboard(self, log_file, port=8090, background=False):
        return None


def test_run_optimizer_mode_prints_test_backtest_section(monkeypatch, capsys):
    monkeypatch.setattr(optimizer, "OptimizationJob", _DummyOptimizationJob)
    monkeypatch.setattr(optimizer.sys, "argv", ["run.py", "--params", "{\"lookback\": 20}"])

    args = _build_args(test_roll_period="3M")
    code = optimizer.run_optimizer_mode(
        args=args,
        fixed_params={"lookback": 20},
        risk_params={},
        symbol_list=["AAPL"],
    )

    out = capsys.readouterr().out
    assert code == 0
    assert "测试集回测结果" in out
    assert "20250101 -> 20250331" in out
    assert "当前基准" in out


def test_run_optimizer_mode_skips_test_backtest_section_without_test_config(monkeypatch, capsys):
    monkeypatch.setattr(optimizer, "OptimizationJob", _DummyOptimizationJob)
    monkeypatch.setattr(optimizer.sys, "argv", ["run.py", "--params", "{\"lookback\": 20}"])

    args = _build_args(test_roll_period=None, test_period=None)
    code = optimizer.run_optimizer_mode(
        args=args,
        fixed_params={"lookback": 20},
        risk_params={},
        symbol_list=["AAPL"],
    )

    out = capsys.readouterr().out
    assert code == 0
    assert "测试集回测结果" not in out


def test_run_test_set_backtest_returns_structured_metrics(monkeypatch):
    class DummyBacktester:
        last_kwargs = None
        display_called = False

        def __init__(self, **kwargs):
            DummyBacktester.last_kwargs = kwargs

        def run(self):
            return None

        def display_results(self):
            DummyBacktester.display_called = True

        def get_performance_metrics(self):
            return _sample_metrics(start="20250101", end="20250331")

    monkeypatch.setattr(optimizer, "Backtester", DummyBacktester)

    job = optimizer.OptimizationJob.__new__(optimizer.OptimizationJob)
    job.test_datas = {"AAPL": object()}
    job.strategy_class = object()
    job.test_range = ("20250101", "20250331")
    job.args = SimpleNamespace(
        cash=100000.0,
        commission=0.0003,
        slippage=0.0,
        timeframe="Days",
        compression=1,
    )
    job.risk_control_classes = []
    job.risk_params = {}

    got = job._run_test_set_backtest(final_params={"lookback": 20}, verbose=False)

    assert got is not None
    assert got["start_date"] == "20250101"
    assert got["end_date"] == "20250331"
    assert got["annual_return"] == 0.12
    assert DummyBacktester.last_kwargs["start_date"] == "20250101"
    assert DummyBacktester.last_kwargs["end_date"] == "20250331"
    assert not DummyBacktester.display_called


def test_request_elevation_skips_for_single_worker(monkeypatch):
    args = SimpleNamespace(n_jobs=1)
    monkeypatch.delenv("QUANTADA_DISABLE_AUTO_ELEVATE", raising=False)
    monkeypatch.setattr(optimizer, "_is_process_elevated", lambda: False)

    called = {"banner": 0}

    def _mark_banner(_):
        called["banner"] += 1

    monkeypatch.setattr(optimizer, "_print_elevation_banner", _mark_banner)

    got = optimizer._request_elevation_if_needed(args)

    assert got is False
    assert called["banner"] == 0


def test_request_elevation_windows_branch(monkeypatch):
    args = SimpleNamespace(n_jobs=4)
    monkeypatch.delenv("QUANTADA_DISABLE_AUTO_ELEVATE", raising=False)
    monkeypatch.setattr(optimizer.sys, "platform", "win32", raising=False)
    monkeypatch.setattr(optimizer, "_is_process_elevated", lambda: False)
    monkeypatch.setattr(optimizer, "_print_elevation_banner", lambda _: None)
    monkeypatch.setattr(optimizer, "_relaunch_windows_as_admin", lambda: True)

    got = optimizer._request_elevation_if_needed(args)

    assert got is True


def test_request_elevation_linux_branch(monkeypatch):
    args = SimpleNamespace(n_jobs=4)
    monkeypatch.delenv("QUANTADA_DISABLE_AUTO_ELEVATE", raising=False)
    monkeypatch.setattr(optimizer.sys, "platform", "linux", raising=False)
    monkeypatch.setattr(optimizer, "_is_process_elevated", lambda: False)
    monkeypatch.setattr(optimizer, "_print_elevation_banner", lambda _: None)
    monkeypatch.setattr(optimizer, "_relaunch_unix_with_sudo", lambda: True)

    got = optimizer._request_elevation_if_needed(args)

    assert got is True
