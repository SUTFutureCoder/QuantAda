from risk_controls.sample_max_drawdown_kill_switch import SampleMaxDrawdownKillSwitch


class _SeqBroker:
    def __init__(self, values, cash_override=None):
        self._values = list(values)
        self._idx = 0
        self._cash_override = cash_override

    def getvalue(self):
        if self._idx >= len(self._values):
            return self._values[-1]
        value = self._values[self._idx]
        self._idx += 1
        return value


class _StaticBroker:
    def __init__(self, value):
        self._value = value

    def getvalue(self):
        return self._value


def test_max_drawdown_kill_switch_triggers_and_latches():
    broker = _SeqBroker([100.0, 120.0, 90.0, 130.0])
    risk = SampleMaxDrawdownKillSwitch(
        broker=broker,
        params={"max_dd_tolerance": 0.2},
    )

    assert risk.check(None) is None
    assert risk.check(None) is None
    assert risk.check(None) == "SELL"
    assert risk.plug_pulled is True
    assert risk.check(None) == "SELL"


def test_max_drawdown_kill_switch_stays_idle_under_tolerance():
    broker = _SeqBroker([100.0, 120.0, 110.0, 115.0])
    risk = SampleMaxDrawdownKillSwitch(
        broker=broker,
        params={"max_dd_tolerance": 0.2},
    )

    assert risk.check(None) is None
    assert risk.check(None) is None
    assert risk.check(None) is None
    assert risk.plug_pulled is False


def test_max_drawdown_kill_switch_handles_invalid_value_gracefully():
    broker = _StaticBroker(None)
    risk = SampleMaxDrawdownKillSwitch(
        broker=broker,
        params={"max_dd_tolerance": 0.1},
    )

    assert risk.check(None) is None
    assert risk.peak_value is None
    assert risk.plug_pulled is False


def test_max_drawdown_kill_switch_prefers_cash_override_as_initial_peak():
    broker = _SeqBroker([90.0], cash_override=100.0)
    risk = SampleMaxDrawdownKillSwitch(
        broker=broker,
        params={"max_dd_tolerance": 0.5},
    )

    assert risk.check(None) is None
    assert risk.peak_value == 100.0
