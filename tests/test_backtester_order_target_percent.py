import pandas as pd

import config
from backtest.backtester import Backtester
from strategies.base_strategy import BaseStrategy


def _make_flat_df():
    idx = pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'])
    return pd.DataFrame(
        {
            'open': [100.0, 100.0, 100.0],
            'high': [100.0, 100.0, 100.0],
            'low': [100.0, 100.0, 100.0],
            'close': [100.0, 100.0, 100.0],
            'volume': [1.0, 1.0, 1.0],
        },
        index=idx,
    )


class _TwoSymbolPercentBuyStrategy(BaseStrategy):
    def init(self):
        self.submitted = False
        self.statuses = []

    def next(self):
        if self.submitted:
            return
        for d in self.broker.datas:
            self.broker.order_target_percent(data=d, target=0.6)
        self.submitted = True

    def notify_order(self, order):
        self.statuses.append(order.getstatusname())


def test_order_target_percent_tracks_virtual_spent_cash_with_multi_symbol_buy(monkeypatch):
    """
    回归：同一 Bar 多标的连续 order_target_percent 买入时，必须扣减本轮已花费现金。
    否则第二笔订单会在回测柜台阶段触发 Margin。
    """
    monkeypatch.setattr(config, 'LOT_SIZE', 1)

    datas = {
        'AAA': _make_flat_df(),
        'BBB': _make_flat_df(),
    }

    bt = Backtester(
        datas=datas,
        strategy_class=_TwoSymbolPercentBuyStrategy,
        cash=1000.0,
        commission=0.0,
        slippage=0.0,
        enable_plot=False,
        verbose=False,
    )
    bt.run()

    wrapper = bt.results[0]
    statuses = wrapper.strategy.statuses
    assert 'Margin' not in statuses, "同 Bar 多标的买入应避免产生 Margin 拒单。"

    final_sizes = {d._name: wrapper.getposition(d).size for d in wrapper.datas}
    assert final_sizes == {'AAA': 6, 'BBB': 4}, "第二笔买单应按剩余现金自动缩量成交，而非被拒单。"
