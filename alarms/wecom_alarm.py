import requests
import json
import time
from .base_alarm import BaseAlarm
import config


class WeComAlarm(BaseAlarm):
    def __init__(self):
        self.webhook = config.WECOM_WEBHOOK
        self.enabled = bool(self.webhook)

    def _send(self, payload):
        if not self.enabled: return
        try:
            resp = requests.post(self.webhook, json=payload, headers={'Content-Type': 'application/json'}, timeout=5)
        except Exception as e:
            print(f"[WeCom Error] Failed to send alarm: {e}")

    def push_text(self, content: str, level: str = 'INFO'):
        color = "info" if level == 'INFO' else "warning"
        md_text = f"<font color=\"{color}\">{content}</font>"
        payload = {
            "msgtype": "markdown",
            "markdown": {"content": md_text}
        }
        self._send(payload)

    def push_exception(self, context: str, error: Exception):
        import traceback
        tb_str = traceback.format_exc()[-500:]

        md_text = f"""### <font color=\"warning\">🚨 QuantAda 异常报警</font>
> **模块**: {context}
> **时间**: {time.strftime('%Y-%m-%d %H:%M:%S')}
> **错误**: <font color=\"warning\">{str(error)}</font>
>
> `{tb_str}`
"""
        payload = {
            "msgtype": "markdown",
            "markdown": {"content": md_text}
        }
        self._send(payload)

    def push_trade(self, order_info: dict):
        action = order_info.get('action')
        color = "warning" if action == 'SELL' else "info"
        action_text = "🔴 卖出" if action == 'SELL' else "🟢 买入"

        md_text = f"""### <font color=\"{color}\">{action_text} 成交通知</font>
**标的**: {order_info.get('symbol')}
**价格**: {order_info.get('price')}
**数量**: {order_info.get('size')}
**金额**: {order_info.get('value', 0):.2f}
**时间**: <font color=\"comment\">{order_info.get('dt')}</font>
"""
        payload = {
            "msgtype": "markdown",
            "markdown": {"content": md_text}
        }
        self._send(payload)

    def push_status(self, status: str, detail: str = ""):
        icon = "🚀" if status.startswith("STARTED") else "💀" if status == "DEAD" else "🛑"
        color = "info" if status.startswith("STARTED") else "warning"

        md_text = f"""### <font color=\"{color}\">{icon} 系统状态: {status}</font>
**时间**: {time.strftime('%Y-%m-%d %H:%M:%S')}
**详情**: {detail}
"""
        payload = {
            "msgtype": "markdown",
            "markdown": {"content": md_text}
        }
        self._send(payload)