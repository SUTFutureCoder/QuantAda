import base64
import hashlib
import hmac
import time
import urllib.parse

import requests

import config
from .base_alarm import BaseAlarm


class DingTalkAlarm(BaseAlarm):
    def __init__(self):
        self.webhook = config.DINGTALK_WEBHOOK
        self.secret = getattr(config, 'DINGTALK_SECRET', '')
        self.enabled = bool(self.webhook)

    def _get_signed_url(self):
        """处理钉钉加签"""
        if not self.secret:
            return self.webhook
        timestamp = str(round(time.time() * 1000))
        secret_enc = self.secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return f"{self.webhook}&timestamp={timestamp}&sign={sign}"

    def _send(self, payload):
        if not self.enabled: return
        try:
            url = self._get_signed_url()
            resp = requests.post(url, json=payload, headers={'Content-Type': 'application/json'}, timeout=5)
            # print(f"[DingTalk] Response: {resp.text}")
        except Exception as e:
            print(f"[DingTalk Error] Failed to send alarm: {e}")

    def push_text(self, content: str, level: str = 'INFO'):
        # 简单文本
        payload = {
            "msgtype": "text",
            "text": {"content": f"[{level}] QuantAda: {content}"}
        }
        self._send(payload)

    def push_exception(self, context: str, error: Exception):
        # 异常使用 Markdown 高亮红色
        import traceback
        tb_str = traceback.format_exc()[-500:]  # 只取最后500字符避免超长

        md_text = f"""### 🚨 QuantAda 异常报警
**模块**: {context}
**错误**: {str(error)}
**时间**: {time.strftime('%Y-%m-%d %H:%M:%S')}
> {tb_str}
"""
        payload = {
            "msgtype": "markdown",
            "markdown": {"title": "QuantAda Exception", "text": md_text}
        }
        self._send(payload)

    def push_trade(self, order_info: dict):
        # 交易信息使用 Markdown 表格或列表
        action_emoji = "🔴 卖出" if order_info.get('action') == 'SELL' else "🟢 买入"

        md_text = f"""### {action_emoji} 交易成交通知
- **标的**: {order_info.get('symbol')}
- **价格**: {order_info.get('price')}
- **数量**: {order_info.get('size')}
- **金额**: {order_info.get('value', 0):.2f}
- **时间**: {order_info.get('dt')}
"""
        payload = {
            "msgtype": "markdown",
            "markdown": {"title": "Trade Notification", "text": md_text}
        }
        self._send(payload)

    def push_status(self, status: str, detail: str = ""):
        # 系统状态：启动/停止/死信
        emoji = "🚀" if status == "STARTED" else "💀" if status == "DEAD" else "🛑"
        md_text = f"""### {emoji} 系统状态: {status}
**时间**: {time.strftime('%Y-%m-%d %H:%M:%S')}
**详情**: {detail}
"""
        payload = {
            "msgtype": "markdown",
            "markdown": {"title": f"System {status}", "text": md_text}
        }
        self._send(payload)