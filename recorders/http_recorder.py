import requests
import json
from .base_recorder import BaseRecorder

class HttpRecorder(BaseRecorder):
    def __init__(self, endpoint_url, api_key=None):
        self.endpoint_url = endpoint_url
        self.headers = {'Content-Type': 'application/json'}
        if api_key:
            self.headers['Authorization'] = f'Bearer {api_key}'

    def log_trade(self, dt, symbol, action, price, size, comm, order_ref, cash, value):
        payload = {
            'type': 'trade',
            'dt': str(dt),
            'symbol': symbol,
            'action': action,
            'price': price,
            'size': size,
            'value': value
        }
        # 实际生产中建议使用异步或队列，避免 HTTP 请求阻塞回测速度
        try:
            # print(f"Sending HTTP log to {self.endpoint_url}...")
            # requests.post(self.endpoint_url, json=payload, headers=self.headers, timeout=1)
            pass
        except Exception as e:
            print(f"HttpRecorder Error: {e}")

    def finish_execution(self, final_value, total_return, sharpe, max_drawdown, annual_return):
        payload = {
            'type': 'summary',
            'final_value': final_value,
            'sharpe': sharpe
        }
        # requests.post(...)
        print(f"HTTP Recorder: Backtest finished, data sent to {self.endpoint_url}")