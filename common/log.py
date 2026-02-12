import datetime
import config

def _fmt_dt(dt=None):
    """
    内部助手：时间格式化
    - 如果传入 dt (回测时间/指定时间)，则格式化它
    - 如果未传入 (实盘)，则取当前系统时间
    """
    if dt:
        # 处理 backtrader 的 float 类型时间或其他类型
        if isinstance(dt, (float, int)):
            s = str(dt)  # 兜底
        elif hasattr(dt, 'isoformat'):
            s = dt.isoformat()
        else:
            s = str(dt)
    else:
        # 实盘/默认情况：取当前系统时间
        s = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 去除零点时间后缀，让日志更清爽
        # 同时处理 'T00:00:00' (isoformat) 和 ' 00:00:00' (str/strftime) 两种情况
    return s.replace('T00:00:00', '').replace(' 00:00:00', '')

def info(msg, dt=None):
    """普通日志"""
    if getattr(config, 'LOG', True):
        time_str = _fmt_dt(dt)
        print(f"[{time_str}] {msg}")


def warning(msg, dt=None):
    """警告日志"""
    time_str = _fmt_dt(dt)
    print(f"⚠️ [{time_str}] {msg}")


def error(msg, dt=None):
    """错误日志"""
    time_str = _fmt_dt(dt)
    print(f"❌ [{time_str}] {msg}")


def signal(action, symbol, size, price, tag="信号触发", dt=None):
    """
    统一的交易信号日志
    :param action: 'BUY' 或 'SELL'
    :param symbol: 标的代码 string
    :param size: 数量
    :param price: 价格
    :param tag: 场景标签 (如: '实盘信号', '回测信号')
    :param dt: (可选) 显式指定时间，回测时传入回测时间，实盘时传 None 自动取当前
    """
    if getattr(config, 'LOG', True):
        emoji = "🚀" if action == 'BUY' else "🔻"
        act_cn = "买入" if action == 'BUY' else "卖出"

        est_val = size * price
        val_str = f"{est_val / 10000:.2f}万" if est_val > 10000 else f"{est_val:.2f}元"

        time_str = _fmt_dt(dt)

        print(f"{emoji} [{tag}] {time_str} {act_cn} {symbol:<12} 数量: {int(size):<8} 价格: {price:.2f} (约 {val_str})")