import sys
import importlib
import inspect
import traceback
import config
from live_trader.adapters.base_broker import BaseLiveBroker


def launch_live(broker_name: str, conn_name: str, strategy_path: str, params: dict, **kwargs):
    """
    通用实盘启动器
    动态加载 live_trader.adapters.{broker_name} 模块并执行其 launch 方法
    """

    # 1. 配置检查 (保持不变)
    if not hasattr(config, 'BROKER_ENVIRONMENTS'):
        print("[Error] 'BROKER_ENVIRONMENTS' missing in config.py")
        sys.exit(1)

    broker_conf = config.BROKER_ENVIRONMENTS.get(broker_name)
    if not broker_conf:
        print(f"[Error] Broker '{broker_name}' not in BROKER_ENVIRONMENTS")
        sys.exit(1)

    conn_cfg = broker_conf.get(conn_name)
    if not conn_cfg:
        print(f"[Error] Connection '{conn_name}' not found")
        sys.exit(1)

    # 2. 动态加载模块
    module_path = f"live_trader.adapters.{broker_name}"
    try:
        adapter_module = importlib.import_module(module_path)
    except Exception:
        print(f"[Error] Failed to import module '{module_path}':")
        traceback.print_exc()
        sys.exit(1)

    # 3. 自动发现 Broker 类
    broker_class = None
    for name, obj in inspect.getmembers(adapter_module):
        # 查找逻辑：是类 + 是BaseLiveBroker的子类 + 不是BaseLiveBroker本身
        if (inspect.isclass(obj)
                and issubclass(obj, BaseLiveBroker)
                and obj is not BaseLiveBroker):
            broker_class = obj
            break

    if not broker_class:
        print(f"[Error] No subclass of 'BaseLiveBroker' found in {module_path}")
        sys.exit(1)

    # 4. 执行协议 (Lazy Check)
    # 这里直接调用，如果用户没覆盖，会抛出基类定义的 NotImplementedError
    try:
        # 备份原始参数（可选，视需要）
        # original_argv = sys.argv[:]

        # 净化 sys.argv，只保留脚本名称。
        # 这一步屏蔽了框架层的参数（如 --selection, --connect），
        # 使得 Broker 底层 SDK（如掘金）解析参数时不会报错。
        sys.argv = [sys.argv[0]]

        broker_class.launch(conn_cfg, strategy_path, params, **kwargs)
    except NotImplementedError as e:
        print(f"[Error] Protocol not implemented: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[Crash] Launch execution failed:")
        traceback.print_exc()
        sys.exit(1)