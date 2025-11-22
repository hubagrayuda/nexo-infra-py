from typing import Mapping, Sequence
from nexo.logging.enums import LogLevel
from .enums import Status


STATUS_ORDER: Sequence[Status] = [
    Status.LOW,
    Status.NORMAL,
    Status.HIGH,
    Status.CRITICAL,
    Status.OVERLOAD,
]


STATUS_LOG_LEVEL: Mapping[Status, LogLevel] = {
    Status.LOW: LogLevel.INFO,
    Status.NORMAL: LogLevel.INFO,
    Status.HIGH: LogLevel.WARNING,
    Status.CRITICAL: LogLevel.ERROR,
    Status.OVERLOAD: LogLevel.CRITICAL,
}
