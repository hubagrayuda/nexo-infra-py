from enum import StrEnum
from nexo.types.string import ListOfStrs


class Status(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNSTABLE = "unstable"
    CRITICAL = "critical"

    @classmethod
    def choices(cls) -> ListOfStrs:
        return [e.value for e in cls]
