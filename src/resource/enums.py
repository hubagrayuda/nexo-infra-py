from enum import StrEnum
from typing import TypeVar
from nexo.types.string import ListOfStrs


class MeasurementType(StrEnum):
    REGULAR = "regular"
    AGGREGATE = "aggregate"

    @classmethod
    def choices(cls) -> ListOfStrs:
        return [e.value for e in cls]


MeasurementTypeT = TypeVar("MeasurementTypeT", bound=MeasurementType)
OptMeasurementType = MeasurementType | None


class AggregateMeasurementType(StrEnum):
    AVERAGE = "average"
    PEAK = "peak"

    @classmethod
    def choices(cls) -> ListOfStrs:
        return [e.value for e in cls]


AggregateMeasurementTypeT = TypeVar(
    "AggregateMeasurementTypeT", bound=AggregateMeasurementType
)
OptAggregateMeasurementType = AggregateMeasurementType | None
OptAggregateMeasurementTypeT = TypeVar(
    "OptAggregateMeasurementTypeT", bound=OptAggregateMeasurementType
)


class Status(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"
    OVERLOAD = "overload"

    @classmethod
    def choices(cls) -> ListOfStrs:
        return [e.value for e in cls]
