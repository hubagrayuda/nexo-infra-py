from datetime import datetime
from google.cloud.pubsub_v1.subscriber.message import Message
from pydantic import BaseModel, Field, model_validator
from typing import Annotated, Generic, Literal, Self, TypeGuard, overload
from nexo.types.integer import OptInt, OptIntT
from .config import ThresholdConfig
from .enums import (
    MeasurementType,
    OptMeasurementType,
    MeasurementTypeT,
    AggregateMeasurementType,
    OptAggregateMeasurementType,
    OptAggregateMeasurementTypeT,
    Status,
)
from .utils import aggregate_status


class CPUUsage(BaseModel):
    raw: Annotated[float, Field(..., description="Raw CPU Usage (%)", ge=0.0)]
    smooth: Annotated[float, Field(..., description="Smooth CPU Usage (%)", ge=0.0)]
    status: Annotated[Status, Field(Status.NORMAL, description="Usage status")] = (
        Status.NORMAL
    )

    @classmethod
    def new(
        cls,
        *,
        raw: float,
        smooth: float,
        threshold: ThresholdConfig,
    ) -> "CPUUsage":
        if smooth < threshold.low:
            status = Status.LOW
        elif smooth < threshold.normal:
            status = Status.NORMAL
        elif smooth < threshold.high:
            status = Status.HIGH
        elif smooth < threshold.critical:
            status = Status.CRITICAL
        else:
            status = Status.OVERLOAD

        return cls(raw=raw, smooth=smooth, status=status)


class MemoryUsage(BaseModel):
    used: Annotated[int, Field(..., description="Memory used (bytes)", ge=0)]
    limit: Annotated[int, Field(..., description="Memory limit (bytes)", ge=0)]
    percentage: Annotated[float, Field(..., description="Percentage of limit", ge=0.0)]
    status: Annotated[Status, Field(Status.NORMAL, description="Usage status")] = (
        Status.NORMAL
    )

    @classmethod
    def new(cls, used: int, limit: int, threshold: ThresholdConfig) -> "MemoryUsage":
        percentage = (used / limit) * 100
        if percentage < threshold.low:
            status = Status.LOW
        elif percentage < threshold.normal:
            status = Status.NORMAL
        elif percentage < threshold.high:
            status = Status.HIGH
        elif percentage < threshold.critical:
            status = Status.CRITICAL
        else:
            status = Status.OVERLOAD

        return cls(used=used, limit=limit, percentage=percentage, status=status)


class Usage(BaseModel):
    cpu: Annotated[CPUUsage, Field(..., description="CPU Usage")]
    memory: Annotated[MemoryUsage, Field(..., description="Memory Usage")]


class GenericMeasurement(
    BaseModel,
    Generic[
        MeasurementTypeT,
        OptAggregateMeasurementTypeT,
        OptIntT,
    ],
):
    type: Annotated[MeasurementTypeT, Field(..., description="Measurement's type")]
    aggregate_type: Annotated[
        OptAggregateMeasurementTypeT,
        Field(..., description="Aggregate measurement's type"),
    ]
    measured_at: Annotated[datetime, Field(..., description="Measured at timestamp")]
    window: Annotated[OptIntT, Field(..., description="Measurement window")]
    status: Annotated[Status, Field(..., description="Aggregate status")]
    usage: Annotated[Usage, Field(..., description="Resource usage")]


class BaseMeasurement(
    GenericMeasurement[MeasurementType, OptAggregateMeasurementType, OptInt]
):
    type: Annotated[MeasurementType, Field(..., description="Measurement's type")]
    aggregate_type: Annotated[
        OptAggregateMeasurementType,
        Field(None, description="Aggregate measurement's type"),
    ] = None
    window: Annotated[OptInt, Field(None, description="Measurement window", ge=1)] = (
        None
    )

    @model_validator(mode="after")
    def validate_measurement(self) -> Self:
        if self.type is MeasurementType.REGULAR:
            if self.aggregate_type is not None:
                raise ValueError("Aggregate type must be None for regular measurement")
            if self.window is not None:
                raise ValueError("Window must be None for regular measurement")
        elif self.type is MeasurementType.AGGREGATE:
            if self.aggregate_type is None:
                raise ValueError(
                    "Aggregate type can not be None for aggregate measurement"
                )
            if self.window is None:
                raise ValueError("Window can not be None for aggregate measurement")
        return self

    @overload
    @classmethod
    def new(
        cls,
        *,
        type: Literal[MeasurementType.REGULAR],
        measured_at: datetime,
        usage: Usage,
    ) -> "BaseMeasurement": ...
    @overload
    @classmethod
    def new(
        cls,
        *,
        type: Literal[MeasurementType.AGGREGATE],
        aggregate_type: AggregateMeasurementType,
        measured_at: datetime,
        window: int,
        usage: Usage,
    ) -> "BaseMeasurement": ...
    @classmethod
    def new(
        cls,
        *,
        type: MeasurementType,
        aggregate_type: OptAggregateMeasurementType = None,
        measured_at: datetime,
        window: OptInt = None,
        usage: Usage,
    ) -> "BaseMeasurement":
        return cls(
            type=type,
            aggregate_type=aggregate_type,
            measured_at=measured_at,
            window=window,
            status=aggregate_status(usage.cpu.status, usage.memory.status),
            usage=usage,
        )


class RegularMeasurement(
    GenericMeasurement[
        Literal[MeasurementType.REGULAR],
        None,
        None,
    ]
):
    type: Annotated[
        Literal[MeasurementType.REGULAR],
        Field(MeasurementType.REGULAR, description="Measurement's type"),
    ] = MeasurementType.REGULAR
    aggregate_type: Annotated[
        None, Field(None, description="Aggregate measurement's type")
    ] = None
    window: Annotated[None, Field(None, description="Measurement window")] = None

    @classmethod
    def new(cls, *, measured_at: datetime, usage: Usage) -> "RegularMeasurement":
        return cls(
            measured_at=measured_at,
            status=aggregate_status(usage.cpu.status, usage.memory.status),
            usage=usage,
        )

    def to_base(self) -> BaseMeasurement:
        return BaseMeasurement.model_validate(self.model_dump())


class GenericAggregateMeasurement(
    GenericMeasurement[
        Literal[MeasurementType.AGGREGATE],
        OptAggregateMeasurementTypeT,
        int,
    ]
):
    type: Annotated[
        Literal[MeasurementType.AGGREGATE],
        Field(MeasurementType.AGGREGATE, description="Measurement's type"),
    ] = MeasurementType.AGGREGATE
    window: Annotated[int, Field(..., description="Measurement window", ge=1)]

    def to_base(self) -> BaseMeasurement:
        return BaseMeasurement.model_validate(self.model_dump())


class AggregateMeasurement(GenericAggregateMeasurement[AggregateMeasurementType]):
    aggregate_type: Annotated[
        AggregateMeasurementType, Field(..., description="Aggregate measurement's type")
    ]

    @classmethod
    def new(
        cls,
        *,
        aggregate_type: AggregateMeasurementType,
        measured_at: datetime,
        window: int,
        usage: Usage,
    ) -> "AggregateMeasurement":
        return cls(
            aggregate_type=aggregate_type,
            measured_at=measured_at,
            window=window,
            status=aggregate_status(usage.cpu.status, usage.memory.status),
            usage=usage,
        )


class AverageMeasurement(
    GenericAggregateMeasurement[Literal[AggregateMeasurementType.AVERAGE]]
):
    aggregate_type: Annotated[
        Literal[AggregateMeasurementType.AVERAGE],
        Field(
            AggregateMeasurementType.AVERAGE, description="Aggregate measurement's type"
        ),
    ] = AggregateMeasurementType.AVERAGE

    @classmethod
    def new(
        cls, *, measured_at: datetime, window: int, usage: Usage
    ) -> "AverageMeasurement":
        return cls(
            measured_at=measured_at,
            window=window,
            status=aggregate_status(usage.cpu.status, usage.memory.status),
            usage=usage,
        )


class PeakMeasurement(
    GenericAggregateMeasurement[Literal[AggregateMeasurementType.PEAK]]
):
    aggregate_type: Annotated[
        Literal[AggregateMeasurementType.PEAK],
        Field(
            AggregateMeasurementType.PEAK, description="Aggregate measurement's type"
        ),
    ] = AggregateMeasurementType.PEAK

    @classmethod
    def new(
        cls, *, measured_at: datetime, window: int, usage: Usage
    ) -> "PeakMeasurement":
        return cls(
            measured_at=measured_at,
            window=window,
            status=aggregate_status(usage.cpu.status, usage.memory.status),
            usage=usage,
        )


AnyMeasurement = (
    BaseMeasurement
    | RegularMeasurement
    | AggregateMeasurement
    | AverageMeasurement
    | PeakMeasurement
)


def is_regular_measurement(
    measurement: AnyMeasurement,
) -> TypeGuard[RegularMeasurement]:
    return (
        measurement.type is MeasurementType.REGULAR
        and measurement.aggregate_type is None
        and measurement.window is None
    )


def is_aggregate_measurement(
    measurement: AnyMeasurement,
) -> TypeGuard[AggregateMeasurement]:
    return (
        measurement.type is MeasurementType.AGGREGATE
        and measurement.aggregate_type is not None
        and measurement.window is not None
    )


def is_average_measurement(
    measurement: AnyMeasurement,
) -> TypeGuard[AverageMeasurement]:
    return (
        is_aggregate_measurement(measurement)
        and measurement.aggregate_type is AggregateMeasurementType.AVERAGE
    )


def is_peak_measurement(
    measurement: AnyMeasurement,
) -> TypeGuard[AverageMeasurement]:
    return (
        is_aggregate_measurement(measurement)
        and measurement.aggregate_type is AggregateMeasurementType.PEAK
    )


class MeasurementFactory:
    @overload
    @classmethod
    def from_message(
        cls,
        message: Message,
        /,
    ) -> BaseMeasurement: ...
    @overload
    @classmethod
    def from_message(
        cls, message: Message, *, type: Literal[MeasurementType.REGULAR]
    ) -> RegularMeasurement: ...
    @overload
    @classmethod
    def from_message(
        cls, message: Message, *, type: Literal[MeasurementType.AGGREGATE]
    ) -> AggregateMeasurement: ...
    @overload
    @classmethod
    def from_message(
        cls,
        message: Message,
        *,
        type: Literal[MeasurementType.AGGREGATE],
        aggregate_type: Literal[AggregateMeasurementType.AVERAGE],
    ) -> AverageMeasurement: ...
    @overload
    @classmethod
    def from_message(
        cls,
        message: Message,
        *,
        type: Literal[MeasurementType.AGGREGATE],
        aggregate_type: Literal[AggregateMeasurementType.PEAK],
    ) -> PeakMeasurement: ...
    @classmethod
    def from_message(
        cls,
        message: Message,
        *,
        type: OptMeasurementType = None,
        aggregate_type: OptAggregateMeasurementType = None,
    ) -> AnyMeasurement:
        if type is None:
            measurement_cls = BaseMeasurement
        elif type is MeasurementType.REGULAR:
            measurement_cls = RegularMeasurement
        elif type is MeasurementType.AGGREGATE:
            if aggregate_type is None:
                measurement_cls = AggregateMeasurement
            elif aggregate_type is AggregateMeasurementType.AVERAGE:
                measurement_cls = AverageMeasurement
            elif aggregate_type is AggregateMeasurementType.PEAK:
                measurement_cls = PeakMeasurement

        return measurement_cls.model_validate_json(message.data.decode("utf-8"))
