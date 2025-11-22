import json
from datetime import datetime
from google.cloud.pubsub_v1.subscriber.message import Message
from pydantic import BaseModel, Field, model_validator
from typing import Annotated, Generic, Literal, Self, TypeGuard, overload
from nexo.types.dict import (
    StrToAnyDict,
    OptStrToAnyDict,
    OptStrToStrDict,
)
from nexo.types.integer import OptInt, OptIntT
from .config import CPUUsageConfig, MemoryUsageConfig
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
        config: CPUUsageConfig,
    ) -> "CPUUsage":
        if smooth < config.threshold.low:
            status = Status.LOW
        elif smooth < config.threshold.normal:
            status = Status.NORMAL
        elif smooth < config.threshold.high:
            status = Status.HIGH
        elif smooth < config.threshold.critical:
            status = Status.CRITICAL
        else:
            status = Status.OVERLOAD

        return cls(raw=raw, smooth=smooth, status=status)


class MemoryUsage(BaseModel):
    raw: Annotated[float, Field(..., description="Raw memory usage (MB)", ge=0.0)]
    percentage: Annotated[float, Field(..., description="Percentage of limit", ge=0.0)]
    status: Annotated[Status, Field(Status.NORMAL, description="Usage status")] = (
        Status.NORMAL
    )

    @classmethod
    def new(cls, raw: float, config: MemoryUsageConfig) -> "MemoryUsage":
        percentage = (raw / config.limit) * 100
        if percentage < config.threshold.low:
            status = Status.LOW
        elif percentage < config.threshold.normal:
            status = Status.NORMAL
        elif percentage < config.threshold.high:
            status = Status.HIGH
        elif percentage < config.threshold.critical:
            status = Status.CRITICAL
        else:
            status = Status.OVERLOAD

        return cls(raw=raw, percentage=percentage, status=status)


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

    @property
    def message_obj(self) -> StrToAnyDict:
        return {
            "type": self.type,
            "aggregate_type": (
                None
                if self.aggregate_type is None
                else {AggregateMeasurementType.__name__: self.aggregate_type}
            ),
            "measured_at": self.measured_at.isoformat(),
            "window": None if self.window is None else {"int": self.window},
            "status": self.status,
            "usage": self.usage.model_dump(mode="json"),
        }

    @property
    def message_bytes(self) -> bytes:
        return json.dumps(self.message_obj).encode()


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

    @classmethod
    def from_message(cls, message: Message) -> "BaseMeasurement":
        message_obj: StrToAnyDict = json.loads(message.data.decode())

        for key in cls.model_fields.keys():
            if key not in message_obj:
                raise ValueError(f"Key '{key}' did not exist in the message")

        # Parse aggregate type
        aggregate_type: OptStrToStrDict = message_obj["aggregate_type"]
        if aggregate_type is None:
            aggregate_type_value = None
        else:
            if not (
                isinstance(aggregate_type, dict)
                and len(aggregate_type) == 1
                and AggregateMeasurementType.__name__ in aggregate_type
                and aggregate_type[AggregateMeasurementType.__name__]
                in AggregateMeasurementType.choices()
            ):
                raise ValueError(
                    f"Aggregate type must be a dict with single element, "
                    f"key of '{AggregateMeasurementType.__name__}', "
                    f"and value in {AggregateMeasurementType.choices()}"
                )

            aggregate_type_value = aggregate_type[AggregateMeasurementType.__name__]
            aggregate_type_value = AggregateMeasurementType(aggregate_type_value)

        message_obj["aggregate_type"] = aggregate_type_value

        # Parse window
        window: OptStrToAnyDict = message_obj["window"]
        if window is None:
            window_value = None
        else:
            if not (
                isinstance(window, dict)
                and len(window) == 1
                and "int" in window
                and isinstance(window["int"], int)
            ):
                raise ValueError(
                    "Window must be a dict with single element and key of 'int' and integer value"
                )
            window_value = window["int"]

        message_obj["window"] = window_value

        return cls.model_validate(message_obj)


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

    @classmethod
    def from_message(cls, message: Message) -> "RegularMeasurement":
        message_obj: StrToAnyDict = json.loads(message.data.decode())

        for key in cls.model_fields.keys():
            if key not in message_obj:
                raise ValueError(f"Key '{key}' did not exist in the message")

        # Parse type
        type = message_obj["type"]
        if type != MeasurementType.REGULAR.value:
            raise ValueError(
                f"Type must be {MeasurementType.REGULAR} for Regular Measurement"
            )

        # Parse aggregate type
        aggregate_type = message_obj["aggregate_type"]
        if aggregate_type is not None:
            raise ValueError("Aggregate type must be None for Regular Measurement")

        # Parse window
        window = message_obj["window"]
        if window is not None:
            raise ValueError("Window must be None for Regular Measurement")

        return cls.model_validate(message_obj)


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

    @classmethod
    def from_message(cls, message: Message) -> "AggregateMeasurement":
        message_obj: StrToAnyDict = json.loads(message.data.decode())

        for key in cls.model_fields.keys():
            if key not in message_obj:
                raise ValueError(f"Key '{key}' did not exist in the message")

        # Parse type
        type = message_obj["type"]
        if type != MeasurementType.AGGREGATE.value:
            raise ValueError(
                f"Type must be {MeasurementType.AGGREGATE} for Aggregate Measurement"
            )

        # Parse aggregate type
        aggregate_type: OptStrToStrDict = message_obj["aggregate_type"]
        if not (
            isinstance(aggregate_type, dict)
            and len(aggregate_type) == 1
            and AggregateMeasurementType.__name__ in aggregate_type
            and aggregate_type[AggregateMeasurementType.__name__]
            in AggregateMeasurementType.choices()
        ):
            raise ValueError(
                f"Aggregate type must be a dict with single element, "
                f"key of '{AggregateMeasurementType.__name__}', "
                f"and value in {AggregateMeasurementType.choices()}"
            )
        message_obj["aggregate_type"] = AggregateMeasurementType(
            aggregate_type[AggregateMeasurementType.__name__]
        )

        # Parse window
        window: OptStrToAnyDict = message_obj["window"]
        if not (
            isinstance(window, dict)
            and len(window) == 1
            and "int" in window
            and isinstance(window["int"], int)
        ):
            raise ValueError(
                "Window must be a dict with single element and key of 'int' and integer value"
            )
        message_obj["window"] = window["int"]

        return cls.model_validate(message_obj)


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

    @classmethod
    def from_message(cls, message: Message) -> "AverageMeasurement":
        message_obj: StrToAnyDict = json.loads(message.data.decode())

        for key in cls.model_fields.keys():
            if key not in message_obj:
                raise ValueError(f"Key '{key}' did not exist in the message")

        # Parse type
        type = message_obj["type"]
        if type != MeasurementType.AGGREGATE.value:
            raise ValueError(
                f"Type must be {MeasurementType.AGGREGATE} for Average Measurement"
            )

        # Parse aggregate type
        aggregate_type: OptStrToStrDict = message_obj["aggregate_type"]
        if not (
            isinstance(aggregate_type, dict)
            and len(aggregate_type) == 1
            and AggregateMeasurementType.__name__ in aggregate_type
            and aggregate_type[AggregateMeasurementType.__name__]
            != AggregateMeasurementType.AVERAGE.value
        ):
            raise ValueError(
                f"Aggregate type must be a dict with single element, "
                f"key of '{AggregateMeasurementType.__name__}', "
                f"and value of '{AggregateMeasurementType.AVERAGE.value}'"
            )
        message_obj["aggregate_type"] = AggregateMeasurementType(
            aggregate_type[AggregateMeasurementType.__name__]
        )

        # Parse window
        window: OptStrToAnyDict = message_obj["window"]
        if not (
            isinstance(window, dict)
            and len(window) == 1
            and "int" in window
            and isinstance(window["int"], int)
        ):
            raise ValueError(
                "Window must be a dict with single element and key of 'int' and integer value"
            )
        message_obj["window"] = window["int"]

        return cls.model_validate(message_obj)


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

    @classmethod
    def from_message(cls, message: Message) -> "PeakMeasurement":
        message_obj: StrToAnyDict = json.loads(message.data.decode())

        for key in cls.model_fields.keys():
            if key not in message_obj:
                raise ValueError(f"Key '{key}' did not exist in the message")

        # Parse type
        type = message_obj["type"]
        if type != MeasurementType.AGGREGATE.value:
            raise ValueError(
                f"Type must be {MeasurementType.AGGREGATE} for Peak Measurement"
            )

        # Parse aggregate type
        aggregate_type: OptStrToStrDict = message_obj["aggregate_type"]
        if not (
            isinstance(aggregate_type, dict)
            and len(aggregate_type) == 1
            and AggregateMeasurementType.__name__ in aggregate_type
            and aggregate_type[AggregateMeasurementType.__name__]
            != AggregateMeasurementType.PEAK.value
        ):
            raise ValueError(
                f"Aggregate type must be a dict with single element, "
                f"key of '{AggregateMeasurementType.__name__}', "
                f"and value of '{AggregateMeasurementType.PEAK.value}'"
            )
        message_obj["aggregate_type"] = AggregateMeasurementType(
            aggregate_type[AggregateMeasurementType.__name__]
        )

        # Parse window
        window: OptStrToAnyDict = message_obj["window"]
        if not (
            isinstance(window, dict)
            and len(window) == 1
            and "int" in window
            and isinstance(window["int"], int)
        ):
            raise ValueError(
                "Window must be a dict with single element and key of 'int' and integer value"
            )
        message_obj["window"] = window["int"]

        return cls.model_validate(message_obj)


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
    @classmethod
    def to_message(cls, measurement: AnyMeasurement) -> StrToAnyDict:
        return measurement.message_obj

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
            return BaseMeasurement.from_message(message)
        elif type is MeasurementType.REGULAR:
            return RegularMeasurement.from_message(message)
        elif type is MeasurementType.AGGREGATE:
            if aggregate_type is None:
                return AggregateMeasurement.from_message(message)
            elif aggregate_type is AggregateMeasurementType.AVERAGE:
                return AverageMeasurement.from_message(message)
            elif aggregate_type is AggregateMeasurementType.PEAK:
                return PeakMeasurement.from_message(message)
