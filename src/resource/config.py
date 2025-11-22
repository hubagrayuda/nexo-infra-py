from pydantic import BaseModel, Field
from typing import Annotated


class FrequencyConfig(BaseModel):
    log: Annotated[int, Field(60, description="Logging Frequency", ge=0)] = 60

    @property
    def should_log(self) -> bool:
        return self.log > 0

    publish: Annotated[int, Field(1, description="Publishing Frequency", ge=0)] = 1

    @property
    def should_publish(self) -> bool:
        return self.publish > 0

    websocket: Annotated[int, Field(1, description="Websocket Frequency", ge=0)] = 1

    @property
    def should_send_websocket(self) -> bool:
        return self.websocket > 0


class MeasurementConfig(BaseModel):
    frequency: Annotated[
        FrequencyConfig,
        Field(FrequencyConfig(), description="Frequency"),
    ] = FrequencyConfig()
    interval: Annotated[
        float, Field(5.0, description="Monitor interval (s)", ge=1.0)
    ] = 5.0
    window: Annotated[int, Field(5, description="Smoothing window", ge=5)] = 5
    retention: Annotated[
        int,
        Field(
            3600,
            description="Monitor data retention (s)",
            ge=60,
            le=7200,
            multiple_of=60,
        ),
    ] = 3600


class ThresholdConfig(BaseModel):
    low: Annotated[float, Field(10.0, description="Low", ge=0.0)] = 10.0
    normal: Annotated[float, Field(75.0, description="Normal", ge=0.0)] = 75.0
    high: Annotated[float, Field(85.0, description="High", ge=0.0)] = 85.0
    critical: Annotated[float, Field(95.0, description="Critical", ge=0.0)] = 95.0


class CPUUsageConfig(BaseModel):
    threshold: Annotated[
        ThresholdConfig, Field(ThresholdConfig(), description="Threshold")
    ] = ThresholdConfig()


class MemoryUsageConfig(BaseModel):
    limit: Annotated[
        float,
        Field(
            256.0, description="Memory limit (MB) applied to raw memory value", ge=0.0
        ),
    ] = 256.0
    threshold: Annotated[
        ThresholdConfig, Field(ThresholdConfig(), description="Threshold")
    ] = ThresholdConfig()


class UsageConfig(BaseModel):
    cpu: Annotated[CPUUsageConfig, Field(CPUUsageConfig(), description="CPU Usage")] = (
        CPUUsageConfig()
    )
    memory: Annotated[
        MemoryUsageConfig,
        Field(MemoryUsageConfig, description="Memory Usage"),
    ] = MemoryUsageConfig()


class ResourceConfig(BaseModel):
    measurement: Annotated[
        MeasurementConfig,
        Field(
            MeasurementConfig,
            description="Resource usage configuration",
        ),
    ] = MeasurementConfig()

    usage: Annotated[UsageConfig, Field(UsageConfig(), description="Usage config")] = (
        UsageConfig()
    )


class ResourceConfigMixin(BaseModel):
    resource: Annotated[
        ResourceConfig,
        Field(ResourceConfig(), description="Resource config"),
    ] = ResourceConfig()
