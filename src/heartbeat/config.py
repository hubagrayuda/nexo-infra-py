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


class HeartbeatConfig(BaseModel):
    frequency: Annotated[
        FrequencyConfig,
        Field(FrequencyConfig(), description="Frequency"),
    ] = FrequencyConfig()
    interval: Annotated[
        float, Field(5.0, description="Monitor interval (s)", ge=1.0)
    ] = 5.0


class HeartbeatConfigMixin(BaseModel):
    heartbeat: Annotated[
        HeartbeatConfig,
        Field(HeartbeatConfig(), description="Heartbeat config"),
    ] = HeartbeatConfig()
