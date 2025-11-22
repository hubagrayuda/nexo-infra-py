from pydantic import BaseModel, Field
from typing import Annotated
from .heartbeat.config import HeartbeatConfigMixin
from .resource.config import ResourceConfigMixin


class InfraConfig(
    ResourceConfigMixin,
    HeartbeatConfigMixin,
):
    pass


class InfraConfigMixin(BaseModel):
    infra: Annotated[InfraConfig, Field(InfraConfig(), description="Infra config")] = (
        InfraConfig()
    )
