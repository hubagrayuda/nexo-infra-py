from datetime import datetime
from pydantic import BaseModel, Field, model_validator
from typing import Annotated, Self
from .enums import Status


class Record(BaseModel):
    requested_at: Annotated[datetime, Field(..., description="Requested At")]
    status_code: Annotated[int, Field(..., description="Status Code", ge=100, le=600)]
    latency: Annotated[float, Field(0.0, description="Latency", ge=0.0)] = 0.0


class Error(BaseModel):
    client: Annotated[int, Field(0, description="Client error", ge=0)] = 0
    server: Annotated[int, Field(0, description="Server error", ge=0)] = 0


class Latency(BaseModel):
    min: Annotated[float, Field(0.0, description="Min Latency", ge=0.0)] = 0.0
    avg: Annotated[float, Field(0.0, description="Avg Latency", ge=0.0)] = 0.0
    max: Annotated[float, Field(0.0, description="Max Latency", ge=0.0)] = 0.0


class Summary(BaseModel):
    total: Annotated[int, Field(0, description="Total", ge=0)]
    error: Annotated[Error, Field(default_factory=Error, description="Error")]  # type: ignore
    latency: Annotated[Latency, Field(default_factory=Latency, description="Latency")]  # type: ignore
    status: Annotated[Status, Field(Status.HEALTHY, description="Status")] = (
        Status.HEALTHY
    )

    @model_validator(mode="after")
    def define_status(self) -> Self:
        if self.total == 0:
            self.status = Status.HEALTHY
            return self
        error_ratio = self.error.server / self.total
        if error_ratio < 0.01:
            self.status = Status.HEALTHY
        elif 0.01 <= error_ratio < 0.05:
            self.status = Status.DEGRADED
        elif 0.05 <= error_ratio < 0.2:
            self.status = Status.UNSTABLE
        else:
            self.status = Status.CRITICAL
        return self
