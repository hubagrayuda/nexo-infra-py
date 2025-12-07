from datetime import datetime
from pydantic import BaseModel, Field, computed_field, model_validator
from typing import Annotated, Self
from .enums import Status


class Record(BaseModel):
    requested_at: Annotated[datetime, Field(..., description="Requested At")]
    status_code: Annotated[int, Field(..., description="Status Code", ge=100, le=600)]
    latency: Annotated[float, Field(0.0, description="Latency", ge=0.0)] = 0.0


class Error(BaseModel):
    client: Annotated[int, Field(0, description="Client error", ge=0)] = 0
    server: Annotated[int, Field(0, description="Server error", ge=0)] = 0


class ErrorRate(BaseModel):
    client: Annotated[float, Field(0.0, description="Client error", ge=0.0)] = 0.0
    server: Annotated[float, Field(0.0, description="Server error", ge=0.0)] = 0.0


class Latency(BaseModel):
    min: Annotated[float, Field(0.0, description="Min Latency", ge=0.0)] = 0.0
    avg: Annotated[float, Field(0.0, description="Avg Latency", ge=0.0)] = 0.0
    max: Annotated[float, Field(0.0, description="Max Latency", ge=0.0)] = 0.0


class Summary(BaseModel):
    total: Annotated[int, Field(0, description="Total", ge=0)]
    error: Annotated[Error, Field(default_factory=Error, description="Error")]  # type: ignore

    @computed_field
    @property
    def error_rate(self) -> ErrorRate:
        return ErrorRate(
            client=0 if self.total <= 0 else self.error.client / self.total,
            server=0 if self.total <= 0 else self.error.server / self.total,
        )

    latency: Annotated[Latency, Field(default_factory=Latency, description="Latency")]  # type: ignore
    status: Annotated[Status, Field(Status.HEALTHY, description="Status")] = (
        Status.HEALTHY
    )

    @model_validator(mode="after")
    def define_status(self) -> Self:
        if self.total == 0:
            self.status = Status.HEALTHY
            return self
        error_rate = self.error.server / self.total
        if error_rate < 0.01:
            self.status = Status.HEALTHY
        elif 0.01 <= error_rate < 0.05:
            self.status = Status.DEGRADED
        elif 0.05 <= error_rate < 0.2:
            self.status = Status.UNSTABLE
        else:
            self.status = Status.CRITICAL
        return self

    @property
    def string_summary(self) -> str:
        client_error_str = (
            f"Client: {self.error.client} ({self.error_rate.client*100:.2f}%)"
        )
        server_error_str = (
            f"Server: {self.error.server} ({self.error_rate.server*100:.2f}%)"
        )
        return (
            "Request "
            f"| Status: {self.status} "
            f"| Total: {self.total} "
            f"| Error; {client_error_str}; {server_error_str} "
            f"| Latency; Min: {self.latency.min:.2f}s; Avg: {self.latency.avg:.2f}s; Max: {self.latency.max:.2f}s"
        )
