from datetime import datetime, timezone
from pydantic import Field, model_validator
from typing import Annotated, Self
from nexo.schemas.mixins.timestamp import StartTimestamp, Uptime
from ..request.schemas import Summary


class Heartbeat(Uptime[float], StartTimestamp[datetime]):
    checked_at: Annotated[
        datetime, Field(datetime.now(tz=timezone.utc), description="Checked At")
    ] = datetime.now(tz=timezone.utc)

    @model_validator(mode="after")
    def calculate_uptime(self) -> Self:
        self.uptime = (self.checked_at - self.started_at).total_seconds()
        return self

    request: Annotated[Summary, Field(default_factory=Summary, description="Request")]  # type: ignore
