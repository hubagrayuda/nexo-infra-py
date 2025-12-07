from datetime import datetime, timezone
from pydantic import Field, computed_field
from typing import Annotated, Self
from nexo.schemas.mixins.timestamp import StartTimestamp, Uptime
from ..request.schemas import Summary


class Heartbeat(StartTimestamp[datetime]):
    checked_at: Annotated[
        datetime, Field(datetime.now(tz=timezone.utc), description="Checked At")
    ] = datetime.now(tz=timezone.utc)

    @computed_field
    @property
    def uptime(self) -> Uptime:
        return Uptime.from_timedelta(self.checked_at - self.started_at)

    request: Annotated[Summary, Field(default_factory=Summary, description="Request")]  # type: ignore

    @classmethod
    def new(cls, started_at: datetime, checked_at: datetime, request: Summary) -> Self:
        return cls(started_at=started_at, checked_at=checked_at, request=request)
