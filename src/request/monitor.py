import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
from nexo.types.datetime import OptDatetime
from .schemas import Record, Error, Latency, Summary


class RequestMonitor:
    def __init__(self, started_at: datetime) -> None:
        self.started_at = started_at
        self.records: deque[Record] = deque[Record]()
        self._lock = asyncio.Lock()

    async def add_record(self, record: Record):
        async with self._lock:
            self.records.append(record)

    async def get_summary(
        self,
        from_timestamp: OptDatetime = None,
        interval: int | timedelta | None = None,
    ) -> Summary:
        if from_timestamp is None:
            from_timestamp = datetime.now(tz=timezone.utc)
        if interval is None:
            cutoff_time = self.started_at
        elif isinstance(interval, int):
            cutoff_time = from_timestamp - timedelta(seconds=interval)
        elif isinstance(interval, timedelta):
            cutoff_time = from_timestamp - interval

        async with self._lock:
            data = [
                record for record in self.records if record.requested_at >= cutoff_time
            ]

        if not data:
            return Summary()  # type: ignore

        return Summary(
            total=len(data),
            error=Error(
                client=len(
                    [
                        dt
                        for dt in data
                        if dt.status_code >= 400 and dt.status_code < 500
                    ]
                ),
                server=len(
                    [
                        dt
                        for dt in data
                        if dt.status_code >= 500 and dt.status_code < 600
                    ]
                ),
            ),
            latency=Latency(
                min=min([dt.latency for dt in data]),
                avg=sum([dt.latency for dt in data]) / len(data),
                max=max([dt.latency for dt in data]),
            ),
        )
