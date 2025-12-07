import asyncio
from datetime import datetime, timezone
from google.cloud.pubsub_v1.publisher.futures import Future
from uuid import UUID, uuid4
from nexo.logging.enums import LogLevel
from nexo.logging.logger import Application
from nexo.schemas.application import ApplicationContext, OptApplicationContext
from nexo.schemas.connection import OptConnectionContext
from nexo.schemas.google import PublisherHandler, ListOfPublisherHandlers
from nexo.schemas.operation.action.system import SystemOperationAction
from nexo.schemas.operation.context import generate
from nexo.schemas.operation.enums import (
    SystemOperationType,
    Origin,
    Layer,
    Target,
)
from nexo.schemas.operation.mixins import Timestamp
from nexo.schemas.operation.system import SuccessfulSystemOperation
from nexo.schemas.response import SingleDataResponse
from nexo.schemas.security.authentication import OptAnyAuthentication
from nexo.schemas.security.authorization import OptAnyAuthorization
from nexo.schemas.security.impersonation import OptImpersonation
from nexo.types.uuid import OptUUID
from nexo.utils.exception import extract_details
from ..request.monitor import RequestMonitor
from .config import HeartbeatConfig
from .schemas import Heartbeat


class HeartbeatMonitor:
    def __init__(
        self,
        operation_id: UUID,
        started_at: datetime,
        config: HeartbeatConfig,
        request_monitor: RequestMonitor,
        logger: Application,
        publish: bool = False,
        publisher: PublisherHandler | None = None,
        operation_publishers: ListOfPublisherHandlers = [],
        application_context: OptApplicationContext = None,
    ) -> None:
        self.application_context = (
            application_context
            if application_context is not None
            else ApplicationContext.new()
        )
        self.operation_id = operation_id
        self.started_at = started_at
        self.config = config
        self.request_monitor = request_monitor
        self.logger = logger
        self.publish = publish
        self.publisher = publisher
        self.operation_publishers = operation_publishers
        self._lock = asyncio.Lock()
        self.is_monitoring = False
        self.monitor_task: asyncio.Task | None = None
        self.latest_heartbeat: Heartbeat | None = None

        # Operation context setup
        self.operation_context = generate(
            origin=Origin.SERVICE, layer=Layer.INFRASTRUCTURE, target=Target.MONITORING
        )
        self.operation_action = SystemOperationAction(
            type=SystemOperationType.HEARTBEAT
        )

    async def start_monitoring(self) -> None:
        """Start the heartbeat monitoring loop."""
        if self.is_monitoring:
            return

        self.is_monitoring = True
        self.monitor_task = asyncio.create_task(self._monitor_loop())

    async def stop_monitoring(self) -> None:
        """Stop the heartbeat monitoring loop."""
        self.is_monitoring = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
            self.monitor_task = None

    async def _monitor_loop(self) -> None:
        """Internal monitoring loop."""
        frequency = self.config.frequency
        log_count: int = 1
        publish_count: int = 1
        while self.is_monitoring:
            try:
                await self._check(
                    frequency.should_log and frequency.log == log_count,
                    frequency.should_publish and frequency.publish == publish_count,
                )

                # Calculate log_count
                if frequency.should_log and log_count >= frequency.log:
                    log_count = 1
                else:
                    log_count += 1

                # Calculate publish_count
                if frequency.should_publish and publish_count >= frequency.publish:
                    publish_count = 1
                else:
                    publish_count += 1

                await asyncio.sleep(self.config.interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(
                    "Unexpected error occured in heartbeat monitoring",
                    exc_info=True,
                    extra={"json_fields": {"exc_details": extract_details(e)}},
                )
                await asyncio.sleep(self.config.interval)

    async def _check(self, log: bool = False, publish: bool = False):
        """Collect current heartbeat."""
        executed_at = datetime.now(tz=timezone.utc)

        request = await self.request_monitor.get_summary(from_timestamp=executed_at)

        heartbeat = Heartbeat(
            started_at=self.started_at,
            checked_at=executed_at,
            request=request,
        )

        timestamp = Timestamp.completed_now(executed_at)

        self.latest_heartbeat = heartbeat

        response = SingleDataResponse[Heartbeat, None].new(data=heartbeat)

        operation = SuccessfulSystemOperation[SingleDataResponse[Heartbeat, None]](
            application_context=self.application_context,
            id=self.operation_id,
            context=self.operation_context,
            action=self.operation_action,
            timestamp=timestamp,
            summary=f"Successfully checked heartbeat - Uptime: {heartbeat.uptime.stringify()} - {heartbeat.request.string_summary}",
            connection_context=None,
            authentication=None,
            authorization=None,
            impersonation=None,
            response=response,
        )

        if log:
            operation.log(self.logger, LogLevel.INFO)
        operation.publish(self.logger, self.operation_publishers)

        if self.publish and publish and self.publisher is not None:
            topic_path = self.publisher.client.topic_path(
                self.publisher.project_id, self.publisher.topic_id
            )
            try:
                future: Future = self.publisher.client.publish(
                    topic=topic_path,
                    data=heartbeat.model_dump_json().encode(),
                    **self.application_context.model_dump(mode="json"),
                )
                message_id: str = future.result()

                self.logger.debug(
                    f"Successfully published heartbeat message {message_id} to {topic_path}",
                    extra={
                        "json_fields": {
                            "heartbeat": heartbeat.model_dump(mode="json"),
                            "message_id": message_id,
                            "topic_path": topic_path,
                        }
                    },
                )

            except Exception as e:
                self.logger.error(
                    f"Failed publishing heartbeat message to {topic_path}",
                    exc_info=True,
                    extra={
                        "json_fields": {
                            "heartbeat": heartbeat.model_dump(mode="json"),
                            "exc_details": extract_details(e),
                        }
                    },
                )

    async def check_instant(
        self,
        *,
        operation_id: OptUUID = None,
        connection_context: OptConnectionContext = None,
        authentication: OptAnyAuthentication = None,
        authorization: OptAnyAuthorization = None,
        impersonation: OptImpersonation = None,
    ) -> SingleDataResponse[Heartbeat, None]:
        """
        Get an instant heartbeat reading without affecting the monitoring loop.
        """
        operation_id = operation_id if operation_id is not None else uuid4()
        executed_at = datetime.now(tz=timezone.utc)

        request = await self.request_monitor.get_summary(from_timestamp=executed_at)

        heartbeat = Heartbeat(
            started_at=self.started_at,
            checked_at=executed_at,
            request=request,
        )

        timestamp = Timestamp.completed_now(executed_at)

        response = SingleDataResponse[Heartbeat, None].new(data=heartbeat)

        operation = SuccessfulSystemOperation[SingleDataResponse[Heartbeat, None]](
            application_context=self.application_context,
            id=self.operation_id,
            context=self.operation_context,
            action=self.operation_action,
            timestamp=timestamp,
            summary=f"Successfully checked heartbeat - Uptime: {heartbeat.uptime.stringify()} - {heartbeat.request.string_summary}",
            connection_context=connection_context,
            authentication=authentication,
            authorization=authorization,
            impersonation=impersonation,
            response=response,
        )
        operation.log(self.logger, LogLevel.INFO)
        operation.publish(self.logger, self.operation_publishers)

        return response
