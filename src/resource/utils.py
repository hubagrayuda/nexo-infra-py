from .constants import STATUS_ORDER
from .enums import Status


def aggregate_status(cpu: Status, memory: Status) -> Status:
    severity_index = {s: i for i, s in enumerate(STATUS_ORDER)}
    return max(cpu, memory, key=lambda s: severity_index[s])
