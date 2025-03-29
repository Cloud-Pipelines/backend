# Based on cloud_pipelines/orchestration/launchers/interfaces.py

import abc
import dataclasses
import datetime
import enum

import typing
from typing import Any

from .. import component_structures as structures

_T = typing.TypeVar("_T")
_TLauncher = typing.TypeVar("_TLauncher", bound="ContainerTaskLauncher")
_TLaunchedContainer = typing.TypeVar("_TLaunchedContainer", bound="LaunchedContainer")


__all__ = [
    "ContainerTaskLauncher",
    "InputArgument",
]


class LauncherError(RuntimeError):
    pass


@dataclasses.dataclass(kw_only=True)
class InputArgument:
    total_size: int
    is_dir: bool
    value: str | None = None
    uri: str | None = None
    staging_uri: str


class ContainerTaskLauncher(typing.Generic[_TLaunchedContainer], abc.ABC):
    @abc.abstractmethod
    def launch_container_task(
        self,
        *,
        component_spec: structures.ComponentSpec,
        input_arguments: dict[str, InputArgument],
        output_uris: dict[str, str],
        log_uri: str,
        annotations: dict[str, Any] | None = None,
    ) -> _TLaunchedContainer:
        raise NotImplementedError()

    def deserialize_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> _TLaunchedContainer:
        raise NotImplementedError()

    def get_refreshed_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> _TLaunchedContainer:
        raise NotImplementedError()


class ContainerStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ERROR = "ERROR"


class LaunchedContainer(abc.ABC, typing.Generic[_TLauncher]):

    # @classmethod
    # def get(cls: typing.Type[_TLaunchedContainer]) -> _TLaunchedContainer:
    #     raise NotImplementedError()

    # @property
    # def id(self) -> str:
    #     raise NotImplementedError()

    @property
    def status(self) -> ContainerStatus:
        raise NotImplementedError()

    @property
    def exit_code(self) -> int | None:
        raise NotImplementedError()

    @property
    def has_ended(self) -> bool:
        raise NotImplementedError()

    @property
    def has_succeeded(self) -> bool:
        raise NotImplementedError()

    @property
    def has_failed(self) -> bool:
        raise NotImplementedError()

    @property
    def started_at(self) -> datetime.datetime | None:
        raise NotImplementedError()

    @property
    def ended_at(self) -> datetime.datetime | None:
        raise NotImplementedError()

    def upload_logs(self):
        raise NotImplementedError()

    def to_dict(self) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> typing.Self:
        raise NotImplementedError()

    def get_refreshed(self, launcher: _TLauncher) -> typing.Self:
        raise NotImplementedError()

    def get_log(self, launcher: _TLauncher) -> str:
        raise NotImplementedError()

    def upload_log(self, launcher: _TLauncher):
        raise NotImplementedError()

    def stream_log_lines(self, launcher: _TLauncher) -> typing.Iterator[str]:
        raise NotImplementedError()


# @dataclasses.dataclass
# class ContainerExecutionResult:
#     start_time: datetime.datetime
#     end_time: datetime.datetime
#     exit_code: int
#     # TODO: Replace with logs_artifact
#     ### log: "ProcessLog"
