from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol, TypeVar

from ai_trading_system.contracts import ArtifactPointer, ResearchLifecycleRecord
from ai_trading_system.research_framework.spec import ExperimentSpec, PluginRef

PluginT = TypeVar("PluginT")


class ResearchPluginError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class ExperimentExecutionContext:
    spec: ExperimentSpec
    sources: Mapping[str, Any]
    source_artifacts: tuple[Mapping[str, object], ...]
    as_of: date


class CalculatorPlugin(Protocol):
    plugin_id: str
    version: str

    def calculate(self, context: ExperimentExecutionContext) -> dict[str, Any]: ...


class ReportPlugin(Protocol):
    plugin_id: str
    version: str

    def section(self, payload: Mapping[str, Any], section_id: str) -> Mapping[str, Any]: ...

    def render_markdown(self, payload: Mapping[str, Any]) -> str: ...


class LifecyclePlugin(Protocol):
    plugin_id: str
    version: str

    def build(
        self,
        context: ExperimentExecutionContext,
        payload: Mapping[str, Any],
        primary_artifact: ArtifactPointer,
        generated_at: datetime,
    ) -> ResearchLifecycleRecord: ...


class PluginRegistry:
    def __init__(
        self,
        *,
        calculators: Sequence[CalculatorPlugin],
        reports: Sequence[ReportPlugin],
        lifecycles: Sequence[LifecyclePlugin] = (),
    ) -> None:
        self._calculators = _index_plugins(calculators, "calculator")
        self._reports = _index_plugins(reports, "report")
        self._lifecycles = _index_plugins(lifecycles, "lifecycle")

    def calculator(self, reference: PluginRef) -> CalculatorPlugin:
        return _resolve_plugin(self._calculators, reference, "calculator")

    def report(self, reference: PluginRef) -> ReportPlugin:
        return _resolve_plugin(self._reports, reference, "report")

    def optional_lifecycle(self, reference: PluginRef) -> LifecyclePlugin | None:
        return self._lifecycles.get((reference.plugin_id, reference.version))


def _index_plugins(plugins: Sequence[PluginT], kind: str) -> dict[tuple[str, str], PluginT]:
    indexed: dict[tuple[str, str], PluginT] = {}
    for plugin in plugins:
        plugin_id = str(getattr(plugin, "plugin_id", ""))
        version = str(getattr(plugin, "version", ""))
        if not plugin_id or not version:
            raise ResearchPluginError("PLUGIN_IDENTITY_REQUIRED", kind)
        key = (plugin_id, version)
        if key in indexed:
            raise ResearchPluginError("DUPLICATE_RESEARCH_PLUGIN", f"{kind}:{key}")
        indexed[key] = plugin
    return indexed


def _resolve_plugin(
    plugins: Mapping[tuple[str, str], PluginT],
    reference: PluginRef,
    kind: str,
) -> PluginT:
    key = (reference.plugin_id, reference.version)
    if key not in plugins:
        raise ResearchPluginError(
            "UNKNOWN_RESEARCH_PLUGIN",
            f"{kind}:{reference.plugin_id}:{reference.version}",
        )
    return plugins[key]


__all__ = [
    "CalculatorPlugin",
    "ExperimentExecutionContext",
    "LifecyclePlugin",
    "PluginRegistry",
    "ReportPlugin",
    "ResearchPluginError",
]
