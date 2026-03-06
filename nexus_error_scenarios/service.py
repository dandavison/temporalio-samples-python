from dataclasses import dataclass

import nexusrpc


@dataclass
class ErrorScenarioInput:
    scenario: str


@dataclass
class ErrorScenarioOutput:
    message: str


SCENARIOS = [
    "application-error",
    "application-error-default",
    "handler-error",
    "sync-handler-error",
    "sync-operation-error",
]


@nexusrpc.service
class ErrorScenarioNexusService:
    workflow_run_operation: nexusrpc.Operation[ErrorScenarioInput, ErrorScenarioOutput]
    sync_operation: nexusrpc.Operation[ErrorScenarioInput, ErrorScenarioOutput]
