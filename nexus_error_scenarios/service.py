from dataclasses import dataclass

import nexusrpc


@dataclass
class ErrorScenarioInput:
    scenario: str


@dataclass
class ErrorScenarioOutput:
    message: str


@nexusrpc.service
class ErrorScenarioNexusService:
    error_scenario_operation: nexusrpc.Operation[
        ErrorScenarioInput, ErrorScenarioOutput
    ]
