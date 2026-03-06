This sample demonstrates how different types of errors propagate from Nexus handler
operations back to the calling workflow.

The caller workflow sends a `scenario` string that controls which error path is exercised.
All errors are caught as `NexusOperationError` on the caller side; the `cause` reveals the
origin.

### Error scenarios

#### 1. `application-error` (workflow_run_operation)

The handler workflow raises a non-retryable `ApplicationError` with type `"InvalidInput"`.
The caller catches `NexusOperationError` whose cause is `ApplicationError`, preserving the
original message, error type, and non-retryable flag.

```
Caught NexusOperationError caused by ApplicationError: message='intentional failure from handler workflow', type='InvalidInput', non_retryable=True
```

#### 2. `application-error-default` (workflow_run_operation)

The handler workflow raises an `ApplicationError` with default settings (retryable, no
explicit type). The caller catches `NexusOperationError` whose cause is `ApplicationError`
with `type='None'` and `non_retryable=False`.

```
Caught NexusOperationError caused by ApplicationError: message='handler workflow failed', type='None', non_retryable=False
```

#### 3. `handler-error` (workflow_run_operation)

The operation handler raises `HandlerError(type=BAD_REQUEST)` *before* starting the backing
workflow. This represents input validation at the Nexus handler level. The caller catches
`NexusOperationError` whose cause is `HandlerError` with the original type preserved.

```
Caught NexusOperationError caused by HandlerError: message='handler error (BAD_REQUEST): handler rejected the request', type=HandlerErrorType.BAD_REQUEST
```

#### 4. `sync-handler-error` (sync_operation)

The sync operation handler raises `HandlerError(type=NOT_FOUND)`. The caller catches
`NexusOperationError` whose cause is `HandlerError`, same error propagation as scenario 3
but from a sync operation instead of a workflow run operation.

```
Caught NexusOperationError caused by HandlerError: message='handler error (NOT_FOUND): sync operation not found error', type=HandlerErrorType.NOT_FOUND
```

#### 5. `sync-operation-error` (sync_operation)

The sync operation handler raises `OperationError(state=FAILED)`, representing a completed
but failed operation result. The caller catches `NexusOperationError` whose cause is
`ApplicationError` with `type='NexusFailure'` and `non_retryable=True` — the SDK converts
the `OperationError` into this form.

```
Caught NexusOperationError caused by ApplicationError: message='sync operation failed', type='NexusFailure', non_retryable=True
```

### Sample directory structure

- [service.py](./service.py) - shared Nexus service definition
- [caller](./caller) - caller workflow that executes the Nexus operations, together with a worker and starter
- [handler](./handler) - Nexus operation handlers, handler workflow, and worker

### Instructions

Start a Temporal server. (See the main samples repo [README](../README.md)).

Run the following to create the caller and handler namespaces, and the Nexus endpoint:

```
temporal operator namespace create --namespace nexus-error-scenarios-handler-namespace
temporal operator namespace create --namespace nexus-error-scenarios-caller-namespace

temporal operator nexus endpoint create \
  --name nexus-error-scenarios-nexus-endpoint \
  --target-namespace nexus-error-scenarios-handler-namespace \
  --target-task-queue nexus-error-scenarios-handler-task-queue \
  --description-file nexus_error_scenarios/endpoint_description.md
```

In one terminal, run the handler worker:
```
uv run nexus_error_scenarios/handler/worker.py
```

In another terminal, run the caller (runs all 5 scenarios):
```
uv run nexus_error_scenarios/caller/app.py
```
