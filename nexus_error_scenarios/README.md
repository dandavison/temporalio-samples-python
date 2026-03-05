This sample demonstrates how errors propagate from a Nexus handler workflow back to the
calling workflow.

### Error scenario: `application-error`

The caller workflow invokes a Nexus operation backed by a handler workflow. The handler
workflow raises a non-retryable `ApplicationError` with type `"InvalidInput"`. This
propagates back to the caller as a `NexusOperationError` whose `cause` is an
`ApplicationError`, preserving the original message, error type, and non-retryable flag.

The caller workflow catches the `NexusOperationError`, inspects the `ApplicationError`
cause, and returns a string describing what it received.

Expected output:

```
Caught NexusOperationError caused by ApplicationError: message='intentional failure from handler workflow', type='InvalidInput', non_retryable=True
```

### Sample directory structure

- [service.py](./service.py) - shared Nexus service definition
- [caller](./caller) - caller workflow that executes the Nexus operation, together with a worker and starter
- [handler](./handler) - Nexus operation handler, handler workflow, and worker

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

In another terminal, run the caller:
```
uv run nexus_error_scenarios/caller/app.py
```
