```bash
temporal operator namespace create --namespace a2a-namespace

temporal operator nexus endpoint create \
  --name a2a-nexus-endpoint \
  --target-namespace a2a-namespace \
  --target-task-queue a2a-handler-task-queue
```