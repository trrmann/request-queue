# issues

## open

- Publish to GitHub and switch dependents from `file:` to `github:` dependencies (owner decision: visibility).
- Idempotency: a retried request whose first try reached the service but lost its response can be applied twice.
  Send an `Idempotency-Key` per job and have services de-duplicate (iam-service first).
- Optional keep-warm: a periodic health ping can keep a free-tier service awake, but it consumes the account's
  free instance hours (shared by all services); decide per service.
