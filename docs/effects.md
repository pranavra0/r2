# Effects

Effects are the only way runtime work asks for outside-world authority.

Policy glossary:

- `volatile`: not cacheable; live world actions.
- `stable`: cacheable without declared inputs; deterministic helpers.
- `declared`: declared provenance, not cacheable.
- `hermetic`: declared provenance and cacheable.

Built-ins:

- `thunk.force`: force delayed work and apply cache policy.
- `thunk.force_all`: force an independent frontier.
- `record.get`: runtime field access helper.

Host effects:

- `fs.read`
- `fs.write`
- `clock.now`
- `clock.sleep`
- `math.*`
- `process.spawn`
- `service.supervise`

Process glossary:

- `argv`: command vector.
- `env_mode`: `clear` or `inherit`.
- `env`: explicit environment overrides.
- `cwd`: optional working directory.
- `stdin`: input bytes.
- `declared_inputs`: files hashed into provenance.
- `declared_outputs`: files captured and rematerialized on cache hits.

Service glossary:

- `restart_policy`: mode, max restart count, and delay.
- `service.supervise`: volatile process supervision with lifecycle trace events.
