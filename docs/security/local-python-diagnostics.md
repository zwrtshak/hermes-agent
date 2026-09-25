# Local Python read diagnostics inside the gateway

The terminal lifecycle guard supports a deliberately narrow read-only Python
form for gateway **foreground execution** on explicitly local backends
(`background=false`). Its rejection message supplies the actual
shell-quoted canonical interpreter path at runtime. Use that path with exactly
`-I -S -`, followed by one literal quoted heredoc:

```sh
/canonical/python -I -S - <<'PY'
from pathlib import Path
import json
data = json.loads(Path('/path/to/diagnostic.json').read_text())
print(data.get('status'))
PY
```

`/canonical/python` above is a placeholder: copy the actual interpreter from
the local tool message, not a PATH name or a virtualenv symlink. Isolation flags
exclude working-directory, environment and site imports. The interpreter and
its standard library remain trusted dependencies.

The whole command must have this form: no wrappers, pipes, other redirections,
additional heredocs, shell prefix/suffix or unquoted delimiter. Remote and
unknown backends have no exemption. Locality requires the selected concrete
`LocalEnvironment` implementation, including when an environment is cached.
Current configuration, backend names, callbacks and arbitrary attributes do
not prove locality. Foreign references are read through their backend, never
through same-named host files; only an actual local backend supplies the local
interpreter hint.

The entire Python program must pass a bounded static read proof. Supported
operations include unaliased `pathlib`/`Path` and `json` imports, literal data,
single-assignment local names, JSON decoding/encoding, data lookup, `print`,
limited `str`/`len`/`list`/`sorted`, and read-only path inspection (`read_text`,
`read_bytes`, `iterdir`, `exists`, `is_file`, `is_dir`, path names/parents).
No unknown calls, executable imports, mutation, aliases of callables,
introspection, control flow, functions/classes, encoding cookies or output
targets are accepted. Proof limits are 1 MiB of command text, 4096 AST nodes
and 64 expression levels. A rejected proof in this exact local form blocks
the command; it does not fall back to the less precise shell scan.

Canonical proven local reads with `background=true` are rejected before either
registry spawn route, including with `force=true` or `pty=true` and when a cached
local environment outlives a change to remote configuration. Background
preparation can change the verified Python input. Run these diagnostics in the
foreground; `pty=true` without background still uses the foreground path.
Ordinary background commands keep their existing dispatch and rewriting.

The real local foreground path repeats this proof before command preparation.
Proven programs retain their stdin text through sudo and compound-background
preparation. A Python variable or data string named `sudo` does not trigger a
password probe, prompt or injection. Other invocations and backends keep the
existing sudo handling.

Direct lifecycle checks still run. A JSON file containing example lifecycle
text can be read, but an inline lifecycle-command string may still be rejected
conservatively. Shell wrappers, substitutions, referenced scripts and commands
after heredoc boundaries retain their separate checks; file permissions do not
grant an exemption.
An unfinished later heredoc retains previously established boundaries and the
unfinished body in the conservative scan. Unknown consumers never receive a
body exemption.

Other Python invocation forms retain the existing conservative classification
and may reject harmless reads. Existing foreground checks can also reject
shell-like data strings; the canonical form does not bypass those checks.
This is not a general Python sandbox or a
complete shell/interpreter analyzer. File reads can still fail or block on
filesystem resources; normal terminal timeouts apply. Genuine gateway control
must use the external operator path rather than a command inside the gateway.
