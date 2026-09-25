# Local Python read diagnostics inside the gateway

The terminal lifecycle guard supports a deliberately narrow read-only Python
form on explicitly local backends. Its rejection message supplies the actual
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
unknown backends have no exemption. A script-reader callback alone does not
identify a remote backend.

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

Direct lifecycle checks still run. A JSON file containing example lifecycle
text can be read, but an inline lifecycle-command string may still be rejected
conservatively. Shell wrappers, substitutions, referenced scripts and commands
after heredoc boundaries retain their separate checks; file permissions do not
grant an exemption.

Other Python invocation forms retain the existing conservative classification
and may reject harmless reads. This is not a general Python sandbox or a
complete shell/interpreter analyzer. File reads can still fail or block on
filesystem resources; normal terminal timeouts apply. Genuine gateway control
must use the external operator path rather than a command inside the gateway.
