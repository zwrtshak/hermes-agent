"""Gateway lifecycle guard for cron job creation (#30719).

An agent running inside a gateway can schedule a cron job that calls
``hermes gateway restart`` (or ``launchctl kickstart ai.hermes.gateway``
or ``systemctl restart hermes-gateway``).  When the cron fires, the
gateway dies, the supervisor (launchd KeepAlive / systemd Restart=)
revives it, auto-resume picks up the offending session, and the resumed
turn re-runs the same logic — a SIGTERM-respawn loop every ~10 seconds
until manually broken.

This module rejects cron job specs whose prompt or script contains a
direct shell-level gateway-lifecycle command.  It is enforced at
``cron.jobs.create_job`` so it fires on every job-creation path: the
``hermes cron create`` CLI subcommand AND the agent's ``cronjob`` model
tool (which calls ``create_job`` directly, bypassing the CLI layer).

The pattern is intentionally command-shaped: it anchors on a concrete
command identifier (``hermes gateway``, ``launchctl ... hermes-gateway``,
``systemctl ... hermes-gateway``, ``pkill`` against the gateway) so it
cannot fire on prose.  A cron ``prompt`` is fed to a future LLM, not a
shell, so an over-broad substring match on English ("Kong API gateway
autoscaling and restart behavior") would produce a high false-positive
rate without preventing the actual foot-gun, which requires a real
command shape.

This is a defence-in-depth layer.  ``tools/terminal_tool.py`` blocks direct
commands and shell scripts they reference when ``_HERMES_GATEWAY=1``. It also
rejects ``launchctl submit`` in gateway sessions because launchd treats that
primitive as a persistent KeepAlive job, not a one-shot task. ``hermes gateway
stop|restart`` separately refuse to self-target from inside the gateway.
Blocking cron specs at creation time as well means the agent gets an immediate,
informative rejection instead of scheduling a job that will only fail
(silently) when it fires.
"""

from __future__ import annotations

import ast
import builtins
import logging
import os
import re
import shlex
import stat
import sys
from pathlib import Path
from typing import Callable, Iterator, Optional

logger = logging.getLogger(__name__)


class GatewayLifecycleBlocked(ValueError):
    """Raised when a cron job spec contains a gateway-lifecycle command."""


# Shell-level command shapes that target the gateway lifecycle. Each branch
# is anchored on a concrete command identifier so a match can only fire on
# actual shell-command-shaped strings, not on prose.
_GATEWAY_LIFECYCLE_PATTERN = re.compile(
    r"(?i)"
    # Branch A: `hermes gateway restart|stop` — the canonical foot-gun.
    # `start` is intentionally excluded: starting a gateway from inside a
    # gateway is benign (a no-op or "already running" error), and a
    # legitimate cron job might start a sibling profile's gateway.
    r"(?:hermes\s+gateway\s+(?:restart|stop))"
    # Branch B: launchctl ops on a hermes-gateway label. macOS launchd
    # labels look like `ai.hermes.gateway` / `hermes-gateway`. Requiring the
    # gateway identifier prevents blocking unrelated hermes services (e.g.
    # `launchctl unload ai.hermes.update-checker.plist`).
    # `submit` and `bootstrap` are included alongside the direct verbs
    # (kickstart/etc.): `launchctl submit -l ai.hermes.gateway-<suffix> --
    # <helper-script>` (or `launchctl bootstrap gui/<uid> <plist>`) creates
    # a NEW keepalive job wrapping an arbitrary helper, which is how a
    # blocked direct restart/kill gets laundered into a persistent restart
    # loop instead (#62891) — same foot-gun, indirect shape. Neutral-label
    # submissions that dodge this text anchor are caught separately by
    # `contains_launchctl_submit_command` (execution-aware, label-independent).
    r"|(?:launchctl\s+(?:kickstart|unload|load|stop|restart|submit|bootstrap)\b[^\n]*\bhermes[.\-]?gateway)"
    # Branch C: systemctl ops on a hermes-gateway unit.
    r"|(?:systemctl\s+(?:-\S+\s+)*(?:restart|stop|start)\b[^\n]*\bhermes[.\-]?gateway)"
    # Branch D: pkill / kill targeting the hermes gateway process. Both
    # token orders because real reproductions show both.
    r"|(?:\bp?kill\b[^\n]*\bhermes\b[^\n]*\bgateway)"
    r"|(?:\bp?kill\b[^\n]*\bgateway\b[^\n]*\bhermes)"
)


# A backslash immediately followed by a newline is a POSIX shell line
# continuation — the shell joins the two lines before parsing. Every branch
# above uses `[^\n]*` between its verb and the gateway identifier so the
# match can't span unrelated lines of a longer cron prompt/script, but that
# also means a real multi-line shell invocation split across continuation
# lines (e.g. `launchctl submit \` / `  -l ai.hermes.gateway-... \` / `  -- ...`,
# the exact reported shape in #62891) would otherwise slip past. Collapse
# continuations to a single space before matching, mirroring what the shell
# itself does, rather than loosening `[^\n]*` and risking false positives
# across genuinely separate lines.
_SHELL_LINE_CONTINUATION = re.compile(r"\\\r?\n[ \t]*")


def contains_gateway_lifecycle_command(text: str) -> bool:
    """Return True if *text* contains a gateway lifecycle command pattern."""
    if not text:
        return False
    normalized = _SHELL_LINE_CONTINUATION.sub(" ", text)
    return bool(_GATEWAY_LIFECYCLE_PATTERN.search(normalized))


_SHELL_EXECUTABLES = frozenset({"sh", "bash", "dash", "ksh", "zsh"})
_SHELL_OPTIONS_WITH_VALUES = frozenset({"-O", "+O", "-o", "+o"})
_MAX_REFERENCED_SCRIPT_BYTES = 1024 * 1024
_MAX_REFERENCED_SCRIPT_DEPTH = 8
_CONTROL_CHARS = frozenset(";&|()")

# Executables whose arguments are DATA, not commands: search patterns, SQL
# statements, log filters. None of these can execute their argument text, so
# a lifecycle-shaped string inside their arguments (a grep pattern hunting
# for `systemctl restart hermes-gateway` in syslog, a SQL LIKE literal over a
# restart-events table) is diagnostics, not a lifecycle command. Deliberately
# conservative: no `awk` (system()), no `sed` (`s///e`), no `echo`/`printf`
# (routinely piped into a shell), no `mysql` (`\\!` and `system` escapes).
_DATA_SINK_EXECUTABLES = frozenset(
    {"grep", "egrep", "fgrep", "rg", "ag", "ack", "journalctl", "sqlite3", "psql"}
)
# Argument shapes that can smuggle execution back INTO a data sink: command
# and process substitution anywhere, sqlite3 dot-commands (`.shell ...`),
# psql backslash escapes (`\! ...`). Any hit disables masking for the whole
# segment — fail closed to the plain regex verdict.
_UNSAFE_DATA_ARG_MARKERS = ("`", "$(", "<(", ">(", "\\!")
# A data sink piped into a shell/interpreter can feed matched lines straight
# to execution (`grep 'systemctl restart hermes-gateway' f | sh`); never mask
# such a line.
_PIPE_TO_INTERPRETER = re.compile(
    r"\|\s*&?\s*(?:sudo\s+)?(?:sh|bash|dash|ksh|zsh|xargs|eval|source)\b"
)

# Executable-image magic numbers: ELF, PE/COFF, Mach-O (universal + thin,
# both endiannesses). A referenced file starting with one of these is a
# compiled binary, never a shell script — don't read or scan it at all.
_BINARY_MAGIC_PREFIXES = (
    b"\x7fELF",
    b"MZ",
    b"\xca\xfe\xba\xbe",
    b"\xcf\xfa\xed\xfe",
    b"\xce\xfa\xed\xfe",
    b"\xfe\xed\xfa\xce",
    b"\xfe\xed\xfa\xcf",
)
_BINARY_SNIFF_BYTES = 4096




_ReadRemoteScriptFn = Callable[[str], Optional[str]]


# A deliberately small literal delimiter grammar, shared by the additional
# conservative boundary scan and the exact Python invocation proof.
_LITERAL_HEREDOC_WORD = r'''(?:[A-Za-z0-9_]|'[A-Za-z0-9_]+'|"[A-Za-z0-9_]+"|\\[A-Za-z0-9_])+'''
_HEREDOC_START = re.compile(r"<<(-?)[ \t]*(" + _LITERAL_HEREDOC_WORD + r")(?=[ \t\n;&|<>)]|$)")


def _heredoc_scan_parts(command: str) -> list[str]:
    """Expose known literal heredoc boundaries without exempting any body.

    This is an ADDITIONAL scan: the original text still goes through all the
    existing checks. Quotes in stdin data cannot hide commands after its real
    delimiter. Quoted shell -c arguments are opened by their own recursive scan.
    Unknown delimiter syntax keeps the original conservative path.
    """
    parts = []
    pending = []
    start = index = 0
    quote = None
    while index < len(command):
        char = command[index]
        if char == "\\" and quote != "'":
            index += 2
            continue
        if quote:
            if char == quote:
                quote = None
        elif char in "\"'":
            quote = char
        elif char == "#":
            end = command.find("\n", index)
            index = len(command) if end < 0 else end
            continue
        elif command.startswith("<<", index) and (index == 0 or command[index - 1] != "<"):
            match = _HEREDOC_START.match(command, index)
            if match:
                pending.append((shlex.split(match[2])[0], bool(match[1])))
                index = match.end()
                continue
        elif char == "\n" and pending:
            parts.append(command[start:index])
            index += 1
            for delimiter, strip_tabs in pending:
                start = index
                while index < len(command):
                    end = command.find("\n", index)
                    if end < 0:
                        end = len(command)
                    line = command[index:end]
                    if (line.lstrip("\t") if strip_tabs else line) == delimiter:
                        parts.append(command[start:index])
                        index = end + 1
                        break
                    index = end + 1
                else:
                    # An unfinished later body cannot erase earlier shell
                    # boundaries. Keep its text too: an unknown consumer may
                    # execute stdin even without a closing delimiter.
                    return [*parts, command[start:]]
            pending = []
            start = index
            continue
        index += 1
    if parts:
        parts.append(command[start:])
    return parts


class _ReadProofError(ValueError):
    pass


class _PythonReadProof:
    """Whole-program, fail-closed type/provenance proof; never evaluate code.

    Types describe only exact trusted stdlib results or literal data. Callable
    and module values cannot be stored in containers, aliased or rebound.
    No generic visitor: every accepted statement/expression consumes all its
    executable children; every other AST kind is rejected.
    """

    _data = frozenset({"text", "bytes", "number", "bool", "none", "data"})
    _values = _data | {"path", "paths"}
    _protected = frozenset(vars(builtins)) | {"Path", "pathlib", "json"}

    def __init__(self):
        self.names: dict[str, str] = {}

    def prove(self, body: str) -> None:
        # stdin is bytes. A coding cookie (e.g. unicode_escape) can turn an
        # apparent comment into executable statements. Reject cookies before
        # parsing, also avoiding codec lookup/imports in the guard process.
        first_lines = body.replace("\r\n", "\n").replace("\r", "\n").split("\n", 2)[:2]
        if any(re.match(r"^[ \t\f]*#.*?coding[:=]", line) for line in first_lines):
            raise _ReadProofError
        tree = ast.parse(body.encode("utf-8"))
        if sum(1 for _ in ast.walk(tree)) > 4096:
            raise _ReadProofError
        for statement in tree.body:
            if isinstance(statement, ast.Import):
                for alias in statement.names:
                    if alias.asname or alias.name not in {"pathlib", "json"} or alias.name in self.names:
                        raise _ReadProofError
                    self.names[alias.name] = alias.name
            elif isinstance(statement, ast.ImportFrom):
                if (statement.level or statement.module != "pathlib"
                        or len(statement.names) != 1 or statement.names[0].name != "Path"
                        or statement.names[0].asname or "Path" in self.names):
                    raise _ReadProofError
                self.names["Path"] = "Path"
            elif isinstance(statement, ast.Assign):
                if len(statement.targets) != 1 or not isinstance(statement.targets[0], ast.Name):
                    raise _ReadProofError
                name = statement.targets[0].id
                kind = self.expression(statement.value)
                if name.startswith("_") or name in self._protected or name in self.names or kind not in self._values:
                    raise _ReadProofError
                self.names[name] = kind
            elif isinstance(statement, ast.Expr):
                if self.expression(statement.value) not in self._values:
                    raise _ReadProofError
            else:
                raise _ReadProofError

    def expression(self, node: ast.AST, depth: int = 0) -> str:
        if depth > 64:
            raise _ReadProofError
        expr = lambda child: self.expression(child, depth + 1)
        if isinstance(node, ast.Constant):
            kind = {str: "text", bytes: "bytes", int: "number", float: "number",
                    bool: "bool", type(None): "none"}.get(type(node.value))
            if kind:
                return kind
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            if node.id in self.names and not node.id.startswith("_"):
                return self.names[node.id]
        elif isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            if all(expr(item) in self._data for item in node.elts):
                return "data"
        elif isinstance(node, ast.Dict):
            if all(key is not None and expr(key) in self._data and expr(value) in self._data
                   for key, value in zip(node.keys, node.values)):
                return "data"
        elif isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            if expr(node.operand) == "number":
                return "number"
        elif isinstance(node, ast.BinOp):
            left, right = expr(node.left), expr(node.right)
            if isinstance(node.op, ast.Div) and left == "path" and right == "text":
                return "path"
            if isinstance(node.op, ast.Add) and left == right and left in {"text", "number"}:
                return left
        elif isinstance(node, ast.Subscript):
            owner, key = expr(node.value), expr(node.slice)
            if owner == "data" and key in {"text", "number"}:
                return "data"
            if owner in {"text", "paths"} and key == "number":
                return "text" if owner == "text" else "path"
        elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load):
            owner = expr(node.value)
            if owner == "pathlib" and node.attr == "Path":
                return "Path"
            if owner == "path":
                if node.attr == "parent":
                    return "path"
                if node.attr in {"name", "suffix", "stem"}:
                    return "text"
        elif isinstance(node, ast.Call):
            # Star args and **kwargs have no statically proven call signature.
            args = [expr(arg) for arg in node.args]
            if any(kw.arg is None for kw in node.keywords):
                raise _ReadProofError
            kwargs = {kw.arg: expr(kw.value) for kw in node.keywords}
            if len(kwargs) != len(node.keywords):
                raise _ReadProofError
            if isinstance(node.func, ast.Name) and node.func.id in {"print", "list", "sorted", "str", "len"}:
                name = node.func.id
                if not kwargs:
                    if name == "print" and all(arg in self._values for arg in args):
                        return "none"
                    if len(args) == 1:
                        if name == "list" and args[0] == "path_iter":
                            return "paths"
                        if name == "sorted" and args[0] == "paths":
                            return "paths"
                        if name == "str" and args[0] in self._values:
                            return "text"
                        if name == "len" and args[0] in {"text", "bytes", "data", "paths"}:
                            return "number"
            elif isinstance(node.func, ast.Attribute):
                owner, method = expr(node.func.value), node.func.attr
                if owner == "json" and not kwargs and len(args) == 1:
                    if method == "loads" and args[0] in {"text", "bytes"}:
                        return "data"
                    if method == "dumps" and args[0] in self._data:
                        return "text"
                if owner == "path":
                    if method == "read_text" and not args and all(
                        kw.arg == "encoding" and isinstance(kw.value, ast.Constant) and kw.value.value == "utf-8"
                        for kw in node.keywords
                    ):
                        return "text"
                    if not args and not kwargs:
                        result = {"read_bytes": "bytes", "iterdir": "path_iter",
                                  "exists": "bool", "is_file": "bool", "is_dir": "bool"}.get(method)
                        if result:
                            return result
                if owner == "data" and method == "get" and not kwargs and 1 <= len(args) <= 2 and all(arg in self._data for arg in args):
                    return "data"
                if owner == "pathlib" and method == "Path" and not kwargs and args == ["text"]:
                    return "path"
            elif expr(node.func) == "Path" and not kwargs and args == ["text"]:
                return "path"
        raise _ReadProofError


def _canonical_python_body(command: str) -> Optional[str]:
    """Recognize the exact whole invocation independently of its AST proof.

    None means another invocation form; an empty body still matches this form.
    No PATH lookup, symlink spellings, wrappers, surrounding shell syntax or
    output redirects. File permissions are deliberately irrelevant.
    """
    try:
        executable = Path(sys.executable).resolve(strict=True)
        if not executable.is_absolute() or not executable.is_file():
            return None
        header, separator, rest = command.partition("\n")
        prefix = shlex.quote(str(executable)) + " -I -S - "
        match = re.fullmatch(re.escape(prefix) + r"<<(" + _LITERAL_HEREDOC_WORD + r")", header)
        if not separator or not match or not any(char in match[1] for char in "'\"\\"):
            return None
        delimiter = shlex.split(match[1])[0]
        lines = rest.split("\n")
        end = lines.index(delimiter)
        if lines[end + 1:] not in ([], [""]):
            return None
        return "\n".join(lines[:end])
    except (ValueError, SyntaxError, RecursionError, OSError, RuntimeError):
        return None


def _is_canonical_python_read(command: str) -> Optional[bool]:
    """None: other form; True: proven read; False: canonical proof rejected.

    Only the explicitly local top-level caller may use this verdict. Once the
    form matches, ANY proof failure must block, including resource limits and
    unexpected checker errors; it must never fall back to the shell scan.
    """
    body = _canonical_python_body(command)
    if body is None:
        return None
    try:
        if len(command.encode("utf-8")) > _MAX_REFERENCED_SCRIPT_BYTES:
            return False
        _PythonReadProof().prove(body)
        return True
    except Exception:
        return False


def _logical_shell_lines(command: str) -> Iterator[str]:
    """Split only unquoted newlines; shlex still owns token interpretation.

    A fresh lexer per physical line loses the enclosing quote of multiline
    interpreter data. Keep that quote boundary without treating quoted newlines
    as command separators. Comments cannot open quotes; escaped quotes cannot
    close them. This is a lexical boundary, not an interpreter exemption.
    """
    start = 0
    quote = None
    escaped = False
    comment = False
    for index, char in enumerate(command):
        if comment:
            if char != '\n':
                continue
            comment = False
        elif escaped:
            escaped = False
            continue
        elif char == '\\' and quote != "'":
            escaped = True
            continue
        elif quote:
            if char == quote:
                quote = None
            continue
        elif char in "\"'":
            quote = char
            continue
        elif char == '#':
            # Match shlex.commenters even when '#' follows an unquoted word.
            comment = True
            continue
        if char == '\n':
            yield command[start:index]
            start = index + 1
    if start < len(command):
        yield command[start:]


def _iter_command_segments(command: str) -> Iterator[list[str]]:
    """Yield shell-tokenized command segments, honoring quotes and comments."""
    normalized = command.replace("\\\n", "")
    for line in _logical_shell_lines(normalized):
        try:
            lexer = shlex.shlex(
                line,
                posix=True,
                punctuation_chars=";&|()",
            )
            lexer.whitespace_split = True
            lexer.commenters = "#"
            tokens = list(lexer)
        except ValueError:
            continue

        segment: list[str] = []
        for token in tokens:
            if token and set(token) <= _CONTROL_CHARS:
                if segment:
                    yield segment
                    segment = []
                continue
            segment.append(token)
        if segment:
            yield segment


def _command_token_index(segment: list[str]) -> Optional[int]:
    """Return the executable token index after simple env assignments."""
    for index, token in enumerate(segment):
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", token):
            continue
        return index
    return None


def _unwrapped_command_token_index(
    segment: list[str], *, skipped: Optional[list[int]] = None,
) -> Optional[int]:
    """Open simple env/command/sudo prefixes for executable scans only.

    Do not use this for data-sink masking: unwrapping must add protection,
    never grant an additional data exemption. Record each skipped executable
    so referenced wrapper files remain visible to the script scan.
    """
    index = _command_token_index(segment)
    flags = {"env": {"-i", "--ignore-environment"}, "command": {"-p"},
             "sudo": {"-n", "-E", "-H"}}
    values = {"env": {"-u", "--unset"}, "command": set(),
              "sudo": {"-u", "-g", "--user", "--group"}}
    while index is not None and index < len(segment):
        name = Path(segment[index]).name
        if name not in flags:
            return index
        original = index
        index += 1
        while index < len(segment):
            token = segment[index]
            if token == "--":
                index += 1
                break
            if token in values[name]:
                index += 2
            elif token in flags[name] or re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", token):
                index += 1
            elif token.startswith("-"):
                return original  # Unknown wrapper syntax: no guessed payload.
            else:
                break
        if skipped is not None:
            skipped.append(original)
    return None


def contains_launchctl_submit_command(command: str) -> bool:
    """Detect an executed ``launchctl submit``/``bootstrap``, not quoted text.

    Label-independent by design: the label of a submitted/bootstrapped job is
    chosen by whoever writes it, so a neutral name (``ai.hermes.svc-reload-tmp``)
    defeats any label-anchored regex (#62891, second reproduction). Both verbs
    register a NEW persistent launchd job (``submit`` jobs get KeepAlive
    semantics; ``bootstrap`` loads an arbitrary plist), which is never safe to
    do from inside the gateway process.
    """
    for segment in _iter_command_segments(command):
        index = _command_token_index(segment)
        if index is None:
            continue
        if Path(segment[index]).name == "launchctl":
            arguments = segment[index + 1 :]
            if arguments and arguments[0].lower() in {"submit", "bootstrap"}:
                return True
    return False


def _mask_data_sink_arguments(text: str) -> str:
    """Replace data-sink executables' arguments with a neutral placeholder.

    The lifecycle regex is command-shaped, but it cannot tell an EXECUTED
    ``systemctl restart hermes-gateway`` from the same characters appearing
    as *data* — a grep/rg pattern, a journalctl filter, a SQL string literal
    passed to sqlite3/psql. Those diagnostics commands were being rejected
    (false positives blocking legitimate cron prompts), e.g.::

        grep -c 'systemctl restart hermes-gateway' /var/log/syslog
        sqlite3 db "SELECT msg FROM log WHERE msg LIKE '%systemctl restart hermes-gateway%'"

    This masker shell-tokenizes each line and, for command segments whose
    executable is a known data sink (``_DATA_SINK_EXECUTABLES``), replaces
    every argument with ``arg``. The caller then re-runs the lifecycle regex
    on the masked text: a match that survives masking sits OUTSIDE any data
    argument and is a real command.

    Strictly fail-closed: masking is skipped (leaving the original,
    regex-matching text in place) whenever the line pipes into a shell or
    interpreter, any argument carries an execution-capable marker
    (substitution, sqlite3 ``.``-commands, psql ``\\!``), or the line cannot
    be tokenized at all. Masking can therefore only ever ALLOW a command the
    plain regex would have blocked — never block one it would have allowed —
    so it runs solely as a second-pass exemption check.
    """
    lines_out: list[str] = []
    changed = False
    for line in text.splitlines() or [text]:
        if _PIPE_TO_INTERPRETER.search(line):
            lines_out.append(line)
            continue
        try:
            lexer = shlex.shlex(line, posix=True, punctuation_chars=";&|()")
            lexer.whitespace_split = True
            lexer.commenters = "#"
            tokens = list(lexer)
        except ValueError:
            lines_out.append(line)
            continue

        segments: list[list[str]] = []
        current: list[str] = []
        for token in tokens:
            if token and set(token) <= _CONTROL_CHARS:
                segments.append(current)
                segments.append([token])
                current = []
                continue
            current.append(token)
        segments.append(current)

        rebuilt: list[str] = []
        for segment in segments:
            if not segment:
                continue
            index = _command_token_index(segment)
            if index is not None and Path(segment[index]).name in _DATA_SINK_EXECUTABLES:
                arguments = segment[index + 1 :]
                if not any(
                    argument.startswith(".")
                    or any(marker in argument for marker in _UNSAFE_DATA_ARG_MARKERS)
                    for argument in arguments
                ):
                    changed = True
                    rebuilt.extend(segment[: index + 1])
                    rebuilt.extend("arg" for _ in arguments)
                    continue
            rebuilt.extend(segment)
        lines_out.append(" ".join(rebuilt))
    if not changed:
        return text
    return "\n".join(lines_out)


def _lifecycle_command_scan_with_data_exemption(text: str) -> bool:
    """Lifecycle-regex scan that exempts matches living inside data arguments.

    Two-pass: the cheap regex first (the overwhelmingly common no-match case
    pays nothing extra); on a raw match, re-scan with data-sink arguments
    masked out. Only a match that survives masking — i.e. one in actual
    command position — blocks.
    """
    if not contains_gateway_lifecycle_command(text):
        return False
    normalized = _SHELL_LINE_CONTINUATION.sub(" ", text)
    return contains_gateway_lifecycle_command(_mask_data_sink_arguments(normalized))


def _direct_lifecycle_scan(command: str) -> bool:
    """Pure-string direct scans: lifecycle regex (data-exempted) + submit."""
    return _lifecycle_command_scan_with_data_exemption(
        command
    ) or contains_launchctl_submit_command(command)


def _expand_candidate_path(candidate: str) -> Optional[Path]:
    """Sanitize a tokenized path candidate at the ingestion boundary.

    Candidate tokens come from shlex-splitting arbitrary command text —
    including text recursively decoded from binaries or remote reads — so
    they can carry NUL bytes or other junk no real filesystem path can
    contain. Every OS-facing ``Path`` operation downstream (``expanduser``,
    ``os.open``, ``resolve``) raises a *different* exception for the same
    junk (``ValueError: embedded null byte``, ``RuntimeError: Could not
    determine home directory`` when HOME is unset under launchd, OSError
    for over-long paths). Rejecting here — once, before any OS call — is
    the whole-class fix; catching per-syscall was the whack-a-mole that
    produced #76762, #77703, #77780, and #78256.

    Returns ``None`` for candidates that cannot be a real path (nothing to
    scan), otherwise the ``expanduser()``-expanded ``Path``.
    """
    if not candidate or "\x00" in candidate:
        return None
    try:
        return Path(candidate).expanduser()
    except (ValueError, RuntimeError, OSError):
        return None


def _resolve_terminal_script_path(candidate: str, cwd: Optional[str]) -> Optional[Path]:
    path = _expand_candidate_path(candidate)
    if path is None:
        return None
    if not path.is_absolute():
        try:
            path = Path(cwd or Path.cwd()) / path
        except OSError:
            # Path.cwd() can raise when the process cwd was deleted.
            return None
    return path


def _iter_referenced_shell_scripts(
    command: str,
    *,
    cwd: Optional[str] = None,
) -> Iterator[Path]:
    """Yield scripts executed directly or through a POSIX shell."""
    for segment in _iter_command_segments(command):
        skipped: list[int] = []
        index = _unwrapped_command_token_index(segment, skipped=skipped)
        for wrapper_index in skipped:
            # A file named env/sudo/command can itself be a shell script.
            # Retain EVERY skipped reference, including intermediate wrappers.
            if "/" in segment[wrapper_index]:
                resolved = _resolve_terminal_script_path(segment[wrapper_index], cwd)
                if resolved is not None:
                    yield resolved
        if index is None:
            continue
        executable = segment[index]
        executable_name = Path(executable).name

        if executable == "." or executable_name == "source":
            if len(segment) > index + 1:
                resolved = _resolve_terminal_script_path(segment[index + 1], cwd)
                if resolved is not None:
                    yield resolved
            continue

        if executable_name in _SHELL_EXECUTABLES:
            arguments = segment[index + 1 :]
            arg_index = 0
            while arg_index < len(arguments):
                argument = arguments[arg_index]
                if argument == "--":
                    arg_index += 1
                    break
                if argument in {"-c", "--command"}:
                    break
                if argument in _SHELL_OPTIONS_WITH_VALUES:
                    arg_index += 2
                    continue
                if argument.startswith("-"):
                    arg_index += 1
                    continue
                break
            if arg_index < len(arguments) and arguments[arg_index] not in {
                "-c",
                "--command",
            }:
                resolved = _resolve_terminal_script_path(arguments[arg_index], cwd)
                if resolved is not None:
                    yield resolved
            continue

        # A bare "/" token is pathlib's division operator in Python sources
        # (e.g. `Path.home() / ".hermes"`), not an executable reference.
        # Resolving it walks to the filesystem root and fails the
        # regular-file check below, hard-blocking innocent .py scripts
        # (#77131). Skip pure-separator tokens.
        if executable.strip("/"):
            if "/" in executable or executable.endswith((".sh", ".bash", ".zsh")):
                resolved = _resolve_terminal_script_path(executable, cwd)
                if resolved is not None:
                    yield resolved


def _iter_shell_substitution_payloads(command: str) -> Iterator[Optional[str]]:
    """Expose executable substitutions without unquoting inert argument data.

    Only outer bodies are emitted; the existing bounded recursive scan handles
    their commands, nested substitutions and referenced scripts. This lexical
    walk does not evaluate shell text or expand variables. None signals syntax
    whose executable extent cannot be determined safely: callers fail closed.
    """
    command = command.replace("\\\n", "")
    stack = []
    end, start, quote, parentheses = None, 0, None, 0
    index = 0
    while index < len(command):
        char = command[index]
        if char == '\\' and quote != "'":
            index += 2
            continue
        if quote == "'":
            if char == "'":
                quote = None
        elif char == '#' and quote is None and (
            index == 0 or command[index - 1] in ' \t\n;|&('
        ):
            newline = command.find('\n', index)
            index = len(command) if newline < 0 else newline
            continue
        elif char == '`' and end == '`':
            if len(stack) == 1:
                # Backtick substitution removes these escapes before its body
                # is interpreted (including escaped nested backticks).
                yield re.sub(r'\\([$`\\])', r'\1', command[start:index])
            end, start, quote, parentheses = stack.pop()
        elif char == '`' or command.startswith('$(', index):
            stack.append((end, start, quote, parentheses))
            end = '`' if char == '`' else ')'
            index += 1 if char == '`' else 2
            start, quote, parentheses = index, None, 0
            continue
        elif char == quote:
            quote = None
        elif quote is None:
            if stack and (command.startswith('<<', index) or (
                    command.startswith('case', index) and
                    (index == start or command[index - 1] in ' \t\r\n;|&()<>') and
                    (index + 4 == len(command) or command[index + 4] in ' \t\r\n;|&()<>'))):
                # case patterns and heredoc data can contain unpaired ')'. Do
                # not guess the close and silently drop executable remainder.
                # Conservatively reject even an unquoted literal case word in
                # a substitution; quoted data and comments never reach here.
                yield None
                return
            if char in "\"'":
                quote = char
            elif end == ')' and char == '(':
                parentheses += 1
            elif end == ')' and char == ')':
                if parentheses:
                    parentheses -= 1
                else:
                    if len(stack) == 1:
                        yield command[start:index]
                    end, start, quote, parentheses = stack.pop()
        index += 1
    if stack:
        # An incomplete body has no trustworthy executable boundary either.
        yield None


def _iter_shell_command_payloads(command: str) -> Iterator[str]:
    """Yield code passed through ``sh|bash|... -c`` for recursive scanning."""
    for segment in _iter_command_segments(command):
        index = _unwrapped_command_token_index(segment)
        if index is None or Path(segment[index]).name not in _SHELL_EXECUTABLES:
            continue
        arguments = segment[index + 1 :]
        for arg_index, argument in enumerate(arguments[:-1]):
            if argument in {"-c", "--command"}:
                yield arguments[arg_index + 1]
                break


def _resolve_script_directory(script_path: str) -> Optional[str]:
    """Return the directory *script_path* resolves to, handling relative names."""
    try:
        path = _resolve_script_path(script_path)
        if path is not None and path.is_absolute():
            return str(path.parent)
    except Exception:
        pass
    return None


def _read_referenced_script(path: Path) -> tuple[Optional[str], bool]:
    """Return ``(text, unsafe)`` using bounded, regular-file-only reads."""
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except (OSError, ValueError):
        # OSError: unreadable / missing / over-long paths. ValueError: an
        # embedded NUL byte in *path* itself — a binary's decoded bytes
        # tokenized into a bogus script path by the recursion (#77703). A
        # guarded read must never crash the guard, so treat either as
        # "nothing to scan" (mirrors the resolve() ValueError guard below).
        return None, False
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            return None, True
        # Sniff a small prefix first: files that are clearly compiled
        # binaries (executable magic, or NUL bytes in the head) are never
        # shell scripts, so skip them WITHOUT reading the rest — reading a
        # megabyte of machine code just to discard it wastes the guard's
        # budget and (pre-#77703) fed decoded garbage into the recursion.
        data = os.read(descriptor, _BINARY_SNIFF_BYTES)
        if data.startswith(_BINARY_MAGIC_PREFIXES) or b"\x00" in data:
            return None, False
        # Read the remainder (bounded). Loop because os.read may return
        # short for non-regular-file-backed descriptors.
        while len(data) <= _MAX_REFERENCED_SCRIPT_BYTES:
            chunk = os.read(
                descriptor, _MAX_REFERENCED_SCRIPT_BYTES + 1 - len(data)
            )
            if not chunk:
                break
            data += chunk
    except OSError:
        return None, False
    finally:
        os.close(descriptor)
    # A NUL byte in the first chunk means this is a binary (ELF/Mach-O/
    # PE), not a shell script — scanning its decoded contents would
    # tokenize machine code and feed junk paths into the recursion
    # (including a `ValueError: embedded null byte` from Path.resolve,
    # #76762). Treat it as "nothing to scan" rather than unsafe: a binary
    # executed by the user is not a referenced *shell script*.
    if b"\x00" in data:
        return None, False
    if len(data) > _MAX_REFERENCED_SCRIPT_BYTES:
        return None, True
    return data.decode("utf-8", errors="replace"), False


def _sanitize_remote_script_text(text: Optional[str]) -> tuple[Optional[str], bool]:
    """Apply the local-read contract to text from a ``read_remote_script`` callback.

    The recursion boundary must not trust its callbacks: any backend (SSH,
    Modal, Daytona, or a future one) can hand back raw binary bytes decoded
    as text, or arbitrarily large output. Mirror
    ``_read_referenced_script``'s semantics exactly — NUL bytes mean binary
    (nothing to scan, checked first, #77703), oversized text fails closed
    like an oversized local file (#76762) — so remote and local reads can
    never diverge again. The size check re-encodes to compare *bytes*
    (matching the local read and the ``head -c`` wire bound): a >1 MiB
    multibyte file truncated at the byte cap decodes to fewer characters
    than bytes, and a character-count check would scan the truncated text
    instead of failing closed. Enforced here rather than inside each
    callback so the guarantee holds for every callback, not just the ones
    we hardened.
    """
    if not text:
        return None, False
    if "\x00" in text:
        return None, False
    if len(text.encode("utf-8", errors="replace")) > _MAX_REFERENCED_SCRIPT_BYTES:
        return None, True
    return text, False


def _contains_unsafe_gateway_action(
    command: str,
    *,
    cwd: Optional[str],
    depth: int,
    visited: set[Path],
    read_remote_script: Optional[_ReadRemoteScriptFn] = None,
    is_local: Optional[bool] = None,
) -> bool:
    if _direct_lifecycle_scan(command):
        return True
    if depth >= _MAX_REFERENCED_SCRIPT_DEPTH:
        return True

    for payload in _iter_shell_substitution_payloads(command):
        if payload is None or _contains_unsafe_gateway_action(
            payload,
            cwd=cwd,
            depth=depth + 1,
            visited=visited,
            read_remote_script=read_remote_script,
            is_local=is_local,
        ):
            return True

    # Add boundary-aware scans; NEVER discard the conservative body/reference
    # walk for an unknown stdin consumer (including executable forwarders).
    for part in _heredoc_scan_parts(command):
        if part and _contains_unsafe_gateway_action(
            part, cwd=cwd, depth=depth + 1, visited=visited,
            read_remote_script=read_remote_script, is_local=is_local,
        ):
            return True

    for payload in _iter_shell_command_payloads(command):
        if _contains_unsafe_gateway_action(
            payload,
            cwd=cwd,
            depth=depth + 1,
            visited=visited,
            read_remote_script=read_remote_script,
            is_local=is_local,
        ):
            return True

    for script_path in _iter_referenced_shell_scripts(command, cwd=cwd):
        try:
            resolved = script_path if is_local is False else script_path.resolve(strict=False)
        except (OSError, ValueError):
            # OSError: unreadable/long paths. ValueError: embedded NUL byte
            # from a binary's decoded contents tokenized as a path — a
            # guarded path must never crash the guard (#76762).
            resolved = script_path
        if resolved in visited:
            continue
        visited.add(resolved)
        script_text, unsafe = (
            (None, False) if is_local is False else _read_referenced_script(script_path)
        )
        if unsafe:
            return True
        if script_text is None and read_remote_script is not None:
            # Explicit remote context always uses the backend; otherwise this
            # retains the existing fallback when no local text was found.
            # The callback's output crosses the same trust boundary as a
            # local read — sanitize it identically before it enters the
            # recursion (binary skip + size fail-closed).
            script_text, unsafe = _sanitize_remote_script_text(
                read_remote_script(str(script_path))
            )
            if unsafe:
                return True
        if not script_text:
            continue
        # Relative references inside a script resolve against that script's
        # directory, not the original command's cwd.
        script_dir = _resolve_script_directory(str(resolved)) or cwd
        if _contains_unsafe_gateway_action(
            script_text,
            cwd=script_dir,
            depth=depth + 1,
            visited=visited,
            read_remote_script=read_remote_script,
            is_local=is_local,
        ):
            return True
    return False


def contains_gateway_lifecycle_command_or_referenced_script(
    command: str,
    *,
    cwd: Optional[str] = None,
    read_remote_script: Optional[_ReadRemoteScriptFn] = None,
    is_local: Optional[bool] = None,
) -> bool:
    """Detect lifecycle/submit commands, including bounded nested scripts.

    ``is_local=True`` enables the exact canonical Python read proof at the
    outermost command only, failing closed if that form's proof is rejected.
    Other forms retain the conservative scan. False makes backend reads
    authoritative; None retains the historical reference scan without granting
    any read exception.

    Total by construction: this function returns a verdict for *every*
    input and never raises. The direct scans below are pure string
    operations; the referenced-script walk touches the filesystem, remote
    backends, and shlex on arbitrary decoded bytes, so it is best-effort
    defense-in-depth — any unexpected failure inside it is logged and
    treated as "walk found nothing" rather than crashing the caller.

    This is the contract #76762 established ("a guarded path must never
    crash the guard") enforced at the boundary instead of per-syscall: a
    guard crash propagates out of ``tools/terminal_tool.py`` and breaks
    every terminal command until the gateway restarts (#77780, #78256),
    which is strictly worse than either verdict.
    """
    try:
        # The callback is also supplied by LOCAL terminal backends; its
        # presence says nothing about execution identity. Never propagate this
        # exemption into a referenced script, substitution or shell -c body.
        if is_local is True:
            read_proof = _is_canonical_python_read(command)
            if read_proof is not None:
                return not read_proof or _direct_lifecycle_scan(command)
        # Includes the direct regex/submit scans at depth 0.
        return _contains_unsafe_gateway_action(
            command,
            cwd=cwd,
            depth=0,
            visited=set(),
            read_remote_script=read_remote_script,
            is_local=is_local,
        )
    except Exception:
        logger.warning(
            "lifecycle guard referenced-script walk failed; "
            "falling back to direct-scan verdict",
            exc_info=True,
        )
        # Pure string scans of the top-level command — cannot raise.
        try:
            return _direct_lifecycle_scan(command)
        except Exception:
            # The data-argument masker tokenizes arbitrary text; if even
            # that fails, fall to the raw regex + submit scan so the guard
            # stays total.
            return contains_gateway_lifecycle_command(
                command
            ) or contains_launchctl_submit_command(command)




def _resolve_script_path(script_path: str) -> Optional[Path]:
    """Resolve a cron ``script`` value the same way the scheduler does.

    The scheduler (``cron.scheduler``) resolves a bare/relative script path
    under ``<HERMES_HOME>/scripts/`` and only accepts absolute paths as-is.
    We MUST mirror that here so the guard scans the file that will actually
    run — otherwise a job whose script lives at the scheduler's real location
    (``~/.hermes/scripts/restart.sh``) but is passed as the bare name
    ``restart.sh`` would read as a nonexistent relative path and silently
    scan prompt-only content, letting the command through.

    Returns ``None`` for values that cannot be a real path (NUL bytes,
    unexpandable ``~``) — the same ingestion contract as
    ``_expand_candidate_path``; such a value can never name a file the
    scheduler would execute, so there is nothing to scan.
    """
    from hermes_constants import get_hermes_home

    raw = _expand_candidate_path(script_path)
    if raw is None:
        return None
    if raw.is_absolute():
        return raw
    try:
        return get_hermes_home() / "scripts" / raw
    except (RuntimeError, OSError):
        # get_hermes_home() falls back to Path.home(), which raises when
        # neither HERMES_HOME nor HOME is resolvable (launchd/systemd
        # environments) — same ingestion contract: nothing to scan.
        return None


def _read_script_for_scanning(script_path: str) -> str:
    """Read a cron script with the bounded terminal-script scanner.

    Non-regular or oversized inputs fail closed by returning a lifecycle-shaped
    sentinel, while missing/unreadable/unresolvable paths remain empty so
    ordinary scheduler path validation can report them.
    """
    resolved = _resolve_script_path(script_path)
    if resolved is None:
        return ""
    script_text, unsafe = _read_referenced_script(resolved)
    if unsafe:
        return "hermes gateway restart"
    return script_text or ""


def check_gateway_lifecycle(
    prompt: Optional[str],
    script: Optional[str] = None,
) -> None:
    """Raise ``GatewayLifecycleBlocked`` if *prompt* or *script* contains a
    gateway-lifecycle command pattern.

    ``prompt`` is scanned directly.  ``script``, when supplied, is read from
    disk and concatenated for the scan.  Both are considered together so a
    job cannot slip through by splitting the command across the prompt and
    the script.

    Callers should let the exception propagate when they want the create to
    fail with a ``ValueError``-shaped error (the agent's ``cronjob`` tool
    surfaces this as a tool error; the CLI prints it in red and exits 1).
    """
    combined = prompt or ""
    python_script = False
    if script:
        resolved_script = _resolve_script_path(script)
        python_script = resolved_script is not None and resolved_script.suffix == ".py"
        script_text = _read_script_for_scanning(script)
        if script_text:
            combined = f"{combined}\n{script_text}"

    if python_script:
        # Python is executed by the interpreter, never through a POSIX
        # shell: the shell-script reference walk is a false-positive
        # generator on Python sources (pathlib's "/" operator resolves to
        # the filesystem root and trips the regular-file check, blocking
        # every innocent .py cron script, #77131). The direct command
        # regex below still scans the full text, so a literal
        # `hermes gateway restart` embedded in a .py script is still
        # blocked. Non-regular/oversized script files still fail closed
        # via the lifecycle-shaped sentinel in _read_script_for_scanning.
        unsafe = _lifecycle_command_scan_with_data_exemption(combined)
    else:
        script_dir = _resolve_script_directory(script) if script else None
        unsafe = contains_gateway_lifecycle_command_or_referenced_script(
            combined,
            cwd=script_dir,
        )
    if unsafe:
        raise GatewayLifecycleBlocked(
            "Blocked: cron job contains a gateway lifecycle command or persistent "
            "launchctl submit operation. This is blocked to prevent agent-driven "
            "SIGTERM-respawn loops under launchd/systemd supervision "
            "(#30719). Run `hermes gateway restart` from a shell outside "
            "the running gateway instead."
        )
