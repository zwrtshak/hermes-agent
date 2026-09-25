"""Real harmless local reads through terminal_tool; unsupported forms stay conservative.

Dangerous candidates below are classified only, never executed.
"""

import json
import os
import shlex
import subprocess
import sys

import pytest

from cron.lifecycle_guard import (
    _is_canonical_python_read,
    contains_gateway_lifecycle_command_or_referenced_script as classify,
)


def canonical(body, delimiter="'PY'"):
    return f"{shlex.quote(os.path.realpath(sys.executable))} -I -S - <<{delimiter}\n{body}\nPY\n"


@pytest.fixture
def terminal_boundary(tmp_path, monkeypatch):
    import tools.terminal_tool as tt

    class Boundary:
        env = {}
        cwd = str(tmp_path)
        expected_read = None

        def __init__(self):
            self.calls = []
            self.remote_files = {}

        def execute(self, command, **kwargs):
            self.calls.append(command)
            if command == self.expected_read:
                # Only explicitly selected harmless read programs get a child.
                result = subprocess.run(
                    ["/bin/sh", "-c", command], cwd=tmp_path,
                    env={"PATH": "/usr/bin:/bin"},
                    capture_output=True, text=True, timeout=10,
                )
                return {"output": result.stdout + result.stderr, "returncode": result.returncode}
            if command.startswith("head -c "):
                return {"output": self.remote_files.get(shlex.split(command)[-1], ""), "returncode": 0}
            # Never execute an unexpected or dangerous command, even on failure.
            return {"output": "unexpected execution boundary", "returncode": 0}

    boundary = Boundary()
    config = {"env_type": "local", "cwd": str(tmp_path), "timeout": 30,
              "lifetime_seconds": 3600, "docker_image": "synthetic-unused"}
    monkeypatch.setattr(tt, "_active_environments", {"default": boundary})
    monkeypatch.setattr(tt, "_last_activity", {"default": 0.0})
    monkeypatch.setattr(tt, "_task_env_overrides", {})
    monkeypatch.setattr(tt, "_get_env_config", lambda: config)
    monkeypatch.setattr(tt, "get_session_cwd", lambda key: None)
    monkeypatch.setattr(tt, "record_session_cwd", lambda *args: None)
    monkeypatch.setenv("_HERMES_GATEWAY", "1")

    def run(command, backend="local", execute_read=False):
        config["env_type"] = backend
        boundary.expected_read = command if execute_read else None
        return json.loads(tt.terminal_tool(command=command, force=True))

    return run, boundary


@pytest.fixture
def read_data(tmp_path):
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"example": "hermes gateway restart"}), encoding="utf-8")
    directory = tmp_path / "directory"
    directory.mkdir()
    (directory / "entry.txt").write_text("synthetic", encoding="utf-8")
    return data, directory


@pytest.mark.parametrize("kind", ["json", "directory"])
@pytest.mark.parametrize("delimiter", ["'PY'", '"PY"', "\\PY", "P'Y'"])
def test_canonical_local_read_form_real_reads_at_terminal_boundary(read_data, terminal_boundary, kind, delimiter):
    data, directory = read_data
    expression = (
        f"json.loads(Path({str(data)!r}).read_text())['example']"
        if kind == "json" else f"list(Path({str(directory)!r}).iterdir())"
    )
    command = canonical("from pathlib import Path\nimport json\n" + f"print({expression})", delimiter)
    run, boundary = terminal_boundary
    result = run(command, execute_read=True)
    assert result["exit_code"] == 0, result
    assert ("hermes gateway restart" if kind == "json" else "entry.txt") in result["output"]
    assert boundary.calls == [command]


@pytest.mark.parametrize("kind", ["json", "directory"])
@pytest.mark.parametrize("delimiter", ["'PY'", '"PY"', "PY", "\\PY"])
def test_unsupported_bare_historical_forms_remain_conservative(tmp_path, read_data, kind, delimiter):
    data, directory = read_data
    expression = (
        f"json.loads(Path({str(data)!r}).read_text())['example']"
        if kind == "json" else f"list(Path({str(directory)!r}).iterdir())"
    )
    command = f"python3 <<{delimiter}\nfrom pathlib import Path\nimport json\nprint({expression})\nPY\n"
    assert classify(command, cwd=str(tmp_path))


@pytest.mark.parametrize("source", [".", "source"])
def test_dot_source_reference_after_heredoc_is_blocked(tmp_path, source):
    control = tmp_path / "control.sh"
    control.write_text("hermes gateway stop\n", encoding="utf-8")
    assert classify(f"cat <<'BODY'\n'\nBODY\n{source} {shlex.quote(str(control))}\n", cwd=str(tmp_path))


def test_remote_same_named_local_file_cannot_hide_remote_control(tmp_path, terminal_boundary):
    local_file = tmp_path / "control.sh"
    local_file.write_text("printf harmless\n", encoding="utf-8")
    run, boundary = terminal_boundary
    boundary.remote_files[str(local_file)] = "hermes gateway stop\n"
    command = f"bash {shlex.quote(str(local_file))}"
    result = run(command, backend="ssh")
    assert result["exit_code"] == 1, result
    assert command not in boundary.calls
    assert any(call.startswith("head -c ") for call in boundary.calls)


def read_body(data):
    return f"from pathlib import Path\nimport json\nprint(json.loads(Path({str(data)!r}).read_text()))"


@pytest.mark.parametrize("context", [None, False])
def test_canonical_local_read_form_requires_explicit_local_identity(tmp_path, read_data, context):
    data, _ = read_data
    command = canonical(read_body(data))
    assert classify(command, cwd=str(tmp_path), is_local=context,
                    read_remote_script=lambda path: '{"example": "hermes gateway restart"}' if path == str(data) else None)
    # The exact same command is safe locally even WITH the callback supplied.
    def unexpected_read(path):
        pytest.fail(f"proven local read must not inspect script references: {path}")

    assert not classify(command, cwd=str(tmp_path), is_local=True, read_remote_script=unexpected_read)


@pytest.mark.parametrize("backend", ["ssh", "docker", "unknown"])
def test_canonical_read_form_remote_or_unknown_terminal_has_no_exception(read_data, terminal_boundary, backend):
    data, _ = read_data
    command = canonical(read_body(data))
    run, boundary = terminal_boundary
    boundary.remote_files[str(data)] = '{"example": "hermes gateway restart"}'
    assert run(command, backend=backend)["exit_code"] == 1
    assert command not in boundary.calls


@pytest.mark.parametrize("change", [
    "path-name", "missing-isolation", "missing-site-isolation", "missing-stdin",
    "unquoted", "env", "sudo", "command", "prelude", "postlude", "pipe",
    "redirect-later-shell", "second-body", "shell-c", "trailing-comment",
    "missing-delimiter", "tab-heredoc", "input-redirect", "escaped-newline",
])
def test_canonical_local_read_form_rejects_shell_surrounding(tmp_path, read_data, change):
    data, _ = read_data
    command = canonical(read_body(data))
    prefix = shlex.quote(os.path.realpath(sys.executable))
    if change == "path-name":
        command = command.replace(prefix, "python3", 1)
    elif change == "missing-isolation":
        command = command.replace(" -I", "", 1)
    elif change == "missing-site-isolation":
        command = command.replace(" -S", "", 1)
    elif change == "missing-stdin":
        command = command.replace(" - <<", " <<", 1)
    elif change == "unquoted":
        command = command.replace("<<'PY'", "<<PY", 1)
    elif change in {"env", "sudo", "command"}:
        command = change + " " + command
    elif change == "prelude":
        command = "printf before\n" + command
    elif change == "postlude":
        command += "printf after\n"
    elif change == "pipe":
        command = command.replace("<<'PY'", "<<'PY' | sh", 1)
    elif change == "redirect-later-shell":
        command = command.replace("<<'PY'", "<<'PY' > emitted.sh", 1) + "sh emitted.sh\n"
    elif change == "second-body":
        command = command.replace("<<'PY'", "<<'PY' <<'OTHER'", 1) + "print('extra')\nOTHER\n"
    elif change == "shell-c":
        command = "sh -c " + shlex.quote(command)
    elif change == "trailing-comment":
        command += "# unsupported surrounding shell text\n"
    elif change == "missing-delimiter":
        command = command.removesuffix("PY\n")
    elif change == "tab-heredoc":
        command = command.replace("<<'PY'", "<<-'PY'", 1)
    elif change == "input-redirect":
        command = command.replace("<<'PY'", "<<'PY' < other.py", 1)
    else:
        command = command.replace(" -I", " \\\n-I", 1)
    assert classify(command, cwd=str(tmp_path), is_local=True)


@pytest.mark.parametrize("at_caller", [False, True], ids=["classifier", "terminal-boundary"])
@pytest.mark.parametrize("statement", [
    "import os", "import subprocess", "import importlib", "from json import loads",
    "import os; os.system('launchctl submit -l neutral -- /bin/true')",
    "import pathlib as p", "from pathlib import Path as P", "import json as print",
    "execute = print", "constructor = Path", "module = json", "reader = Path('x').read_text",
    "Path = str", "json = {}", "print = str", "len = 2", "open = 0",
    "x = 1\nx = 2", "json.loads = print", "x = {}\nx['a'] = 1",
    "x = []\nx.append(1)", "del json", "x = 1\nx += 2",
    "print(Path.__class__)", "print(Path('x').__class__)", "print(json.__dict__)",
    "getattr(Path, 'read_text')", "print(globals())", "unknown()",
    "exec('pass')", "eval('1')", "compile('1', 'x', 'eval')", "open('x')",
    "def hidden():\n    pass", "class Hidden:\n    pass", "x = lambda: 1",
    "if False:\n    unknown()", "for x in []:\n    unknown()",
    "try:\n    unknown()\nexcept:\n    pass", "[unknown() for x in []]",
    "print(*(1,))", "print(**{})", "print('x', file=Path('x'))",
    "json.loads('{}', object_hook=print)", "json.dumps({}, default=print)",
    "Path('x').write_text('x')", "Path('x').chmod(0o700)", "Path('x').unlink()",
    "print(Path('x').read_text(encoding='custom_codec'))", "print(f'{unknown()}')",
    "x: unknown() = 1", "print((x := 1))", "assert False", "raise ValueError()",
])
def test_canonical_local_read_form_unknown_ast_fails_closed(tmp_path, terminal_boundary, statement, at_caller):
    # No data-file reference or lifecycle-text trigger may be needed to reject
    # an unproven program in the exact canonical form. Never execute it.
    command = canonical("from pathlib import Path\nimport json\n" + statement)
    assert not _is_canonical_python_read(command)
    if at_caller:
        run, boundary = terminal_boundary
        result = run(command)
        assert boundary.calls == [], result
        assert result["exit_code"] == 1, result
        assert result["error"].startswith("Blocked:"), result
    else:
        assert classify(command, cwd=str(tmp_path), is_local=True)


def test_canonical_local_read_form_bindings_and_pure_operations(read_data, terminal_boundary):
    data, directory = read_data
    body = (
        "import pathlib\nimport json\n"
        f"root = pathlib.Path({str(directory)!r})\n"
        "entry = root / 'entry.txt'\n"
        "names = sorted(list(root.iterdir()))\n"
        "print(names[0].name, root.is_dir(), entry.is_file(), entry.exists())\n"
        "print(entry.read_bytes(), entry.read_text(encoding='utf-8'), len(names))\n"
        f"data = json.loads(pathlib.Path({str(data)!r}).read_text())\n"
        "print(json.dumps(data), data.get('example'), str(root.parent))\n"
        "literal = {'a': [1, -2, True, None], 'b': ('text' + 'data', 1 + 2)}\n"
        "print(json.dumps(literal))"
    )
    run, _ = terminal_boundary
    result = run(canonical(body), execute_read=True)
    assert result["exit_code"] == 0, result
    assert "entry.txt True True True" in result["output"]
    assert "hermes gateway restart" in result["output"]


@pytest.mark.parametrize("name", ["python3", "stdin-forwarder"])
def test_canonical_local_read_form_does_not_trust_executable_names(tmp_path, read_data, monkeypatch, name):
    data, _ = read_data
    wrapper = tmp_path / name
    wrapper.write_text("#!/bin/sh\nexec sh\n", encoding="utf-8")
    wrapper.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path))
    command = canonical(read_body(data))
    for executable in [name, shlex.quote(str(wrapper))]:
        candidate = command.replace(shlex.quote(os.path.realpath(sys.executable)), executable, 1)
        assert classify(candidate, cwd=str(tmp_path), is_local=True)


def test_canonical_local_read_form_rejects_symlink_spelling(tmp_path, read_data):
    data, _ = read_data
    link = tmp_path / "python3"
    link.symlink_to(os.path.realpath(sys.executable))
    candidate = canonical(read_body(data)).replace(shlex.quote(os.path.realpath(sys.executable)), str(link), 1)
    assert classify(candidate, cwd=str(tmp_path), is_local=True)


def test_canonical_local_read_form_isolates_import_shadowing(tmp_path, read_data, terminal_boundary):
    for module in ["json", "pathlib", "sitecustomize"]:
        (tmp_path / (module + ".py")).write_text("raise RuntimeError('cwd shadow imported')\n", encoding="utf-8")
    data, _ = read_data
    command = canonical(read_body(data))
    run, _ = terminal_boundary
    result = run(command, execute_read=True)
    assert result["exit_code"] == 0, result
    assert "hermes gateway restart" in result["output"]
    assert classify(command.replace(" -I -S", "", 1), cwd=str(tmp_path), is_local=True)


@pytest.mark.parametrize("mode", [0o600, 0o700])
def test_script_permissions_cannot_make_later_execution_safe(tmp_path, mode):
    script = tmp_path / "control"
    script.write_text("hermes gateway stop\n", encoding="utf-8")
    script.chmod(mode)
    assert classify(f"chmod +x {script}; {script}", cwd=str(tmp_path), is_local=True)


@pytest.mark.parametrize("body", [
    "print('hermes gateway stop')",
    "import os\nos.system('hermes gateway restart')",
    "import subprocess\nsubprocess.run(['hermes', 'gateway', 'stop'])",
])
def test_canonical_local_read_form_keeps_direct_or_unproven_control(tmp_path, read_data, body):
    data, _ = read_data
    assert classify(canonical(read_body(data) + "\n" + body), cwd=str(tmp_path), is_local=True)


def test_canonical_local_read_form_cannot_hide_execution_of_referenced_script(tmp_path):
    control = tmp_path / "control.sh"
    control.write_text("hermes gateway stop\n", encoding="utf-8")
    body = "from pathlib import Path\nimport subprocess\n" + f"subprocess.run(['bash', str(Path({str(control)!r}))], check=True)"
    assert classify(canonical(body), cwd=str(tmp_path), is_local=True)


def test_diagnostic_error_explains_read_form_and_real_control(read_data, terminal_boundary):
    data, _ = read_data
    run, boundary = terminal_boundary
    command = canonical(read_body(data)).replace(" -I -S", "", 1)
    result = run(command)
    assert result["exit_code"] == 1
    assert "-I -S -" in result["error"]
    assert shlex.quote(os.path.realpath(sys.executable)) + " -I -S - <<'PY'" in result["error"]
    assert "read" in result["error"]
    assert "stop/restart remains blocked" in result["error"]
    assert command not in boundary.calls


@pytest.mark.parametrize("backend", ["ssh", "docker", "unknown"])
def test_diagnostic_error_does_not_offer_local_interpreter_to_remote_backend(terminal_boundary, backend):
    run, boundary = terminal_boundary
    result = run("hermes gateway stop", backend=backend)
    assert result["exit_code"] == 1
    assert "no canonical Python read exemption" in result["error"]
    assert shlex.quote(os.path.realpath(sys.executable)) not in result["error"]
    assert "hermes gateway stop" not in boundary.calls


@pytest.mark.parametrize("name", ["env", "command", "sudo"])
def test_wrapper_unwrapping_keeps_original_executable_reference(tmp_path, name):
    wrapper = tmp_path / name
    wrapper.write_text("hermes gateway stop\n", encoding="utf-8")
    assert classify(f"{wrapper} sh -c 'printf harmless'", cwd=str(tmp_path), is_local=True)


@pytest.mark.parametrize("at_caller", [False, True], ids=["classifier", "terminal-boundary"])
@pytest.mark.parametrize("failure", ["size", "expression-depth", "node-count", "syntax"])
def test_canonical_local_read_form_proof_limits_fail_closed(tmp_path, terminal_boundary, failure, at_caller):
    from cron.lifecycle_guard import _MAX_REFERENCED_SCRIPT_BYTES

    body = {
        "size": "#" + "x" * _MAX_REFERENCED_SCRIPT_BYTES,
        "expression-depth": "print(" + "[0," * 70 + "0" + "]" * 70 + ")",
        "node-count": "print([" + "0," * 4200 + "])",
        "syntax": "this is not valid Python !!!",
    }[failure]
    command = canonical(body)
    if at_caller:
        run, boundary = terminal_boundary
        result = run(command)
        assert boundary.calls == [], result
        assert result["exit_code"] == 1, result
        assert result["error"].startswith("Blocked:"), result
    else:
        assert classify(command, cwd=str(tmp_path), is_local=True)


@pytest.mark.parametrize("action", ["hermes gateway stop", "hermes gateway restart"])
def test_canonical_local_read_form_never_allows_actual_control_at_caller(terminal_boundary, action):
    run, boundary = terminal_boundary
    command = canonical("import os\nos.system(" + repr(action) + ")")
    result = run(command)
    assert result["exit_code"] == 1
    assert command not in boundary.calls


@pytest.mark.parametrize("at_caller", [False, True], ids=["classifier", "terminal-boundary"])
@pytest.mark.parametrize("cookie", [
    "# coding: unicode_escape\n", "# first\n# coding=unicode_escape\n",
    "#\vcomment\vdata\n# coding: unicode_escape\n",
])
def test_canonical_local_read_form_encoding_cookie_cannot_hide_ast(tmp_path, terminal_boundary, cookie, at_caller):
    # With unicode_escape, stdin decoding turns this apparent comment into
    # additional statements. Classify only; no child runs this source.
    body = cookie + "print('harmless')\n#\\u000aimport os\\u000aunknown()"
    command = canonical(body)
    assert not _is_canonical_python_read(command)
    if at_caller:
        run, boundary = terminal_boundary
        result = run(command)
        assert boundary.calls == [], result
        assert result["exit_code"] == 1, result
        assert result["error"].startswith("Blocked:"), result
    else:
        assert classify(command, cwd=str(tmp_path), is_local=True)


@pytest.mark.parametrize("form", ["bare-python", "env", "shell-c", "nonlocal", "unknown-backend"])
def test_ast_rejection_does_not_extend_to_other_invocation_forms(tmp_path, form):
    # The new fail-closed AST contract is scoped to the exact local form.
    # These inert programs have no lifecycle action or referenced script.
    command = canonical("import os\nunknown()")
    is_local = True
    if form == "bare-python":
        command = command.replace(shlex.quote(os.path.realpath(sys.executable)), "python3", 1)
    elif form == "env":
        command = "env " + command
    elif form == "shell-c":
        command = "sh -c " + shlex.quote(command)
    else:
        is_local = False if form == "nonlocal" else None
    assert not classify(command, cwd=str(tmp_path), is_local=is_local)
