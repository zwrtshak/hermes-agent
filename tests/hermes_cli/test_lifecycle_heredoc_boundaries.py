"""F6 follow-up contracts: classify synthetic text, never execute its contents."""

import shlex

import pytest

from cron.lifecycle_guard import (
    contains_gateway_lifecycle_command_or_referenced_script as classify,
)


@pytest.fixture
def control(tmp_path):
    path = tmp_path / "control.sh"
    path.write_text("hermes gateway stop\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("name", ["forwarder", "forwarder.py", "python3"])
@pytest.mark.parametrize("prefix", ["", "env ", "env MODE=check ", "command ", "sudo "])
def test_absolute_executable_heredoc_body_keeps_references(tmp_path, control, name, prefix):
    wrapper = tmp_path / name
    wrapper.write_text("#!/bin/sh\nexec sh\n", encoding="utf-8")
    wrapper.chmod(0o700)
    command = (
        f"{prefix}{shlex.quote(str(wrapper))} <<'BODY'\n"
        f"bash {shlex.quote(str(control))}\nBODY\n"
    )
    assert classify(command, cwd=str(tmp_path))


@pytest.mark.parametrize("consumer", [
    "dash", "bash -s", "/bin/sh", "env bash -s", "command dash", "sudo bash -s",
])
def test_shell_stdin_reference_is_preserved(tmp_path, control, consumer):
    command = f"{consumer} <<'BODY'\nbash {shlex.quote(str(control))}\nBODY\n"
    assert classify(command, cwd=str(tmp_path))


@pytest.mark.parametrize("shell", ["sh", "dash", "bash"])
def test_quoted_multiline_shell_c_around_heredoc(tmp_path, control, shell):
    payload = f"bash -s <<'BODY'\nbash {shlex.quote(str(control))}\nBODY\n"
    command = f"{shell} -c {shlex.quote(payload)}"
    assert classify(command, cwd=str(tmp_path))


@pytest.mark.parametrize("prefix", ["env ", "command ", "sudo "])
def test_wrapped_shell_c_around_heredoc(tmp_path, control, prefix):
    payload = f"dash <<'BODY'\nbash {shlex.quote(str(control))}\nBODY\n"
    assert classify(prefix + "sh -c " + shlex.quote(payload), cwd=str(tmp_path))


@pytest.mark.parametrize("delimiter", ["BODY", "'BODY'", '"BODY"', "\\BODY"])
def test_heredoc_delimiter_spelling_keeps_reference(tmp_path, control, delimiter):
    command = f"sh <<{delimiter}\nbash {shlex.quote(str(control))}\nBODY\n"
    assert classify(command, cwd=str(tmp_path))


def test_tab_stripped_heredoc_keeps_reference(tmp_path, control):
    command = f"bash -s <<-'BODY'\n\tbash {shlex.quote(str(control))}\n\tBODY\n"
    assert classify(command, cwd=str(tmp_path))


@pytest.mark.parametrize("dangerous_body", ["first", "second"])
def test_multiple_bodies_keep_conservative_reference_scan(tmp_path, control, dangerous_body):
    dangerous = f"bash {shlex.quote(str(control))}\n"
    first = dangerous if dangerous_body == "first" else "printf safe\n"
    second = dangerous if dangerous_body == "second" else "printf safe\n"
    # Even an overridden stdin body retains the existing conservative check.
    command = f"sh <<FIRST <<'SECOND'\n{first}FIRST\n{second}SECOND\n"
    assert classify(command, cwd=str(tmp_path))


@pytest.mark.parametrize("body", ["plain data", "'", '"'])
def test_command_after_heredoc_delimiter_remains_visible(tmp_path, control, body):
    # Quotes inside cat's quoted heredoc are data; they cannot quote the next
    # shell command. This also tests a lexically valid body with unpaired quotes.
    command = f"cat <<'BODY'\n{body}\nBODY\nbash {shlex.quote(str(control))}\n"
    assert classify(command, cwd=str(tmp_path))


@pytest.mark.parametrize("body", ["'", '"'])
@pytest.mark.parametrize("tail", ["cat <<'B'\ntext\n", "cat <<-'B'\n\ttext", "cat <<'B' <<'C'\ntext\nB\ntext\n"])
def test_open_later_heredoc_keeps_known_execution_boundary(tmp_path, control, body, tail):
    # First row is the review reproduction verbatim, with only cwd in tmp_path.
    # Shells can execute control.sh before reaching the unfinished next body.
    command = f"cat <<'A'\n{body}\nA\nbash control.sh\n{tail}"
    assert classify(command, cwd=str(tmp_path), is_local=True)


@pytest.mark.parametrize("tail", ["cat <<'B'\ntext\n", "unknown-consumer <<'B'\ntext\n"])
def test_open_later_heredoc_keeps_unknown_consumer_body(tmp_path, control, tail):
    command = "unknown-consumer <<'A'\nbash control.sh\nA\n" + tail
    assert classify(command, cwd=str(tmp_path), is_local=True)


@pytest.mark.parametrize("delimiter", ["PY", "'PY'"])
def test_python_heredoc_literal_lifecycle_stays_blocked(tmp_path, delimiter):
    command = f"python3 <<{delimiter}\nimport os\nos.system('hermes gateway stop')\nPY\n"
    assert classify(command, cwd=str(tmp_path))


def test_python_heredoc_pathlib_execution_reference_stays_blocked(tmp_path, control):
    # The same Path(...) lexical shape as the read-only reproduction can be
    # passed to a process. A blanket Python-body exemption loses this check.
    command = (
        "python3 <<'PY'\nfrom pathlib import Path\nimport subprocess\n"
        f"subprocess.run(['bash', str(Path({str(control)!r}))], check=True)\nPY\n"
    )
    assert classify(command, cwd=str(tmp_path))


@pytest.mark.parametrize("substitution", ["$(bash SCRIPT)", "`bash SCRIPT`"])
def test_expanding_python_heredoc_keeps_shell_substitution(tmp_path, control, substitution):
    body = substitution.replace("SCRIPT", shlex.quote(str(control)))
    command = f"python3 <<PY\nprint(\"{body}\")\nPY\n"
    assert classify(command, cwd=str(tmp_path))


def test_heredoc_reference_keeps_size_limit(tmp_path):
    from cron.lifecycle_guard import _MAX_REFERENCED_SCRIPT_BYTES

    script = tmp_path / "large.sh"
    script.write_text("#" * (_MAX_REFERENCED_SCRIPT_BYTES + 1), encoding="utf-8")
    assert classify(f"sh <<'BODY'\nbash {shlex.quote(str(script))}\nBODY\n", cwd=str(tmp_path))


def test_heredoc_reference_keeps_depth_limit(tmp_path):
    from cron.lifecycle_guard import _MAX_REFERENCED_SCRIPT_DEPTH

    for index in range(_MAX_REFERENCED_SCRIPT_DEPTH):
        path = tmp_path / f"level{index}.sh"
        path.write_text(f"bash level{index + 1}.sh\n", encoding="utf-8")
    assert classify("sh <<'BODY'\nbash level0.sh\nBODY\n", cwd=str(tmp_path))


def test_heredoc_reference_uses_remote_callback_without_network(tmp_path):
    missing = tmp_path / "missing.sh"
    reads = []

    def read_remote(path):
        reads.append(path)
        return "hermes gateway stop\n" if path == str(missing) else None

    command = f"unknown-consumer <<'BODY'\nbash {shlex.quote(str(missing))}\nBODY\n"
    assert classify(command, cwd=str(tmp_path), read_remote_script=read_remote)
    assert str(missing) in reads


@pytest.mark.parametrize("body", ["'", '"'])
@pytest.mark.parametrize("action", ["substitution", "shell-payload", "dot-source"])
def test_real_delimiter_preserves_postlude_execution_paths(tmp_path, control, body, action):
    reference = f"bash {shlex.quote(str(control))}"
    if action == "substitution":
        postlude = f'printf "%s" "$(\n{reference}\n)"'
    elif action == "shell-payload":
        postlude = "env command sh -c " + shlex.quote(reference)
    else:
        postlude = f". {shlex.quote(str(control))}"
    assert classify(f"cat <<'BODY'\n{body}\nBODY\n{postlude}\n", cwd=str(tmp_path))


@pytest.mark.parametrize("prefix", ["env MODE=check ", "command -p ", "sudo -n -u test-user -- ", "env command sudo "])
def test_wrapped_heredoc_payload_keeps_simple_wrapper_arguments(tmp_path, control, prefix):
    payload = f"sh <<'BODY'\nbash {shlex.quote(str(control))}\nBODY\n"
    assert classify(prefix + "sh -c " + shlex.quote(payload), cwd=str(tmp_path))
