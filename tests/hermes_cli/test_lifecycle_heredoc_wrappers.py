"""Classify inert script fixtures; never execute lifecycle control text."""
import shlex
from pathlib import Path
from tempfile import TemporaryDirectory
import pytest
from cron.lifecycle_guard import contains_gateway_lifecycle_command_or_referenced_script as classify


@pytest.mark.parametrize('name', ['wrapper', 'wrapper.py'])
def test_heredoc_unknown_executable_can_forward_stdin_to_shell(tmp_path, name):
    control = tmp_path / 'control.sh'
    control.write_text('hermes gateway stop\n')
    wrapper = tmp_path / name
    wrapper.write_text('#!/bin/sh\nexec sh\n')
    wrapper.chmod(0o700)
    command = f"{shlex.quote(str(wrapper))} <<'BODY'\nbash {shlex.quote(str(control))}\nBODY\n"
    assert classify(command, cwd=str(tmp_path))


@pytest.mark.parametrize("relative", [False, True], ids=["absolute", "relative"])
@pytest.mark.parametrize("prefix, unsafe_wrapper", [
    ("env {sudo}", "sudo"),
    ("{env} {sudo} {command}", "env"),
    ("{env} {sudo} {command}", "sudo"),
    ("{env} {sudo} {command}", "command"),
    ("{env} {sudo} {command}", None),
])
def test_all_unwrapped_file_references_are_scanned(prefix, unsafe_wrapper, relative):
    # Synthetic /tmp/.../sudo etc. are only classified, never executed.
    # Even non-executable files must stay protected (e.g. a prior chmod).
    with TemporaryDirectory(prefix="f6-tightening-wrappers-", dir="/tmp") as directory:
        root = Path(directory)
        paths = {}
        for name in ("env", "sudo", "command"):
            wrapper = root / name
            wrapper.write_text(
                "hermes gateway stop\n" if name == unsafe_wrapper else "printf harmless\n",
                encoding="utf-8",
            )
            paths[name] = shlex.quote("./" + name if relative else str(wrapper))
        command = prefix.format(**paths) + " sh -c 'printf harmless'"
        assert classify(command, cwd=directory, is_local=True) is (unsafe_wrapper is not None)


@pytest.mark.parametrize("tail", ["sh -c 'printf harmless'", "--unknown sh -c 'printf harmless'", ""])
def test_wrapper_references_survive_partial_or_complete_unwrapping(tmp_path, tail):
    wrapper = tmp_path / "sudo"
    wrapper.write_text("hermes gateway stop\n", encoding="utf-8")
    command = f"env {shlex.quote(str(wrapper))} command {tail}"
    assert classify(command, cwd=str(tmp_path), is_local=True)


def test_wrapper_option_values_are_not_executable_references(tmp_path):
    data = tmp_path / "sudo"
    data.write_text("hermes gateway stop\n", encoding="utf-8")
    # -u's value is an environment key, not a wrapper to unwrap or execute.
    assert not classify(f"env -u {shlex.quote(str(data))} sh -c 'printf harmless'", cwd=str(tmp_path))


def test_remote_intermediate_wrapper_uses_backend_reference(tmp_path):
    wrapper = tmp_path / "sudo"
    wrapper.write_text("printf harmless\n", encoding="utf-8")
    reads = []

    def read_remote(path):
        reads.append(path)
        return "hermes gateway stop\n" if path == str(wrapper) else None

    assert classify(
        f"env {shlex.quote(str(wrapper))} sh -c 'printf harmless'",
        cwd=str(tmp_path), is_local=False, read_remote_script=read_remote,
    )
    assert str(wrapper) in reads
