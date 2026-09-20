"""Disabled guard imports must not require POSIX locking."""
import builtins
import importlib.util
from pathlib import Path


def test_import_and_plain_terminal_without_fcntl(monkeypatch):
    original = builtins.__import__

    def without_fcntl(name, *args, **kwargs):
        if name == "fcntl":
            raise ModuleNotFoundError("no fcntl on Windows")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_fcntl)
    path = Path(__file__).resolve().parents[2] / "hermes_cli/diggr_continuation.py"
    spec = importlib.util.spec_from_file_location("guard_import_probe", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    args = {"command": "harmless"}
    assert module.terminal_dispatch(args, lambda received: received) is args
