"""Inert lexical regressions: never execute submitted command text."""
import shlex
from pathlib import Path
import pytest
from cron.lifecycle_guard import (
    _iter_command_segments, _iter_referenced_shell_scripts,
    contains_gateway_lifecycle_command_or_referenced_script as classify,
)


@pytest.mark.parametrize('interpreter', ['python3 -c', 'node -e'])
def test_multiline_directory_is_data(tmp_path, interpreter):
    code = f'from pathlib import Path\nroot=Path("{tmp_path}")\nprint(root)'
    command = interpreter + ' ' + shlex.quote(code)
    assert list(_iter_referenced_shell_scripts(command)) == []
    assert not classify(command, cwd=str(tmp_path))


def test_exact_denied_shape_inert(tmp_path):
    # The submitted algorithm/argv are text only; replace its machine paths
    # with hermetic equivalents. The directory is the original trigger.
    root = tmp_path / 'apps/desktop/dist'
    root.mkdir(parents=True)
    code = '''from pathlib import Path
import hashlib,subprocess
root=Path("ROOT");h=hashlib.sha256()
for p in sorted(root.rglob("*")):
 if p.is_file():h.update(str(p.relative_to(root)).encode());h.update(p.read_bytes())
a=["node","app49-passive-harness.mjs","--dry-run","--endpoint","http://127.0.0.1:9222","--target-id","FA184757188A3EE0EBCCBA613A31F70A","--renderer-url",(root/"renderer/index.html").as_uri(),"--scope","app49-postreboot-epoch-001","--scope-policy","main-stop-rearm","--revision","7a4b39108a7f16987b79f6da7154fa14ff7a99b8","--build",h.hexdigest(),"--cache","cold","--follow","absent-base","--plan","PLAN","--sample-ms","50","--dwell-ms","1000","--step-ms","10000","--overall-ms","90000","--output","OUTPUT"]
print("build",h.hexdigest());subprocess.run(a,check=True)'''
    code = code.replace('ROOT', str(root)).replace('PLAN', str(tmp_path/'plan.json')).replace('OUTPUT', str(tmp_path/'out.json'))
    assert not classify('python3 -c ' + shlex.quote(code), cwd=str(tmp_path))
    assert not (tmp_path/'out.json').exists()


@pytest.mark.parametrize('prefix', ['printf "two\\nlines"\n', "echo x#'comment\n", "printf 'two\nlines'\n", 'echo safe # comment\n', 'echo safe\n', 'X=y\necho safe\n'])
def test_real_command_after_newline_still_scanned(tmp_path, prefix):
    script = tmp_path/'real.sh'
    script.write_text('hermes gateway stop\n')
    assert classify(prefix + 'bash ' + str(script), cwd=str(tmp_path))


def test_quoted_newline_is_not_control():
    assert list(_iter_command_segments("printf '\n'\necho done")) == [['printf', '\n'], ['echo', 'done']]


def test_nested_shell_payload(tmp_path):
    script = tmp_path/'real.sh'
    script.write_text('hermes gateway restart\n')
    assert classify('sh -c ' + shlex.quote('echo safe\nbash '+str(script)))
