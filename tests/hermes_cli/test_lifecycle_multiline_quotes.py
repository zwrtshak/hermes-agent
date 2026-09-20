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


@pytest.mark.parametrize('case', ['dollar_submit', 'dollar_script', 'backtick_submit'])
def test_executable_multiline_substitution(tmp_path, case):
    script = tmp_path/'real.sh'
    script.write_text('hermes gateway stop\n')
    body = ('bash ' + shlex.quote(str(script)) if case == 'dollar_script'
            else 'launchctl submit -l neutral -- /bin/true')
    substitution = ('`\n' + body + '\n`' if case == 'backtick_submit'
                    else '$(\n' + body + '\n)')
    # Classify text only: never execute launchctl or the lifecycle fixture.
    assert classify('printf "%s" "' + substitution + '"', cwd=str(tmp_path))


@pytest.mark.parametrize('substitution', ['$(\nBODY\n)', '`\nBODY\n`'])
@pytest.mark.parametrize('consumer', ['printf %s', 'python3 -c', 'node -e'])
def test_single_quoted_substitution_is_inert(tmp_path, substitution, consumer):
    script = tmp_path/'real.sh'
    script.write_text('hermes gateway stop\n')
    data = substitution.replace('BODY', 'bash ' + str(script))
    assert not classify(consumer + ' ' + shlex.quote(data), cwd=str(tmp_path))


@pytest.mark.parametrize('data', [
    '\\$(\nlaunchctl submit -l neutral -- /bin/true\n)',
    '\\`\nlaunchctl submit -l neutral -- /bin/true\n\\`',
    'ordinary\nmultiline data',
])
def test_double_quoted_inert_data(data):
    assert not classify('printf "%s" "' + data + '"')


@pytest.mark.parametrize('body', [
    'printf "%s" "$(\nbash SCRIPT\n)"',
    'printf "%s" "`\nbash SCRIPT\n`"',
    'printf ")"\n# ignored ) and quote "\nbash SCRIPT',
    '(printf safe)\nbash SCRIPT',
])
def test_substitution_quote_comment_and_nested_boundaries(tmp_path, body):
    script = tmp_path/'real.sh'
    script.write_text('hermes gateway stop\n')
    body = body.replace('SCRIPT', shlex.quote(str(script)))
    assert classify('printf "%s" "$(\n' + body + '\n)"', cwd=str(tmp_path))


def test_substitution_referenced_script_recursion(tmp_path):
    scripts = tmp_path/'scripts'
    scripts.mkdir()
    (scripts/'outer.sh').write_text('bash inner.sh\n')
    (scripts/'inner.sh').write_text('hermes gateway stop\n')
    assert classify('printf "%s" "$(\nbash scripts/outer.sh\n)"', cwd=str(tmp_path))


def test_substitution_remote_script_scan():
    reads = []

    def read_remote(path):
        reads.append(path)
        return 'hermes gateway stop\n'

    assert classify('printf "%s" "$(\nbash /missing-review-fixture/real.sh\n)"',
                    read_remote_script=read_remote)
    assert reads == ['/missing-review-fixture/real.sh']


def test_substitution_uses_existing_depth_bound():
    from cron.lifecycle_guard import _MAX_REFERENCED_SCRIPT_DEPTH
    body = 'printf safe'
    assert not classify('printf "%s" "$(\n' + body + '\n)"')
    for _ in range(_MAX_REFERENCED_SCRIPT_DEPTH):
        body = 'printf "%s" "$(\n' + body + '\n)"'
    assert classify(body)


def test_comment_does_not_open_substitution():
    assert not classify('printf safe # $( ` "\nprintf done')


@pytest.mark.parametrize('substitution', [
    '`\nbash SCRIPT\n`',
    '`printf "%s" \\`\nbash SCRIPT\n\\``',
    '$(\nprintf safe\n)$(\nbash SCRIPT\n)',
])
def test_substitution_sibling_and_backtick_scripts(tmp_path, substitution):
    script = tmp_path/'real.sh'
    script.write_text('hermes gateway stop\n')
    command = 'printf "%s" "' + substitution.replace('SCRIPT', str(script)) + '"'
    assert classify(command, cwd=str(tmp_path))


def test_substitution_script_cycle_uses_visited_paths(tmp_path):
    (tmp_path/'cycle.sh').write_text('bash cycle.sh\n')
    assert not classify('printf "%s" "$(\nbash cycle.sh\n)"', cwd=str(tmp_path))


def test_substitution_preserves_script_size_bound(tmp_path):
    from cron.lifecycle_guard import _MAX_REFERENCED_SCRIPT_BYTES
    (tmp_path/'large.sh').write_text('#' * (_MAX_REFERENCED_SCRIPT_BYTES + 1))
    assert classify('printf "%s" "$(\nbash large.sh\n)"', cwd=str(tmp_path))


@pytest.mark.parametrize('action', [
    'launchctl submit -l neutral -- /bin/true',
    'bash /nonexistent-review-fixture/real.sh',
])
def test_case_arm_must_not_truncate_substitution(action):
    command = 'printf "%s" "$(\ncase x in\nx) printf safe ;;\nesac\n' + action + '\n)"'

    def read_remote(path):
        if path == '/nonexistent-review-fixture/real.sh':
            return 'hermes gateway stop\n'
        return None

    # Exact review reproductions: classify only, never run the shell text.
    assert classify(command, read_remote_script=read_remote)


@pytest.mark.parametrize('body', [
    "cat <<'END'\n)\nEND\nlaunchctl submit -l neutral -- /bin/true",
    'printf "%s" "$(printf safe',
])
def test_ambiguous_or_incomplete_substitution_fails_closed(body):
    assert classify('printf "%s" "$(' + body + ')"')


@pytest.mark.parametrize('body', [
    'case x in x) printf safe ;; esac',
    'case x in (x) printf safe ;; esac',
    'ca\\\nse x in x) printf safe ;; esac',
    'printf safe;case x in x|y) printf safe ;; esac',
    'printf %s "$(case x in x) printf safe ;; esac)"',
    'printf %s "`case x in x) printf safe ;; esac`"',
    'case x in x) case y in y) printf safe ;; esac ;; esac',
])
def test_case_substitution_conservatively_rejected(body):
    assert classify('printf "%s" "$(' + body + ')"')


@pytest.mark.parametrize('consumer', ['python3 -c', 'node -e', 'printf %s'])
def test_ambiguous_substitution_single_quoted_data_is_inert(consumer):
    data = '$(case x in x) printf safe ;; esac\ncat <<END\n)\nEND\n)'
    assert not classify(consumer + ' ' + shlex.quote(data))


@pytest.mark.parametrize('body', [
    "printf %s 'case x in x) printf safe ;; esac'",
    'printf %s "case"',
    'printf showcase',
    'printf case_name',
    'printf safe # case x in x)\nprintf done',
    "printf %s '<<END'",
])
def test_substitution_quoted_comment_and_word_controls(body):
    assert not classify('printf "%s" "$(' + body + ')"')
