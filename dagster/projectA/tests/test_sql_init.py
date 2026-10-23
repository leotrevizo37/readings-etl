import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


_ROOT = Path(__file__).resolve().parents[3]
_INIT = _ROOT / "sql" / "init"
_PHASES = (
    "CREATE_DATABASE",
    "init/010_first.sql",
    "procedures/010_global.sql",
    "user_codes/test/init/010_tenant.sql",
    "user_codes/test/procedures/010_tenant_proc.sql",
)


def test_event_migration_suppresses_row_counts_before_transaction():
    script = (_INIT / "029_alerting_event_model.sql").read_text(encoding="utf-8")
    assert script.index("SET NOCOUNT ON;") < script.index("BEGIN TRANSACTION;")
    assert "SET NOCOUNT OFF" not in script
    assert script.index("COMMIT TRANSACTION;") < script.index(
        "CREATE TABLE alerting.alert_action_event"
    )


def test_alert_state_view_dependencies_are_created_before_the_view():
    view_path = _INIT / "031_serving_alert_state.sql"
    view = view_path.read_text(encoding="utf-8")
    dependencies = set(re.findall(r"\b(?:FROM|JOIN)\s+(alerting\.\w+)", view))
    assert "alerting.alert_action_event" in dependencies
    preceding_scripts = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(_INIT.glob("*.sql"))
        if path.name < view_path.name
    )
    tables = set(re.findall(r"\bCREATE TABLE\s+(alerting\.\w+)", preceding_scripts))
    assert dependencies <= tables


@pytest.fixture
def sql_init_runner(tmp_path):
    shell = shutil.which("sh")
    if shell is None:
        pytest.skip("The SQL initializer requires a POSIX shell")

    compose = yaml.safe_load(
        (_ROOT / "compose" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )
    )
    command = compose["services"]["sql-init"]["command"]
    assert command[:2] == ["sh", "-c"]
    fake_sqlcmd = tmp_path / "sqlcmd"
    fake_sqlcmd.write_text(
        """#!/bin/sh
set -eu
phase=CREATE_DATABASE
while [ "$#" -gt 0 ]; do
  case "$1" in
    -i) shift; phase="${1#"$SQLCMD_TEST_ROOT"/}" ;;
    -Q) shift; [ "$1" != "SELECT 1" ] || exit 0 ;;
  esac
  shift
done
printf '%s\\n' "$phase" >> "$SQLCMD_TEST_CALLS"
if [ "$phase" = "$SQLCMD_TEST_FAIL_PHASE" ]; then
  printf '%s\\n' "$SQLCMD_TEST_ERROR" >&"$SQLCMD_TEST_ERROR_FD"
  exit "$SQLCMD_TEST_EXIT_CODE"
fi
echo "SQL batch completed."
""",
        encoding="utf-8",
    )
    fake_sqlcmd.chmod(0o755)
    for phase in _PHASES[1:]:
        path = tmp_path / phase
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    calls = tmp_path / "calls"
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    script = (
        command[2]
        .replace("$$", "$")
        .replace("/opt/mssql-tools/bin/sqlcmd", str(fake_sqlcmd))
        .replace("/sql/", f"{tmp_path}/")
    )

    def run(fail_phase="", error="", exit_code=0, error_fd=2):
        env = {
            **os.environ,
            "MSSQL_HOST": "localhost",
            "MSSQL_PORT": "1433",
            "MSSQL_USER": "test",
            "MSSQL_PASSWORD": "unused-test-password",
            "USER_CODE_DBS": "test=InitTest",
            "SQLCMD_TEST_ROOT": str(tmp_path),
            "SQLCMD_TEST_CALLS": str(calls),
            "SQLCMD_TEST_FAIL_PHASE": fail_phase,
            "SQLCMD_TEST_ERROR": error,
            "SQLCMD_TEST_ERROR_FD": str(error_fd),
            "SQLCMD_TEST_EXIT_CODE": str(exit_code),
            "TMPDIR": str(log_dir),
        }
        result = subprocess.run(
            [shell, "-c", script],
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert not list(log_dir.iterdir())
        return result, calls.read_text(encoding="utf-8").splitlines()

    return run


def test_sql_init_runs_all_phases_in_order(sql_init_runner):
    result, calls = sql_init_runner()
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls == list(_PHASES)
    assert "SQL init completed." in result.stdout


@pytest.mark.parametrize("phase", _PHASES)
@pytest.mark.parametrize(
    ("error", "exit_code", "error_fd"),
    (
        ("SqlState 24000, Invalid cursor state", 0, 1),
        ("SqlState 24000, Invalid cursor state", 0, 2),
        ("Sqlcmd: Error: Connection failure.", 0, 2),
        ("Msg 208, Level 16, State 1: Invalid object name.", 7, 1),
    ),
)
def test_sql_init_stops_at_the_first_error(
    sql_init_runner, phase, error, exit_code, error_fd
):
    result, calls = sql_init_runner(phase, error, exit_code, error_fd)
    assert result.returncode == (exit_code or 1)
    assert error in result.stdout
    assert calls == list(_PHASES[: _PHASES.index(phase) + 1])
    assert "SQL init completed." not in result.stdout
