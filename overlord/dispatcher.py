from __future__ import annotations

import logging
import re
import subprocess
from pathlib import Path

from overlord.config import Settings
from overlord.models import DispatchRole, DispatchStatus, OperatorCommandCreate, OperatorCommandLaunch


class DispatchLaunchError(RuntimeError):
    pass


logger = logging.getLogger("overlord.dispatcher")
DEFAULT_CODEX_MODEL = "gpt-5.4"


class CodexDispatcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def dispatch(self, command: OperatorCommandCreate) -> OperatorCommandLaunch:
        dispatch_dir = self.settings.data_dir / "dispatches"
        dispatch_dir.mkdir(parents=True, exist_ok=True)

        stamp = command.created_at.strftime("%Y%m%dT%H%M%SZ")
        worker_slug = _slugify(command.general_worker_id)
        prompt_path = dispatch_dir / f"{stamp}-{worker_slug}.prompt.txt"
        log_path = dispatch_dir / f"{stamp}-{worker_slug}.log"
        prompt_path.write_text(_build_dispatch_prompt(command), encoding="utf-8")

        with prompt_path.open("rb") as prompt_file, log_path.open("ab") as log_file:
            try:
                process = subprocess.Popen(
                    [
                        "codex",
                        "exec",
                        "-m",
                        DEFAULT_CODEX_MODEL,
                        "--dangerously-bypass-approvals-and-sandbox",
                        "--skip-git-repo-check",
                        "-",
                    ],
                    cwd=command.repo_path,
                    stdin=prompt_file,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            except OSError as exc:
                logger.exception(
                    "event=dispatch_launch_failed general_worker_id=%r repo_path=%r",
                    command.general_worker_id,
                    command.repo_path,
                )
                raise DispatchLaunchError(f"failed to launch codex exec: {exc}") from exc

        launch = OperatorCommandLaunch(
            status=DispatchStatus.LAUNCHED,
            pid=process.pid,
            prompt_path=str(prompt_path),
            log_path=str(log_path),
        )
        logger.info(
            "event=dispatch_launched general_worker_id=%r repo_path=%r pid=%r prompt_path=%r log_path=%r",
            command.general_worker_id,
            command.repo_path,
            launch.pid,
            launch.prompt_path,
            launch.log_path,
        )
        return launch


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "general"


def _build_dispatch_prompt(command: OperatorCommandCreate) -> str:
    if command.dispatch_role == DispatchRole.CAPTAIN:
        return _build_captain_prompt(command)
    return _build_general_prompt(command)


def _build_general_prompt(command: OperatorCommandCreate) -> str:
    branch_hint = command.branch_hint or "none provided"
    return f"""$codex-general

You are a GENERAL worker handling one operator command.

Follow the local source-of-truth hierarchy and behavior contracts from:
- /Users/matthewschwartz/.codex/skills/codex-general/SKILL.md
- /Users/matthewschwartz/.codex/skills/codex-captain/SKILL.md

Execution contract:
- role: general
- general worker id: {command.general_worker_id}
- repo root: {command.repo_path}
- branch hint: {branch_hint}
- operator command source: Overlord localhost dashboard
- report to Overlord when available at http://127.0.0.1:8080
- use the official helper CLI instead of hand-written curl payloads:
  - self-report: `overlord-worker-status --worker-id <id> --worker-token <token> --role general|captain|worker --current-phase assigned|scouting|... --repo-path <path> --status-line <text> [--next-step <text> for planned/implementing/validating]`
  - register member: `overlord-worker-status register-member --member-id <id> --member-token <token> --role captain|worker --parent-member-id <parent_id> --repo-path <path> --status-line <text> [--process-id <pid>] [--process-started-at <iso8601>] [--host-id <host>]`
  - parent report: `overlord-worker-status parent-report --member-id <subject_id> --reporter-member-id <parent_id> --reporter-token <token> --observed-status-line <text> [--event-type spawned_underling|replaced_underling|terminated_underling] [--related-member-id <id>] [--observed-state <state>]`
  - message: `overlord-worker-status post-message --member-id <thread_owner_id> --sender-member-id <sender_id> --sender-token <token> --message-type launch|start|progress|check|blocker|violation|replacement|stop|complete|failure --body <text> [--related-member-id <id>]`
- messages are required, not optional
- immediately self-report, then immediately post a `launch` or `start` message for yourself
- immediately register each spawned captain with the helper-backed `/api/members/register` path using exact current field names
- immediately post a parent `launch` message on each captain thread after registration
- periodically send parent accountability reports for each captain with the helper-backed `/api/members/{{member_id}}/parent-report` path
- post required progress/check/blocker/violation/complete/failure messages on your own thread as the mission evolves
- post required parent messages on captain threads when you launch them, check them, detect drift, replace them, stop them, or confirm completion
- send explicit parent reports for captain spawn, replacement, termination, and recovery events including event_type, related_member_id, and observed_state when applicable
- require each captain to register its workers with the helper-backed API path and to parent-report on those workers
- require each captain and worker to post required messages between major state transitions and under extenuating circumstances such as long-running work, blockers, replacements, or safety stops
- require all army members to self-report their own status with pid and process-start metadata using the helper
- do not use `planned`, `implementing`, or `validating` self-reports without `--next-step`; use `assigned` or `scouting` first if that field is not ready
- launch every captain with `codex exec -m {DEFAULT_CODEX_MODEL}` unless the operator instruction explicitly requires a different model
- append terminal summary to /Users/matthewschwartz/WORKER_LOG.md

Operator instruction:
{command.operator_instruction.strip()}
"""


def _build_captain_prompt(command: OperatorCommandCreate) -> str:
    branch_hint = command.branch_hint or "none provided"
    return f"""$codex-captain

You are a CAPTAIN worker handling one operator command directly from Overlord.

Follow the local source-of-truth hierarchy and behavior contracts from:
- /Users/matthewschwartz/.codex/skills/codex-captain/SKILL.md

Execution contract:
- role: captain
- captain worker id: {command.general_worker_id}
- repo root: {command.repo_path}
- branch hint: {branch_hint}
- operator command source: Overlord localhost dashboard
- report to Overlord when available at http://127.0.0.1:8080
- use the official helper CLI instead of hand-written curl payloads:
  - self-report: `overlord-worker-status --worker-id <id> --worker-token <token> --role captain|worker --current-phase assigned|scouting|... --repo-path <path> --status-line <text> [--next-step <text> for planned/implementing/validating]`
  - register member: `overlord-worker-status register-member --member-id <id> --member-token <token> --role worker --parent-member-id <parent_id> --repo-path <path> --status-line <text> [--process-id <pid>] [--process-started-at <iso8601>] [--host-id <host>]`
  - parent report: `overlord-worker-status parent-report --member-id <subject_id> --reporter-member-id <parent_id> --reporter-token <token> --observed-status-line <text> [--event-type spawned_underling|replaced_underling|terminated_underling] [--related-member-id <id>] [--observed-state <state>]`
  - message: `overlord-worker-status post-message --member-id <thread_owner_id> --sender-member-id <sender_id> --sender-token <token> --message-type launch|start|progress|check|blocker|violation|replacement|stop|complete|failure --body <text> [--related-member-id <id>]`
- messages are required, not optional
- immediately self-report with the helper using exact current field names and valid phase requirements
- immediately post a `launch` or `start` message for yourself after self-report
- immediately register each spawned worker with the helper-backed `/api/members/register` path
- immediately post a parent `launch` message on each worker thread after registration
- periodically send parent accountability reports for each worker with the helper-backed `/api/members/{{member_id}}/parent-report` path
- post required progress/check/blocker/violation/complete/failure messages on your own thread as the mission evolves
- post required parent messages on worker threads when you launch them, check them, detect drift, replace them, stop them, or confirm completion
- send explicit parent reports for worker spawn, replacement, termination, drift, and recovery events including event_type, related_member_id, and observed_state when applicable
- require all workers to self-report their own status with pid and process-start metadata using the helper
- require all workers to post required messages between major state transitions and under extenuating circumstances such as long-running work, blockers, replacements, or safety stops
- do not use `planned`, `implementing`, or `validating` self-reports without `--next-step`; use `assigned` or `scouting` first if that field is not ready
- launch every worker with `codex exec -m {DEFAULT_CODEX_MODEL}` unless the operator instruction explicitly requires a different model
- append terminal summary to /Users/matthewschwartz/WORKER_LOG.md

Operator instruction:
{command.operator_instruction.strip()}
"""
