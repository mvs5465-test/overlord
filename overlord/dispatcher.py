from __future__ import annotations

import re
import subprocess
from pathlib import Path

from overlord.config import Settings
from overlord.models import DispatchStatus, OperatorCommandCreate, OperatorCommandLaunch


class DispatchLaunchError(RuntimeError):
    pass


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
        prompt_path.write_text(_build_general_prompt(command), encoding="utf-8")

        with prompt_path.open("rb") as prompt_file, log_path.open("ab") as log_file:
            try:
                process = subprocess.Popen(
                    [
                        "codex",
                        "exec",
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
                raise DispatchLaunchError(f"failed to launch codex exec: {exc}") from exc

        return OperatorCommandLaunch(
            status=DispatchStatus.LAUNCHED,
            pid=process.pid,
            prompt_path=str(prompt_path),
            log_path=str(log_path),
        )


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "general"


def _build_general_prompt(command: OperatorCommandCreate) -> str:
    branch_hint = command.branch_hint or "none provided"
    return f"""You are a GENERAL worker handling one operator command.

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
- append terminal summary to /Users/matthewschwartz/WORKER_LOG.md

Operator instruction:
{command.operator_instruction.strip()}
"""
