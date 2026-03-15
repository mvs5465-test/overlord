from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib import error, request


DEFAULT_CONTROL_PLANE_URL = "http://127.0.0.1:8080"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="overlord-worker-status",
        description="Post a worker phase transition to a local Overlord control plane.",
    )
    parser.add_argument("--worker-id", required=True, help="Stable worker identifier.")
    parser.add_argument(
        "--worker-token",
        default=os.environ.get("OVERLORD_WORKER_TOKEN"),
        help="Scoped capability token for this worker.",
    )
    parser.add_argument("--current-phase", required=True, help="New worker phase.")
    parser.add_argument("--previous-phase", help="Stored worker phase before this transition.")
    parser.add_argument("--status-line", default="worker update", help="Short status summary.")
    parser.add_argument("--note", help="Short phase note.")
    parser.add_argument("--repo-path", default=os.getcwd(), help="Owned repo path. Defaults to cwd.")
    parser.add_argument("--branch", help="Owned branch.")
    parser.add_argument("--worktree", help="Owned worktree path.")
    parser.add_argument("--owned-artifact", help="Owned artifact path.")
    parser.add_argument("--next-step", help="Next irreversible step or unblock needed.")
    parser.add_argument("--blocker", help="Concrete blocker description.")
    parser.add_argument("--pr-url", help="Pull request URL when relevant.")
    parser.add_argument(
        "--control-plane-url",
        default=os.environ.get("OVERLORD_CONTROL_PLANE_URL", DEFAULT_CONTROL_PLANE_URL),
        help="Base URL for the local Overlord API.",
    )
    return parser


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "worker_id": args.worker_id,
        "worker_token": args.worker_token,
        "current_phase": args.current_phase,
        "repo_path": str(Path(args.repo_path).resolve()),
        "status_line": args.status_line,
    }
    optional_fields = {
        "previous_phase": args.previous_phase,
        "note": args.note,
        "branch": args.branch,
        "worktree": args.worktree,
        "owned_artifact": args.owned_artifact,
        "next_irreversible_step": args.next_step,
        "blocker": args.blocker,
        "pr_url": args.pr_url,
    }
    for key, value in optional_fields.items():
        if value:
            payload[key] = value
    return payload


def post_worker_status(payload: dict[str, Any], *, control_plane_url: str) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    endpoint = f"{control_plane_url.rstrip('/')}/api/workers/events"
    req = request.Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not args.worker_token:
        parser.error("--worker-token or OVERLORD_WORKER_TOKEN is required")
    payload = build_payload(args)

    try:
        result = post_worker_status(payload, control_plane_url=args.control_plane_url)
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace").strip()
        print(f"request failed: {exc.code} {detail}", file=sys.stderr)
        return 1
    except error.URLError as exc:
        print(f"request failed: {exc.reason}", file=sys.stderr)
        return 1

    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
