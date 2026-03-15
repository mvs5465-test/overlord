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
        description="Post a worker status transition to a local Overlord control plane.",
    )
    parser.add_argument("--worker-id", required=True, help="Stable worker identifier.")
    parser.add_argument("--status", required=True, help="Worker phase or terminal status.")
    parser.add_argument("--previous-status", help="Previous worker phase if known.")
    parser.add_argument("--note", help="Short worker note for this transition.")
    parser.add_argument("--repo-path", default=os.getcwd(), help="Owned repo path. Defaults to cwd.")
    parser.add_argument("--branch", help="Branch or worktree label.")
    parser.add_argument("--artifact", help="Owned artifact for this transition.")
    parser.add_argument("--next-step", help="Next irreversible step or unblock needed.")
    parser.add_argument(
        "--control-plane-url",
        default=os.environ.get("OVERLORD_CONTROL_PLANE_URL", DEFAULT_CONTROL_PLANE_URL),
        help="Base URL for the local Overlord API.",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("OVERLORD_WORKER_TOKEN"),
        help="Optional worker write token for Authorization: Bearer auth.",
    )
    return parser


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "workerId": args.worker_id,
        "status": args.status,
        "repoPath": str(Path(args.repo_path).resolve()),
    }
    optional_fields = {
        "previousStatus": args.previous_status,
        "note": args.note,
        "branch": args.branch,
        "artifact": args.artifact,
        "nextStep": args.next_step,
    }
    for key, value in optional_fields.items():
        if value:
            payload[key] = value
    return payload


def post_worker_status(
    payload: dict[str, Any],
    *,
    control_plane_url: str,
    token: str | None = None,
) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    endpoint = f"{control_plane_url.rstrip('/')}/api/worker-status"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = request.Request(endpoint, data=body, headers=headers, method="POST")
    with request.urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    payload = build_payload(args)

    try:
        result = post_worker_status(
            payload,
            control_plane_url=args.control_plane_url,
            token=args.token,
        )
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
