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
        description="Post worker/member status updates to a local Overlord control plane.",
    )
    parser.set_defaults(command="event")
    parser.add_argument(
        "--control-plane-url",
        default=os.environ.get("OVERLORD_CONTROL_PLANE_URL", DEFAULT_CONTROL_PLANE_URL),
        help="Base URL for the local Overlord API.",
    )
    parser.add_argument("--worker-id", help="Stable worker identifier.")
    parser.add_argument(
        "--worker-token",
        default=os.environ.get("OVERLORD_WORKER_TOKEN"),
        help="Scoped capability token for this worker.",
    )
    parser.add_argument("--current-phase", help="New worker phase.")
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
    parser.add_argument("--role", help="Member role for event mode.")
    parser.add_argument("--parent-worker-id", help="Parent worker id for event mode.")
    parser.add_argument("--host-id", default=os.environ.get("OVERLORD_HOST_ID"), help="Host identifier.")
    parser.add_argument("--process-id", type=int, help="Process id to report.")
    parser.add_argument("--process-started-at", help="ISO8601 process start time.")

    subparsers = parser.add_subparsers(dest="command_name")

    register = subparsers.add_parser("register-member", help="Register a captain or worker with Overlord.")
    register.add_argument("--member-id", required=True, help="Stable member identifier.")
    register.add_argument(
        "--member-token",
        default=os.environ.get("OVERLORD_WORKER_TOKEN"),
        required=False,
        help="Scoped capability token for this member.",
    )
    register.add_argument("--role", required=True, help="Role: general, captain, or worker.")
    register.add_argument("--parent-member-id", help="Parent member id.")
    register.add_argument("--parent-token", help="Parent capability token.")
    register.add_argument("--repo-path", required=True, help="Owned repo path.")
    register.add_argument("--branch", help="Owned branch.")
    register.add_argument("--worktree", help="Owned worktree path.")
    register.add_argument("--owned-artifact", help="Owned artifact path.")
    register.add_argument("--host-id", default=os.environ.get("OVERLORD_HOST_ID"), help="Host identifier.")
    register.add_argument("--process-id", type=int, help="Process id to report.")
    register.add_argument("--process-started-at", help="ISO8601 process start time.")
    register.add_argument("--phase", default="assigned", help="Initial phase.")
    register.add_argument("--status-line", required=True, help="Short registration status.")
    register.add_argument("--note", help="Optional registration note.")

    parent = subparsers.add_parser("parent-report", help="Submit a parent accountability report.")
    parent.add_argument("--member-id", required=True, help="Subject member id and URL target.")
    parent.add_argument("--reporter-member-id", required=True, help="Reporter member id.")
    parent.add_argument(
        "--reporter-token",
        default=os.environ.get("OVERLORD_WORKER_TOKEN"),
        required=False,
        help="Reporter capability token.",
    )
    parent.add_argument("--event-type", help="Structured event type such as spawned_underling.")
    parent.add_argument("--related-member-id", help="Related worker/member for replacement or termination events.")
    parent.add_argument("--observed-phase", help="Observed current phase.")
    parent.add_argument("--observed-status-line", required=True, help="Short observed status summary.")
    parent.add_argument("--observed-state", help="Observed effective state.")
    parent.add_argument("--blocker", help="Concrete blocker description.")
    parent.add_argument("--note", help="Optional note.")
    parent.add_argument("--process-id", type=int, help="Observed process id if relevant.")

    message = subparsers.add_parser("post-message", help="Post a member message to Overlord.")
    message.add_argument("--member-id", required=True, help="Member id that owns the message thread.")
    message.add_argument("--sender-member-id", required=True, help="Member id sending the message.")
    message.add_argument(
        "--sender-token",
        default=os.environ.get("OVERLORD_WORKER_TOKEN"),
        required=False,
        help="Sender capability token.",
    )
    message.add_argument("--message-type", required=True, help="Message type such as start, progress, or blocker.")
    message.add_argument("--body", required=True, help="Short message body.")
    message.add_argument("--related-member-id", help="Optional related member id.")
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
        "role": args.role,
        "parent_worker_id": args.parent_worker_id,
        "host_id": args.host_id,
        "process_id": args.process_id,
        "process_started_at": args.process_started_at,
    }
    for key, value in optional_fields.items():
        if value not in (None, ""):
            payload[key] = value
    return payload


def build_registration_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "member_id": args.member_id,
        "member_token": args.member_token,
        "role": args.role,
        "repo_path": str(Path(args.repo_path).resolve()),
        "phase": args.phase,
        "status_line": args.status_line,
    }
    optional_fields = {
        "parent_member_id": args.parent_member_id,
        "parent_token": args.parent_token,
        "branch": args.branch,
        "worktree": args.worktree,
        "owned_artifact": args.owned_artifact,
        "host_id": args.host_id,
        "process_id": args.process_id,
        "process_started_at": args.process_started_at,
        "note": args.note,
    }
    for key, value in optional_fields.items():
        if value not in (None, ""):
            payload[key] = value
    return payload


def build_parent_report_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "subject_member_id": args.member_id,
        "reporter_member_id": args.reporter_member_id,
        "reporter_token": args.reporter_token,
        "observed_status_line": args.observed_status_line,
    }
    optional_fields = {
        "event_type": args.event_type,
        "related_member_id": args.related_member_id,
        "observed_phase": args.observed_phase,
        "observed_state": args.observed_state,
        "blocker": args.blocker,
        "note": args.note,
        "process_id": args.process_id,
    }
    for key, value in optional_fields.items():
        if value not in (None, ""):
            payload[key] = value
    return payload


def build_member_message_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "member_id": args.member_id,
        "sender_member_id": args.sender_member_id,
        "sender_token": args.sender_token,
        "message_type": args.message_type,
        "body": args.body,
    }
    if args.related_member_id not in (None, ""):
        payload["related_member_id"] = args.related_member_id
    return payload


def post_json(payload: dict[str, Any], *, endpoint: str) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def post_worker_status(payload: dict[str, Any], *, control_plane_url: str) -> dict[str, Any]:
    endpoint = f"{control_plane_url.rstrip('/')}/api/workers/events"
    return post_json(payload, endpoint=endpoint)


def post_member_registration(payload: dict[str, Any], *, control_plane_url: str) -> dict[str, Any]:
    endpoint = f"{control_plane_url.rstrip('/')}/api/members/register"
    return post_json(payload, endpoint=endpoint)


def post_parent_report(payload: dict[str, Any], *, control_plane_url: str, member_id: str) -> dict[str, Any]:
    endpoint = f"{control_plane_url.rstrip('/')}/api/members/{member_id}/parent-report"
    return post_json(payload, endpoint=endpoint)


def post_member_message(payload: dict[str, Any], *, control_plane_url: str, member_id: str) -> dict[str, Any]:
    endpoint = f"{control_plane_url.rstrip('/')}/api/members/{member_id}/messages"
    return post_json(payload, endpoint=endpoint)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command_name or args.command
    control_plane_url = args.control_plane_url

    try:
        if command == "register-member":
            if not args.member_token:
                parser.error("--member-token or OVERLORD_WORKER_TOKEN is required")
            result = post_member_registration(
                build_registration_payload(args),
                control_plane_url=control_plane_url,
            )
        elif command == "parent-report":
            if not args.reporter_token:
                parser.error("--reporter-token or OVERLORD_WORKER_TOKEN is required")
            result = post_parent_report(
                build_parent_report_payload(args),
                control_plane_url=control_plane_url,
                member_id=args.member_id,
            )
        elif command == "post-message":
            if not args.sender_token:
                parser.error("--sender-token or OVERLORD_WORKER_TOKEN is required")
            result = post_member_message(
                build_member_message_payload(args),
                control_plane_url=control_plane_url,
                member_id=args.member_id,
            )
        else:
            if not args.worker_id:
                parser.error("--worker-id is required")
            if not args.worker_token:
                parser.error("--worker-token or OVERLORD_WORKER_TOKEN is required")
            if not args.current_phase:
                parser.error("--current-phase is required")
            result = post_worker_status(
                build_payload(args),
                control_plane_url=control_plane_url,
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
