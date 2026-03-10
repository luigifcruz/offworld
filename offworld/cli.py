#!/usr/bin/env python3

from __future__ import annotations

import argparse
import copy
import importlib
import concurrent.futures
import threading
import time
import hashlib
from collections import deque
import os
import platform
import posixpath
import re
import select
import shlex
import shutil
import subprocess
import sys
import termios
import traceback
import tty
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, cast, TextIO

try:
    import yaml
except Exception as exc:  # pragma: no cover
    print(f"Failed to import PyYAML: {exc}", file=sys.stderr)
    print("Install it with: python3 -m pip install pyyaml", file=sys.stderr)
    sys.exit(2)

try:
    Console = importlib.import_module("rich.console").Console
    Group = importlib.import_module("rich.console").Group
    Live = importlib.import_module("rich.live").Live
    Layout = importlib.import_module("rich.layout").Layout
    escape = importlib.import_module("rich.markup").escape
    Panel = importlib.import_module("rich.panel").Panel
    Table = importlib.import_module("rich.table").Table
    Text = importlib.import_module("rich.text").Text
    box = importlib.import_module("rich.box")
    Progress = importlib.import_module("rich.progress").Progress
    SpinnerColumn = importlib.import_module("rich.progress").SpinnerColumn
    BarColumn = importlib.import_module("rich.progress").BarColumn
    TextColumn = importlib.import_module("rich.progress").TextColumn
    TimeElapsedColumn = importlib.import_module("rich.progress").TimeElapsedColumn
    ProgressBar = importlib.import_module("rich.progress_bar").ProgressBar
except Exception as exc:  # pragma: no cover
    print(f"Failed to import rich: {exc}", file=sys.stderr)
    print("Install it with: python3 -m pip install rich", file=sys.stderr)
    sys.exit(2)


CONTAINER_CLI_ORDER = ("docker", "podman", "nerdctl")

# Local runner state directory (workspaces, caches).
# Intentionally fixed to `.build` to avoid churn/fragmentation.
STATE_DIR_NAME = ".build"
_CANCEL_NOTICE = threading.Event()
_CANCEL_REQUEST = threading.Event()
_RUNNING_PROCS_LOCK = threading.Lock()
_RUNNING_PROCS: set[subprocess.Popen[Any]] = set()


def notify_cancel_once() -> None:
    if _CANCEL_NOTICE.is_set():
        return
    _CANCEL_NOTICE.set()
    Console(stderr=True).print(
        "[bold yellow]Ctrl-C received.[/bold yellow] "
        "Stopping running steps and cleaning up. This may take a moment..."
    )


def register_running_proc(proc: subprocess.Popen[Any]) -> None:
    with _RUNNING_PROCS_LOCK:
        _RUNNING_PROCS.add(proc)


def unregister_running_proc(proc: subprocess.Popen[Any]) -> None:
    with _RUNNING_PROCS_LOCK:
        _RUNNING_PROCS.discard(proc)


def terminate_all_running_procs() -> None:
    with _RUNNING_PROCS_LOCK:
        procs = list(_RUNNING_PROCS)
    for proc in procs:
        try:
            proc.terminate()
        except Exception:
            pass
    for proc in procs:
        try:
            proc.wait(timeout=0.5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


def request_cancel() -> None:
    _CANCEL_REQUEST.set()


def clear_cancel() -> None:
    _CANCEL_REQUEST.clear()


def is_cancel_requested() -> bool:
    return _CANCEL_REQUEST.is_set()


STATUS_BORDER: dict[str, str] = {
    "pending": "grey27",
    "queued": "grey27",
    "copying": "yellow",
    "running": "cyan",
    "ok": "green",
    "failed": "red",
}


STATUS_SUBTITLE_STYLE: dict[str, str] = {
    "pending": "dim",
    "queued": "dim",
    "copying": "dim yellow",
    "running": "dim cyan",
    "ok": "dim green",
    "failed": "dim red",
}


@dataclass
class RuntimeSpec:
    name: str
    kind: str
    base: str | None
    workdir: str
    env: dict[str, str]


@dataclass
class Step:
    run: str
    cwd: str | None
    env: dict[str, str]
    name: str | None


@dataclass
class Validation:
    kind: str
    name: str
    steps: list[Step]


@dataclass
class FailedStepContext:
    job_name: str
    step: str
    step_cmd: str


class JobExecutionError(Exception):
    def __init__(
        self,
        *,
        context: FailedStepContext,
        cause: subprocess.CalledProcessError,
    ) -> None:
        self.context = context
        self.cause = cause
        super().__init__(str(cause))


def fail(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def looks_like_git_url(value: str) -> bool:
    v = value.strip()
    if not v:
        return False
    return (
        v.startswith("git@")
        or v.startswith("ssh://")
        or v.startswith("https://")
        or v.startswith("http://")
        or v.endswith(".git")
    )


def derive_repo_slug(repo_url: str) -> str:
    tail = repo_url.rstrip("/").split("/")[-1]
    if tail.endswith(".git"):
        tail = tail[:-4]
    slug = sanitize_container_name(tail)
    return slug if slug else "repo"


def canonical_architecture_id(raw_arch: str) -> str:
    arch = raw_arch.strip().lower().replace("-", "_")
    aliases = {
        "amd64": "x86_64",
        "x64": "x86_64",
        "x86_64": "x86_64",
        "arm64": "aarch64",
        "arm_64": "aarch64",
        "aarch64": "aarch64",
    }
    return aliases.get(arch, arch)


def current_system_architecture() -> str:
    return canonical_architecture_id(platform.machine())


def format_elapsed_duration(seconds: float) -> str:
    total = max(0, int(round(seconds)))
    mins, secs = divmod(total, 60)
    hours, mins = divmod(mins, 60)
    if hours:
        return f"{hours}h {mins}m {secs}s"
    if mins:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def path_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return total


def clone_repo_into_build(
    *, repo_url: str, host_root: Path, verbose: bool, dry_run: bool
) -> Path:
    repos_root = host_root / STATE_DIR_NAME / "repos"
    repos_root.mkdir(parents=True, exist_ok=True)

    slug = derive_repo_slug(repo_url)
    digest = hashlib.sha1(repo_url.encode("utf-8")).hexdigest()[:8]
    target = repos_root / f"{slug}-{digest}-{uuid.uuid4().hex[:8]}"

    if dry_run:
        Console().print(
            "[yellow]Note:[/yellow] cloning repository to resolve pipeline before dry-run."
        )

    run_cmd(
        ["git", "clone", "--depth", "1", repo_url, str(target)],
        cwd=host_root,
        env={},
        dry_run=False,
        verbose=verbose,
        tui=False,
        title=f"clone {repo_url}",
    )
    return target


def load_pipeline(path: Path) -> dict[str, Any]:
    if not path.exists():
        fail(f"Pipeline file not found: {path}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        fail("Pipeline root must be a mapping.")

    version_raw = data.get("version", 1)
    if isinstance(version_raw, bool):
        fail("Pipeline field 'version' must be integer 1.")
    if isinstance(version_raw, str):
        if not version_raw.strip().isdigit():
            fail("Pipeline field 'version' must be integer 1.")
        version_raw = int(version_raw.strip())
    if not isinstance(version_raw, int):
        fail("Pipeline field 'version' must be integer 1.")
    version = int(version_raw)
    if version != 1:
        fail(f"Unsupported pipeline version: {version}. Supported version is 1.")

    tree = data.get("tree")
    if not isinstance(tree, dict):
        fail("Pipeline version 1 requires top-level 'tree' mapping.")

    jobs = resolve_tree_jobs(cast(dict[str, Any], tree))
    if not jobs:
        fail("Pipeline tree resolved to no runnable jobs.")
    data["jobs"] = jobs
    data.setdefault("runtimes", {})
    return data


def merge_node_config(parent: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(parent)
    for key in ("kind", "base", "workdir"):
        if key in node:
            out[key] = copy.deepcopy(node[key])

    if "env" in node:
        env = node.get("env")
        if not isinstance(env, dict):
            fail("Tree node field 'env' must be mapping.")
        base_env = out.get("env", {})
        if not isinstance(base_env, dict):
            fail("Inherited 'env' must be mapping.")
        merged_env: dict[str, Any] = {}
        merged_env.update(cast(dict[str, Any], base_env))
        merged_env.update(cast(dict[str, Any], env))
        out["env"] = merged_env

    for key in ("steps", "validate", "artifacts"):
        if key in node:
            value = node.get(key)
            if not isinstance(value, list):
                fail(f"Tree node field '{key}' must be list.")
            base_list = out.get(key, [])
            if not isinstance(base_list, list):
                fail(f"Inherited field '{key}' must be list.")
            out[key] = copy.deepcopy(cast(list[Any], base_list)) + copy.deepcopy(
                cast(list[Any], value)
            )

    return out


def merge_tree_overlay(
    parent: dict[str, Any], overlay: dict[str, Any], context_name: str
) -> dict[str, Any]:
    out = copy.deepcopy(parent)

    for key, val in overlay.items():
        if key in {"kind", "base", "workdir"}:
            out[key] = copy.deepcopy(val)
            continue

        if key == "env":
            if not isinstance(val, dict):
                fail(f"Tree node '{context_name}' field 'env' must be mapping.")
            base = out.get("env", {})
            if not isinstance(base, dict):
                fail(f"Inherited 'env' for tree node '{context_name}' must be mapping.")
            merged_env: dict[str, Any] = {}
            merged_env.update(cast(dict[str, Any], base))
            merged_env.update(cast(dict[str, Any], val))
            out["env"] = merged_env
            continue

        if key in {"steps", "validate", "artifacts"}:
            if not isinstance(val, list):
                fail(f"Tree node '{context_name}' field '{key}' must be list.")
            base_list = out.get(key, [])
            if not isinstance(base_list, list):
                fail(
                    f"Inherited field '{key}' for tree node '{context_name}' must be list."
                )
            out[key] = copy.deepcopy(cast(list[Any], base_list)) + copy.deepcopy(
                cast(list[Any], val)
            )
            continue

        if key == "targets":
            if not isinstance(val, dict):
                fail(f"Tree node '{context_name}' field 'targets' must be mapping.")
            out[key] = copy.deepcopy(val)
            continue

        if not isinstance(val, dict):
            fail(f"Tree child '{key}' under '{context_name}' must be a mapping.")

        base_child = out.get(key)
        if base_child is None:
            out[key] = copy.deepcopy(val)
            continue
        if not isinstance(base_child, dict):
            fail(f"Tree child '{key}' under '{context_name}' must be a mapping.")
        out[key] = merge_tree_overlay(
            cast(dict[str, Any], base_child),
            cast(dict[str, Any], val),
            f"{context_name}.{key}",
        )

    return out


def inject_target_metadata(job: dict[str, Any], target_key: str) -> dict[str, Any]:
    out = copy.deepcopy(job)
    env = out.get("env", {})
    if not isinstance(env, dict):
        fail("Inherited 'env' must be mapping.")
    merged_env: dict[str, Any] = {}
    merged_env.update(cast(dict[str, Any], env))
    merged_env["OFFWORLD_TARGET"] = target_key
    merged_env["OFFWORLD_TARGET_ARCH"] = target_key
    out["env"] = merged_env
    out["arch"] = target_key
    return out


def resolve_tree_jobs(tree: dict[str, Any]) -> dict[str, Any]:
    jobs: dict[str, Any] = {}
    config_keys = {
        "kind",
        "base",
        "workdir",
        "env",
        "steps",
        "validate",
        "artifacts",
        "targets",
        "arch",
    }

    def walk(
        node_name: str,
        node_value: Any,
        path_parts: list[str],
        inherited: dict[str, Any],
        target_key: str | None = None,
    ) -> None:
        if not isinstance(node_value, dict):
            fail(f"Tree node '{node_name}' must be a mapping.")

        targets_raw = node_value.get("targets")
        if targets_raw is not None:
            if not isinstance(targets_raw, dict):
                fail(f"Tree node '{node_name}' field 'targets' must be mapping.")

            base_node = copy.deepcopy(cast(dict[str, Any], node_value))
            base_node.pop("targets", None)
            for raw_target_key, overlay_any in targets_raw.items():
                if not isinstance(raw_target_key, str) or not raw_target_key.strip():
                    fail(
                        f"Tree node '{node_name}' target keys must be non-empty strings."
                    )
                if not isinstance(overlay_any, dict):
                    fail(
                        f"Tree target '{raw_target_key}' under '{node_name}' must be a mapping."
                    )
                concrete_name = f"{path_parts[-1]}[{raw_target_key}]"
                expanded_node = merge_tree_overlay(
                    base_node,
                    cast(dict[str, Any], overlay_any),
                    f"{node_name}[{raw_target_key}]",
                )
                walk(
                    node_name,
                    expanded_node,
                    path_parts[:-1] + [concrete_name],
                    inherited,
                    target_key=raw_target_key,
                )
            return

        merged = merge_node_config(inherited, cast(dict[str, Any], node_value))

        child_keys: list[str] = []
        for k, v in node_value.items():
            if k in config_keys:
                continue
            if not isinstance(v, dict):
                fail(f"Tree child '{k}' under '{node_name}' must be a mapping.")
            child_keys.append(k)

        visible_path = [
            p for p in path_parts if not p.startswith(".") and not p.startswith("_")
        ]
        if not child_keys:
            job_name = ":".join(visible_path)
            if not job_name:
                # Hidden/meta leaf with no visible job name.
                return
            if target_key is not None:
                merged = inject_target_metadata(merged, target_key)
            if not isinstance(merged.get("kind"), str):
                fail(f"Tree leaf '{job_name}' must define/inherit runtime 'kind'.")
            if not isinstance(merged.get("steps"), list) or not merged.get("steps"):
                fail(f"Tree leaf '{job_name}' must define/inherit non-empty 'steps'.")
            if job_name in jobs:
                fail(f"Duplicate job name generated from tree: {job_name}")
            jobs[job_name] = merged
            return

        for k in child_keys:
            walk(k, node_value[k], path_parts + [k], merged, target_key=target_key)

    for root_name, root_value in tree.items():
        walk(root_name, root_value, [root_name], {})

    return jobs


def merge_job_definition(
    *, parent: dict[str, Any], child: dict[str, Any], child_name: str
) -> dict[str, Any]:
    out = copy.deepcopy(parent)

    for key, val in child.items():
        if key == "extends":
            continue

        if key == "env":
            if not isinstance(val, dict):
                fail(f"Job '{child_name}' field 'env' must be mapping.")
            base = out.get("env", {})
            if not isinstance(base, dict):
                fail(f"Parent env for job '{child_name}' must be mapping.")
            merged: dict[str, Any] = {}
            merged.update(base)
            merged.update(val)
            out["env"] = merged
            continue

        if key in {"steps", "validate", "artifacts"}:
            if not isinstance(val, list):
                fail(f"Job '{child_name}' field '{key}' must be list.")
            base_list = out.get(key, [])
            if not isinstance(base_list, list):
                fail(f"Parent field '{key}' for job '{child_name}' must be list.")
            out[key] = copy.deepcopy(base_list) + copy.deepcopy(val)
            continue

        out[key] = copy.deepcopy(val)

    out.pop("extends", None)
    return out


def resolve_job_inheritance(jobs: dict[str, Any]) -> dict[str, Any]:
    resolved: dict[str, dict[str, Any]] = {}
    state: dict[str, int] = {}
    stack: list[str] = []

    def resolve(name: str) -> dict[str, Any]:
        if name in resolved:
            return resolved[name]

        st = state.get(name, 0)
        if st == 1:
            cycle = " -> ".join(stack + [name])
            fail(f"Circular job inheritance detected: {cycle}")
        if st == 2:
            return resolved[name]

        raw_any = jobs.get(name)
        if not isinstance(raw_any, dict):
            fail(f"Job '{name}' must be a mapping.")
        raw: dict[str, Any] = cast(dict[str, Any], raw_any)

        state[name] = 1
        stack.append(name)
        try:
            extends = raw.get("extends")
            if extends is None:
                merged = copy.deepcopy(raw)
                merged.pop("extends", None)
                resolved[name] = merged
            else:
                if not isinstance(extends, str) or not extends.strip():
                    fail(f"Job '{name}' field 'extends' must be non-empty string.")
                parent_name = extends.strip()
                if parent_name == name:
                    fail(f"Job '{name}' cannot extend itself.")
                if parent_name not in jobs:
                    fail(f"Job '{name}' extends unknown job '{parent_name}'.")
                parent = resolve(parent_name)
                resolved[name] = merge_job_definition(
                    parent=parent, child=raw, child_name=name
                )
        finally:
            stack.pop()
            state[name] = 2

        return resolved[name]

    for job_name in jobs.keys():
        resolve(job_name)

    return resolved


def parse_runtime(name: str, payload: dict[str, Any]) -> RuntimeSpec:
    if not isinstance(payload, dict):
        fail(f"Runtime '{name}' must be a mapping.")
    kind_raw = payload.get("kind")
    if not isinstance(kind_raw, str):
        fail(f"Runtime '{name}' must define string kind.")
    kind: str = cast(str, kind_raw)
    if kind not in {"host", "container"}:
        fail(f"Runtime '{name}' has invalid kind: {kind}")

    base = payload.get("base")
    if kind == "container" and not isinstance(base, str):
        fail(f"Runtime '{name}' of kind '{kind}' must define 'base'.")

    workdir = payload.get("workdir", "/work")
    if not isinstance(workdir, str):
        fail(f"Runtime '{name}' field 'workdir' must be string.")

    env = payload.get("env", {})
    if not isinstance(env, dict):
        fail(f"Runtime '{name}' field 'env' must be a mapping.")

    return RuntimeSpec(
        name=name,
        kind=kind,
        base=base,
        workdir=workdir,
        env={str(k): str(v) for k, v in env.items()},
    )


def parse_step(payload: str | dict[str, Any]) -> Step:
    if isinstance(payload, str):
        return Step(run=payload, cwd=None, env={}, name=None)

    if not isinstance(payload, dict):
        fail(f"Invalid step entry: {payload}")

    run_raw = payload.get("run")
    if not isinstance(run_raw, str):
        fail(f"Step must define non-empty string 'run': {payload}")
    run: str = cast(str, run_raw)
    if not isinstance(run, str) or not run.strip():
        fail(f"Step must define non-empty string 'run': {payload}")

    cwd = payload.get("cwd")
    if cwd is not None and not isinstance(cwd, str):
        fail(f"Step field 'cwd' must be string: {payload}")

    env = payload.get("env", {})
    if not isinstance(env, dict):
        fail(f"Step field 'env' must be mapping: {payload}")

    name = payload.get("name")
    if name is not None and not isinstance(name, str):
        fail(f"Step field 'name' must be string: {payload}")

    return Step(
        run=run, cwd=cwd, env={str(k): str(v) for k, v in env.items()}, name=name
    )


def parse_validation(payload: Any) -> Validation:
    if not isinstance(payload, dict):
        fail(f"Invalid validate entry: {payload}")

    kind_raw = payload.get("kind")
    if not isinstance(kind_raw, str) or not kind_raw.strip():
        fail(f"Validate entry must define non-empty string 'kind': {payload}")

    name_raw = payload.get("name")
    if not isinstance(name_raw, str) or not name_raw.strip():
        fail(f"Validate entry must define non-empty string 'name': {payload}")

    steps_raw = payload.get("steps")
    if not isinstance(steps_raw, list) or not steps_raw:
        fail(f"Validate entry must define non-empty list 'steps': {payload}")

    kind = cast(str, kind_raw).strip()
    name = cast(str, name_raw).strip()

    return Validation(
        kind=kind,
        name=name,
        steps=[parse_step(step) for step in cast(list[Any], steps_raw)],
    )


def count_job_steps(job: dict[str, Any]) -> int:
    total = 0

    steps_raw = job.get("steps")
    if isinstance(steps_raw, list):
        total += len(steps_raw)

    validate_raw = job.get("validate", [])
    if isinstance(validate_raw, list):
        for entry in validate_raw:
            if isinstance(entry, dict) and isinstance(entry.get("steps"), list):
                total += len(cast(list[Any], entry.get("steps")))

    return total


def candidate_container_clis() -> tuple[str, ...]:
    if sys.platform == "darwin":
        return ("container", "docker", "podman", "nerdctl")
    return CONTAINER_CLI_ORDER


def infer_container_cli() -> str:
    override = os.environ.get("CE_CONTAINER_CLI")
    if override:
        if shutil.which(override):
            return override
        fail(
            f"CE_CONTAINER_CLI is set to '{override}' but command was not found in PATH."
        )

    for cli in candidate_container_clis():
        if shutil.which(cli):
            return cli

    fail(
        "No container runtime CLI found. Install one of: "
        + ", ".join(candidate_container_clis())
        + ", or set CE_CONTAINER_CLI."
    )
    raise AssertionError("unreachable")


def cli_supports_oci_commit(container_cli: str) -> bool:
    return container_cli != "container"


def run_cmd(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    dry_run: bool,
    check: bool = True,
    verbose: bool = False,
    tui: bool = False,
    title: str | None = None,
    on_line: Callable[[str], None] | None = None,
    ignore_cancel: bool = False,
) -> None:
    if is_cancel_requested() and not ignore_cancel:
        raise KeyboardInterrupt
    cmd_preview = " ".join(shlex.quote(x) for x in argv)
    if dry_run:
        print(f"$ {cmd_preview}")
    elif verbose and not tui and on_line is None:
        print(f"$ {cmd_preview}")
    if dry_run:
        return
    proc_env = os.environ.copy()
    proc_env.update(env)

    if on_line is not None:
        captured: list[str] = []
        proc = subprocess.Popen(
            argv,
            cwd=str(cwd),
            env=proc_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        register_running_proc(proc)
        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                if is_cancel_requested():
                    raise KeyboardInterrupt
                clean = line.rstrip("\n")
                captured.append(clean)
                on_line(clean)
            proc.wait()
        except KeyboardInterrupt:
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                proc.wait(timeout=0.5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            raise
        finally:
            unregister_running_proc(proc)
        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode,
                argv,
                output="\n".join(captured),
            )
        return

    if tui and sys.stdout.isatty():
        stream_tui_command(
            argv, cwd=cwd, env=proc_env, check=check, title=title or cmd_preview
        )
        return

    proc = subprocess.run(
        argv,
        cwd=str(cwd),
        env=proc_env,
        check=False,
        capture_output=True,
        text=True,
    )
    if is_cancel_requested() and not ignore_cancel:
        raise KeyboardInterrupt
    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode,
            argv,
            output=proc.stdout,
            stderr=proc.stderr,
        )


def stream_tui_command(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    check: bool,
    title: str,
) -> None:
    console = Console()
    proc = subprocess.Popen(
        argv,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    register_running_proc(proc)

    logs: list[str] = []

    def render_panel(status: str = "running") -> Any:
        max_lines = max(8, console.size.height - 8)
        tail = logs[-max_lines:]
        body = Text("\n".join(tail) if tail else "Waiting for output...")
        title_style = (
            "bold cyan"
            if status == "running"
            else ("bold green" if status == "ok" else "bold red")
        )
        subtitle = Text(f"Status: {status.upper()}", style=title_style)
        return Panel(
            body,
            title=f"[bold]{title}[/bold]",
            subtitle=subtitle,
            border_style=STATUS_BORDER.get(status, "bright_blue"),
        )

    assert proc.stdout is not None
    try:
        with Live(
            render_panel(), console=console, refresh_per_second=12, transient=True
        ) as live:
            for line in proc.stdout:
                logs.append(line.rstrip("\n"))
                live.update(render_panel("running"))

            proc.wait()
            status = "ok" if proc.returncode == 0 else "failed"
            live.update(render_panel(status))
    except KeyboardInterrupt:
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.wait(timeout=0.5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        raise
    finally:
        unregister_running_proc(proc)

    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, argv)


@dataclass
class MultiJobState:
    status: str = "pending"  # pending|copying|running|ok|failed
    step: str = ""
    step_idx: int = 0
    total_steps: int = 0
    step_cmd: str = ""
    last_line: str = ""
    log_path: str = ""
    logs: deque[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.logs is None:
            self.logs = deque(maxlen=3000)


class StateSpinnerColumn(SpinnerColumn):
    def render(self, task: Any) -> Any:
        state = task.fields.get("state")
        if state in {"queued", "pending"}:
            return Text("  ", style="grey35")
        if state == "ok":
            return Text("✓ ", style="bold green")
        return super().render(task)


class StateBarColumn(BarColumn):
    def render(self, task: Any) -> Any:
        if task.fields.get("state") in {"queued", "pending"}:
            total = task.total if task.total is not None else 1
            return ProgressBar(
                total=max(1, int(total)),
                completed=0,
                width=self.bar_width,
                pulse=False,
                style="grey23",
                complete_style="grey35",
                finished_style="grey35",
                pulse_style="grey23",
            )
        return super().render(task)


class MultiJobUI:
    def __init__(
        self,
        jobs: list[str],
        step_counts: dict[str, int],
        job_defs: dict[str, Any],
        *,
        system_arch: str,
        max_parallel: int,
    ) -> None:
        self._lock = threading.Lock()
        self._max_parallel = max_parallel
        self._system_arch = system_arch
        self._states: dict[str, MultiJobState] = {
            j: MultiJobState(total_steps=int(step_counts.get(j, 0) or 0)) for j in jobs
        }

        self._console = Console()
        self._progress = Progress(
            StateSpinnerColumn(),
            TextColumn("{task.description}"),
            StateBarColumn(bar_width=None),
            TextColumn("{task.fields[state]}", justify="right"),
            TextColumn("{task.fields[step]}", justify="right"),
            TimeElapsedColumn(),
            console=self._console,
        )
        self._progress_task_ids: dict[str, Any] = {}
        self._task_numbers: dict[str, int] = {}
        self._focused_job: str | None = jobs[0] if jobs else None
        for idx, j in enumerate(jobs, start=1):
            total = int(step_counts.get(j, 0) or 1)
            self._task_numbers[j] = idx
            job_label = j
            job_def = job_defs.get(j)
            if isinstance(job_def, dict):
                label_job = cast(dict[str, Any], copy.deepcopy(job_def))
                if (
                    not isinstance(label_job.get("arch"), str)
                    or not str(label_job.get("arch")).strip()
                ):
                    label_job["arch"] = self._system_arch
                job_label = format_job_name_for_list(j, label_job)
            self._progress_task_ids[j] = self._progress.add_task(
                f"Task #{idx}: {escape(job_label)}",
                total=total,
                state="queued",
                step=f"0/{total}",
                start=False,
            )

    def set_log_path(self, job: str, path: Path) -> None:
        with self._lock:
            self._states[job].log_path = str(path)

    def get_failed_step_context(self, job: str) -> FailedStepContext:
        with self._lock:
            st = self._states[job]
            return FailedStepContext(
                job_name=job,
                step=st.step,
                step_cmd=st.step_cmd,
            )

    def mark_running(self, job: str) -> None:
        with self._lock:
            st = self._states[job]
            st.status = "running"
            task_id = self._progress_task_ids.get(job)
            if task_id is not None:
                self._progress.update(task_id, state="running")
                try:
                    self._progress.start_task(task_id)
                except Exception:
                    pass

    def mark_copying(self, job: str) -> None:
        with self._lock:
            st = self._states[job]
            st.status = "copying"
            st.step = "copying workspace"
            st.step_cmd = ""
            task_id = self._progress_task_ids.get(job)
            if task_id is not None:
                self._progress.update(task_id, state="copying", step="0/0")
                try:
                    self._progress.start_task(task_id)
                except Exception:
                    pass

    def set_copy_progress(self, job: str, copied_bytes: int, total_bytes: int) -> None:
        with self._lock:
            st = self._states[job]
            st.status = "copying"
            st.step = f"copying {_format_gb(copied_bytes)} / {_format_gb(total_bytes)}"
            st.step_cmd = ""
            task_id = self._progress_task_ids.get(job)
            if task_id is not None:
                total_units = max(1, int(total_bytes))
                done_units = max(0, min(int(copied_bytes), total_units))
                self._progress.update(
                    task_id,
                    total=total_units,
                    completed=done_units,
                    state="copying",
                    step=f"{_format_gb(copied_bytes)} / {_format_gb(total_bytes)}",
                )

    def set_step(self, job: str, idx: int, total: int, title: str, cmd: str) -> None:
        with self._lock:
            st = self._states[job]
            st.status = "running"
            st.step_idx = idx
            st.total_steps = total
            st.step = f"{idx}/{total} {title}" if title else f"{idx}/{total}"
            st.step_cmd = cmd

            task_id = self._progress_task_ids.get(job)
            if task_id is not None:
                self._progress.update(
                    task_id,
                    total=max(1, total),
                    completed=max(0, idx - 1),
                    state="running",
                    step=f"{idx}/{total}",
                )

    def append(self, job: str, line: str) -> None:
        with self._lock:
            st = self._states[job]
            st.last_line = line
            st.logs.append(line)

    def set_done(self, job: str, ok: bool) -> None:
        with self._lock:
            st = self._states[job]
            st.status = "ok" if ok else "failed"
            task_id = self._progress_task_ids.get(job)
            if task_id is not None:
                total = int(st.total_steps or 1)
                self._progress.update(
                    task_id,
                    completed=total,
                    state=("ok" if ok else "failed"),
                    step=f"{total}/{total}",
                )

    def focus_prev(self) -> None:
        with self._lock:
            jobs = list(self._task_numbers.keys())
            if not jobs:
                return
            current = self._focused_job if self._focused_job in jobs else jobs[0]
            idx = jobs.index(current)
            self._focused_job = jobs[(idx - 1) % len(jobs)]

    def focus_next(self) -> None:
        with self._lock:
            jobs = list(self._task_numbers.keys())
            if not jobs:
                return
            current = self._focused_job if self._focused_job in jobs else jobs[0]
            idx = jobs.index(current)
            self._focused_job = jobs[(idx + 1) % len(jobs)]

    def mark_cancelled(self) -> None:
        with self._lock:
            for job, st in self._states.items():
                if st.status in {"ok", "failed"}:
                    continue
                st.status = "failed"
                st.step = (st.step + " cancelled").strip() if st.step else "cancelled"
                task_id = self._progress_task_ids.get(job)
                if task_id is not None:
                    self._progress.update(task_id, state="failed")

    def render(self) -> Any:
        _cols, rows = shutil.get_terminal_size((120, 40))
        header_h = 3
        # Bottom panel should hug the visible progress rows (no extra blank line).
        progress_limit = max(1, len(self._states))
        visible_progress_rows = max(1, min(progress_limit, len(self._states)))
        bottom_h = visible_progress_rows + 2  # panel borders
        bottom_h = min(bottom_h, max(3, rows - header_h - 8))
        bottom_h = max(3, bottom_h)
        # Fill the remaining height; avoid leaving a stray blank row between cards and progress.
        logs_h = max(8, rows - header_h - bottom_h)

        with self._lock:
            items = list(self._states.items())

        header = Panel(
            Text("Offworld CI", style="bold white", justify="center"),
            box=box.ROUNDED,
            padding=(0, 1),
            border_style="grey27",
        )

        # Show progress rows for every job in the bottom panel.
        limit = progress_limit
        by_status: dict[str, list[str]] = {
            "running": [],
            "copying": [],
            "pending": [],
            "failed": [],
            "ok": [],
        }
        for job, st in items:
            by_status.setdefault(st.status, []).append(job)

        ordered_for_progress = (
            by_status["running"]
            + by_status["copying"]
            + by_status["pending"]
            + by_status["failed"]
            + by_status["ok"]
        )
        visible_jobs = set(ordered_for_progress[:limit])
        for job, task_id in self._progress_task_ids.items():
            self._progress.update(task_id, visible=(job in visible_jobs))

        # Keep header unchanged; parallel details live in progress + cards.

        def border_for(status: str) -> str:
            return STATUS_BORDER.get(status, "bright_blue")

        show_log_nav = len(items) > 1
        panels_by_job: dict[str, Any] = {}
        # Keep deterministic visual order using task number.
        ordered_items = sorted(
            items, key=lambda x: self._task_numbers.get(x[0], 10_000)
        )
        for job, st in ordered_items:
            reserved = 2
            tail_lines = max(0, logs_h - reserved)
            tail = list(st.logs)[-tail_lines:]
            if tail_lines and len(tail) < tail_lines:
                tail = ([""] * (tail_lines - len(tail))) + tail

            body = Text()
            cmd_line = (st.step_cmd or "").strip()
            if cmd_line:
                body.append(f"$ {cmd_line}", style="bold cyan")
            else:
                body.append(" ")
            body.append("\n\n")
            if tail:
                body.append("\n".join(tail))

            status_label = "queued" if st.status == "pending" else st.status
            step_text = (st.step or "").strip()
            if step_text.lower().startswith(status_label.lower()):
                subtitle_text = step_text
            else:
                subtitle_text = " ".join([p for p in [status_label, step_text] if p])
            subtitle = Text(
                subtitle_text,
                style=STATUS_SUBTITLE_STYLE.get(status_label, "dim"),
            )

            card_title = f"Task #{self._task_numbers.get(job, 0)}"
            if show_log_nav:
                card_title = f"< {card_title} >"

            panels_by_job[job] = Panel(
                body,
                title=f"[bold]{card_title}[/bold]",
                subtitle=subtitle,
                border_style=border_for(st.status),
            )

        # Show a single live log card. Prefer running jobs, then queued, then failed/ok.
        priority = (
            by_status["running"]
            + by_status["copying"]
            + by_status["pending"]
            + by_status["failed"]
            + by_status["ok"]
        )
        focus_job = self._focused_job if self._focused_job in panels_by_job else None
        if focus_job is None:
            focus_job = next((j for j in priority if j in panels_by_job), None)
        if focus_job is None and panels_by_job:
            focus_job = next(iter(panels_by_job))
        cards: Any = panels_by_job[focus_job] if focus_job is not None else Panel("")

        bottom = Panel(
            self._progress,
            box=box.ROUNDED,
            padding=(0, 1),
            border_style="grey27",
        )

        root = Layout(name="root")
        root.split_column(
            Layout(header, name="header", size=header_h),
            Layout(cards, name="cards"),
            Layout(bottom, name="bottom", size=bottom_h),
        )
        return root

    def render_progress_only(
        self,
        *,
        elapsed_seconds: float | None = None,
        artifact_count: int | None = None,
        artifact_bytes: int | None = None,
    ) -> Any:
        progress_panel = Panel(
            self._progress,
            box=box.ROUNDED,
            padding=(0, 1),
            border_style="grey27",
            title="[bold]Progress[/bold]",
        )
        if elapsed_seconds is None:
            return progress_panel
        total_time_panel = Panel(
            Text(
                format_elapsed_duration(elapsed_seconds),
                style="bold cyan",
                justify="center",
            ),
            box=box.ROUNDED,
            padding=(0, 0),
            height=3,
            border_style="grey27",
            title="[bold]Total Time[/bold]",
        )
        artifact_count_panel = Panel(
            Text(str(artifact_count or 0), style="bold cyan", justify="center"),
            box=box.ROUNDED,
            padding=(0, 0),
            height=3,
            border_style="grey27",
            title="[bold]Artifacts[/bold]",
        )
        artifact_size_panel = Panel(
            Text(_format_gb(artifact_bytes or 0), style="bold cyan", justify="center"),
            box=box.ROUNDED,
            padding=(0, 0),
            height=3,
            border_style="grey27",
            title="[bold]Artifact Size[/bold]",
        )
        summary_row = Table.grid(expand=True)
        summary_row.add_column(ratio=1)
        summary_row.add_column(ratio=1)
        summary_row.add_column(ratio=1)
        summary_row.add_row(total_time_panel, artifact_count_panel, artifact_size_panel)
        return Group(progress_panel, summary_row)


def make_on_line(
    ui: MultiJobUI, job: str, log_file: Any, step_title: str
) -> Callable[[str], None]:
    def _on_line(line: str) -> None:
        ui.append(job, line)
        log_file.write(f"[{step_title}] {line}\n")

    return _on_line


def build_failure_panel(
    *,
    cmd: str,
    output: str,
    title: str = "Build failed",
    task: str | None = None,
    step: str | None = None,
    failure_line: str | None = None,
) -> Any:
    body = Text()
    body.append(f"$ {cmd}", style="bold cyan")
    body.append("\n")
    body.append(output if output.strip() else "(no command output captured)")
    if failure_line:
        body.append("\n\n")
        body.append(failure_line, style="bold red")

    subtitle_parts: list[str] = []
    if task:
        subtitle_parts.append(f"Task: {task}")
    if step:
        subtitle_parts.append(f"Step: {step}")

    return Panel(
        body,
        title=title,
        subtitle=Text(" | ".join(subtitle_parts), style="dim red")
        if subtitle_parts
        else None,
        border_style="red",
    )


def build_internal_error_panel(message: str) -> Any:
    return Panel(
        message,
        title="Internal error",
        subtitle=Text("Offworld failed before/during orchestration", style="dim red"),
        border_style="red",
    )


def collect_job_artifacts(
    pipeline: dict[str, Any], job_name: str, from_ws: Path, root_ws: Path
) -> tuple[int, int]:
    job = pipeline["jobs"].get(job_name)
    if not isinstance(job, dict):
        return 0, 0
    artifacts = job.get("artifacts", [])
    if not isinstance(artifacts, list):
        return 0, 0

    collected_count = 0
    collected_bytes = 0

    def copy_path(rel: str) -> None:
        nonlocal collected_count, collected_bytes
        src = from_ws / rel
        dst = root_ws / rel
        if not src.exists():
            return
        collected_count += 1
        collected_bytes += path_size_bytes(src)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src, dst, symlinks=True, dirs_exist_ok=True)
            return
        try:
            os.link(src, dst)
        except Exception:
            shutil.copy2(src, dst)

    for a in artifacts:
        if not isinstance(a, dict):
            continue
        k = a.get("kind")
        if k in {"archive", "file"}:
            p = a.get("path")
            if isinstance(p, str):
                copy_path(p)
        elif k == "report":
            paths = a.get("paths")
            if isinstance(paths, list):
                for p in paths:
                    if isinstance(p, str):
                        copy_path(p)
        elif k == "oci":
            pub = a.get("publish")
            if isinstance(pub, dict):
                save = pub.get("save")
                if isinstance(save, str):
                    copy_path(save)

    return collected_count, collected_bytes


def copytree_filtered(src: Path, dst: Path) -> None:
    copytree_filtered_with_progress(src, dst, on_progress=None)


def _format_gb(nbytes: int) -> str:
    return f"{(max(0, nbytes) / (1024**3)):.2f} GB"


def copytree_filtered_with_progress(
    src: Path,
    dst: Path,
    *,
    on_progress: Callable[[int, int], None] | None,
) -> None:
    src_resolved = src.resolve()

    if is_cancel_requested():
        raise KeyboardInterrupt

    static_skip = {
        STATE_DIR_NAME,
        ".dist",
        "dist",
        "build",
        "build-static",
        "build-shared",
        "build-wasm",
    }

    git_ignored: set[str] = set()
    if shutil.which("git"):
        try:
            proc = subprocess.run(
                [
                    "git",
                    "-C",
                    str(src_resolved),
                    "ls-files",
                    "--others",
                    "-i",
                    "--exclude-standard",
                    "--directory",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if proc.returncode == 0:
                for line in (proc.stdout or "").splitlines():
                    item = line.strip().strip("/")
                    if item:
                        git_ignored.add(item)
        except Exception:
            pass

    def is_ignored(rel_posix: str) -> bool:
        rel_posix = rel_posix.strip("/")
        if not rel_posix:
            return False
        head = rel_posix.split("/", 1)[0]
        if head in static_skip:
            return True
        for ign in git_ignored:
            if rel_posix == ign or rel_posix.startswith(ign + "/"):
                return True
        return False

    files: list[tuple[Path, Path, int]] = []

    for root, dirs, names in os.walk(src_resolved, topdown=True, followlinks=False):
        if is_cancel_requested():
            raise KeyboardInterrupt
        root_path = Path(root)
        try:
            root_rel = root_path.relative_to(src_resolved)
        except Exception:
            continue

        kept_dirs: list[str] = []
        for d in dirs:
            rel = (root_rel / d).as_posix() if str(root_rel) != "." else d
            if is_ignored(rel):
                continue
            kept_dirs.append(d)
        dirs[:] = kept_dirs

        for n in names:
            rel = (root_rel / n).as_posix() if str(root_rel) != "." else n
            if is_ignored(rel):
                continue
            s = src_resolved / rel
            d = dst / rel
            try:
                size = 0 if s.is_symlink() else int(s.stat().st_size)
            except Exception:
                size = 0
            files.append((s, d, size))

    total_bytes = sum(sz for _, _, sz in files)
    copied_bytes = 0
    if on_progress is not None:
        on_progress(copied_bytes, total_bytes)

    for s, d, sz in files:
        if is_cancel_requested():
            raise KeyboardInterrupt
        d.parent.mkdir(parents=True, exist_ok=True)
        if s.is_symlink():
            try:
                if d.exists() or d.is_symlink():
                    d.unlink()
            except Exception:
                pass
            target = os.readlink(s)
            os.symlink(target, d)
        else:
            shutil.copy2(s, d)
        copied_bytes += sz
        if on_progress is not None:
            on_progress(copied_bytes, total_bytes)


def probe_container_cli_ready(
    container_cli: str, workspace: Path, dry_run: bool
) -> str | None:
    if dry_run:
        return None

    probe_cmd = (
        [container_cli, "system", "status"]
        if container_cli == "container"
        else [container_cli, "info"]
    )

    proc = subprocess.run(
        probe_cmd,
        cwd=str(workspace),
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        return None

    details = (proc.stderr or proc.stdout or "").strip()
    if not details:
        details = "container runtime not reachable"
    return details


def infer_ready_container_cli(
    workspace: Path, dry_run: bool, require_oci_commit: bool
) -> str:
    override = os.environ.get("CE_CONTAINER_CLI")
    if override:
        cli = infer_container_cli()
        if require_oci_commit and not cli_supports_oci_commit(cli):
            fail(
                "Selected container CLI does not support commit-based OCI publishing: "
                f"{cli}. Use docker/podman/nerdctl for this job."
            )
        reason = probe_container_cli_ready(cli, workspace, dry_run)
        if reason:
            fail(f"Container runtime '{cli}' is installed but unavailable. {reason}")
        return cli

    unavailable: list[str] = []
    for cli in candidate_container_clis():
        if not shutil.which(cli):
            continue
        if require_oci_commit and not cli_supports_oci_commit(cli):
            continue
        reason = probe_container_cli_ready(cli, workspace, dry_run)
        if not reason:
            return cli
        unavailable.append(f"{cli}: {reason}")

    fail(
        "No usable container runtime was found. Checked: "
        + ", ".join(candidate_container_clis())
        + (f". Details: {'; '.join(unavailable)}" if unavailable else "")
    )
    raise AssertionError("unreachable")


def build_shell_command(command: str, cwd: str | None, base_workdir: str) -> str:
    if cwd:
        if cwd.startswith("/"):
            target = cwd
        else:
            target = posixpath.normpath(posixpath.join(base_workdir, cwd))
        return f"cd {shlex.quote(target)} && {command}"
    return command


def ensure_parent(path: Path, dry_run: bool) -> None:
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)


def sanitize_container_name(name: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_.-]", "-", name)
    clean = clean.strip("-.")
    if not clean:
        clean = "job"
    return clean.lower()


def resolve_workspace_path(workspace: Path, item: str) -> Path:
    p = Path(item)
    if p.is_absolute():
        return p
    return workspace / p


def handle_artifacts(
    artifacts: list[dict[str, Any]],
    *,
    workspace: Path,
    runtime: RuntimeSpec,
    container_cli: str | None,
    container_name: str | None,
    dry_run: bool,
    emit: bool,
) -> None:
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            fail(f"Artifact entry must be a mapping: {artifact}")

        kind = artifact.get("kind")
        if kind == "archive" or kind == "file":
            path_raw = artifact.get("path")
            if not isinstance(path_raw, str):
                fail(f"Artifact '{kind}' requires string 'path'.")
            path_str: str = cast(str, path_raw)
            full = resolve_workspace_path(workspace, path_str)
            if emit:
                print(f"Verifying artifact path: {full}")
            if not dry_run and not full.exists():
                fail(f"Artifact not found: {full}")

        elif kind == "report":
            paths_raw = artifact.get("paths")
            if not isinstance(paths_raw, list) or not paths_raw:
                fail("Artifact 'report' requires non-empty 'paths' list.")
            for p in cast(list[Any], paths_raw):
                if not isinstance(p, str):
                    fail(f"Report artifact path must be string: {p}")
                full = resolve_workspace_path(workspace, p)
                if emit:
                    print(f"Verifying report path: {full}")
                if not dry_run and not full.exists():
                    fail(f"Report file not found: {full}")

        elif kind == "oci":
            if runtime.kind != "container":
                fail("Artifact kind 'oci' can only be used with container runtimes.")
            if not container_cli or not container_name:
                fail("Internal error: missing container context for oci artifact.")
            container_cli_s: str = cast(str, container_cli)
            container_name_s: str = cast(str, container_name)

            publish = artifact.get("publish", {})
            if not isinstance(publish, dict):
                fail("Artifact kind 'oci' field 'publish' must be mapping.")

            tag = publish.get("tag")
            if not isinstance(tag, str) or not tag:
                fail("Artifact kind 'oci' requires publish.tag.")
            tag_s: str = cast(str, tag)

            if container_cli_s == "container":
                fail(
                    "Artifact kind 'oci' publish is not "
                    "supported by Apple's 'container' CLI yet (no commit command). "
                    "Use docker/podman for this job."
                )

            run_cmd(
                [container_cli_s, "commit", container_name_s, tag_s],
                cwd=workspace,
                env={},
                dry_run=dry_run,
            )

            save_raw = publish.get("save")
            if save_raw is not None:
                if not isinstance(save_raw, str) or not save_raw:
                    fail("Artifact kind 'oci' publish.save must be a non-empty string.")
                save_path = resolve_workspace_path(workspace, save_raw)
                ensure_parent(save_path, dry_run)
                run_cmd(
                    [container_cli_s, "save", "-o", str(save_path), tag_s],
                    cwd=workspace,
                    env={},
                    dry_run=dry_run,
                )

        elif kind == "vm-template":
            fail("Artifact kind 'vm-template' is not implemented yet.")

        else:
            fail(f"Unknown artifact kind: {kind}")


class RuntimeRunner:
    def __init__(
        self,
        *,
        job_name: str,
        runtime: RuntimeSpec,
        workspace: Path,
        job_env: dict[str, Any],
        dry_run: bool,
        verbose: bool,
        emit: bool,
        ui: MultiJobUI | None,
        total_steps: int,
    ) -> None:
        self.job_name = job_name
        self.runtime = runtime
        self.workspace = workspace
        self.job_env = {str(k): str(v) for k, v in job_env.items()}
        self.dry_run = dry_run
        self.verbose = verbose
        self.emit = emit
        self.ui = ui
        self.total_steps = total_steps

    def prepare(self, artifacts: list[dict[str, Any]]) -> None:
        _ = artifacts

    def run_step(
        self,
        *,
        step: Step,
        title: str,
        on_line: Callable[[str], None] | None,
    ) -> None:
        raise NotImplementedError

    def finalize_artifacts(self, artifacts: list[dict[str, Any]]) -> None:
        handle_artifacts(
            artifacts,
            workspace=self.workspace,
            runtime=self.runtime,
            container_cli=None,
            container_name=None,
            dry_run=self.dry_run,
            emit=self.emit,
        )

    def cleanup(self) -> None:
        return

    def merged_step_env(self, step: Step) -> dict[str, str]:
        env: dict[str, str] = {}
        env.update(self.runtime.env)
        env.update(self.job_env)
        env.update(step.env)
        return env


class HostRunner(RuntimeRunner):
    def run_step(
        self,
        *,
        step: Step,
        title: str,
        on_line: Callable[[str], None] | None,
    ) -> None:
        host_cwd = (
            resolve_workspace_path(self.workspace, step.cwd)
            if step.cwd
            else self.workspace
        )
        run_cmd(
            ["bash", "-lc", step.run],
            cwd=host_cwd,
            env=self.merged_step_env(step),
            dry_run=self.dry_run,
            verbose=self.verbose,
            tui=False,
            title=title,
            on_line=on_line,
        )


class ContainerEngine:
    def __init__(self, cli: str) -> None:
        self.cli = cli

    def create(
        self,
        *,
        workspace: Path,
        name: str,
        workdir: str,
        base: str,
        dry_run: bool,
        verbose: bool,
    ) -> None:
        run_cmd(
            [
                self.cli,
                "create",
                "--name",
                name,
                "-v",
                f"{workspace}:{workdir}",
                "-w",
                workdir,
                base,
                "sleep",
                "infinity",
            ],
            cwd=workspace,
            env={},
            dry_run=dry_run,
            verbose=verbose,
            tui=False,
        )

    def start(
        self, *, workspace: Path, name: str, dry_run: bool, verbose: bool
    ) -> None:
        run_cmd(
            [self.cli, "start", name],
            cwd=workspace,
            env={},
            dry_run=dry_run,
            verbose=verbose,
            tui=False,
        )

    def exec_step(
        self,
        *,
        workspace: Path,
        name: str,
        workdir: str,
        step: Step,
        env: dict[str, str],
        dry_run: bool,
        verbose: bool,
        title: str,
        on_line: Callable[[str], None] | None,
    ) -> None:
        cmd = [self.cli, "exec"]
        for key, val in env.items():
            cmd.extend(["-e", f"{key}={val}"])
        cmd.extend(
            [
                name,
                "bash",
                "-lc",
                build_shell_command(step.run, step.cwd, workdir),
            ]
        )
        run_cmd(
            cmd,
            cwd=workspace,
            env={},
            dry_run=dry_run,
            verbose=verbose,
            tui=False,
            title=title,
            on_line=on_line,
        )

    def remove(
        self, *, workspace: Path, name: str, dry_run: bool, verbose: bool
    ) -> None:
        run_cmd(
            [self.cli, "rm", "-f", name],
            cwd=workspace,
            env={},
            dry_run=dry_run,
            check=False,
            verbose=verbose,
            tui=False,
            ignore_cancel=True,
        )


class ContainerRunner(RuntimeRunner):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.engine: ContainerEngine | None = None
        self.container_name: str | None = None

    def prepare(self, artifacts: list[dict[str, Any]]) -> None:
        require_oci_commit = any(
            isinstance(a, dict) and a.get("kind") == "oci" for a in artifacts
        )
        container_cli = infer_ready_container_cli(
            self.workspace, self.dry_run, require_oci_commit
        )
        self.engine = ContainerEngine(container_cli)
        if self.emit:
            print(f"Container CLI inferred: {container_cli}")

        if self.runtime.base is None:
            fail(
                f"Runtime '{self.runtime.name}' must define a base for container jobs."
            )

        self.container_name = f"cyberether-build-{sanitize_container_name(self.job_name)}-{uuid.uuid4().hex[:8]}"
        self.engine.create(
            workspace=self.workspace,
            name=self.container_name,
            workdir=self.runtime.workdir,
            base=cast(str, self.runtime.base),
            dry_run=self.dry_run,
            verbose=self.verbose,
        )
        self.engine.start(
            workspace=self.workspace,
            name=self.container_name,
            dry_run=self.dry_run,
            verbose=self.verbose,
        )

    def run_step(
        self,
        *,
        step: Step,
        title: str,
        on_line: Callable[[str], None] | None,
    ) -> None:
        if self.engine is None or self.container_name is None:
            fail("Internal error: container runner not prepared.")
        assert self.engine is not None and self.container_name is not None
        self.engine.exec_step(
            workspace=self.workspace,
            name=self.container_name,
            workdir=self.runtime.workdir,
            step=step,
            env=self.merged_step_env(step),
            dry_run=self.dry_run,
            verbose=self.verbose,
            title=title,
            on_line=on_line,
        )

    def finalize_artifacts(self, artifacts: list[dict[str, Any]]) -> None:
        handle_artifacts(
            artifacts,
            workspace=self.workspace,
            runtime=self.runtime,
            container_cli=(self.engine.cli if self.engine is not None else None),
            container_name=self.container_name,
            dry_run=self.dry_run,
            emit=self.emit,
        )

    def cleanup(self) -> None:
        if self.engine is None or self.container_name is None:
            return
        try:
            self.engine.remove(
                workspace=self.workspace,
                name=self.container_name,
                dry_run=self.dry_run,
                verbose=self.verbose,
            )
        except Exception:
            pass


def make_runtime_runner(
    *,
    job_name: str,
    runtime: RuntimeSpec,
    workspace: Path,
    job_env: dict[str, Any],
    dry_run: bool,
    verbose: bool,
    emit: bool,
    ui: MultiJobUI | None,
    total_steps: int,
) -> RuntimeRunner:
    kwargs: dict[str, Any] = {
        "job_name": job_name,
        "runtime": runtime,
        "workspace": workspace,
        "job_env": job_env,
        "dry_run": dry_run,
        "verbose": verbose,
        "emit": emit,
        "ui": ui,
        "total_steps": total_steps,
    }
    if runtime.kind == "host":
        return HostRunner(**kwargs)
    if runtime.kind == "container":
        return ContainerRunner(**kwargs)
    fail(f"Unsupported runtime kind: {runtime.kind}")
    raise AssertionError("unreachable")


def run_job(
    *,
    pipeline: dict[str, Any],
    job_name: str,
    workspace: Path,
    dry_run: bool,
    verbose: bool,
    ui: MultiJobUI | None = None,
    log_file: TextIO | None = None,
) -> None:
    jobs_raw = pipeline["jobs"]

    job = jobs_raw.get(job_name)
    if not isinstance(job, dict):
        fail(f"Unknown job: {job_name}")

    runtime_ref = job.get("runtime")
    if isinstance(runtime_ref, str):
        runtimes_raw = pipeline.get("runtimes", {})
        runtime_payload = (
            runtimes_raw.get(runtime_ref) if isinstance(runtimes_raw, dict) else None
        )
        if not isinstance(runtime_payload, dict):
            fail(f"Runtime '{runtime_ref}' is not defined for job '{job_name}'.")
        runtime = parse_runtime(
            cast(str, runtime_ref), cast(dict[str, Any], runtime_payload)
        )
    else:
        runtime = parse_runtime(job_name, job)
    job_env = job.get("env", {})
    if not isinstance(job_env, dict):
        fail(f"Job '{job_name}' field 'env' must be mapping.")

    steps_raw = job.get("steps")
    if not isinstance(steps_raw, list) or not steps_raw:
        fail(f"Job '{job_name}' must define a non-empty steps list.")
    steps = [parse_step(s) for s in cast(list[Any], steps_raw)]

    validate_raw = job.get("validate", [])
    if not isinstance(validate_raw, list):
        fail(f"Job '{job_name}' field 'validate' must be list.")
    validations = [parse_validation(v) for v in cast(list[Any], validate_raw)]

    artifacts_raw = job.get("artifacts", [])
    if not isinstance(artifacts_raw, list):
        fail(f"Job '{job_name}' field 'artifacts' must be list.")

    emit = ui is None

    if emit:
        print(f"Running job: {job_name}")
        print(f"Runtime: {runtime.name} ({runtime.kind})")

    total_steps = len(steps) + sum(len(validation.steps) for validation in validations)

    runner = make_runtime_runner(
        job_name=job_name,
        runtime=runtime,
        workspace=workspace,
        job_env=cast(dict[str, Any], job_env),
        dry_run=dry_run,
        verbose=verbose,
        emit=emit,
        ui=ui,
        total_steps=total_steps,
    )

    try:
        runner.prepare(cast(list[dict[str, Any]], artifacts_raw))
        for idx, step in enumerate(steps, start=1):
            if is_cancel_requested():
                raise KeyboardInterrupt
            title = step.name or step.run
            if emit:
                print(f"[{idx}/{total_steps}] {title}")
            if ui is not None:
                ui.set_step(job_name, idx, total_steps, step.name or "", step.run)

            on_line: Callable[[str], None] | None = None
            if ui is not None and log_file is not None:
                on_line = make_on_line(ui, job_name, log_file, title)

            runner.run_step(step=step, title=title, on_line=on_line)

        step_idx = len(steps)
        for validation in validations:
            for validation_step in validation.steps:
                if is_cancel_requested():
                    raise KeyboardInterrupt
                step_idx += 1
                title = validation_step.name or validation_step.run
                if emit:
                    print(f"[{step_idx}/{total_steps}] {title}")
                if ui is not None:
                    ui.set_step(
                        job_name,
                        step_idx,
                        total_steps,
                        validation_step.name or "",
                        validation_step.run,
                    )

                on_line = None
                if ui is not None and log_file is not None:
                    on_line = make_on_line(ui, job_name, log_file, title)

                runner.run_step(step=validation_step, title=title, on_line=on_line)

        runner.finalize_artifacts(cast(list[dict[str, Any]], artifacts_raw))
    finally:
        runner.cleanup()


def select_jobs_for_local_arch(
    jobs: list[str], pipeline_jobs: dict[str, Any], system_arch: str
) -> tuple[list[str], list[tuple[str, str]]]:
    selected: list[str] = []
    skipped: list[tuple[str, str]] = []

    for job_name in jobs:
        job = pipeline_jobs.get(job_name)
        if not isinstance(job, dict):
            continue
        raw_arch = job.get("arch")
        if raw_arch is None:
            selected.append(job_name)
            continue
        if not isinstance(raw_arch, str) or not raw_arch.strip():
            fail(f"Job '{job_name}' field 'arch' must be a non-empty string.")
        job_arch = canonical_architecture_id(raw_arch)
        if job_arch == system_arch:
            selected.append(job_name)
            continue
        skipped.append((job_name, job_arch))

    return selected, skipped


def format_job_name_for_list(job_name: str, job: dict[str, Any]) -> str:
    raw_arch = job.get("arch")
    if not isinstance(raw_arch, str) or not raw_arch.strip() or "[" in job_name:
        return job_name
    arch = canonical_architecture_id(raw_arch)
    if ":" in job_name:
        head, tail = job_name.split(":", 1)
        return f"{head}[{arch}]:{tail}"
    return f"{job_name}[{arch}]"


def group_job_names_for_list(pipeline_jobs: dict[str, Any]) -> list[str]:
    grouped: dict[tuple[str, str], list[str]] = {}
    passthrough: list[str] = []

    for name in sorted(pipeline_jobs.keys()):
        if name.startswith(".") or name.startswith("_"):
            continue
        match = re.match(r"^(.*)\[([^\]]+)\](?::(.*))?$", name)
        if not match:
            job = pipeline_jobs.get(name)
            passthrough.append(
                format_job_name_for_list(name, cast(dict[str, Any], job))
                if isinstance(job, dict)
                else name
            )
            continue

        prefix = match.group(1)
        arch = match.group(2)
        suffix = match.group(3) or ""
        grouped.setdefault((prefix, suffix), []).append(arch)

    collapsed = [
        f"{prefix}[{','.join(sorted(arches))}]" + (f":{suffix}" if suffix else "")
        for (prefix, suffix), arches in sorted(grouped.items())
    ]
    return sorted(passthrough + collapsed)


def command_list(pipeline: dict[str, Any]) -> None:
    public_jobs = group_job_names_for_list(cast(dict[str, Any], pipeline["jobs"]))
    console = Console()

    console.print(
        f"[bold]Available Architectures[/bold]\n- [cyan]{current_system_architecture()}[/cyan]\n"
    )
    console.print("[bold]Available Jobs[/bold]")
    for name in public_jobs:
        console.print(Text(f"- {name}", style="cyan"))

    console.print("\n[bold]Groups[/bold]")
    console.print("- [magenta]all[/magenta]")
    console.print("- [magenta]runtime[/magenta]")
    console.print("- [magenta]static[/magenta]")
    console.print("- [magenta]test[/magenta]")
    console.print("- [magenta]all:runtime[/magenta]")
    console.print("- [magenta]all:static[/magenta]")
    console.print("- [magenta]all:test[/magenta]")


def expand_job_selectors(selectors: list[str], all_jobs: list[str]) -> list[str]:
    job_set = set(all_jobs)

    def by_prefix(prefix: str) -> list[str]:
        p = prefix.rstrip(":")
        return sorted(
            [j for j in all_jobs if j.startswith(p + ":") or j.startswith(p + "[")]
        )

    def by_segment(segment: str) -> list[str]:
        s = segment.strip().lower()
        return sorted(
            [j for j in all_jobs if s in [part.lower() for part in j.split(":")]]
        )

    out: list[str] = []
    for sel in selectors:
        if sel in job_set:
            out.append(sel)
            continue

        normalized = sel.strip().lower()
        if normalized == "all":
            out.extend(
                [j for j in all_jobs if not j.startswith(".") and not j.startswith("_")]
            )
            continue

        if normalized in {"test", "tests", "test:all", "all:test", "all:tests"}:
            out.extend(by_segment("test"))
            continue

        if normalized in {"static", "static:all", "all:static"}:
            out.extend(by_segment("static"))
            continue

        if normalized in {"runtime", "runtime:all", "all:runtime"}:
            out.extend(by_segment("runtime"))
            continue

        if normalized.startswith("all:"):
            target = normalized.split(":", 1)[1]
            pref = by_prefix(target)
            out.extend(pref if pref else by_segment(target))
            continue

        pref = by_prefix(normalized)
        if pref:
            out.extend(pref)
            continue

        fail(f"Unknown job: {sel}")

    seen: set[str] = set()
    final: list[str] = []
    for j in out:
        if j in seen:
            continue
        seen.add(j)
        final.append(j)
    return final


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offworld CI runtime build runner.")
    parser.add_argument(
        "--pipeline",
        default="ci.yml",
        help="Path to pipeline YAML file (default: ci.yml)",
    )

    subparsers = parser.add_subparsers(dest="command", required=False)
    subparsers.add_parser("list", help="List available jobs")

    run_parser = subparsers.add_parser("run", help="Run one or more jobs")
    run_parser.add_argument("jobs", nargs="+", help="Job name(s)")
    run_parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Max concurrent jobs (default: 1)",
    )
    run_parser.add_argument(
        "--dry-run", action="store_true", help="Print commands without executing"
    )
    run_parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce output to step progress and final errors",
    )
    run_parser.add_argument(
        "--repo",
        help="Git URL to clone into .build/repos before running",
    )

    cleanup_parser = subparsers.add_parser(
        "cleanup",
        help="Remove local build scratch directories",
    )
    cleanup_parser.add_argument(
        "--logs",
        action="store_true",
        help="Also remove .dist/logs",
    )
    cleanup_parser.add_argument(
        "--all",
        action="store_true",
        help="Remove all .build state and .dist/logs",
    )

    return parser


def command_cleanup(
    *, root_workspace: Path, include_logs: bool, all_state: bool
) -> None:
    removed: list[Path] = []

    if all_state:
        targets = [root_workspace / STATE_DIR_NAME]
        include_logs = True
    else:
        targets = [root_workspace / STATE_DIR_NAME / "workspaces"]

    if include_logs:
        targets.append(root_workspace / ".dist" / "logs")

    for p in targets:
        if not p.exists():
            continue
        try:
            shutil.rmtree(p, ignore_errors=False)
            removed.append(p)
        except FileNotFoundError:
            continue
        except Exception as exc:
            fail(f"Failed to remove {p}: {exc}")

    if removed:
        for p in removed:
            print(f"Removed: {p}")
    else:
        print("Nothing to clean.")


class LiveUIRefresher:
    def __init__(self, live: Any, ui: MultiJobUI) -> None:
        self._live = live
        self._ui = ui
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._thread.join(timeout=1)
        except Exception:
            pass

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._live.update(self._ui.render())
            except Exception:
                pass
            time.sleep(0.05)


class LiveUIKeyListener:
    def __init__(self, ui: MultiJobUI) -> None:
        self._ui = ui
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        if not sys.stdin.isatty():
            return
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._thread.join(timeout=1)
        except Exception:
            pass

    def _run(self) -> None:
        if not sys.stdin.isatty():
            return
        fd = sys.stdin.fileno()
        try:
            old_settings = termios.tcgetattr(fd)
        except Exception:
            return

        try:
            tty.setcbreak(fd)
            while not self._stop.is_set():
                try:
                    ready, _, _ = select.select([sys.stdin], [], [], 0.1)
                except Exception:
                    break
                if not ready:
                    continue
                try:
                    chunk = os.read(fd, 1)
                except Exception:
                    break
                if chunk == b"\x1b":
                    try:
                        ready, _, _ = select.select([sys.stdin], [], [], 0.01)
                    except Exception:
                        ready = []
                    if ready:
                        try:
                            chunk += os.read(fd, 2)
                        except Exception:
                            pass
                if chunk in {b"\x1b[D", b"<"}:
                    self._ui.focus_prev()
                elif chunk in {b"\x1b[C", b">"}:
                    self._ui.focus_next()
                elif chunk == b",":
                    self._ui.focus_prev()
                elif chunk == b".":
                    self._ui.focus_next()
        finally:
            try:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            except Exception:
                pass


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    pipeline_arg = Path(cast(str, args.pipeline))
    pipeline_path = pipeline_arg.resolve()
    root_workspace = pipeline_path.parent.resolve()
    if (
        not (root_workspace / ".git").exists()
        and (root_workspace.parent / ".git").exists()
    ):
        root_workspace = root_workspace.parent.resolve()

    if args.command == "cleanup":
        command_cleanup(
            root_workspace=root_workspace,
            include_logs=bool(getattr(args, "logs", False)),
            all_state=bool(getattr(args, "all", False)),
        )
        return

    if args.command in {None, "list"}:
        pipeline = load_pipeline(pipeline_path)
        command_list(pipeline)
        return

    if args.command == "run":
        _CANCEL_NOTICE.clear()
        clear_cancel()
        quiet = bool(getattr(args, "quiet", False))
        verbose = not quiet
        selectors = list(cast(list[str], getattr(args, "jobs")))

        repo_url = cast(str | None, getattr(args, "repo", None))
        if selectors and looks_like_git_url(selectors[0]):
            if repo_url is None:
                repo_url = selectors.pop(0)
            else:
                fail(
                    "Specify repository URL only once (either --repo or first selector)."
                )
        if not selectors:
            fail("No job selectors provided.")

        run_root_workspace = root_workspace
        run_pipeline_path = pipeline_path

        if repo_url is not None:
            try:
                run_root_workspace = clone_repo_into_build(
                    repo_url=repo_url,
                    host_root=root_workspace,
                    verbose=verbose,
                    dry_run=bool(args.dry_run),
                )
            except subprocess.CalledProcessError as exc:
                cmd = " ".join(shlex.quote(str(x)) for x in exc.cmd)
                out = ((exc.stderr or "") + "\n" + (exc.output or "")).strip()
                msg = f"Failed to clone repository: {repo_url}\nCommand: {cmd}"
                if out:
                    msg += f"\n--- git output ---\n{out}"
                fail(msg)
            if pipeline_arg.is_absolute():
                run_pipeline_path = pipeline_arg
            elif cast(str, args.pipeline) == "ci.yml":
                run_pipeline_path = run_root_workspace / "ci.yml"
            else:
                run_pipeline_path = run_root_workspace / pipeline_arg

            if not run_pipeline_path.exists():
                fail(
                    "Cloned repository does not contain a pipeline file at: "
                    f"{run_pipeline_path}\n"
                    "Provide one with --pipeline <path> or add ci.yml to the repo."
                )

        pipeline = load_pipeline(run_pipeline_path)

        jobs = expand_job_selectors(selectors, sorted(pipeline["jobs"].keys()))
        system_arch = current_system_architecture()
        jobs, skipped_jobs = select_jobs_for_local_arch(
            jobs, pipeline["jobs"], system_arch
        )
        if skipped_jobs and not quiet:
            Console(stderr=True).print(
                "[yellow]Skipping jobs for other architectures:[/yellow] "
                + ", ".join(f"{name} ({arch})" for name, arch in skipped_jobs)
            )
        if not jobs:
            fail(f"No runnable jobs match this runner architecture '{system_arch}'.")

        step_counts: dict[str, int] = {}
        for j in jobs:
            job_def = pipeline["jobs"].get(j)
            if isinstance(job_def, dict):
                step_counts[j] = count_job_steps(job_def)

        live = None
        ui: MultiJobUI | None = None
        refresher: LiveUIRefresher | None = None
        key_listener: LiveUIKeyListener | None = None
        executor: concurrent.futures.ThreadPoolExecutor | None = None
        use_ui = False
        progress_printed = False
        run_started_at = time.monotonic()
        artifact_totals_lock = threading.Lock()
        artifact_count = 0
        artifact_bytes = 0

        def close_live_view(*, show_progress: bool = False) -> None:
            nonlocal live, ui, refresher, key_listener, progress_printed
            if key_listener is not None:
                key_listener.stop()
                key_listener = None
            if refresher is not None:
                refresher.stop()
                refresher = None
            if live is not None and ui is not None:
                try:
                    live.update(ui.render())
                except Exception:
                    pass
                try:
                    live.__exit__(None, None, None)
                except Exception:
                    pass
                live = None
            if show_progress and ui is not None and not progress_printed:
                try:
                    Console().print(
                        ui.render_progress_only(
                            elapsed_seconds=(time.monotonic() - run_started_at),
                            artifact_count=artifact_count,
                            artifact_bytes=artifact_bytes,
                        )
                    )
                    progress_printed = True
                except Exception:
                    pass

        try:
            if args.dry_run:
                for j in jobs:
                    run_job(
                        pipeline=pipeline,
                        job_name=j,
                        workspace=run_root_workspace,
                        dry_run=True,
                        verbose=True,
                    )
                return

            max_workers = max(1, int(getattr(args, "parallel", 1) or 1))
            max_workers = min(max_workers, len(jobs))
            use_ui = sys.stdout.isatty() and not quiet

            if use_ui:
                ui = MultiJobUI(
                    jobs,
                    step_counts,
                    pipeline["jobs"],
                    system_arch=system_arch,
                    max_parallel=max_workers,
                )
                live = Live(ui.render(), refresh_per_second=12, transient=True)
                live.__enter__()

                refresher = LiveUIRefresher(live, ui)
                refresher.start()
                key_listener = LiveUIKeyListener(ui)
                key_listener.start()

            # Single and parallel runs share the same isolated workspace path.
            work_root = (
                run_root_workspace
                / STATE_DIR_NAME
                / "workspaces"
                / uuid.uuid4().hex[:8]
            )
            logs_root = run_root_workspace / ".dist" / "logs"
            work_root.mkdir(parents=True, exist_ok=True)
            logs_root.mkdir(parents=True, exist_ok=True)

            keep = os.environ.get("CEBUILD_KEEP_WORKSPACES") == "1"
            run_verbose = bool(verbose and not use_ui and max_workers == 1)

            def job_thread(job_name: str) -> None:
                nonlocal artifact_count, artifact_bytes
                safe = sanitize_container_name(job_name)
                ws = work_root / safe
                if ui is not None:
                    ui_local = ui
                    ui_local.mark_copying(job_name)
                    ui_local.append(job_name, f"[runner] copying workspace to {ws}")
                    last_update = 0.0

                    def _copy_progress(done: int, total: int) -> None:
                        nonlocal last_update
                        now = time.monotonic()
                        if done != total and (now - last_update) < 0.1:
                            return
                        last_update = now
                        ui_local.set_copy_progress(job_name, done, total)

                    copytree_filtered_with_progress(
                        run_root_workspace,
                        ws,
                        on_progress=_copy_progress,
                    )
                else:
                    copytree_filtered(run_root_workspace, ws)
                log_path = logs_root / f"{safe}.log"
                with open(log_path, "w", encoding="utf-8") as lf:
                    if ui is not None:
                        ui.set_log_path(job_name, log_path)
                        ui.mark_running(job_name)
                        ui.append(job_name, "[runner] started")
                    ok = True
                    try:
                        run_job(
                            pipeline=pipeline,
                            job_name=job_name,
                            workspace=ws,
                            dry_run=False,
                            verbose=run_verbose,
                            ui=ui,
                            log_file=lf,
                        )
                    except subprocess.CalledProcessError as exc:
                        ok = False
                        lf.write(f"[runner] {exc}\n")
                        context = FailedStepContext(
                            job_name=job_name,
                            step="",
                            step_cmd="",
                        )
                        if ui is not None:
                            context = ui.get_failed_step_context(job_name)
                        raise JobExecutionError(context=context, cause=exc) from exc
                    except BaseException as exc:
                        ok = False
                        lf.write(f"[runner] {exc}\n")
                        raise
                    finally:
                        if ui is not None:
                            ui.set_done(job_name, ok)
                            ui.append(
                                job_name,
                                f"[runner] finished ({'ok' if ok else 'failed'})",
                            )

                collected_count, collected_size = collect_job_artifacts(
                    pipeline, job_name, ws, run_root_workspace
                )
                with artifact_totals_lock:
                    artifact_count += collected_count
                    artifact_bytes += collected_size
                if not keep:
                    shutil.rmtree(ws, ignore_errors=True)

            executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
            futs = {executor.submit(job_thread, j): j for j in jobs}
            while futs:
                done, _ = concurrent.futures.wait(
                    futs.keys(),
                    timeout=0.1,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for f in done:
                    _ = futs.pop(f)
                    f.result()  # raise if failed

            if not keep:
                shutil.rmtree(work_root, ignore_errors=True)
        except KeyboardInterrupt:
            if ui is not None:
                ui.mark_cancelled()
            request_cancel()
            terminate_all_running_procs()
            if executor is not None:
                try:
                    executor.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
            close_live_view()
            notify_cancel_once()
            raise SystemExit(130)
        except JobExecutionError as exc:
            cause = exc.cause
            cmd = " ".join(shlex.quote(str(x)) for x in cause.cmd)
            output = (
                (cause.stderr or "")
                + ("\n" if cause.stderr and cause.output else "")
                + (cause.output or "")
            )
            context = exc.context
            step_label = context.step or "(unknown)"
            step_cmd = context.step_cmd or cmd

            if use_ui:
                close_live_view(show_progress=True)
                ui = None

                Console(stderr=True).print(
                    build_failure_panel(
                        cmd=step_cmd,
                        output=output,
                        task=context.job_name,
                        step=step_label,
                    )
                )
                raise SystemExit(1)

            tail = "\n".join(
                [line for line in output.strip().splitlines() if line][-40:]
            )
            message = "\n".join(
                [
                    f"Task: {context.job_name}",
                    f"Step: {step_label}",
                    f"Cmd: {step_cmd}",
                    f"Command failed with exit code {cause.returncode}.",
                ]
            )
            if tail and (not use_ui or quiet):
                message += f"\n--- command output (last lines) ---\n{tail}"
            if use_ui:
                close_live_view()
            fail(message)
        except subprocess.CalledProcessError as exc:
            cmd = " ".join(shlex.quote(str(x)) for x in exc.cmd)
            output = (
                (exc.stderr or "")
                + ("\n" if exc.stderr and exc.output else "")
                + (exc.output or "")
            )
            message = f"Command failed with exit code {exc.returncode}: {cmd}"
            tail = "\n".join(
                [line for line in output.strip().splitlines() if line][-40:]
            )
            if tail and (not use_ui or quiet):
                message += f"\n--- command output (last lines) ---\n{tail}"
            if use_ui:
                close_live_view(show_progress=True)
                Console(stderr=True).print(
                    build_failure_panel(
                        cmd=cmd,
                        output=output,
                        title="Internal command failed",
                        failure_line=message,
                    )
                )
                raise SystemExit(1)
            fail(message)
        except Exception as exc:
            internal_message = "\n".join(
                [
                    f"{exc.__class__.__name__}: {exc}",
                    "",
                    traceback.format_exc().rstrip(),
                ]
            )
            if use_ui:
                close_live_view(show_progress=True)
                Console(stderr=True).print(build_internal_error_panel(internal_message))
                raise SystemExit(1)
            fail(
                "Internal error in Offworld:\n"
                f"{internal_message}\n"
                f"Logs are in: {run_root_workspace / '.dist' / 'logs'}"
            )
        finally:
            if executor is not None:
                try:
                    executor.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
            close_live_view(show_progress=use_ui)
        return

    fail(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
