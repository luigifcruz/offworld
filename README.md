# Offworld CI

Offworld is a runtime-aware build runner for CyberEther-style pipelines.

It runs jobs from `ci.yml` on host or container runtimes, supports
parallel execution with isolated workspaces, and provides a Rich TUI.

## Install

Using pip + venv:

```bash
cd offworld
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e .
```

Using uv:

```bash
cd offworld
uv venv
source .venv/bin/activate
uv pip install -e .
```

## Usage

From repo root:

```bash
offworld list
offworld run ubuntu:test
offworld run runtime --parallel 2
offworld cleanup --all
```

## Disclaimer

Offworld was created with the assistance of generative AI.
