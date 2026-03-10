import tempfile
import textwrap
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from offworld.cli import (
    MultiJobUI,
    canonical_architecture_id,
    command_list,
    expand_job_selectors,
    format_job_name_for_list,
    group_job_names_for_list,
    load_pipeline,
    select_jobs_for_local_arch,
)


class TargetExpansionTests(unittest.TestCase):
    def load_pipeline_text(self, payload: str) -> dict:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "ci.yml"
            path.write_text(textwrap.dedent(payload))
            return load_pipeline(path)

    def test_nodes_without_targets_are_unchanged(self) -> None:
        pipeline = self.load_pipeline_text(
            """
            version: 1
            tree:
              demo:
                kind: host
                steps:
                  - echo hello
            """
        )

        self.assertEqual(list(pipeline["jobs"].keys()), ["demo"])
        self.assertEqual(pipeline["jobs"]["demo"]["steps"], ["echo hello"])
        self.assertEqual(pipeline["jobs"]["demo"].get("env", {}), {})

    def test_two_targets_expand_to_two_jobs(self) -> None:
        pipeline = self.load_pipeline_text(
            """
            version: 1
            tree:
              manylinux:
                kind: container
                workdir: /work
                targets:
                  x86_64:
                    base: quay.io/pypa/manylinux_2_34_x86_64
                  aarch64:
                    base: quay.io/pypa/manylinux_2_34_aarch64
                wheel:
                  steps:
                    - echo wheel
            """
        )

        self.assertEqual(
            sorted(pipeline["jobs"].keys()),
            ["manylinux[aarch64]:wheel", "manylinux[x86_64]:wheel"],
        )
        self.assertEqual(
            pipeline["jobs"]["manylinux[x86_64]:wheel"]["base"],
            "quay.io/pypa/manylinux_2_34_x86_64",
        )
        self.assertEqual(
            pipeline["jobs"]["manylinux[aarch64]:wheel"]["base"],
            "quay.io/pypa/manylinux_2_34_aarch64",
        )

    def test_target_env_merge_and_arch_injection(self) -> None:
        pipeline = self.load_pipeline_text(
            """
            version: 1
            tree:
              build:
                kind: host
                env:
                  SHARED: parent
                  PARENT_ONLY: keep
                  OFFWORLD_TARGET_ARCH: wrong
                targets:
                  aarch64:
                    env:
                      SHARED: child
                      TARGET_ONLY: present
                      OFFWORLD_TARGET: wrong
                steps:
                  - env
            """
        )

        job = pipeline["jobs"]["build[aarch64]"]
        self.assertEqual(job["env"]["SHARED"], "child")
        self.assertEqual(job["env"]["PARENT_ONLY"], "keep")
        self.assertEqual(job["env"]["TARGET_ONLY"], "present")
        self.assertEqual(job["env"]["OFFWORLD_TARGET"], "aarch64")
        self.assertEqual(job["env"]["OFFWORLD_TARGET_ARCH"], "aarch64")
        self.assertEqual(job["arch"], "aarch64")

    def test_target_overlay_inherits_and_overrides_subtargets(self) -> None:
        pipeline = self.load_pipeline_text(
            """
            version: 1
            tree:
              manylinux:
                kind: container
                targets:
                  x86_64:
                    base: quay.io/pypa/manylinux_2_34_x86_64
                  aarch64:
                    base: quay.io/pypa/manylinux_2_34_aarch64
                    wheel:
                      env:
                        WHEEL_TARGET: arm64
                bin:
                  steps:
                    - echo bin
                wheel:
                  env:
                    WHEEL_PARENT: yes
                  steps:
                    - echo wheel
                  validate:
                    - kind: shell
                      name: smoke
                      steps:
                        - echo smoke
                  artifacts:
                    - kind: file
                      path: dist/out.whl
            """
        )

        self.assertEqual(
            sorted(pipeline["jobs"].keys()),
            [
                "manylinux[aarch64]:bin",
                "manylinux[aarch64]:wheel",
                "manylinux[x86_64]:bin",
                "manylinux[x86_64]:wheel",
            ],
        )

        arm_wheel = pipeline["jobs"]["manylinux[aarch64]:wheel"]
        x86_bin = pipeline["jobs"]["manylinux[x86_64]:bin"]

        self.assertEqual(arm_wheel["base"], "quay.io/pypa/manylinux_2_34_aarch64")
        self.assertEqual(arm_wheel["env"]["WHEEL_PARENT"], True)
        self.assertEqual(arm_wheel["env"]["WHEEL_TARGET"], "arm64")
        self.assertEqual(arm_wheel["validate"][0]["name"], "smoke")
        self.assertEqual(arm_wheel["artifacts"][0]["path"], "dist/out.whl")
        self.assertEqual(x86_bin["base"], "quay.io/pypa/manylinux_2_34_x86_64")

    def test_target_overlay_appends_steps_and_validations(self) -> None:
        pipeline = self.load_pipeline_text(
            """
            version: 1
            tree:
              build:
                kind: host
                targets:
                  aarch64:
                    steps:
                      - echo target-step
                    validate:
                      - kind: shell
                        name: target-validate
                        steps:
                          - echo target-validate
                steps:
                  - echo base-step
                validate:
                  - kind: shell
                    name: base-validate
                    steps:
                      - echo base-validate
            """
        )

        job = pipeline["jobs"]["build[aarch64]"]
        self.assertEqual(job["steps"], ["echo base-step", "echo target-step"])
        self.assertEqual(
            [entry["name"] for entry in job["validate"]],
            ["base-validate", "target-validate"],
        )

    def test_target_overlay_can_override_nested_subtarget_fields(self) -> None:
        pipeline = self.load_pipeline_text(
            """
            version: 1
            tree:
              build:
                kind: container
                workdir: /work
                targets:
                  aarch64:
                    base: arm-base
                    wheel:
                      base: wheel-arm-base
                      workdir: /wheel
                wheel:
                  base: wheel-base
                  steps:
                    - echo wheel
            """
        )

        job = pipeline["jobs"]["build[aarch64]:wheel"]
        self.assertEqual(job["base"], "wheel-arm-base")
        self.assertEqual(job["workdir"], "/wheel")

    def test_target_expansion_preserves_existing_visible_name_filtering(self) -> None:
        pipeline = self.load_pipeline_text(
            """
            version: 1
            tree:
              _hidden:
                kind: host
                targets:
                  x86_64:
                    env:
                      HIDDEN: yes
                wheel:
                  steps:
                    - echo hidden
            """
        )

        self.assertEqual(list(pipeline["jobs"].keys()), ["wheel"])
        self.assertEqual(
            pipeline["jobs"]["wheel"]["env"]["OFFWORLD_TARGET_ARCH"], "x86_64"
        )

    def test_target_prefix_selectors_include_expanded_jobs(self) -> None:
        jobs = [
            "manylinux[aarch64]:bin",
            "manylinux[aarch64]:wheel",
            "manylinux[x86_64]:bin",
            "manylinux[x86_64]:wheel",
            "lint",
        ]

        self.assertEqual(
            expand_job_selectors(["manylinux"], jobs),
            [
                "manylinux[aarch64]:bin",
                "manylinux[aarch64]:wheel",
                "manylinux[x86_64]:bin",
                "manylinux[x86_64]:wheel",
            ],
        )
        self.assertEqual(
            expand_job_selectors(["all:manylinux"], jobs),
            [
                "manylinux[aarch64]:bin",
                "manylinux[aarch64]:wheel",
                "manylinux[x86_64]:bin",
                "manylinux[x86_64]:wheel",
            ],
        )

    def test_duplicate_expanded_names_are_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            self.load_pipeline_text(
                """
                version: 1
                tree:
                  build:
                    kind: host
                    targets:
                      x86_64:
                        env:
                          ONE: yes
                    test:
                      steps:
                        - echo from-target
                  build[x86_64]:
                    kind: host
                    test:
                      steps:
                        - echo explicit
                """
            )

    def test_non_mapping_targets_are_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            self.load_pipeline_text(
                """
                version: 1
                tree:
                  bad:
                    kind: host
                    targets:
                      - x86_64
                    steps:
                      - echo nope
                """
            )

    def test_non_mapping_target_overlay_is_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            self.load_pipeline_text(
                """
                version: 1
                tree:
                  bad:
                    kind: host
                    targets:
                      x86_64: nope
                    steps:
                      - echo nope
                """
            )

    def test_empty_target_key_is_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            self.load_pipeline_text(
                """
                version: 1
                tree:
                  bad:
                    kind: host
                    targets:
                      "":
                        env:
                          BAD: yes
                    steps:
                      - echo nope
                """
            )

    def test_architecture_aliases_are_normalized(self) -> None:
        self.assertEqual(canonical_architecture_id("amd64"), "x86_64")
        self.assertEqual(canonical_architecture_id("x86-64"), "x86_64")
        self.assertEqual(canonical_architecture_id("arm64"), "aarch64")
        self.assertEqual(canonical_architecture_id("AARCH64"), "aarch64")

    def test_local_arch_selection_keeps_matching_and_archless_jobs(self) -> None:
        pipeline_jobs = {
            "lint": {"kind": "host", "steps": ["echo lint"]},
            "manylinux[x86_64]:wheel": {
                "kind": "host",
                "steps": ["echo x86"],
                "arch": "amd64",
            },
            "manylinux[aarch64]:wheel": {
                "kind": "host",
                "steps": ["echo arm"],
                "arch": "arm64",
            },
        }

        selected, skipped = select_jobs_for_local_arch(
            ["lint", "manylinux[x86_64]:wheel", "manylinux[aarch64]:wheel"],
            pipeline_jobs,
            "x86_64",
        )

        self.assertEqual(selected, ["lint", "manylinux[x86_64]:wheel"])
        self.assertEqual(skipped, [("manylinux[aarch64]:wheel", "aarch64")])

    def test_command_list_prints_current_system_architecture(self) -> None:
        pipeline = self.load_pipeline_text(
            """
            version: 1
            tree:
              demo:
                kind: host
                steps:
                  - echo hello
            """
        )

        with patch("offworld.cli.current_system_architecture", return_value="x86_64"):
            with patch("sys.stdout", new=StringIO()) as stdout:
                command_list(pipeline)
                output = stdout.getvalue()

        self.assertIn("Available Architectures", output)
        self.assertIn("x86_64", output)

    def test_command_list_preserves_arch_suffixes_in_output(self) -> None:
        pipeline = {
            "jobs": {
                "container[aarch64]:dev": {
                    "kind": "host",
                    "steps": ["echo noop"],
                    "arch": "aarch64",
                },
                "container[aarch64]:prod": {
                    "kind": "host",
                    "steps": ["echo noop"],
                    "arch": "aarch64",
                },
                "container[x86_64]:dev": {
                    "kind": "host",
                    "steps": ["echo noop"],
                    "arch": "x86_64",
                },
                "container[x86_64]:prod": {
                    "kind": "host",
                    "steps": ["echo noop"],
                    "arch": "x86_64",
                },
            }
        }

        with patch("offworld.cli.current_system_architecture", return_value="x86_64"):
            with patch("sys.stdout", new=StringIO()) as stdout:
                command_list(pipeline)
                output = stdout.getvalue()

        self.assertIn("container[aarch64,x86_64]:dev", output)
        self.assertIn("container[aarch64,x86_64]:prod", output)

    def test_group_job_names_for_list_collapses_target_variants(self) -> None:
        grouped = group_job_names_for_list(
            {
                "container[aarch64]:dev": {"arch": "aarch64"},
                "container[x86_64]:dev": {"arch": "x86_64"},
                "manylinux[aarch64]:wheel": {"arch": "aarch64"},
                "manylinux[x86_64]:wheel": {"arch": "x86_64"},
                "windows:bin": {"kind": "host", "steps": ["echo noop"]},
            }
        )

        self.assertEqual(
            grouped,
            [
                "container[aarch64,x86_64]:dev",
                "manylinux[aarch64,x86_64]:wheel",
                "windows:bin",
            ],
        )

    def test_progress_labels_include_arch_tag(self) -> None:
        ui = MultiJobUI(
            ["container:dev", "manylinux[aarch64]:wheel"],
            {"container:dev": 1, "manylinux[aarch64]:wheel": 1},
            {
                "container:dev": {"arch": "amd64"},
                "manylinux[aarch64]:wheel": {"arch": "aarch64"},
            },
            system_arch="x86_64",
            max_parallel=2,
        )

        task_ids = ui._progress_task_ids
        tasks = ui._progress.tasks
        descriptions = {
            task.description
            for task in tasks
            if task.id
            in {task_ids["container:dev"], task_ids["manylinux[aarch64]:wheel"]}
        }

        self.assertIn("Task #1: container\\[x86_64]:dev", descriptions)
        self.assertIn("Task #2: manylinux\\[aarch64]:wheel", descriptions)

    def test_progress_labels_fallback_to_runner_arch(self) -> None:
        ui = MultiJobUI(
            ["windows:bin"],
            {"windows:bin": 1},
            {"windows:bin": {"kind": "host", "steps": ["echo noop"]}},
            system_arch="x86_64",
            max_parallel=1,
        )

        descriptions = {task.description for task in ui._progress.tasks}
        self.assertIn("Task #1: windows\\[x86_64]:bin", descriptions)

    def test_format_job_name_for_list_includes_arch_suffix(self) -> None:
        self.assertEqual(
            format_job_name_for_list("container:dev", {"arch": "amd64"}),
            "container[x86_64]:dev",
        )
        self.assertEqual(
            format_job_name_for_list("manylinux[aarch64]:wheel", {"arch": "aarch64"}),
            "manylinux[aarch64]:wheel",
        )


if __name__ == "__main__":
    unittest.main()
