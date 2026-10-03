#!/usr/bin/env python3
"""Contract for the REPS/dToR/GLB smoke runner."""
import importlib.util
from pathlib import Path
import tempfile


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "experiments/smoke/run_reps_dtor_glb_smoke.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("reps_dtor_glb_smoke", RUNNER)
    if spec is None or spec.loader is None:
        raise AssertionError("cannot load smoke runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    runner = load_runner()
    assert runner.SCHEMES == ("reps", "dtor", "glb")
    assert runner.DEFAULT_TOPOLOGY == "leaf_spine_128_100G_OS2"
    environment = runner.waf_environment({"PATH": "/usr/bin"}, "/opt/python2/bin")
    assert environment["PATH"] == "/opt/python2/bin:/usr/bin"

    with tempfile.TemporaryDirectory() as directory:
        args = runner.parse_args([
            "--out", directory, "--dry-run", "--seed", "13", "--netload", "50",
        ])
        specs = runner.build_specs(args)

    assert [spec["scheme"] for spec in specs] == list(runner.SCHEMES)
    assert len({spec["run_id"] for spec in specs}) == 3
    for spec in specs:
        command = spec["command"]
        assert command[:2] == ["python3", "run.py"]
        assert command[command.index("--lb") + 1] == spec["scheme"]
        assert command[command.index("--irn") + 1] == "1"
        assert command[command.index("--pfc") + 1] == "1"
        assert command[command.index("--netload") + 1] == "50"
        assert command[command.index("--seed") + 1] == "13"
        assert command[command.index("--topo") + 1] == runner.DEFAULT_TOPOLOGY
    print("REPS/dToR/GLB smoke runner contract passed")


if __name__ == "__main__":
    main()
