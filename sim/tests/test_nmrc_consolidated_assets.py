#!/usr/bin/env python3

import importlib.util
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
EXPERIMENTS = ROOT / "experiments/n-mrc"

RETAINED_RUNNERS = (
    "run_mrc_exact_bounded_recovery.py",
    "run_dcqcn_narrow_trim_lb_compare.py",
    "run_exact_bounded_before_after_schemes.py",
    "run_exact_bounded_representative_schemes.py",
    "run_sglb_exact_bounded_interaction_diag.py",
    "run_sglb_nmrc_quantized_topk_compare.py",
)

OBSOLETE_ASSETS = (
    "run_nmrc_share_cap_diag.py",
    "run_nmrc_six_scenario_soft_weight_tuning.py",
    "run_nmrc_universal_parameter_tuning.py",
    "run_nmrc_universal_final_validation.py",
    "run_nmrc_weight_threshold_robustness.py",
    "run_nmrc_joint_weight_threshold_sweep.py",
    "run_nmrc_stageA_feedback5_check.py",
    "run_nmrc_stageA_vs_tuned_auto.py",
    "run_nmrc_feedback_sweep.py",
    "run_feedback_cadence_evaluation.py",
    "run_nmrc_dctcp_variant_narrow_compare.py",
    "run_phase2_nmrc_sglbq_followup.py",
    "run_phase3_medium_eval.py",
    "run_branch123_ai_tuning.py",
    "build_nmrc_universal_tuning_report.py",
    "build_nmrc_weight_threshold_robustness_report.py",
    "build_nmrc_joint_weight_threshold_report.py",
)

FORBIDDEN_PATTERNS = {
    "standalone ShareCap": re.compile(r"(?<!good_)share_cap"),
    "ProfileSoft": re.compile(r"profile_soft", re.IGNORECASE),
    "RankFill": re.compile(r"rank_fill|RankFill", re.IGNORECASE),
    "ordinal GoodCap": re.compile(r"ordinal_good_cap"),
    "confidence 4210 cap": re.compile(r"confidence_4210_cap"),
    "threshold headroom": re.compile(r"threshold_headroom"),
    "breadth blend": re.compile(r"breadth_blend"),
    "4310 candidate": re.compile(r"4310"),
    "0.15/0.45/0.70 candidate": re.compile(
        r"0\.15.{0,24}0\.45.{0,24}0\.70"
    ),
    "n-MRC cadence override": re.compile(
        r"-nmrc_feedback_(?:pkts|min_us|max_us)"
    ),
}


def import_runner(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


def active_sources():
    roots = (ROOT / "sim", EXPERIMENTS)
    suffixes = {".cpp", ".h", ".py", ".md"}
    excluded = {
        ROOT / "sim/EXAMPLES",
        ROOT / "sim/tests",
        EXPERIMENTS / "output",
    }
    for root in roots:
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in suffixes:
                continue
            if any(parent == path or parent in path.parents
                   for parent in excluded):
                continue
            if path == Path(__file__).resolve():
                continue
            yield path


def main():
    for name in RETAINED_RUNNERS:
        import_runner(EXPERIMENTS / name)

    present = [name for name in OBSOLETE_ASSETS
               if (EXPERIMENTS / name).exists()]
    if present:
        raise AssertionError(f"obsolete experiment assets remain: {present}")

    violations = []
    for path in active_sources():
        text = path.read_text(encoding="utf-8", errors="ignore")
        for label, pattern in FORBIDDEN_PATTERNS.items():
            if pattern.search(text):
                violations.append(f"{path.relative_to(ROOT)}: {label}")
    if violations:
        raise AssertionError("forbidden active candidates:\n" +
                             "\n".join(violations))


if __name__ == "__main__":
    main()
