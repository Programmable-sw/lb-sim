#!/usr/bin/env python3
"""The 64-downlink/64-uplink Clos requires physical device index 128."""
import re
from pathlib import Path


def main():
    header = (Path(__file__).resolve().parents[1] /
              "src/point-to-point/model/switch-node.h").read_text(encoding="utf-8")
    match = re.search(r"pCnt\s*=\s*(\d+)", header)
    assert match, "SwitchNode pCnt is missing"
    assert int(match.group(1)) >= 129, "64 downlinks + 64 uplinks require device index 128"
    print("SwitchNode supports the fixed-64-spine Clos port range")


if __name__ == "__main__":
    main()
