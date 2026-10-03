#!/usr/bin/env python3
"""Contract for the fixed-64-spine two-tier Clos topology generator."""
from clos_topology import build_two_tier_clos


def main():
    topology = build_two_tier_clos(2)
    lines = topology.strip().splitlines()
    assert lines[0] == "194 66 256"
    assert lines[1].split() == [str(node) for node in range(128, 194)]
    links = [tuple(line.split()[:2]) for line in lines[2:]]
    assert len(links) == 128 + 2 * 64
    assert links[0] == ("0", "128")
    assert links[63] == ("63", "128")
    assert links[64] == ("64", "129")
    assert links[128] == ("128", "130")
    assert links[-1] == ("129", "193")
    print("fixed-64-spine Clos topology contract passed")


if __name__ == "__main__":
    main()
