"""Topology text generation for a two-tier Clos with fixed fanout."""


def build_two_tier_clos(leaf_count, nics_per_leaf=64, spine_count=64,
                        rate="100Gbps", delay="1000ns", error_rate="0"):
    if leaf_count < 2:
        raise ValueError("leaf_count must be at least two")
    if nics_per_leaf != 64 or spine_count != 64:
        raise ValueError("the aligned benchmark requires 64 NICs per leaf and 64 spines")

    host_count = leaf_count * nics_per_leaf
    leaf_start = host_count
    spine_start = leaf_start + leaf_count
    node_count = spine_start + spine_count
    link_count = host_count + leaf_count * spine_count
    lines = ["{} {} {}".format(node_count, leaf_count + spine_count, link_count)]
    lines.append(" ".join(str(node) for node in range(leaf_start, node_count)))

    for host in range(host_count):
        leaf = leaf_start + host // nics_per_leaf
        lines.append("{} {} {} {} {}".format(host, leaf, rate, delay, error_rate))
    for leaf in range(leaf_start, spine_start):
        for spine in range(spine_start, node_count):
            lines.append("{} {} {} {} {}".format(leaf, spine, rate, delay, error_rate))
    return "\n".join(lines) + "\n"
