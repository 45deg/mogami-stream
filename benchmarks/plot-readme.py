# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib==3.11.2"]
# ///
"""Render saved Node.js/Bun reports; captions and methodology live in README.md."""
import argparse
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--node", required=True, type=Path)
parser.add_argument("--bun", required=True, type=Path)
parser.add_argument("--svg", required=True, type=Path)
parser.add_argument("--png", type=Path)
args = parser.parse_args()
reports = {
    name: json.loads(path.read_text())
    for name, path in [("node", args.node), ("bun", args.bun)]
}
node, bun = reports["node"], reports["bun"]
if node["configuration"] != bun["configuration"]:
    raise ValueError("Node.js and Bun benchmark configurations differ")
rows = {
    name: {(row["case"], row["library"]): row for row in report["results"]}
    for name, report in reports.items()
}
gc_modes = set()
for name, report in reports.items():
    runtime = report["runtime"]
    if runtime["name"] != name:
        raise ValueError(f"Expected {name} runtime")
    # Schema 2 reports predate the explicit option; retain their recorded mode.
    forced = runtime["forcedGcBetweenRuns"]
    if not isinstance(forced, bool):
        raise ValueError("Missing or invalid GC condition")
    mode = runtime.get("gc", "between" if forced else "none")
    if mode not in ("none", "between") or forced != (mode == "between"):
        raise ValueError("Inconsistent runtime GC condition")
    if report["configuration"].get("gc", mode) != mode:
        raise ValueError("Configuration and runtime GC conditions differ")
    gc_modes.add(mode)
    if len(rows[name]) != len(report["results"]):
        raise ValueError("Duplicate benchmark results")
if len(gc_modes) != 1:
    raise ValueError("Node.js and Bun GC conditions differ")
gc_mode = gc_modes.pop()
if {key: row["version"] for key, row in rows["node"].items()} != {
    key: row["version"] for key, row in rows["bun"].items()
}:
    raise ValueError("Benchmark cases or library versions differ between runtimes")

panels = [
    ("async-map-serial", ["mogami", "ix", "iter-tools", "p-map-iterable"]),
    ("async-map-concurrent", ["mogami", "p-map-iterable"]),
    ("async-pipeline", ["mogami", "ix", "iter-tools"]),
]
panels = [
    (case, libraries)
    for case, libraries in panels
    if any(w["name"] == case for w in node["workloads"])
]
if not panels:
    raise ValueError("No supported async workloads")
labels = {
    "mogami": "Mogami",
    "ix": "IxJS",
    "iter-tools": "iter-tools",
    "p-map-iterable": "pMapIterable",
}
expected = {(case, library) for case, libraries in panels for library in libraries}
if set(rows["node"]) != expected:
    raise ValueError("Missing or unexpected benchmark results")
for report in reports.values():
    for row in report["results"]:
        if len(row["samples"]) != report["configuration"]["samples"]:
            raise ValueError("Incomplete sample set")
        value = row["elapsedMs"]["median"]
        if not math.isfinite(value) or value <= 0:
            raise ValueError("Invalid elapsed time")

plt.style.use("default")
plt.rcParams["svg.hashsalt"] = "mogami-async-benchmarks"
fig, axes = plt.subplots(
    len(panels),
    1,
    figsize=(9, sum(len(libraries) for _, libraries in panels)),
    gridspec_kw={"height_ratios": [len(libraries) for _, libraries in panels]},
    squeeze=False,
)
for ax, (case, libraries) in zip(axes.flat, panels):
    maximum = 0
    for runtime, offset, color, label in [
        ("node", -.18, "C0", "Node.js"),
        ("bun", .18, "C1", "Bun"),
    ]:
        seconds = [
            rows[runtime][case, library]["elapsedMs"]["median"] / 1000
            for library in libraries
        ]
        maximum = max(maximum, max(seconds))
        bars = ax.barh(
            [i + offset for i in range(len(libraries))],
            seconds,
            height=.34,
            color=color,
            label=label,
        )
        ax.bar_label(
            bars, labels=[f"{value:.3f}" for value in seconds], padding=4, fontsize=9
        )
    ax.set_yticks(range(len(libraries)), [labels[library] for library in libraries])
    ax.invert_yaxis()
    ax.set_xlim(0, maximum * 1.16)
    ax.set_xlabel("Time (s)")
fig.legend(
    *axes[0, 0].get_legend_handles_labels(),
    loc="upper center",
    bbox_to_anchor=(.5, 1),
    ncol=2,
    frameon=False,
)
fig.text(.5, .01, "GC: normal" if gc_mode == "none" else "GC: manual before each iteration", ha="center")
fig.tight_layout(rect=(0, .03, 1, .96), h_pad=2)
fig.savefig(args.svg, metadata={"Date": None})
# Matplotlib emits trailing spaces in multiline SVG path attributes.
args.svg.write_text(
    "\n".join(line.rstrip() for line in args.svg.read_text().splitlines()) + "\n"
)
if args.png:
    fig.savefig(args.png, dpi=170)
plt.close(fig)
