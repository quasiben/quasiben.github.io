"""Generate the spill-sweep figure for the YABAS shuffle blog post.

Data source: table in docs/blog/posts/soupy-can-shuffle.md (Example Outputs).
"""
import os

os.environ.setdefault("MPLCONFIGDIR", os.path.join(os.path.dirname(__file__), ".mplcache"))

import matplotlib.pyplot as plt
import numpy as np

# --- data from the blog table -------------------------------------------------
# (label, spill_limit_GiB, input_per_rank_GiB, peak_device_GiB, perf_GiBps,
#  vs_baseline, d2h_GiB, d2h_ms, h2d_GiB, h2d_ms)
rows = [
    ("no-spill",            64, 32, 56.0, 35.9, 1.00, 0.0,     0.0,   0.0,      0.0),
    ("near-threshold",      56, 32, 56.0, 36.5, 1.02, 0.0,     0.0,   0.03125,  0.211),
    ("light-spill",         32, 32, 35.0, 43.9, 1.22, 0.03125, 0.242, 24.0,     986.9),
    ("moderate-spill",      28, 32, 35.0, 40.5, 1.13, 4.0,     49.4,  25.0,     1270.0),
    ("heavy-spill",         24, 32, 35.0, 42.4, 1.18, 8.0,     85.1,  26.0,     1470.0),
    ("very-heavy-spill",    20, 32, 35.0, 42.5, 1.18, 12.0,    125.1, 27.0,     1680.0),
    ("extreme-spill",       16, 32, 35.0, 43.3, 1.21, 16.0,    155.9, 28.0,     2050.0),
    ("large-extreme-spill", 16, 64, 67.0, 35.6, 0.99, 48.0,    510.8, 60.0,     5310.0),
]

labels   = [r[0] for r in rows]
spill    = np.array([r[1] for r in rows], dtype=float)
inp      = np.array([r[2] for r in rows], dtype=float)
perf     = np.array([r[4] for r in rows], dtype=float)
vs_base  = np.array([r[5] for r in rows], dtype=float)
d2h_gib  = np.array([r[6] for r in rows], dtype=float)
d2h_ms   = np.array([r[7] for r in rows], dtype=float)
h2d_gib  = np.array([r[8] for r in rows], dtype=float)
h2d_ms   = np.array([r[9] for r in rows], dtype=float)

x = np.arange(len(rows))
# the last row is a different regime (64 GiB/rank input); mark it apart
is_big_input = inp > 32
xticklabels = [f"{lab}\n(limit {int(s)}G, in {int(i)}G)"
               for lab, s, i in zip(labels, spill, inp)]

BASE = perf[0]  # 35.9 GiB/s baseline (no-spill, 32 GiB/rank)

plt.style.use("seaborn-v0_8-whitegrid")
fig, (ax1, ax2) = plt.subplots(
    2, 1, figsize=(12, 9), sharex=True,
    gridspec_kw={"height_ratios": [1, 1], "hspace": 0.12},
)

# --- Panel A: local shuffle performance --------------------------------------
colors = ["#c9c9c9" if big else "#2a7fb8" for big in is_big_input]
bars = ax1.bar(x, perf, color=colors, edgecolor="black", linewidth=0.6, width=0.68)
ax1.axhline(BASE, color="#d1495b", ls="--", lw=1.4, zorder=1)
ax1.text(-0.35, BASE - 1.6, f"no-spill baseline = {BASE:.1f} GiB/s",
         color="#d1495b", ha="left", va="top", fontsize=9)

for xi, p, vb, big in zip(x, perf, vs_base, is_big_input):
    ax1.text(xi, p + 0.4, f"{p:.1f}\n({vb:.2f}\u00d7)",
             ha="center", va="bottom", fontsize=8.5,
             color="#555" if big else "#12405a")

ax1.set_ylabel("Local shuffle perf (GiB/s)")
ax1.set_ylim(0, max(perf) * 1.22)
ax1.set_title("RAPIDSMPF shuffle: spilling to host barely dents throughput "
              "(32 GiB/rank sweep)", fontsize=13, fontweight="bold")

# --- Panel B: data spilled + copy time ---------------------------------------
w = 0.38
b1 = ax2.bar(x - w / 2, d2h_gib, width=w, color="#4c9f70",
             edgecolor="black", linewidth=0.5, label="device \u2192 pinned host (GiB)")
b2 = ax2.bar(x + w / 2, h2d_gib, width=w, color="#e08214",
             edgecolor="black", linewidth=0.5, label="pinned host \u2192 device (GiB)")
ax2.set_ylabel("Data copied (GiB)")
ax2.set_ylim(0, max(h2d_gib) * 1.2)

axt = ax2.twinx()
axt.plot(x - w / 2, d2h_ms, "o--", color="#276419", lw=1.3, ms=5,
         label="device \u2192 host time (ms)")
axt.plot(x + w / 2, h2d_ms, "s--", color="#8c3b00", lw=1.3, ms=5,
         label="host \u2192 device time (ms)")
axt.set_ylabel("Copy time (ms)")
axt.set_ylim(0, max(h2d_ms) * 1.15)
axt.grid(False)

h_bars, l_bars = ax2.get_legend_handles_labels()
h_line, l_line = axt.get_legend_handles_labels()
ax2.legend(h_bars + h_line, l_bars + l_line, loc="upper left",
           fontsize=8.5, framealpha=0.9, ncol=2)

ax2.set_xticks(x)
ax2.set_xticklabels(xticklabels, rotation=30, ha="right", fontsize=8.5)
ax2.set_title("Spill traffic grows with tighter limits, but stays host-bandwidth bound",
              fontsize=12, fontweight="bold")

fig.suptitle("")
out = os.path.join(os.path.dirname(__file__), "spill-sweep.png")
fig.savefig(out, dpi=150, bbox_inches="tight")
print("wrote", out)
