"""Print one table of median times from every results/<dataset>/bench.json.

Usage: python summarize.py [--markdown]
"""
import json
import sys

from common import ROOT

QUERIES = ["asof_join", "validity_range", "union_last_value", "join_then_rank"]


def main():
    rows = []
    for path in sorted((ROOT / "results").glob("*/bench.json")):
        b = json.loads(path.read_text())
        per_product = b["price_changes"] / int(path.parent.name.split("_")[0][1:])
        cells = [
            f"{b['timings'][q]['median_s']:.1f}" if q in b["timings"] else "-" for q in QUERIES
        ]
        rows.append((per_product, path.parent.name, b["candidate_pairs"], cells))

    if "--markdown" not in sys.argv:
        widths = [len(q) + 3 for q in QUERIES]
        print(f"{'changes':>7}" + "".join(f"{q:>{w}}" for q, w in zip(QUERIES, widths)))
        for per_product, name, _, cells in sorted(rows):
            if name.endswith("no_broadcast"):
                continue
            times = "".join(f"{c + ' s' if c != '-' else 'not run':>{w}}" for c, w in zip(cells, widths))
            print(f"{per_product:>7,.0f}" + times)
        return

    print("| results folder | price changes per product | order x earlier price pairs | "
          + " | ".join(f"{q} (s)" for q in QUERIES) + " |")
    print("|---" * (3 + len(QUERIES)) + "|")
    for per_product, name, pairs, cells in sorted(rows):
        print(f"| {name} | {per_product:,.0f} | {pairs:,} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
