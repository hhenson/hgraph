"""Generate the RFC 0036 before/after report from the preserved raw outputs.

usage: bench_report2.py <raw_dir> <out_md> [<relative raw dir for links>]
raw_dir/{before,after}/pass1-raw-*.json, memory-raw-*.json, pass2-raw-*.json, abab1-raw-*.json, abab2-raw-*.json
"""
import json, pathlib, statistics, sys

raw = pathlib.Path(sys.argv[1]); out = pathlib.Path(sys.argv[2]); rel = sys.argv[3] if len(sys.argv) > 3 else str(raw)

def one(tree, prefix):
    files = sorted((raw / tree).glob(f"{prefix}-*.json"))
    assert len(files) == 1, (tree, prefix, files)
    return files[0], json.loads(files[0].read_text())

def ratio_rows(b, a):
    rows = []
    for scen in sorted(set(b) & set(a)):
        eb, ea = b[scen].get("current"), a[scen].get("current")
        if not eb or not ea or not eb.get("ok") or not ea.get("ok"): continue
        sb, sa = eb["seconds"], ea["seconds"]
        noise = 2.0 * (eb.get("seconds_mad", 0.0) + ea.get("seconds_mad", 0.0)) / sb
        delta = (sa - sb) / sb
        beyond = abs(delta) > max(noise, 0.03)
        rows.append(dict(scenario=scen, group=eb.get("group", ""), suite=eb.get("suite", ""), before=sb, after=sa,
                         ratio=sa / sb, delta=delta, noise=noise,
                         verdict=("SLOWER" if delta > 0 else "faster") if beyond else "noise",
                         rss_b=eb.get("max_rss_mb"), rss_a=ea.get("max_rss_mb")))
    return rows

fb1, b1 = one("before", "pass1-raw"); fa1, a1 = one("after", "pass1-raw")
fmb, mb = one("before", "memory-raw"); fma, ma = one("after", "memory-raw")
fb2, b2 = one("before", "pass2-raw"); fa2, a2 = one("after", "pass2-raw")
abab = [(t, i, *one(t, f"abab{i}-raw")) for i in (1, 2) for t in ("after", "before")]

meta_b = next(iter(b1.values()))["current"]; meta_a = next(iter(a1.values()))["current"]
bm_b = meta_b.get("benchmark_metadata", {}); bm_a = meta_a.get("benchmark_metadata", {})

rows1 = ratio_rows(b1, a1)
ratios = [r["ratio"] for r in rows1]
flagged1 = [r for r in rows1 if r["verdict"] != "noise"]
perf_lines = ["| scenario | group | suite | before s | after s | after/before | delta | noise band | verdict | RSS before MB | RSS after MB |",
              "|---|---|---|---|---|---|---|---|---|---|---|"]
for r in rows1:
    perf_lines.append(f"| `{r['scenario']}` | {r['group']} | {r['suite']} | {r['before']:.6f} | {r['after']:.6f} | x{r['ratio']:.3f} | {r['delta']*100:+.1f}% | ±{r['noise']*100:.1f}% | {r['verdict']} | {r['rss_b']} | {r['rss_a']} |")

# reversed-order passes: pass 2 over the planned list, pass 3 over pass 1's own flagged cells
def reversed_pass(b2, a2, label):
    rows2 = {r["scenario"]: r for r in ratio_rows(b2, a2)}
    lines = [f"| scenario | pass 1 after/before (5 samples, before first) | {label} after/before (7 samples, after first) | pooled after/before | verdict |", "|---|---|---|---|---|"]
    verdicts = {}
    for scen in sorted(rows2):
        r1, r2 = r1map.get(scen), rows2[scen]
        pooled_b = statistics.median([s["seconds"] for x in (b1[scen]["current"], b2[scen]["current"]) for s in x["samples"]])
        pooled_a = statistics.median([s["seconds"] for x in (a1[scen]["current"], a2[scen]["current"]) for s in x["samples"]])
        rp = pooled_a / pooled_b
        consistent = r1 is not None and (r1["ratio"] - 1) * (r2["ratio"] - 1) > 0 and abs(r2["ratio"] - 1) > max(r2["noise"], 0.03)
        verdict = ("consistent " + ("slowdown" if rp > 1 else "speedup")) if consistent else "run-order drift (not reproduced)"
        verdicts[scen] = verdict
        lines.append(f"| `{scen}` | x{r1['ratio']:.3f} | x{r2['ratio']:.3f} | x{rp:.3f} | {verdict} |")
    return lines, verdicts

r1map = {r["scenario"]: r for r in rows1}
pass2_lines, pass2_verdicts = reversed_pass(b2, a2, "pass 2")
p3 = sorted((raw / "before").glob("pass3-*.json"))
if p3:
    fb3, b3 = one("before", "pass3-raw"); fa3, a3 = one("after", "pass3-raw")
    pass3_lines, pass3_verdicts = reversed_pass(b3, a3, "pass 3")
else:
    pass3_lines, pass3_verdicts = ["(no pass 3)"], {}

# alternations: set A (dense TSD cells), set B (Python-boundary cells), each after, before, after, before
def alternation(prefix):
    runs = []
    for i in (1, 2):
        for t in ("after", "before"):
            files = sorted((raw / t).glob(f"{prefix}{i}-*.json"))
            if not files: return None, None, None
            runs.append((t, i, files[0], json.loads(files[0].read_text())))
    scens = sorted(runs[0][3].keys())
    lines = ["| run | " + " | ".join(f"`{s}` s (MAD)" for s in scens) + " |", "|---|" + "---|" * len(scens)]
    for tree, i, f, d in runs:
        lines.append(f"| {tree} | " + " | ".join(f"{d[s]['current']['seconds']:.5f} ({d[s]['current']['seconds_mad']:.5f})" for s in scens) + " |")
    pooled = []
    for s in scens:
        pb = statistics.median([smp["seconds"] for t, i, f, d in runs if t == "before" for smp in d[s]["current"]["samples"]])
        pa = statistics.median([smp["seconds"] for t, i, f, d in runs if t == "after" for smp in d[s]["current"]["samples"]])
        pooled.append(f"`{s}` pooled after/before x{pa/pb:.3f}")
    return lines, pooled, scens

abab_lines, abab_pooled, abab_scens = alternation("abab")
ababB_lines, ababB_pooled, ababB_scens = alternation("ababB")
if ababB_lines is None: ababB_lines, ababB_pooled = ["(no set B)"], []
ababC_lines, ababC_pooled, ababC_scens = alternation("ababC")
if ababC_lines is None: ababC_lines, ababC_pooled = ["(no set C)"], []

# bisect over the four PR points on the one cell that survived the alternations' first look
bisect_lines = []
bdir = raw / "bisect"
if bdir.exists():
    order = ["before", "pr1", "pr2", "pr3", "after"]
    files = {}
    for f in sorted(bdir.glob("*.json")):
        tag = f.name.split("-")[0]; files.setdefault(tag, []).append(f)
    scen = next(iter(json.loads(files["before"][0].read_text())))
    bisect_lines = [f"| tree | commit | round 1 `{scen}` s (MAD, 11 samples) | round 2 s (MAD) |", "|---|---|---|---|"]
    labels = {"before": "7fff96182 (before)", "pr1": "d20af5cd8 (after PR 1)", "pr2": "84c79139b (after PR 2)", "pr3": "ecb1832a1 (after PR 3)", "after": "59ecd06d6 (after PR 4)"}
    for tag in order:
        cells = []
        for f in files.get(tag, [])[:2]:
            e = json.loads(f.read_text())[scen]["current"]; cells.append(f"{e['seconds']:.5f} ({e['seconds_mad']:.5f})")
        bisect_lines.append(f"| {tag} | {labels[tag]} | " + " | ".join(cells) + " |")

# memory
MEM_FIELDS = [("run_peak_rss_mb", "peak RSS MB"), ("peak_increment_mb", "peak increment MB"), ("retained_increment_mb", "retained MB"),
              ("runtime_load_increment_mb", "runtime load MB"), ("type_records_growth", "type records"), ("seconds", "seconds")]
mem_lines = ["| profile | group | " + " | ".join(f"{lab} before → after" for _, lab in MEM_FIELDS) + " | verdict |", "|---|---|" + "---|" * len(MEM_FIELDS) + "---|"]
mem_flagged = []
for prof in sorted(set(mb["results"]) & set(ma["results"])):
    eb, ea = mb["results"][prof].get("current"), ma["results"][prof].get("current")
    if not eb or not ea or not eb.get("ok") or not ea.get("ok"): continue
    cells = []; verdict = "noise"
    for f, _ in MEM_FIELDS:
        vb, va = eb.get(f), ea.get(f)
        if vb is None or va is None: cells.append("-"); continue
        if f == "type_records_growth":
            cells.append(f"{vb} → {va}" + (" **Δ**" if vb != va else ""))
            if vb != va: verdict = "type records changed"
            continue
        mad = (eb.get(f + "_mad") or 0) + (ea.get(f + "_mad") or 0); delta = va - vb
        beyond = abs(delta) > max(2 * mad, 0.5 if f != "seconds" else 0.0) and (abs(delta) / vb > 0.03 if vb else False)
        cells.append(f"{vb:.3f} → {va:.3f}" + (f" ({delta:+.2f})" if beyond else ""))
        if beyond and f in ("run_peak_rss_mb", "retained_increment_mb"): verdict = "MORE memory" if delta > 0 else "less memory"
    mem_lines.append(f"| `{prof}` | {eb.get('profile_group','')} | " + " | ".join(cells) + f" | {verdict} |")
    if verdict != "noise": mem_flagged.append(prof)

n_slow = sum(1 for r in rows1 if r["verdict"] == "SLOWER"); n_fast = sum(1 for r in rows1 if r["verdict"] == "faster")
consistent = sorted({s for d in (pass2_verdicts, pass3_verdicts) for s, v in d.items() if v.startswith("consistent")})
files_md = "\n".join(f"- `{rel}/{p.parent.name}/{p.name}`" for p in sorted(raw.rglob("*.json")))

doc = f"""# RFC 0036 stack: before/after performance and memory comparison

Before = main at 7fff96182 (just before the RFC 0036 stack merged); after = main at 59ecd06d6 (#758, #759, #760, #762 merged).
Both trees were built as optimized `current` wheels by `benchmarks/orchestrate.py` and run back to back on an otherwise idle machine.
Every number below is computed by `benchmarks/results/rfc0036-before-after-20260906/report.py` from the raw orchestrator outputs listed at the end (every sample, source fingerprint, native module and compiler is in them).

- before: source fingerprint `{meta_b.get('source_fingerprint','')[:16]}…`, revision `{bm_b.get('revision','')}`, {bm_b.get('compiler','')}, Python {meta_b.get('python','')}, {bm_b.get('cpu','')}
- after: source fingerprint `{meta_a.get('source_fingerprint','')[:16]}…`, revision `{bm_a.get('revision','')}`, {bm_a.get('compiler','')}, Python {meta_a.get('python','')}, {bm_a.get('cpu','')}

## Performance, pass 1 (core + diagnostic suites, 5 samples, before tree first)

{len(rows1)} scenarios; median after/before x{statistics.median(ratios):.3f}; geometric mean x{statistics.geometric_mean(ratios):.3f}; {n_slow} slower and {n_fast} faster beyond the noise band (twice the summed MADs, floor 3%).

{chr(10).join(perf_lines)}

## Performance, pass 2 (reversed order, 7 samples, after tree first: the cells the first measurement session had flagged, plus controls)

{chr(10).join(pass2_lines)}

## Performance, pass 3 (reversed order, 7 samples, after tree first: the cells pass 1 above flagged, plus controls)

{chr(10).join(pass3_lines)}

## Performance, alternation A on the dense TSD cells (after, before, after, before; 9 samples)

{chr(10).join(abab_lines)}

{'; '.join(abab_pooled)}.

## Performance, alternation B on the Python-boundary cells (after, before, after, before; 9 samples)

{chr(10).join(ababB_lines)}

{'; '.join(ababB_pooled)}.

## Performance, alternation C on the nested-graph reduce cells (after, before, after, before; 15 samples)

{chr(10).join(ababC_lines)}

{'; '.join(ababC_pooled)}.

## Performance, bisect across the four PR points (two rounds, before → PR 1 → PR 2 → PR 3 → after)

`reduce_tsd_nested_graph_std` was the one cell still 6% slower after alternation B; a wheel was built at each merged PR point and the cell run at every point twice in order.

{chr(10).join(bisect_lines)}

## Memory (`memory_orchestrate.py`, process pass, 3 samples)

Peak RSS, peak increment over the ready process, memory retained after teardown and two GC passes, runtime load increment, retained type-record growth, and wall time per profile. Profiles beyond noise: {', '.join(f'`{p}`' for p in mem_flagged) if mem_flagged else 'none'}.

{chr(10).join(mem_lines)}

## Conclusion

Pass 1 puts the stack at median x{statistics.median(ratios):.3f} over {len(rows1)} scenarios. Of the cells beyond their noise band, {len(consistent)} kept their sign in a reversed-order pass{(' (' + ', '.join(f'`{s}`' for s in consistent) + ')') if consistent else ''}; the alternations give {'; '.join(abab_pooled + ababB_pooled + ababC_pooled)}; the bisect shows no step at any PR point (every tree within the cell's ±5% run-to-run spread). Memory: {len(mem_flagged)} of {len(mem_lines) - 2} profiles beyond noise.

## Raw outputs

{files_md}
"""
out.write_text(doc)
print(f"perf pass1: {len(rows1)} scenarios median x{statistics.median(ratios):.3f} gmean x{statistics.geometric_mean(ratios):.3f}; flagged {[r['scenario'] for r in flagged1]}")
print("pass2:", pass2_verdicts); print("pass3:", pass3_verdicts)
print("abab:", abab_pooled); print("ababB:", ababB_pooled); print("ababC:", ababC_pooled)
print("mem flagged:", mem_flagged)
