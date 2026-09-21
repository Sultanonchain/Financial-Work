#!/usr/bin/env python3
"""Snapshot the numbers a ticker page shows, so a valuation fix can be judged.

Run it against a local VALUS instance before and after a change and diff the
two outputs:

    VALUS_DEV=1 PORT=5077 python3 app.py &
    python3 scripts/valuation_check.py --port 5077 > /tmp/before.txt
    ... make the fix ...
    python3 scripts/valuation_check.py --port 5077 > /tmp/after.txt
    diff /tmp/before.txt /tmp/after.txt

It reports, per ticker: the discount-rate inputs, both growth stages, each
scenario card's label and value, the hero range, fair value, and the latest
Valuation History entry -- plus the invariants those numbers have to satisfy,
so an ordering or weighting bug shows up as a FAIL line rather than as a
number you have to eyeball.

Read-only: it calls the HTTP API and changes nothing.
"""
import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

TICKERS = ["AAPL", "JNJ", "MSFT", "KO", "NVDA"]
SCENARIOS = ["bear", "base", "bull"]


def get(base, path, **params):
    url = f"{base}{path}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=300) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return {"_http_error": f"{e.code} {e.reason}"}
    except Exception as e:                                   # noqa: BLE001
        return {"_error": str(e)}


def num(v, nd=2):
    return "n/a" if v is None else f"{float(v):.{nd}f}"


def check(label, ok, detail=""):
    """One invariant. Prints PASS/FAIL so a diff shows a fix flipping it."""
    print(f"    [{'PASS' if ok else 'FAIL'}] {label}{(' -- ' + detail) if detail else ''}")
    return ok


def report(base, ticker):
    print(f"\n{'=' * 70}\n{ticker}\n{'=' * 70}")
    d = get(base, "/api/analyze", ticker=ticker)
    if "_error" in d or "_http_error" in d:
        print(f"  ANALYZE FAILED: {d.get('_error') or d.get('_http_error')}")
        return False

    px  = d.get("current_price")
    ke  = d.get("cost_of_equity")
    kd  = d.get("cost_of_debt")
    wacc = d.get("wacc")
    tax = d.get("tax_rate")
    s1, s2 = d.get("stage1_growth"), d.get("stage2_growth")
    tg  = d.get("terminal_growth")
    iv  = d.get("intrinsic_value")
    lo, hi = d.get("iv_range_low"), d.get("iv_range_high")
    sc  = d.get("scenarios") or {}

    print(f"  price {num(px)}   fair value {num(iv)}   "
          f"tier {(d.get('priced_for') or {}).get('label')!r}")

    print("\n  -- discount rate --")
    kd_after_tax = None if (kd is None or tax is None) else kd * (1 - tax / 100.0)
    print(f"    cost of equity      {num(ke)}%")
    print(f"    cost of debt        {num(kd)}%   (after tax {num(kd_after_tax)}%, tax {num(tax)}%)")
    print(f"    WACC                {num(wacc)}%")
    if None not in (ke, kd_after_tax, wacc):
        check("WACC <= cost of equity", wacc <= ke + 1e-9,
              f"WACC {num(wacc)} vs Ke {num(ke)}")
        check("WACC >= after-tax cost of debt", wacc >= kd_after_tax - 1e-9,
              f"WACC {num(wacc)} vs Kd(1-t) {num(kd_after_tax)}")

    print("\n  -- growth --")
    print(f"    stage 1             {num(s1)}%")
    print(f"    stage 2             {num(s2)}%")
    print(f"    terminal            {num(tg)}%")
    if None not in (s1, s2):
        check("stage 2 differs from stage 1", abs(s1 - s2) > 1e-9,
              f"s1 {num(s1)}% vs s2 {num(s2)}%")
    if None not in (s2, tg):
        check("stage 2 >= terminal growth", s2 >= tg - 1e-9,
              f"s2 {num(s2)} vs tg {num(tg)}")

    print("\n  -- scenario cards --")
    vals = {}
    for name in SCENARIOS:
        row = sc.get(name) or {}
        vals[name] = row.get("value")
        print(f"    {name:5s} value {num(row.get('value')):>10s}   "
              f"wacc {num(row.get('wacc')):>6s}%  s1 {num(row.get('s1')):>6s}%  "
              f"weight {row.get('weight')}  basis {row.get('basis')!r}")
    print(f"    weighted        {num(sc.get('weighted')):>10s}   "
          f"(probability-weighted, basis {sc.get('weight_basis')!r})")
    if all(vals[n] is not None for n in SCENARIOS):
        check("bear <= base <= bull", vals["bear"] <= vals["base"] <= vals["bull"],
              f"{num(vals['bear'])} / {num(vals['base'])} / {num(vals['bull'])}")

    print("\n  -- hero range --")
    print(f"    range           {num(lo)} .. {num(hi)}   width {num(d.get('iv_range_width_pct'), 1)}%")
    print(f"    fair value      {num(iv)}")
    if None not in (lo, hi, iv):
        check("fair value inside hero range", lo <= iv <= hi,
              f"{num(iv)} vs [{num(lo)}, {num(hi)}]")
    if all(vals[n] is not None for n in ("bear", "bull")) and None not in (lo, hi):
        check("hero range ends == bear/bull scenario values",
              abs(vals["bear"] - lo) < 0.01 and abs(vals["bull"] - hi) < 0.01,
              f"scenarios [{num(vals['bear'])}, {num(vals['bull'])}] vs range [{num(lo)}, {num(hi)}]")

    print("\n  -- valuation history (latest entry) --")
    vh = get(base, "/api/valuation-history", ticker=ticker)
    if not vh.get("available"):
        print(f"    unavailable: {vh.get('reason') or vh.get('_error') or vh.get('_http_error')}")
    else:
        pts = vh.get("iv_points") or []
        if not pts:
            print(f"    no iv_points; keys = {sorted(vh.keys())}")
        else:
            last = pts[-1]
            print(f"    method {vh.get('method')!r}   {json.dumps(last, sort_keys=True)}")
            h_iv = last.get("iv")
            if h_iv is not None and iv:
                drift = (float(h_iv) / float(iv) - 1.0) * 100.0
                print(f"    latest history IV {num(h_iv)} vs hero fair value {num(iv)}"
                      f"   drift {drift:+.1f}%")
                check("history latest IV within 1% of hero fair value", abs(drift) < 1.0,
                      f"drift {drift:+.1f}%")
            g_used = last.get("growth_used")
            if g_used is not None and s1 is not None:
                check("history growth input == live stage-1 growth",
                      abs(float(g_used) - float(s1)) < 1e-9,
                      f"history {num(g_used)}% vs live s1 {num(s1)}%")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", default="5077")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--tickers", default=",".join(TICKERS))
    a = ap.parse_args()
    base = f"http://{a.host}:{a.port}"
    print(f"valuation_check against {base}")
    ok = True
    for t in [x.strip().upper() for x in a.tickers.split(",") if x.strip()]:
        ok = report(base, t) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
