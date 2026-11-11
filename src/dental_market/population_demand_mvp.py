"""Exploratory resident allocation and finite-pool food attraction scenarios.

Existing M4 releases are never modified. Residential supports must declare
complete coverage per administrative area; missing coverage stays unallocated.
"""
from __future__ import annotations

import csv
import hashlib
import html
import io
import json
import math
import re
from datetime import date
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

CONTRACT = "population-demand-mvp-v1"
AGE_KEYS = ("0_14", "15_29", "30_49", "50_64", "65_plus")
QUALITY_LABELS = {"direct": "직접 통계", "estimated": "공간 추정", "missing": "자료 부족"}


class PopulationDemandError(ValueError):
    pass


def number(value: Any, label: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise PopulationDemandError(label + " must be numeric")
    try:
        result = float(value)
    except (ValueError, TypeError) as exc:
        raise PopulationDemandError(label + " must be numeric") from exc
    if not math.isfinite(result) or result < 0 or (positive and result == 0):
        raise PopulationDemandError(label + " must be finite and nonnegative")
    return result


def position(row: Mapping[str, Any]) -> tuple[float, float]:
    try:
        lon, lat = float(row["longitude"]), float(row["latitude"])
    except (KeyError, ValueError, TypeError) as exc:
        raise PopulationDemandError("WGS84 position missing") from exc
    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        raise PopulationDemandError("WGS84 position invalid")
    return lon, lat


def distance_m(a: Mapping[str, Any], b: Mapping[str, Any]) -> float:
    lon1, lat1 = position(a)
    lon2, lat2 = position(b)
    p1, p2 = math.radians(lat1), math.radians(lat2)
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 6371008.8 * 2 * math.asin(math.sqrt(min(1, h)))


def validate_config(config: Mapping[str, Any]) -> None:
    if config.get("contract") != CONTRACT or tuple(config.get("age_keys", [])) != AGE_KEYS:
        raise PopulationDemandError("config contract/age keys differ")
    last_radius, last_weight = 0.0, math.inf
    for radius, weight in config["distance_bands_m"]:
        radius = number(radius, "radius", positive=True)
        weight = number(weight, "distance weight")
        if radius <= last_radius or weight > last_weight:
            raise PopulationDemandError("distance bands must increase; weights must decrease")
        last_radius, last_weight = radius, weight
    if not config["distance_bands_m"]:
        raise PopulationDemandError("distance bands empty")
    for key in ("outside_weight", "food_reference_count", "food_signal_cap", "food_density_reference_per_km2"):
        number(config[key], key, positive=True)
    for key in ("food_diversity_bonus", "food_density_bonus"):
        number(config[key], key)
    if not config.get("scenarios") or config["scenarios"].get("off") != 0:
        raise PopulationDemandError("scenarios must include off=0")
    for value in config["scenarios"].values():
        number(value, "scenario")
    for profile in config["profiles"].values():
        if len(profile["age_weights"]) != len(AGE_KEYS):
            raise PopulationDemandError("profile age weights differ")
        for value in profile["age_weights"]:
            number(value, "age weight")
        number(profile["food_response"], "food response")


def parse_mois_age(raw: bytes, *, month: str, region_codes: Sequence[str]) -> list[dict]:
    """Retain only official 읍면동 codes; reconcile all 21 five-year bins."""
    if not re.fullmatch(r"\d{6}", month) or not 1 <= int(month[4:]) <= 12:
        raise PopulationDemandError("month must be YYYYMM")
    for encoding in ("utf-8-sig", "cp949"):
        try:
            decoded = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise PopulationDemandError("MOIS encoding unsupported")
    prefix = month[:4] + "년" + month[4:] + "월_계_"
    labels = [str(i) + "~" + str(i + 4) + "세" for i in range(0, 100, 5)] + ["100세 이상"]
    rows, seen = [], set()
    for row in csv.DictReader(io.StringIO(decoded)):
        match = re.search(r"\((\d{10})\)\s*$", row.get("행정구역", ""))
        if not match:
            continue
        code = match.group(1)
        if code[:5] not in region_codes or code[5:] == "00000":
            continue
        if code in seen:
            raise PopulationDemandError("duplicate MOIS admin code")
        seen.add(code)
        try:
            bins = [number(row[prefix + label].replace(",", ""), label) for label in labels]
            total = number(row[prefix + "총인구수"].replace(",", ""), "population")
        except KeyError as exc:
            raise PopulationDemandError("MOIS five-year age header missing") from exc
        if any(not value.is_integer() for value in bins + [total]) or sum(bins) != total:
            raise PopulationDemandError("MOIS age total does not reconcile")
        ages = dict(zip(AGE_KEYS, [sum(bins[:3]), sum(bins[3:6]), sum(bins[6:10]), sum(bins[10:13]), sum(bins[13:])]))
        rows.append({"admin_code": code, "admin_name": row["행정구역"][:match.start()].strip(), "source_as_of": month, "age_population": ages, "quality": "direct"})
    if not rows:
        raise PopulationDemandError("no eup-myeon-dong population rows")
    return sorted(rows, key=lambda r: r["admin_code"])


def allocate_population(populations: Sequence[Mapping], supports: Mapping) -> dict:
    """Normalize one common residential basis per area, never partial samples."""
    cells_by_code = defaultdict(list)
    seen = set()
    for cell in supports.get("cells", []):
        if not cell.get("cell_id") or cell["cell_id"] in seen:
            raise PopulationDemandError("duplicate or missing residential cell ID")
        seen.add(cell["cell_id"])
        position(cell)
        number(cell["weight"], "residential weight")
        cells_by_code[cell["admin_code"]].append(cell)
    allocated, pending, source_totals, seen_codes = [], [], dict.fromkeys(AGE_KEYS, 0.0), set()
    for pop in sorted(populations, key=lambda r: r["admin_code"]):
        code = pop["admin_code"]
        if not re.fullmatch(r"\d{10}", code) or code in seen_codes:
            raise PopulationDemandError("population key must be unique official 10-digit code")
        seen_codes.add(code)
        if set(pop["age_population"]) != set(AGE_KEYS) or not pop.get("source_as_of"):
            raise PopulationDemandError("population ages/date missing")
        ages = {a: number(pop["age_population"][a], a) for a in AGE_KEYS}
        for a in AGE_KEYS:
            source_totals[a] += ages[a]
        coverage = supports.get("areas", {}).get(code, {})
        cells = cells_by_code.get(code, [])
        basis = coverage.get("weight_basis")
        if coverage.get("complete") is not True or not cells:
            pending.append({**pop, "reason": "complete_residential_support_missing"})
            continue
        if basis not in ("dwelling_units", "residential_floor_area", "residential_land_area", "census_population") or not coverage.get("source_as_of"):
            raise PopulationDemandError("residential basis/date missing")
        total_weight = sum(number(c["weight"], "weight") for c in cells)
        if total_weight <= 0:
            pending.append({**pop, "reason": "residential_weights_zero"})
            continue
        fraction = number(coverage.get("coverage_fraction", 1.0), "coverage fraction")
        if fraction > 1:
            raise PopulationDemandError("coverage fraction exceeds one")
        has_age_weights = any("age_weights" in c for c in cells)
        if has_age_weights and not all(set(c.get("age_weights", {})) == set(AGE_KEYS) for c in cells):
            raise PopulationDemandError("age weights must cover every cell and band")
        age_denominators = {a: sum(number(c["age_weights"][a], "age support") for c in cells) for a in AGE_KEYS} if has_age_weights else {}
        if has_age_weights and any(age_denominators[a] == 0 and ages[a] > 0 for a in AGE_KEYS):
            raise PopulationDemandError("positive source age has no spatial support")
        if fraction < 1:
            pending.append({**pop, "age_population": {a: ages[a] * (1-fraction) for a in AGE_KEYS},
                            "reason": "suppressed_census_population_reserve", "allocation_available": True})
        for cell in sorted(cells, key=lambda r: r["cell_id"]):
            shares = {a: number(cell["age_weights"][a], "age support") / age_denominators[a] if age_denominators[a] else 0 for a in AGE_KEYS} if has_age_weights else {a: number(cell["weight"], "weight") / total_weight for a in AGE_KEYS}
            allocated.append({**cell, "source_as_of": pop["source_as_of"], "support_as_of": coverage["source_as_of"], "weight_basis": basis,
                              "age_population": {a: ages[a] * shares[a] * fraction for a in AGE_KEYS}, "quality": "estimated"})
    if set(cells_by_code) - seen_codes:
        raise PopulationDemandError("residential support has no matching population code")
    return {"cells": allocated, "pending_areas": pending, "source_age_totals": source_totals}


def _in_ring(point: tuple[float, float], ring: Sequence) -> bool:
    x, y = point
    inside = False
    for (x1, y1), (x2, y2) in zip(ring, ring[1:]):
        cross = (x - x1) * (y2 - y1) - (y - y1) * (x2 - x1)
        if abs(cross) < 1e-12 and min(x1, x2) <= x <= max(x1, x2) and min(y1, y2) <= y <= max(y1, y2):
            return True
        if (y1 > y) != (y2 > y) and x < (x2 - x1) * (y - y1) / (y2 - y1) + x1:
            inside = not inside
    return inside


def inside_geometry(cell: Mapping, geometry: Mapping) -> bool:
    if geometry.get("type") not in ("Polygon", "MultiPolygon"):
        raise PopulationDemandError("market geometry must be Polygon/MultiPolygon")
    polygons = [geometry["coordinates"]] if geometry["type"] == "Polygon" else geometry["coordinates"]
    point = position(cell)
    for rings in polygons:
        if not rings or any(len(r) < 4 or r[0] != r[-1] for r in rings):
            raise PopulationDemandError("market polygon ring invalid")
        for ring in rings:
            for lon, lat in ring:
                position({"longitude": lon, "latitude": lat})
        if _in_ring(point, rings[0]) and not any(_in_ring(point, r) for r in rings[1:]):
            return True
    return False


def food_signal(market: Mapping, config: Mapping) -> dict:
    count = number(market["food_count"], "food count")
    categories = market.get("food_category_counts")
    diversity = None
    if categories is not None:
        values = [number(v, "food category count") for v in categories.values()]
        if not math.isclose(sum(values), count):
            raise PopulationDemandError("food categories do not reconcile")
        diversity = 0.0 if count == 0 else 1 - sum((v / count) ** 2 for v in values)
    area = market.get("area_km2")
    density = count / number(area, "market area", positive=True) if area is not None else None
    size = min(config["food_signal_cap"], math.log1p(count) / math.log1p(config["food_reference_count"]))
    signal = size * (1 + config["food_diversity_bonus"] * (diversity or 0) + config["food_density_bonus"] *
                     (min(1, density / config["food_density_reference_per_km2"]) if density is not None else 0))
    return {"food_count": count, "size_signal": size, "diversity": diversity, "density_per_km2": density,
            "signal": signal, "quality": "estimated", "missing_optional_inputs": [k for k, v in (("food_diversity", diversity), ("food_density", density)) if v is None]}


def change_proxy(market: Mapping) -> dict:
    events = market.get("commercial_events")
    if events is None:
        return {"net_openings": None, "turnover": None, "population_growth": None, "quality": "missing"}
    try:
        start, end = date.fromisoformat(events["start"]), date.fromisoformat(events["end"])
    except (ValueError, KeyError, TypeError) as exc:
        raise PopulationDemandError("commercial event dates missing") from exc
    if end <= start or events.get("same_boundary") is not True:
        raise PopulationDemandError("commercial change requires ordered dates and same boundary")
    opened, closed, stock = (number(events[k], k) for k in ("opened", "closed", "opening_stock"))
    return {"net_openings": opened - closed, "turnover": (opened + closed) / stock if stock else None,
            "population_growth": None, "quality": "estimated", "start": events["start"], "end": events["end"]}


def distance_to_market_m(point: Mapping, market: Mapping) -> float:
    """Shortest straight distance to the polygon; local tangent plane in metres."""
    if inside_geometry(point, market["geometry"]):
        return 0.0
    lon, lat = point["longitude"], point["latitude"]
    sx = 111195.08 * math.cos(math.radians(lat))
    sy = 111195.08
    geometry = market["geometry"]
    polygons = [geometry["coordinates"]] if geometry["type"] == "Polygon" else geometry["coordinates"]
    best = float("inf")
    for polygon in polygons:
        for ring in polygon:
            for first, second in zip(ring, ring[1:]):
                ax, ay = (first[0]-lon)*sx, (first[1]-lat)*sy
                bx, by = (second[0]-lon)*sx, (second[1]-lat)*sy
                dx, dy = bx-ax, by-ay
                denom = dx*dx+dy*dy
                t = max(0., min(1., -(ax*dx+ay*dy)/denom)) if denom else 0.
                best = min(best, math.hypot(ax+t*dx, ay+t*dy))
    if not math.isfinite(best):
        raise PopulationDemandError("market boundary has no segments")
    return best


def compute(markets: Sequence[Mapping], populations: Sequence[Mapping], supports: Mapping, config: Mapping) -> dict:
    validate_config(config)
    markets = sorted(markets, key=lambda r: r["market_area_id"])
    if not markets or len({m["market_area_id"] for m in markets}) != len(markets):
        raise PopulationDemandError("markets empty or duplicate")
    for m in markets:
        position(m)
        inside_geometry(m, m["geometry"])
    allocation = allocate_population(populations, supports)
    cells = allocation["cells"]
    signals = {m["market_area_id"]: food_signal(m, config) for m in markets}
    weights, inside = {}, {}
    for cell in cells:
        for market in markets:
            key = cell["cell_id"], market["market_area_id"]
            distance = distance_to_market_m(cell, market) if config.get("distance_basis") == "boundary" else distance_m(cell, market)
            weights[key] = next((w for radius, w in config["distance_bands_m"] if distance <= radius), 0.0)
            inside[key] = inside_geometry(cell, market["geometry"])
    outputs = []
    allocation_totals = {a: sum(c["age_population"][a] for c in cells) for a in AGE_KEYS}
    for market in markets:
        mid = market["market_area_id"]
        internal = {a: sum(c["age_population"][a] for c in cells if inside[c["cell_id"], mid]) for a in AGE_KEYS}
        catchment = {a: sum(c["age_population"][a] for c in cells if weights[c["cell_id"], mid] > 0) for a in AGE_KEYS}
        total = sum(catchment.values())
        known = bool(cells) and (not allocation["pending_areas"] or any(weights[c["cell_id"], mid] > 0 for c in cells))
        incomplete = bool(allocation["pending_areas"])
        outputs.append({"market_area_id": mid, "label": market.get("label", mid), "region_codes": market.get("region_codes", []),
                        "existing_analysis": market.get("existing_analysis", {}), "source_as_of": market.get("source_as_of"),
                        "food_attraction": signals[mid], "population_quality": "estimated" if known else "missing",
                        "population_coverage": "partial" if known and incomplete else "available_support_only" if known else "missing",
                        "internal_age_population": internal if known else None, "catchment_age_population": catchment if known else None,
                        "age_shares": {a: catchment[a] / total if total else None for a in AGE_KEYS} if known else None,
                        "household_proxy": {"family_coexistence": min(catchment["0_14"], catchment["30_49"]) / total if total else None,
                                            "older_share": catchment["65_plus"] / total if total else None, "household_count": None},
                        "activity_proxy": {"food_attraction_signal": signals[mid]["signal"], "visitor_count": None, "worker_count": None},
                        "change_proxy": change_proxy(market),
                        "scenarios": {}})
    balances = {}
    for scenario, strength in config["scenarios"].items():
        outside_ages = dict.fromkeys(AGE_KEYS, 0.0)
        by_market = {m["market_area_id"]: dict.fromkeys(AGE_KEYS, 0.0) for m in markets}
        profile_results = {m["market_area_id"]: dict.fromkeys(config["profiles"], 0.0) for m in markets}
        for cell in cells:
            raw = {m["market_area_id"]: weights[cell["cell_id"], m["market_area_id"]] * (1 + strength * signals[m["market_area_id"]]["signal"]) for m in markets}
            denominator = config["outside_weight"] + sum(raw.values())
            for a in AGE_KEYS:
                outside_ages[a] += cell["age_population"][a] * config["outside_weight"] / denominator
                for mid, w in raw.items():
                    by_market[mid][a] += cell["age_population"][a] * w / denominator
            # Each profile has its own choice model; no extra people are added.
            for pid, profile in config["profiles"].items():
                demand = sum(cell["age_population"][a] * w for a, w in zip(AGE_KEYS, profile["age_weights"]))
                profile_raw = {m["market_area_id"]: weights[cell["cell_id"], m["market_area_id"]] *
                               (1 + strength * profile["food_response"] * signals[m["market_area_id"]]["signal"]) for m in markets}
                denom = config["outside_weight"] + sum(profile_raw.values())
                for mid, w in profile_raw.items():
                    profile_results[mid][pid] += demand * w / denom
        for output in outputs:
            mid = output["market_area_id"]
            output["scenarios"][scenario] = {"allocated_age_units": by_market[mid] if output["population_quality"] != "missing" else None,
                                             "relative_demand_units": profile_results[mid] if output["population_quality"] != "missing" else None}
        residual = {a: allocation_totals[a] - outside_ages[a] - sum(v[a] for v in by_market.values()) for a in AGE_KEYS}
        if any(abs(v) > max(1e-7, allocation_totals[a] * 1e-10) for a, v in residual.items()):
            raise PopulationDemandError("scenario population mass not conserved")
        balances[scenario] = {"outside_age_units": outside_ages, "residual": residual}
    overlaps = []
    for index, a in enumerate(markets):
        for b in markets[index + 1:]:
            common = sum(sum(c["age_population"].values()) for c in cells if weights[c["cell_id"], a["market_area_id"]] > 0 and weights[c["cell_id"], b["market_area_id"]] > 0)
            if common:
                overlaps.append({"market_a": a["market_area_id"], "market_b": b["market_area_id"], "shared_catchment_population": common})
    return {"contract": CONTRACT, "status": "computed" if cells and not allocation["pending_areas"] else "partial" if cells else "awaiting_residential_support",
            "config": config, "markets": outputs, "population_allocation": allocation,
            "residential_evidence": supports.get("evidence", {}),
            "scenario_balances": balances, "overlaps": overlaps, "canonical_publish_allowed": False,
            "notes": [("상권 경계까지의 최단 직선거리 기반이며 이동시간이 아님" if config.get("distance_basis") == "boundary" else "상권 중심점 직선거리 기반이며 이동시간이 아님"), "상권 표시 경계와 주거구역 대표점으로 계산한 공간 추정",
                      "배후인구와 경계 내부 인구는 상권 간 중첩될 수 있어 합산 금지", "작은 상권의 경계 내부 인구는 대표점 간격에 민감하므로 배후 비교를 우선", "수요는 가정에 따른 상대 단위이며 환자수·방문인구·매출이 아님",
                      "가구는 연령 공존 대리지표, 변화는 상업활동 지표이며 실제 가구수·인구 증가율이 아님",
                      "부분 주거 입력은 관측된 배후만 포함하며 0을 지역 전체의 인구 0으로 해석하지 않음",
                      ("집계구별 연령 분포로 동별 최신 인구를 정규화하고 집계구 내부는 균등 배분" if any("age_weights" in c for c in supports.get("cells", [])) else "동 내부 연령 구성은 동일하다고 가정하며 주거 가중치로 공간 배분")]}


def render_report(result: Mapping, path: Path, *, title: str) -> None:
    payload = json.dumps(result, ensure_ascii=False, allow_nan=False).replace("<", "\\u003c")
    template = '''<!doctype html><html lang="ko"><meta charset="utf-8"><title>__TITLE__</title>
<style>body{font:15px system-ui;margin:32px;background:#f5f7fa;color:#182437}h1{font-size:24px}table{border-collapse:collapse;width:100%;background:white}td,th{padding:10px;text-align:left;border-bottom:1px solid #ddd}th{background:#e8edf5}small{color:#526174}input,select{padding:9px;margin:8px}details{padding:12px;background:white;margin-top:14px}.note{padding:16px;background:#fff5d9}</style>
<h1>__TITLE__</h1><p id="source-note" class="note"></p><div class="note">__STATUS__ · 인구 __TOTAL__명 중 공간 배분 __ALLOCATED__명. 실제 유동인구·환자수·매출 추정이 아닙니다.</div>
<p>직선거리 기반의 배후 비교입니다. 가구·활동·변화는 대리지표로 표시합니다.</p><input id="search" placeholder="상권 이름·ID 검색"><label>집객 가정 <select id="scenario"></select></label>
<table><thead><tr><th>상권 / 음식점</th><th>품질</th><th>내부 / 배후 주민</th><th>배분된 상대 인구</th><th>가족 / 보철 / 심미 수요 단위</th></tr></thead><tbody id="rows"></tbody></table>
<section id="detail" style="background:white;padding:18px;margin-top:18px">상권 행을 선택하면 인구 구성·가구·활동·변화를 볼 수 있습니다.</section>
<details><summary>계산 가정·중첩·입력 근거</summary><pre id="evidence" style="white-space:pre-wrap;overflow-wrap:anywhere"></pre></details>
<script type="application/json" id="data">__PAYLOAD__</script><script>
const data=JSON.parse(document.getElementById('data').textContent),rows=document.getElementById('rows'),sel=document.getElementById('scenario');
const labels={off:'없음',weak:'약함',medium:'중간',strong:'강함'},quality={direct:'직접 통계',estimated:'공간 추정',missing:'자료 부족'};
Object.keys(data.config.scenarios).forEach(k=>{const o=document.createElement('option');o.value=k;o.textContent=labels[k]||k;sel.append(o)});
const fmt=v=>v==null?'—':Math.round(v).toLocaleString(),sum=o=>o==null?null:Object.values(o).reduce((a,b)=>a+b,0);
let selected=null;
function detail(m){selected=m;const box=document.getElementById('detail');box.replaceChildren();const put=(tag,t)=>{const e=document.createElement(tag);e.textContent=t;box.append(e)};put('h2',m.label);const ages={'0_14':'0–14세','15_29':'15–29세','30_49':'30–49세','50_64':'50–64세','65_plus':'65세 이상'};for(const [a,label]of Object.entries(ages))put('p',label+': '+fmt(m.catchment_age_population?.[a])+'명 / '+(m.age_shares?.[a]==null?'—':(m.age_shares[a]*100).toFixed(1)+'%'));put('p','가구 성격: 아동·부모 연령 공존 '+(m.household_proxy.family_coexistence==null?'자료 부족':(m.household_proxy.family_coexistence*100).toFixed(1)+'%')+' · 실제 가족 가구수는 추정하지 않음');put('p','활동 성격: 음식 집객 신호 '+m.food_attraction.signal.toFixed(2)+' · 방문객·직장인 수는 자료 없음');put('p','변화: 순개업 '+fmt(m.change_proxy.net_openings)+'개 · 회전율 '+(m.change_proxy.turnover==null?'자료 부족':(m.change_proxy.turnover*100).toFixed(1)+'%')+' · 인구 성장률로 해석하지 않음');const d=m.scenarios[sel.value].relative_demand_units,b=m.scenarios.off.relative_demand_units;if(d&&b)put('p','집객 없음 대비 수요 단위 변화: 가족 '+fmt(d.family-b.family)+' / 보철 '+fmt(d.prosthetic-b.prosthetic)+' / 심미 '+fmt(d.aesthetic-b.aesthetic));put('p','원천 시점: 상권 '+(m.source_as_of||'미제공'));const overlap=data.overlaps.filter(o=>o.market_a===m.market_area_id||o.market_b===m.market_area_id);put('p','배후 중첩 상권: '+overlap.length+'곳 · 중첩 주민수는 합산하지 않음');}
function draw(){rows.replaceChildren();const q=document.getElementById('search').value.toLowerCase();const filtered=data.markets.filter(m=>(m.label+' '+m.market_area_id).toLowerCase().includes(q));for(const m of filtered){const s=m.scenarios[sel.value],d=s.relative_demand_units;const tr=document.createElement('tr');tr.style.cursor='pointer';tr.onclick=()=>detail(m);for(const t of [m.label+' / '+fmt(m.food_attraction.food_count)+'개',quality[m.population_quality]+(m.population_coverage==='partial'?' (일부 배후)':''),fmt(sum(m.internal_age_population))+' / '+fmt(sum(m.catchment_age_population)),fmt(sum(s.allocated_age_units)),d?[fmt(d.family),fmt(d.prosthetic),fmt(d.aesthetic)].join(' / '):'주거 분포 입력 대기']){const td=document.createElement('td');td.textContent=t;tr.append(td)}rows.append(tr)}if(selected){if(filtered.includes(selected))detail(selected);else{selected=null;document.getElementById('detail').textContent='검색 결과에서 상권을 선택하세요.'}}}
document.getElementById('source-note').textContent=data.residential_evidence?.statistics_year?'주거 분포: '+data.residential_evidence.statistics_year+'년 집계구 통계 / '+data.residential_evidence.boundary_year+'년 경계 · '+data.residential_evidence.current_population_month+' 주민등록 인구로 정규화 · 집계구 내부 균등 배분 가정 · 북구 주민만 포함 · 좁은 경계 내부 인구는 표본 간격에 민감':'주거 분포 입력 상태는 표와 근거를 확인하세요.';sel.onchange=draw;document.getElementById('search').oninput=draw;draw();document.getElementById('evidence').textContent=JSON.stringify({notes:data.notes,residential_evidence:data.residential_evidence,config:data.config,inputs:data.inputs,pending_areas:data.population_allocation.pending_areas,overlaps:data.overlaps},null,2);
</script></html>'''
    total = sum(result["population_allocation"]["source_age_totals"].values())
    allocated = sum(sum(c["age_population"].values()) for c in result["population_allocation"]["cells"])
    status_label = {"computed": "계산 완료", "partial": "일부 주거지 계산", "awaiting_residential_support": "주거 분포 입력 대기"}[result["status"]]
    for token, value in {"__TITLE__": html.escape(title), "__STATUS__": status_label, "__TOTAL__": f"{total:,.0f}",
                         "__ALLOCATED__": f"{allocated:,.0f}", "__PAYLOAD__": payload}.items():
        template = template.replace(token, value)
    path.write_text(template, encoding="utf-8")


def write_release(result: dict, output: Path, *, title: str) -> Path:
    raw = json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()
    digest = hashlib.sha256(raw).hexdigest()
    root = output / ("pdm-" + digest[:24])
    root.mkdir(parents=True, exist_ok=True)
    bundle = root / "bundle.json"
    if bundle.exists() and bundle.read_bytes() != raw:
        raise PopulationDemandError("release content collision")
    bundle.write_bytes(raw)
    render_report(result, root / "report.html", title=title)
    return root
