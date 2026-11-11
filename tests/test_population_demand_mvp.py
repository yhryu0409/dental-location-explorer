import copy
import json
from pathlib import Path

import pytest

from dental_market.population_demand_mvp import (
    AGE_KEYS, PopulationDemandError, allocate_population, compute, food_signal,
    inside_geometry, parse_mois_age, render_report, write_release,
)

ROOT = Path(__file__).resolve().parents[1]
CONFIG = json.loads((ROOT / "metadata/population_demand_mvp_v1.json").read_text())


def market(mid="a", food=10, lon=129.0):
    return {"market_area_id": mid, "label": mid, "longitude": lon, "latitude": 35.0,
            "food_count": food, "geometry": {"type": "Polygon", "coordinates": [[[lon-.001,34.999],[lon+.001,34.999],[lon+.001,35.001],[lon-.001,35.001],[lon-.001,34.999]]]}}


def inputs():
    pop = [{"admin_code": "2632051000", "source_as_of": "202608", "age_population": dict.fromkeys(AGE_KEYS, 100)}]
    supports = {"areas": {"2632051000": {"complete": True, "weight_basis": "dwelling_units", "source_as_of": "202608"}},
                "cells": [{"cell_id": "one", "admin_code": "2632051000", "weight": 1, "longitude": 129.0, "latitude": 35.0},
                          {"cell_id": "two", "admin_code": "2632051000", "weight": 3, "longitude": 129.005, "latitude": 35.0}]}
    return pop, supports


def test_spatial_mass_and_outside_choice_are_conserved():
    pop, supports = inputs()
    result = compute([market("a",10), market("b",200,129.004)], pop, supports, CONFIG)
    assert result["status"] == "computed"
    assert result["population_allocation"]["cells"][0]["age_population"]["0_14"] == 25
    for a in AGE_KEYS:
        assert sum(c["age_population"][a] for c in result["population_allocation"]["cells"]) == 100
        for scenario, balance in result["scenario_balances"].items():
            assigned = sum(m["scenarios"][scenario]["allocated_age_units"][a] for m in result["markets"])
            assert assigned + balance["outside_age_units"][a] == pytest.approx(100)
            assert balance["outside_age_units"][a] > 0
    assert result["overlaps"][0]["shared_catchment_population"] == 500


def test_attraction_changes_choice_not_source_population_and_profile_response():
    pop, supports = inputs()
    result = compute([market("a",10),market("b",200)],pop,supports,CONFIG)
    a,b = result["markets"]
    assert b["scenarios"]["off"] == a["scenarios"]["off"]
    assert b["scenarios"]["strong"]["allocated_age_units"]["0_14"] > a["scenarios"]["strong"]["allocated_age_units"]["0_14"]
    assert result["population_allocation"]["source_age_totals"] == dict.fromkeys(AGE_KEYS,100)
    p=CONFIG["profiles"]
    for scenario in CONFIG["scenarios"]:
        for pid in p:
            total=sum(m["scenarios"][scenario]["relative_demand_units"][pid] for m in result["markets"])
            assert total <= sum(w*100 for w in p[pid]["age_weights"])


def test_missing_and_partial_support_never_force_allocate_population():
    pop,supports=inputs()
    empty=compute([market()],pop,{"areas":{},"cells":[]},CONFIG)
    assert empty["status"] == "awaiting_residential_support"
    assert empty["markets"][0]["catchment_age_population"] is None
    supports["areas"]["2632051000"]["complete"]=False
    assert allocate_population(pop,supports)["cells"] == []
    extra=copy.deepcopy(pop[0]);extra["admin_code"]="2632052000"
    partial=compute([market()],pop+[extra],inputs()[1],CONFIG)
    assert partial["status"] == "partial"
    assert partial["markets"][0]["population_coverage"] == "partial"
    assert len(partial["population_allocation"]["pending_areas"]) == 1


def test_far_population_goes_outside_and_known_zero_is_zero():
    pop,supports=inputs()
    result=compute([market(lon=130)],pop,supports,CONFIG)
    assert result["markets"][0]["catchment_age_population"] == dict.fromkeys(AGE_KEYS,0)
    assert result["scenario_balances"]["strong"]["outside_age_units"] == dict.fromkeys(AGE_KEYS,100)


@pytest.mark.parametrize("bad", [float("nan"),float("inf"),-1,True,None])
def test_invalid_numeric_input_is_rejected(bad):
    pop,supports=inputs();supports["cells"][0]["weight"]=bad
    with pytest.raises(PopulationDemandError):
        compute([market()],pop,supports,CONFIG)


def test_duplicate_keys_and_optional_category_reconciliation():
    pop,supports=inputs()
    with pytest.raises(PopulationDemandError): allocate_population(pop+pop,supports)
    supports["cells"].append(supports["cells"][0])
    with pytest.raises(PopulationDemandError): allocate_population(pop,supports)
    m=market();m["food_category_counts"]={"korean":2}
    with pytest.raises(PopulationDemandError): food_signal(m,CONFIG)


def test_holes_and_multiple_polygons():
    m=market();g=copy.deepcopy(m["geometry"])
    g["coordinates"].append([[128.9995,34.9995],[129.0005,34.9995],[129.0005,35.0005],[128.9995,35.0005],[128.9995,34.9995]])
    assert not inside_geometry(m,g)
    assert inside_geometry({"longitude":129.0008,"latitude":35},g)
    assert inside_geometry(m,{"type":"MultiPolygon","coordinates":[m["geometry"]["coordinates"]]})


def test_mois_codes_age_reconciliation_and_encoding():
    prefix="2026년08월_계_"
    labels=[str(i)+"~"+str(i+4)+"세" for i in range(0,100,5)]+["100세 이상"]
    text=",".join(["행정구역",prefix+"총인구수"]+[prefix+l for l in labels])+"\n"
    text+="북구 (2632000000),21,"+",".join(["1"]*21)+"\n"
    text+="구포1동 (2632051000),21,"+",".join(["1"]*21)+"\n"
    rows=parse_mois_age(text.encode("cp949"),month="202608",region_codes=["26320"])
    assert len(rows)==1
    assert rows[0]["age_population"]==dict(zip(AGE_KEYS,[3,3,4,3,8]))
    with pytest.raises(PopulationDemandError): parse_mois_age(text.replace(",21,",",22,").encode(),month="202608",region_codes=["26320"])


def test_deterministic_release_html_safety_and_commercial_change(tmp_path):
    pop,supports=inputs();m=market();m["label"]="</script><script>alert(1)</script>"
    m["commercial_events"]={"start":"2025-06-30","end":"2026-06-30","same_boundary":True,"opened":4,"closed":2,"opening_stock":10}
    result=compute([m],pop,supports,CONFIG)
    assert result["markets"][0]["change_proxy"]["turnover"] == .6
    assert result["markets"][0]["change_proxy"]["population_growth"] is None
    assert compute([m],pop,{**supports,"cells":list(reversed(supports["cells"]))},CONFIG)==result
    root=write_release(result,tmp_path,title="Example")
    assert write_release(result,tmp_path,title="Example")==root
    text=(root/"report.html").read_text()
    assert "</script><script>alert(1)</script>" not in text
    assert "가족" in text
    m["commercial_events"]["same_boundary"]=False
    with pytest.raises(PopulationDemandError): compute([m],pop,supports,CONFIG)


def test_age_specific_support_and_suppression_reserve_conserve_each_age():
    pop, supports = inputs()
    supports["areas"][pop[0]["admin_code"]].update(weight_basis="census_population", coverage_fraction=.8)
    supports["cells"][0]["age_weights"] = dict(zip(AGE_KEYS,[9,1,8,2,7]))
    supports["cells"][1]["age_weights"] = dict(zip(AGE_KEYS,[1,9,2,8,3]))
    result = allocate_population(pop,supports)
    assert result["cells"][0]["age_population"]["0_14"] == pytest.approx(72)
    assert result["cells"][0]["age_population"]["15_29"] == pytest.approx(8)
    for a in AGE_KEYS:
        assert sum(c["age_population"][a] for c in result["cells"]) + sum(p["age_population"][a] for p in result["pending_areas"]) == pytest.approx(100)


def test_age_support_requires_all_cells_and_positive_denominators():
    pop,supports=inputs()
    supports["cells"][0]["age_weights"]=dict.fromkeys(AGE_KEYS,1)
    with pytest.raises(PopulationDemandError):allocate_population(pop,supports)
    supports["cells"][1]["age_weights"]=dict.fromkeys(AGE_KEYS,1)
    for c in supports["cells"]:c["age_weights"]["0_14"]=0
    with pytest.raises(PopulationDemandError):allocate_population(pop,supports)
