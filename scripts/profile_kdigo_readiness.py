#!/usr/bin/env python3
"""
Profiles MIMIC source CSVs for KDIGO readiness and writes:
1) docs/reports/kdigo_readiness_report.md
2) configs/creatinine_itemids.yml
3) configs/urine_output_itemids.yml
"""

from __future__ import annotations

import csv
import os
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = ROOT / "DATA"
HOSPITAL = DATA_ROOT / "Hospital Data"
ICU = DATA_ROOT / "ICU DATA"
REPORTS = ROOT / "docs" / "reports"
CONFIGS = ROOT / "configs"

FILES = {
    "patients": HOSPITAL / "patients.csv",
    "admissions": HOSPITAL / "admissions-2.csv",
    "labevents": HOSPITAL / "labevents.csv",
    "d_labitems": HOSPITAL / "d_labitems.csv",
    "icustays": ICU / "icustays.csv",
    "outputevents": ICU / "outputevents.csv",
    "d_items": ICU / "d_items.csv",
}

REQUIRED = {
    "patients": ["subject_id"],
    "admissions": ["subject_id", "hadm_id", "admittime", "dischtime"],
    "icustays": ["subject_id", "hadm_id", "stay_id", "intime", "outtime"],
    "labevents": ["subject_id", "hadm_id", "itemid", "charttime", "valuenum", "valueuom"],
    "d_labitems": ["itemid", "label"],
    "outputevents": ["subject_id", "hadm_id", "stay_id", "itemid", "charttime", "value", "valueuom"],
    "d_items": ["itemid", "label"],
}


def ensure_dirs() -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    CONFIGS.mkdir(parents=True, exist_ok=True)


def read_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as f:
        return next(csv.reader(f))


def load_creatinine_itemids(d_labitems: Path) -> tuple[set[str], dict[str, str]]:
    item_to_label: dict[str, str] = {}
    creat_ids: set[str] = set()
    with d_labitems.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            iid = row.get("itemid", "")
            label = row.get("label", "")
            item_to_label[iid] = label
            if "creatinine" in label.lower():
                creat_ids.add(iid)
    return creat_ids, item_to_label


def load_urine_candidates(d_items: Path) -> tuple[set[str], dict[str, dict[str, str]]]:
    meta: dict[str, dict[str, str]] = {}
    urine_ids: set[str] = set()
    with d_items.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            iid = row.get("itemid", "")
            label = row.get("label", "")
            category = row.get("category", "")
            meta[iid] = {"label": label, "category": category}
            if "urine" in label.lower() or "urine" in category.lower():
                urine_ids.add(iid)
    return urine_ids, meta


def main() -> None:
    ensure_dirs()
    missing = [name for name, path in FILES.items() if not path.exists()]
    if missing:
        raise SystemExit(f"Missing required files: {missing}")

    headers = {name: read_header(path) for name, path in FILES.items()}
    missing_columns = {
        name: [c for c in REQUIRED[name] if c not in headers[name]] for name in REQUIRED
    }

    creat_like_ids, labitem_labels = load_creatinine_itemids(FILES["d_labitems"])
    urine_like_ids, d_items_meta = load_urine_candidates(FILES["d_items"])

    # Curated sets focused on KDIGO modeling.
    curated_creatinine_ids = {"50912", "51081", "52024", "52546"}
    curated_urine_ids = {"226559", "226560", "226561", "226567", "226627", "226631", "227489"}

    # Scan labevents for curated creatinine.
    creat_rows = 0
    creat_units = Counter()
    creat_nulls = Counter()
    creat_hadm: set[str] = set()
    with FILES["labevents"].open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            iid = row.get("itemid", "")
            if iid in curated_creatinine_ids:
                unit = (row.get("valueuom") or "").strip()
                if unit != "mg/dL":
                    continue
                creat_rows += 1
                for col in ("subject_id", "hadm_id", "charttime", "valuenum"):
                    if not row.get(col):
                        creat_nulls[col] += 1
                if row.get("hadm_id"):
                    creat_hadm.add(row["hadm_id"])
                creat_units[unit] += 1

    # Scan outputevents for curated urine.
    urine_rows = 0
    urine_units = Counter()
    urine_nulls = Counter()
    urine_stays: set[str] = set()
    with FILES["outputevents"].open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            iid = row.get("itemid", "")
            if iid in curated_urine_ids:
                urine_rows += 1
                for col in ("subject_id", "hadm_id", "stay_id", "charttime", "value"):
                    if not row.get(col):
                        urine_nulls[col] += 1
                if row.get("stay_id"):
                    urine_stays.add(row["stay_id"])
                urine_units[(row.get("valueuom") or "").strip()] += 1

    icu_stays: set[str] = set()
    icu_hadm: set[str] = set()
    with FILES["icustays"].open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("stay_id"):
                icu_stays.add(row["stay_id"])
            if row.get("hadm_id"):
                icu_hadm.add(row["hadm_id"])

    report = REPORTS / "kdigo_readiness_report.md"
    with report.open("w", encoding="utf-8") as out:
        out.write("# KDIGO Readiness Report\n\n")
        out.write("## Required Column Coverage\n")
        for name, miss in missing_columns.items():
            status = "OK" if not miss else f"MISSING: {', '.join(miss)}"
            out.write(f"- `{name}`: {status}\n")
        out.write("\n## Dictionary Discovery\n")
        out.write(f"- creatinine-like itemids in `d_labitems`: {len(creat_like_ids)}\n")
        out.write(f"- urine-like candidate itemids in `d_items`: {len(urine_like_ids)}\n")
        out.write("\n## Curated KDIGO Inputs\n")
        out.write(f"- curated serum creatinine itemids: {sorted(curated_creatinine_ids)}\n")
        out.write(f"- curated urine output itemids: {sorted(curated_urine_ids)}\n")
        out.write("\n## Coverage Metrics\n")
        out.write(f"- curated serum creatinine rows (mg/dL): {creat_rows}\n")
        out.write(f"- curated urine rows: {urine_rows}\n")
        out.write(f"- creatinine hadm overlap with icu hadm: {len(creat_hadm & icu_hadm)} / {len(creat_hadm)}\n")
        out.write(f"- urine stay overlap with icu stay: {len(urine_stays & icu_stays)} / {len(urine_stays)}\n")
        out.write("\n## Missingness in KDIGO-Critical Fields\n")
        out.write(f"- creatinine nulls: {dict(creat_nulls)}\n")
        out.write(f"- urine nulls: {dict(urine_nulls)}\n")
        out.write("\n## Unit Distributions\n")
        out.write(f"- creatinine units: {dict(creat_units)}\n")
        out.write(f"- urine units: {dict(urine_units)}\n")
        out.write("\n## Notes\n")
        out.write("- Keep KDIGO logic restricted to curated itemids and ICU-time windows.\n")
        out.write("- Add weight source later if strict urine mL/kg/hr rules are required.\n")

    with (CONFIGS / "creatinine_itemids.yml").open("w", encoding="utf-8") as out:
        out.write("itemids:\n")
        for iid in sorted(curated_creatinine_ids):
            out.write(f"  - '{iid}' # {labitem_labels.get(iid, 'unknown')}\n")

    with (CONFIGS / "urine_output_itemids.yml").open("w", encoding="utf-8") as out:
        out.write("itemids:\n")
        for iid in sorted(curated_urine_ids):
            meta = d_items_meta.get(iid, {"label": "unknown"})
            out.write(f"  - '{iid}' # {meta['label']}\n")

    print(f"Wrote report: {report}")
    print(f"Wrote itemid files in: {CONFIGS}")


if __name__ == "__main__":
    main()
