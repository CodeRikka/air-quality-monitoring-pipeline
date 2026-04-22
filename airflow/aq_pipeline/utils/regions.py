import os
import re
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
REGIONS_CONFIG = REPO_ROOT / "configs" / "regions.yaml"


def load_regions(*, env_var: str = "AQS_BOOTSTRAP_REGION_IDS") -> list[dict[str, Any]]:
    with REGIONS_CONFIG.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}

    regions = [region for region in payload.get("regions", []) if isinstance(region, dict)]
    requested_region_ids = {
        region_id.strip()
        for region_id in os.getenv(env_var, "").split(",")
        if region_id.strip()
    }
    if not requested_region_ids:
        return regions

    selected = [
        region
        for region in regions
        if str(region.get("id", "")).strip() in requested_region_ids
    ]
    if selected:
        return selected
    raise ValueError(f"No regions matched {env_var}.")


def normalize_aqsid(aqsid: str | None) -> str | None:
    digits = re.sub(r"\D+", "", str(aqsid or ""))
    if len(digits) >= 12 and digits.startswith("840"):
        return digits[-9:]
    if len(digits) >= 9:
        return digits[-9:]
    return None


def iter_region_state_codes(region: dict[str, Any]) -> set[str]:
    return {
        str(value).strip().zfill(2)
        for value in region.get("state_codes", [])
        if str(value).strip()
    }


def iter_region_state_county_pairs(region: dict[str, Any]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    explicit_pairs = region.get("state_county_pairs", [])
    if not isinstance(explicit_pairs, list):
        return pairs

    for pair in explicit_pairs:
        if not isinstance(pair, dict):
            continue
        state = str(pair.get("state_code", "")).strip()
        county = str(pair.get("county_code", "")).strip()
        if state and county:
            pairs.add((state.zfill(2), county.zfill(3)))
    return pairs


def airnow_aqsid_in_regions(*, aqsid: str | None, regions: list[dict[str, Any]]) -> bool:
    normalized_aqsid = normalize_aqsid(aqsid)
    if not normalized_aqsid:
        return False

    allowed_states = set().union(*(iter_region_state_codes(region) for region in regions))
    allowed_pairs = set().union(*(iter_region_state_county_pairs(region) for region in regions))
    if not allowed_states and not allowed_pairs:
        return True

    state_code = normalized_aqsid[0:2]
    county_code = normalized_aqsid[2:5]
    return state_code in allowed_states or (state_code, county_code) in allowed_pairs
