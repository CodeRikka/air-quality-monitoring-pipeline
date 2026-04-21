import json
import re
from hashlib import sha256
from typing import Any


def build_natural_key(
    *,
    location_id: str,
    pollutant_code: str,
    metric_type: str,
    data_granularity: str,
    timestamp_start_utc: str,
) -> str:
    return "|".join(
        [
            location_id,
            pollutant_code,
            metric_type,
            data_granularity,
            timestamp_start_utc,
        ]
    )


def build_record_hash(record: dict[str, Any]) -> str:
    canonical = json.dumps(record, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()


def build_site_natural_location_id(
    *,
    state_code: str | None = None,
    county_code: str | None = None,
    site_number: str | None = None,
    aqsid: str | None = None,
    fallback_location_id: str | None = None,
) -> str:
    digits = re.sub(r"\D+", "", str(aqsid or ""))
    if len(digits) >= 12 and digits.startswith("840"):
        digits = digits[-9:]
    elif len(digits) >= 9:
        digits = digits[-9:]
    else:
        digits = ""

    if digits:
        state = digits[0:2]
        county = digits[2:5]
        site = digits[5:9]
        return f"aqs_site:{state}:{county}:{site}"

    state = str(state_code or "").strip()
    county = str(county_code or "").strip()
    site = str(site_number or "").strip()
    if state and county and site:
        return f"aqs_site:{state.zfill(2)}:{county.zfill(3)}:{site.zfill(4)}"

    return str(fallback_location_id or "unknown_location")
