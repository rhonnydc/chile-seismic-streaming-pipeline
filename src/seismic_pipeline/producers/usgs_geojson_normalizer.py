"""Map USGS GeoJSON features to the source-independent raw event model."""

import math
import unicodedata
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from seismic_pipeline.common.earthquake_event import EarthquakeEvent

# Deliberately small place-name rules. Unrecognized locations remain UNKNOWN.
CHILEAN_REGION_HINTS = (
    ("arica", "Arica y Parinacota"),
    ("iquique", "Tarapacá"),
    ("tarapaca", "Tarapacá"),
    ("antofagasta", "Antofagasta"),
    ("atacama", "Atacama"),
    ("copiapo", "Atacama"),
    ("coquimbo", "Coquimbo"),
    ("la serena", "Coquimbo"),
    ("valparaiso", "Valparaíso"),
    ("santiago", "Metropolitana"),
    ("concepcion", "Biobío"),
    ("biobio", "Biobío"),
    ("temuco", "La Araucanía"),
    ("araucania", "La Araucanía"),
    ("valdivia", "Los Ríos"),
    ("los rios", "Los Ríos"),
    ("puerto montt", "Los Lagos"),
    ("los lagos", "Los Lagos"),
    ("aysen", "Aysén"),
    ("punta arenas", "Magallanes"),
    ("magallanes", "Magallanes"),
)


def _plain_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.casefold())
    return "".join(character for character in normalized if not unicodedata.combining(character))


def infer_chilean_region(place: str) -> str:
    """Infer a region from a few explicit names, without an external geocoder."""
    if "chile" not in _plain_text(place):
        return "UNKNOWN"
    normalized_place = _plain_text(place)
    for hint, region in CHILEAN_REGION_HINTS:
        if hint in normalized_place:
            return region
    return "UNKNOWN"


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"USGS feature requires a nonempty {field}")
    return value.strip()


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"USGS feature requires numeric {field}")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"USGS feature requires finite {field}")
    return number


def _utc_timestamp(value: Any, field: str) -> str:
    milliseconds = _number(value, field)
    try:
        timestamp = datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
    except (OverflowError, OSError, ValueError) as error:
        raise ValueError(f"USGS feature has invalid {field}") from error
    return timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def normalize_usgs_feature(
    feature: Mapping[str, Any], *, now: datetime | None = None
) -> EarthquakeEvent:
    """Convert one feature; raise ValueError when required data is unusable."""
    if not isinstance(feature, Mapping):
        raise ValueError("USGS feature must be an object")
    source_id = _required_text(feature.get("id"), "id")
    properties = feature.get("properties")
    geometry = feature.get("geometry")
    if not isinstance(properties, Mapping) or not isinstance(geometry, Mapping):
        raise ValueError(f"USGS feature {source_id} requires properties and geometry")
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, (list, tuple)) or len(coordinates) < 3:
        raise ValueError(f"USGS feature {source_id} requires longitude, latitude, and depth")

    longitude = _number(coordinates[0], "longitude")
    latitude = _number(coordinates[1], "latitude")
    depth_km = _number(coordinates[2], "depth")
    magnitude = _number(properties.get("mag"), "magnitude")
    if not -180 <= longitude <= 180 or not -90 <= latitude <= 90:
        raise ValueError(f"USGS feature {source_id} has coordinates outside valid ranges")
    if depth_km < 0 or not 0 <= magnitude <= 10:
        raise ValueError(f"USGS feature {source_id} has measurements outside quality ranges")

    place = _required_text(properties.get("place"), "place")
    tsunami = properties.get("tsunami")
    if tsunami not in (0, 1) or isinstance(tsunami, bool):
        raise ValueError(f"USGS feature {source_id} requires tsunami as 0 or 1")
    magnitude_type = properties.get("magType")
    if magnitude_type is not None:
        magnitude_type = _required_text(magnitude_type, "magType")
    url = properties.get("url")
    if url is not None:
        url = _required_text(url, "url")

    received_at = now if now is not None else datetime.now(UTC)
    if received_at.tzinfo is None:
        raise ValueError("now must include a UTC offset")
    ingested_at = (
        received_at.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )

    return EarthquakeEvent(
        event_id=f"usgs:{source_id}",
        source="usgs",
        source_event_id=source_id,
        event_time_utc=_utc_timestamp(properties.get("time"), "time"),
        updated_at_utc=_utc_timestamp(properties.get("updated"), "updated"),
        place=place,
        country="Chile" if "chile" in _plain_text(place) else "UNKNOWN",
        region=infer_chilean_region(place),
        magnitude=magnitude,
        magnitude_type=magnitude_type,
        depth_km=depth_km,
        latitude=latitude,
        longitude=longitude,
        status=_required_text(properties.get("status"), "status"),
        event_type=_required_text(properties.get("type"), "type"),
        tsunami=bool(tsunami),
        url=url,
        ingested_at=ingested_at,
    )
