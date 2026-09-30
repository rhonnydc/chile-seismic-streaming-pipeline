"""Generate plausible Chilean earthquakes without publishing them."""

import random
from datetime import UTC, datetime
from uuid import uuid4

from seismic_pipeline.common.earthquake_event import EarthquakeEvent

# City, region, latitude, longitude. Coordinates are approximate city centers.
CHILEAN_LOCATIONS = (
    ("Antofagasta", "Antofagasta", -23.65, -70.40),
    ("La Serena", "Coquimbo", -29.90, -71.25),
    ("Valparaíso", "Valparaíso", -33.05, -71.62),
    ("Santiago", "Metropolitana", -33.45, -70.67),
    ("Concepción", "Biobío", -36.82, -73.05),
    ("Valdivia", "Los Ríos", -39.81, -73.25),
    ("Puerto Montt", "Los Lagos", -41.47, -72.94),
    ("Punta Arenas", "Magallanes", -53.16, -70.91),
)


def generate_fake_earthquake(
    *, rng: random.Random | None = None, now: datetime | None = None
) -> EarthquakeEvent:
    """Build one simulated event using Chilean locations and UTC timestamps."""
    rng = rng if rng is not None else random.Random()
    timestamp = (now if now is not None else datetime.now(UTC)).astimezone(UTC)
    timestamp_utc = timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    city, region, latitude, longitude = rng.choice(CHILEAN_LOCATIONS)
    event_id = f"fake-{timestamp:%Y%m%d}-{uuid4().hex[:12]}"

    return EarthquakeEvent(
        event_id=event_id,
        source="simulator",
        source_event_id=event_id,
        event_time_utc=timestamp_utc,
        updated_at_utc=timestamp_utc,
        place=f"Near {city}, Chile",
        country="Chile",
        region=region,
        magnitude=round(rng.uniform(2.5, 6.5), 1),
        magnitude_type="ml",
        depth_km=round(rng.uniform(5.0, 180.0), 1),
        latitude=round(latitude + rng.uniform(-0.15, 0.15), 4),
        longitude=round(longitude + rng.uniform(-0.15, 0.15), 4),
        status="simulated",
        event_type="earthquake",
        tsunami=False,
        url=None,
        ingested_at=timestamp_utc,
    )
