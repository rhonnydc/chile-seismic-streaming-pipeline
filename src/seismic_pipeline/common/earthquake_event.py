"""Source-independent earthquake event carried by the raw event stream."""

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class EarthquakeEvent:
    """Normalized seismic event; UTC timestamps use ISO 8601 strings."""

    event_id: str
    source: str
    source_event_id: str
    event_time_utc: str
    updated_at_utc: str
    place: str
    country: str
    region: str
    magnitude: float
    magnitude_type: str | None
    depth_km: float
    latitude: float
    longitude: float
    status: str
    event_type: str
    tsunami: bool
    url: str | None
    ingested_at: str

    def to_dict(self) -> dict[str, str | float | bool | None]:
        """Return a JSON-serializable representation of the event."""
        return asdict(self)
