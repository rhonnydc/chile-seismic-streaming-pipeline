"""Verify the boundary between USGS GeoJSON and the internal event model."""

from datetime import UTC, datetime

import pytest

from seismic_pipeline.common.earthquake_event import EarthquakeEvent
from seismic_pipeline.producers.usgs_geojson_normalizer import normalize_usgs_feature

RECEIVED_AT = datetime(2026, 10, 11, 18, 21, tzinfo=UTC)


def feature() -> dict:
    return {
        "id": "us7000abcd",
        "properties": {
            "mag": 4.2,
            "magType": "mb",
            "place": "35 km NE of La Serena, Chile",
            "time": 1790646900000,
            "updated": 1790647200000,
            "status": "reviewed",
            "tsunami": 0,
            "type": "earthquake",
            "url": "https://earthquake.usgs.gov/example",
        },
        "geometry": {"coordinates": [-71.2, -29.9, 45.8]},
    }


def test_feature_maps_to_internal_event() -> None:
    event = normalize_usgs_feature(feature(), now=RECEIVED_AT)

    assert isinstance(event, EarthquakeEvent)
    assert (event.event_id, event.source, event.source_event_id) == (
        "usgs:us7000abcd",
        "usgs",
        "us7000abcd",
    )
    assert (event.longitude, event.latitude, event.depth_km) == (-71.2, -29.9, 45.8)
    assert (event.magnitude, event.magnitude_type) == (4.2, "mb")
    assert (event.event_time_utc, event.updated_at_utc) == (
        "2026-09-29T01:55:00.000Z",
        "2026-09-29T02:00:00.000Z",
    )
    assert (event.place, event.country, event.region) == (
        "35 km NE of La Serena, Chile",
        "Chile",
        "Coquimbo",
    )
    assert (event.status, event.event_type, event.tsunami) == (
        "reviewed",
        "earthquake",
        False,
    )
    assert event.url == "https://earthquake.usgs.gov/example"
    assert event.ingested_at == "2026-10-11T18:21:00.000Z"


def test_optional_magnitude_type_and_unknown_region() -> None:
    raw = feature()
    raw["properties"].pop("magType")
    raw["properties"]["place"] = "120 km off the coast of Chile"

    event = normalize_usgs_feature(raw, now=RECEIVED_AT)

    assert event.magnitude_type is None
    assert event.country == "Chile"
    assert event.region == "UNKNOWN"


def test_place_outside_chile_is_not_labeled_chilean() -> None:
    raw = feature()
    raw["properties"]["place"] = "10 km south of Lima, Peru"

    event = normalize_usgs_feature(raw, now=RECEIVED_AT)

    assert event.country == "UNKNOWN"
    assert event.region == "UNKNOWN"


@pytest.mark.parametrize("tsunami, expected", [(0, False), (1, True)])
def test_tsunami_flag_is_boolean(tsunami: int, expected: bool) -> None:
    raw = feature()
    raw["properties"]["tsunami"] = tsunami

    assert normalize_usgs_feature(raw, now=RECEIVED_AT).tsunami is expected


@pytest.mark.parametrize(
    ("change", "error"),
    [
        (lambda raw: raw.pop("id"), "id"),
        (
            lambda raw: raw["geometry"].update(coordinates=[-71.2]),
            "longitude, latitude, and depth",
        ),
        (lambda raw: raw["properties"].update(mag=None), "magnitude"),
        (lambda raw: raw["geometry"].update(coordinates=[-71.2, None, 45.8]), "latitude"),
    ],
)
def test_incomplete_feature_is_rejected(change, error: str) -> None:
    raw = feature()
    change(raw)

    with pytest.raises(ValueError, match=error):
        normalize_usgs_feature(raw, now=RECEIVED_AT)
