import pytest

from vlbimeta.telstate_inputs import (
    ANTENNA_ACTIVITY_KEY_TEMPLATE,
    ANTENNA_TARGET_KEY_TEMPLATE,
    OBSERVATION_SCRIPT_LOG_KEY,
    TIME_REFERENCE_KEY,
    describe_online_input_keys,
    format_antenna_key,
    load_antenna_activity_histories,
    load_antenna_target_histories,
    load_history,
    load_observation_script_log,
    load_time_reference_history,
)


class FakeTelstate:
    def __init__(self, values):
        self.values = values

    def __contains__(self, key):
        return key in self.values

    def __getitem__(self, key):
        value = self.values[key]
        if isinstance(value, KeyError):
            raise value
        return value

    def get_range(self, key, st=0):
        value = self.values[key]
        if isinstance(value, KeyError):
            raise value
        return value


def test_format_antenna_key_uses_default_templates():
    assert format_antenna_key(ANTENNA_ACTIVITY_KEY_TEMPLATE, "m001") == "m001_activity"
    assert format_antenna_key(ANTENNA_TARGET_KEY_TEMPLATE, "m001") == "m001_target"


def test_load_history_returns_samples_values_and_timestamps():
    telstate = FakeTelstate({"m001_activity": [("slew", 1.0), ("track", 2.0)]})

    history = load_history(telstate, "m001_activity")

    assert history.key == "m001_activity"
    assert history.samples == (("slew", 1.0), ("track", 2.0))
    assert history.values == ("slew", "track")
    assert history.timestamps == (1.0, 2.0)


def test_load_history_fails_clearly_for_required_missing_key():
    telstate = FakeTelstate({})

    with pytest.raises(KeyError, match="m001_activity"):
        load_history(telstate, "m001_activity")


def test_load_history_allows_optional_missing_key():
    telstate = FakeTelstate({})

    assert load_history(telstate, "m001_activity", required=False) is None


def test_load_history_fails_clearly_for_required_empty_history():
    telstate = FakeTelstate({"m001_activity": []})

    with pytest.raises(RuntimeError, match="no samples"):
        load_history(telstate, "m001_activity")


def test_load_antenna_histories_are_keyed_by_antenna():
    telstate = FakeTelstate(
        {
            "m001_activity": [("track", 1.0)],
            "m002_activity": [("slew", 1.0)],
            "m001_target": [("J1029A, radec, 10:29:13.95, 26:23:17.96", 1.0)],
            "m002_target": [("J1029A, radec, 10:29:13.95, 26:23:17.96", 1.0)],
        }
    )

    activities = load_antenna_activity_histories(telstate, ("m001", "m002"))
    targets = load_antenna_target_histories(telstate, ("m001", "m002"))

    assert activities["m001"].values == ("track",)
    assert activities["m002"].values == ("slew",)
    assert targets["m001"].key == "m001_target"
    assert targets["m002"].key == "m002_target"


def test_load_time_reference_history_uses_default_key():
    telstate = FakeTelstate({TIME_REFERENCE_KEY: [(12.5, 1.0), (12.6, 61.0)]})

    history = load_time_reference_history(telstate)

    assert history.key == TIME_REFERENCE_KEY
    assert history.values == (12.5, 12.6)


def test_load_observation_script_log_accepts_line_list():
    telstate = FakeTelstate(
        {
            OBSERVATION_SCRIPT_LOG_KEY: [
                "2026-01-28 10:02:18.009 INFO Observing scan: scan No0000",
                "2026-01-28 10:06:01.482 INFO New compound scan: 'scan No0001'",
            ]
        }
    )

    lines = load_observation_script_log(telstate)

    assert lines == (
        "2026-01-28 10:02:18.009 INFO Observing scan: scan No0000",
        "2026-01-28 10:06:01.482 INFO New compound scan: 'scan No0001'",
    )


def test_load_observation_script_log_accepts_newline_string():
    telstate = FakeTelstate({OBSERVATION_SCRIPT_LOG_KEY: "line one\nline two\n"})

    assert load_observation_script_log(telstate) == ("line one", "line two")


def test_load_observation_script_log_accepts_range_if_scalar_lookup_fails():
    telstate = FakeTelstate({OBSERVATION_SCRIPT_LOG_KEY: KeyError(OBSERVATION_SCRIPT_LOG_KEY)})
    telstate.get_range = lambda key, st=0: [("line one", 1.0), ("line two", 2.0)]

    assert load_observation_script_log(telstate) == ("line one", "line two")


def test_load_observation_script_log_fails_clearly_for_required_missing_key():
    telstate = FakeTelstate({})

    with pytest.raises(KeyError, match=OBSERVATION_SCRIPT_LOG_KEY):
        load_observation_script_log(telstate)


def test_describe_online_input_keys_records_contract():
    keys = describe_online_input_keys(("m001", "m002"))

    assert keys == {
        "antenna_activity": {"m001": "m001_activity", "m002": "m002_activity"},
        "antenna_target": {"m001": "m001_target", "m002": "m002_target"},
        "observation_script_log": OBSERVATION_SCRIPT_LOG_KEY,
        "time_reference": TIME_REFERENCE_KEY,
    }
