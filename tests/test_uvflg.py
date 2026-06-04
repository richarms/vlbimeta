from datetime import date, datetime, timezone

import pytest

from vlbimeta.manifest import ScanManifestEntry
from vlbimeta.telstate_inputs import TelstateHistory
from vlbimeta.uvflg import (
    FlagRange,
    build_scan_time_axis,
    evaluate_uvflg,
    format_timerang,
    generate_uvflg_lines,
    included_scans,
    nearest_history_value,
    normalise_target_name,
    render_uvflg_lines,
)


def dt(hour, minute, second):
    return datetime(2026, 1, 28, hour, minute, second, tzinfo=timezone.utc)


def ts(hour, minute, second):
    return dt(hour, minute, second).timestamp()


def manifest_entry(scan_id, start_time, end_time, target_name, *, include=True):
    return ScanManifestEntry(
        scan_id=scan_id,
        start_time=start_time,
        end_time=end_time,
        target_name=target_name,
        tags=("science",) if include else ("calibrator",),
        include=include,
        reason="science scan" if include else "excluded calibrator scan",
    )


def history(key, samples):
    return TelstateHistory(key=key, samples=tuple(samples))


def activity_histories(values):
    return {antenna: history(f"{antenna}_activity", samples) for antenna, samples in values.items()}


def target_histories(values):
    return {antenna: history(f"{antenna}_target", samples) for antenna, samples in values.items()}


def test_included_scans_filters_manifest_entries():
    entries = [
        manifest_entry("scan cal00", dt(10, 0, 0), dt(10, 0, 1), "J1939-6342", include=False),
        manifest_entry("scan No0001", dt(10, 1, 0), dt(10, 1, 1), "J1029A"),
    ]

    assert [entry.scan_id for entry in included_scans(entries)] == ["scan No0001"]


def test_build_scan_time_axis_uses_included_scan_windows():
    entries = [
        manifest_entry("scan No0001", dt(10, 0, 0), dt(10, 0, 2), "J1029A"),
        manifest_entry("mpcc No0001", dt(10, 1, 0), dt(10, 1, 2), "J1939-6342", include=False),
    ]

    timestamps, targets = build_scan_time_axis(entries, interval=1.0)

    assert timestamps == (ts(10, 0, 0), ts(10, 0, 1), ts(10, 0, 2))
    assert targets == ("J1029A", "J1029A", "J1029A")


def test_nearest_history_value_uses_nearest_timestamp():
    sensor = history("m001_activity", [("slew", 10.0), ("track", 20.0), ("stop", 30.0)])

    assert nearest_history_value(sensor, 14.9) == "slew"
    assert nearest_history_value(sensor, 15.0) == "slew"
    assert nearest_history_value(sensor, 15.1) == "track"
    assert nearest_history_value(sensor, 40.0) == "stop"


def test_normalise_target_name_handles_katpoint_and_malformed_values():
    assert normalise_target_name("*J1029A, radec, 10:29:13.95, 26:23:17.96") == "J1029A"
    assert normalise_target_name("J1029A|alias, radec, 10:29:13.95, 26:23:17.96") == "J1029A"
    assert normalise_target_name("  *J1029A  ") == "J1029A"


def test_evaluate_uvflg_all_antennas_valid_has_no_flags():
    entries = [manifest_entry("scan No0001", dt(10, 0, 0), dt(10, 0, 2), "J1029A")]
    activities = activity_histories(
        {
            "m001": [("track", ts(10, 0, 0))],
            "m002": [("track", ts(10, 0, 0))],
            "m003": [("track", ts(10, 0, 0))],
        }
    )
    targets = target_histories(
        {
            "m001": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
            "m002": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
            "m003": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
        }
    )

    evaluation = evaluate_uvflg(entries, activities, targets, quorum=0.67, interval=1.0)

    assert evaluation.threshold == 3
    assert [sample.data_valid for sample in evaluation.samples] == [True, True, True]
    assert evaluation.flag_ranges == ()


def test_evaluate_uvflg_allows_one_bad_antenna_below_quorum_threshold():
    entries = [manifest_entry("scan No0001", dt(10, 0, 0), dt(10, 0, 1), "J1029A")]
    activities = activity_histories(
        {
            "m001": [("track", ts(10, 0, 0))],
            "m002": [("track", ts(10, 0, 0))],
            "m003": [("slew", ts(10, 0, 0))],
        }
    )
    targets = target_histories(
        {
            "m001": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
            "m002": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
            "m003": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
        }
    )

    evaluation = evaluate_uvflg(entries, activities, targets, quorum=0.66, interval=1.0)

    assert evaluation.threshold == 2
    assert evaluation.flag_ranges == ()


def test_evaluate_uvflg_flags_when_activity_quorum_fails():
    entries = [manifest_entry("scan No0001", dt(10, 0, 0), dt(10, 0, 2), "J1029A")]
    activities = activity_histories(
        {
            "m001": [("track", ts(10, 0, 0))],
            "m002": [("slew", ts(10, 0, 0))],
            "m003": [("track", ts(10, 0, 0))],
        }
    )
    targets = target_histories(
        {
            "m001": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
            "m002": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
            "m003": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
        }
    )

    evaluation = evaluate_uvflg(entries, activities, targets, quorum=0.90, interval=1.0)

    assert evaluation.threshold == 3
    assert evaluation.flag_ranges == (FlagRange(ts(10, 0, 0), ts(10, 0, 2)),)


def test_evaluate_uvflg_flags_when_target_quorum_fails_for_single_sample():
    entries = [manifest_entry("scan No0001", dt(10, 0, 0), dt(10, 0, 2), "J1029A")]
    activities = activity_histories(
        {
            "m001": [("track", ts(10, 0, 0))],
            "m002": [("track", ts(10, 0, 0))],
            "m003": [("track", ts(10, 0, 0))],
        }
    )
    targets = target_histories(
        {
            "m001": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
            "m002": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))],
            "m003": [
                ("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0)),
                ("J1939-6342, radec, 19:39:25.03, -63:42:45.7", ts(10, 0, 1)),
                ("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 2)),
            ],
        }
    )

    evaluation = evaluate_uvflg(entries, activities, targets, quorum=0.90, interval=1.0)

    assert [sample.target_count for sample in evaluation.samples] == [3, 2, 3]
    assert evaluation.flag_ranges == (FlagRange(ts(10, 0, 1), ts(10, 0, 1)),)


def test_evaluate_uvflg_requires_included_scan_samples():
    entries = [manifest_entry("scan cal00", dt(10, 0, 0), dt(10, 0, 1), "J1939-6342", include=False)]
    activities = activity_histories({"m001": [("track", ts(10, 0, 0))]})
    targets = target_histories({"m001": [("J1939-6342, radec, 19:39:25.03, -63:42:45.7", ts(10, 0, 0))]})

    with pytest.raises(ValueError, match="No included scan samples"):
        evaluate_uvflg(entries, activities, targets)


def test_evaluate_uvflg_requires_matching_antenna_history_sets():
    entries = [manifest_entry("scan No0001", dt(10, 0, 0), dt(10, 0, 1), "J1029A")]
    activities = activity_histories({"m001": [("track", ts(10, 0, 0))]})
    targets = target_histories({"m002": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))]})

    with pytest.raises(ValueError, match="history sets differ"):
        evaluate_uvflg(entries, activities, targets)


def test_render_uvflg_lines_uses_timerang_format():
    lines = render_uvflg_lines(
        "me",
        "ep134f",
        (FlagRange(ts(10, 5, 59), ts(10, 6, 0)),),
        created_date=date(2026, 6, 4),
    )

    assert lines == (
        "! The following is flagging information for ME in experiment ep134f,",
        "! extracted on 2026-06-04.",
        "opcode='FLAG'",
        "dtimrang = 1   timeoff=0",
        "ant_name='ME' timerang=028,10,05,59, 028,10,06,00 reason='Antenna off source' /",
    )
    assert format_timerang(ts(10, 5, 59), ts(10, 6, 0)) == "028,10,05,59, 028,10,06,00"


def test_generate_uvflg_lines_returns_evaluation_and_lines():
    entries = [manifest_entry("scan No0001", dt(10, 0, 0), dt(10, 0, 0), "J1029A")]
    activities = activity_histories({"m001": [("slew", ts(10, 0, 0))]})
    targets = target_histories({"m001": [("J1029A, radec, 10:29:13.95, 26:23:17.96", ts(10, 0, 0))]})

    evaluation, lines = generate_uvflg_lines(
        entries,
        activities,
        targets,
        station_code="me",
        experiment="ep134f",
        created_date=date(2026, 6, 4),
    )

    assert evaluation.flag_ranges == (FlagRange(ts(10, 0, 0), ts(10, 0, 0)),)
    assert lines[-1] == "ant_name='ME' timerang=028,10,00,00, 028,10,00,00 reason='Antenna off source' /"
