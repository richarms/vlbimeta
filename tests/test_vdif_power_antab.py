from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

pytest.importorskip("matplotlib")
from vlbimeta.vdif_power_antab import StationCalibrator


class FakeTelstate:
    def __init__(self, values):
        self.values = values

    def __contains__(self, key):
        return key in self.values

    def __getitem__(self, key):
        if key not in self.values:
            raise KeyError(key)
        return self.values[key]

    def get_range(self, key, st=0):
        if key not in self.values:
            raise KeyError(key)
        value = self.values[key]
        if isinstance(value, list):
            return value
        raise TypeError(f"Key {key} does not contain a range-like value")


@dataclass
class FakeAnt:
    name: str


def _make_dataset(telstate, freqs, ants=None):
    if ants is None:
        ants = [FakeAnt("m000"), FakeAnt("m001")]
    return SimpleNamespace(
        ants=ants,
        freqs=np.asarray(freqs, dtype=np.float64),
        source=SimpleNamespace(telstate=telstate),
    )


def _make_calibrator(tmp_path: Path, monkeypatch, telstate, freqs, fc_chans, ants=None):
    calibrator = StationCalibrator(
        obs_cbid="177",
        exp_name="es116a",
        fc_chans=fc_chans,
        bw_chan=100.0,
        rdb_dir=tmp_path,
        out_dir=tmp_path,
        telstate_source=None,
        gain_tab=0.5,
    )
    monkeypatch.setattr(calibrator, "_open_dataset", lambda: _make_dataset(telstate, freqs, ants=ants))
    return calibrator


def test_compute_cal_sols_supports_single_bandpass_part(tmp_path: Path, monkeypatch) -> None:
    g = np.ones((2, 2), dtype=np.complex64)
    b0 = np.ones((2, 2, 2), dtype=np.complex64)
    telstate = FakeTelstate(
        {
            "cal_pol_ordering": ["v", "h"],
            "177_cal_product_G": [(g, 1.0)],
            "177_product_B_parts": 1,
            "177_cal_product_B0": [(b0, 1.0)],
        }
    )
    calibrator = _make_calibrator(
        tmp_path,
        monkeypatch,
        telstate,
        freqs=[100.0, 200.0],
        fc_chans={"lsb-pol0": 150.0, "lsb-pol1": 150.0},
    )

    calibrator.compute_cal_sols(clean_bandpass=False)

    assert list(calibrator.G_vlbi.keys()) == [1.0]
    assert set(calibrator.G_vlbi[1.0]) == {"lsb-pol0", "lsb-pol1"}


def test_compute_cal_sols_supports_multiple_bandpass_parts(tmp_path: Path, monkeypatch) -> None:
    g = np.ones((2, 2), dtype=np.complex64)
    b0 = np.ones((2, 2, 2), dtype=np.complex64)
    b1 = 2.0 * np.ones((2, 2, 2), dtype=np.complex64)
    telstate = FakeTelstate(
        {
            "cal_pol_ordering": ["v", "h"],
            "cal_product_G": [(g, 2.0)],
            "product_B_parts": 2,
            "product_B0": [(b0, 2.0)],
            "product_B1": [(b1, 2.0)],
        }
    )
    calibrator = _make_calibrator(
        tmp_path,
        monkeypatch,
        telstate,
        freqs=[100.0, 200.0, 300.0, 400.0],
        fc_chans={"usb-pol0": 350.0, "usb-pol1": 350.0},
    )

    calibrator.compute_cal_sols(clean_bandpass=False)

    assert list(calibrator.G_vlbi.keys()) == [2.0]
    assert set(calibrator.G_vlbi[2.0]) == {"usb-pol0", "usb-pol1"}


def test_compute_cal_sols_matches_bandpass_parts_with_small_timestamp_skew(
    tmp_path: Path, monkeypatch
) -> None:
    g = np.ones((2, 2), dtype=np.complex64)
    b0 = np.ones((2, 2, 2), dtype=np.complex64)
    b1 = 2.0 * np.ones((2, 2, 2), dtype=np.complex64)
    telstate = FakeTelstate(
        {
            "cal_pol_ordering": ["v", "h"],
            "cal_product_G": [(g, 5.0)],
            "product_B_parts": 2,
            "product_B0": [(b0, 5.0)],
            "product_B1": [(b1, 5.0005)],
        }
    )
    calibrator = _make_calibrator(
        tmp_path,
        monkeypatch,
        telstate,
        freqs=[100.0, 200.0, 300.0, 400.0],
        fc_chans={"usb-pol0": 350.0, "usb-pol1": 350.0},
    )

    calibrator.compute_cal_sols(clean_bandpass=False)

    assert list(calibrator.G_vlbi.keys()) == [5.0]


def test_compute_cal_sols_uses_previous_bandpass_when_exact_timestamp_missing(
    tmp_path: Path, monkeypatch
) -> None:
    g = np.ones((2, 2), dtype=np.complex64)
    b0 = np.ones((2, 2, 2), dtype=np.complex64)
    b1 = 2.0 * np.ones((2, 2, 2), dtype=np.complex64)
    telstate = FakeTelstate(
        {
            "cal_pol_ordering": ["v", "h"],
            "cal_product_G": [(g, 10.0), (g, 20.0)],
            "product_B_parts": 2,
            "product_B0": [(b0, 10.0), (b0, 20.0)],
            "product_B1": [(b1, 10.0)],
        }
    )
    calibrator = _make_calibrator(
        tmp_path,
        monkeypatch,
        telstate,
        freqs=[100.0, 200.0, 300.0, 400.0],
        fc_chans={"usb-pol0": 350.0, "usb-pol1": 350.0},
    )

    calibrator.compute_cal_sols(clean_bandpass=False)

    assert list(calibrator.G_vlbi.keys()) == [10.0, 20.0]


def test_compute_cal_sols_falls_forward_when_no_previous_bandpass_exists(
    tmp_path: Path, monkeypatch
) -> None:
    g = np.ones((2, 2), dtype=np.complex64)
    b0 = np.ones((2, 2, 2), dtype=np.complex64)
    telstate = FakeTelstate(
        {
            "cal_pol_ordering": ["v", "h"],
            "cal_product_G": [(g, 5.0)],
            "product_B_parts": 1,
            "product_B0": [(b0, 10.0)],
        }
    )
    calibrator = _make_calibrator(
        tmp_path,
        monkeypatch,
        telstate,
        freqs=[100.0, 200.0],
        fc_chans={"lsb-pol0": 150.0, "lsb-pol1": 150.0},
    )

    calibrator.compute_cal_sols(clean_bandpass=False)

    assert list(calibrator.G_vlbi.keys()) == [5.0]


def test_compute_cal_sols_handles_single_frequency_bandpass_product(
    tmp_path: Path, monkeypatch
) -> None:
    g = np.ones((2, 2), dtype=np.complex64)
    b0 = np.ones((1, 2, 2), dtype=np.complex64)
    telstate = FakeTelstate(
        {
            "cal_pol_ordering": ["v", "h"],
            "cal_product_G": [(g, 7.0)],
            "product_B_parts": 1,
            "product_B0": [(b0, 7.0)],
        }
    )
    calibrator = _make_calibrator(
        tmp_path,
        monkeypatch,
        telstate,
        freqs=[100.0, 200.0, 300.0, 400.0],
        fc_chans={"usb-pol0": 350.0, "usb-pol1": 350.0},
    )

    calibrator.compute_cal_sols(clean_bandpass=False)

    assert list(calibrator.G_vlbi.keys()) == [7.0]


def test_compute_cal_sols_uses_bls_ordering_when_dataset_has_no_antennas(
    tmp_path: Path, monkeypatch
) -> None:
    g = np.ones((2, 2), dtype=np.complex64)
    b0 = np.ones((1, 2, 2), dtype=np.complex64)
    telstate = FakeTelstate(
        {
            "cal_pol_ordering": ["x", "y"],
            "bls_ordering": [("m000x", "m000x"), ("m001x", "m001x"), ("m000y", "m001y")],
            "cal_product_G": [(g, 8.0)],
            "product_B_parts": 1,
            "product_B0": [(b0, 8.0)],
        }
    )
    calibrator = _make_calibrator(
        tmp_path,
        monkeypatch,
        telstate,
        freqs=[100.0, 200.0, 300.0, 400.0],
        fc_chans={"usb-pol0": 350.0, "usb-pol1": 350.0},
        ants=[],
    )

    calibrator.compute_cal_sols(clean_bandpass=False)

    assert list(calibrator.G_vlbi.keys()) == [8.0]
