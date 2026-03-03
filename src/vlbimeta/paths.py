"""Path helpers for packaged assets and container defaults."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from importlib.resources import files as resource_files
except ImportError:
    from importlib_resources import files as resource_files


def package_asset_dir(name: str) -> Path:
    """Return the installed package asset directory (best-effort filesystem path)."""
    return Path(resource_files("vlbimeta").joinpath(name))


def image_app_root() -> Path:
    return Path(os.environ.get("VLBIMETA_APP_ROOT", "/opt/vlbimeta"))


def default_catalogue_dir() -> Path:
    explicit = os.environ.get("VLBIMETA_CATALOGUE_DIR")
    if explicit:
        return Path(explicit)
    packaged = package_asset_dir("catalogues")
    return packaged if packaged.exists() else image_app_root() / "catalogues"


def default_metadata_dir() -> Path:
    explicit = os.environ.get("VLBIMETA_METADATA_DIR")
    if explicit:
        return Path(explicit)
    packaged = package_asset_dir("metadata")
    return packaged if packaged.exists() else image_app_root() / "metadata"
