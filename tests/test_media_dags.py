from pathlib import Path

import yaml


def test_channels_config():
    path = Path(__file__).parents[1] / "config" / "channels.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert data["channels"]


def test_media_config_has_quality_threshold():
    path = Path(__file__).parents[1] / "config" / "media_config.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert 0 < data["quality_threshold"] <= 1
