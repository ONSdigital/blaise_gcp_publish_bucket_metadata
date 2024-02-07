import os
from unittest import mock

import blaise_dds
import pytest

from main import publishMsg
from models.config import Config


def test_config():
    config = Config(
        on_prem_subfolder="OPN", project_id="foobar", topic_name="barfoo", env="test"
    )
    assert config.on_prem_subfolder == "OPN"
    assert config.project_id == "foobar"
    assert config.topic_name == "barfoo"
    assert config.env == "test"


@mock.patch.dict(
    os.environ,
    {
        "PROJECT_ID": "test_project_id",
        "ENV": "test",
        "TOPIC_NAME": "nifi-notify",
        "ON-PREM-SUBFOLDER": "DEV",
    },
)
def test_config_from_env():
    config = Config.from_env()
    assert config.on_prem_subfolder == "DEV"
    assert config.project_id == "test_project_id"
    assert config.topic_name == "nifi-notify"
    assert config.env == "test"


@mock.patch.dict(
    os.environ,
    {"TOPIC_NAME": "nifi-notify"},
)
@mock.patch.object(blaise_dds.Client, "update_state")
@pytest.mark.parametrize(
    "instrument",
    [
        ("LMC2102R"),
        ("OPN2102R"),
        ("LMS2102R"),
    ],
)
def test_project_id_not_set(_mock_update_state, dd_event, capsys, instrument):
    dd_event = dd_event(instrument)
    publishMsg(dd_event, None)
    captured = capsys.readouterr()
    assert captured.out == (
        "Configuration: Project ID: None\n"
        + "Configuration: Topic Name: nifi-notify\n"
        + "Configuration: ON-PREM-SUBFOLDER: None\n"
        + "Configuration: Env: None\n"
        + f"Configuration: File name: dd_{instrument}_0103202021_16428.zip\n"
        + "Configuration: Bucket Name: ons-blaise-v2-nifi\n"
        + "project_id not set, publish failed\n"
    )
