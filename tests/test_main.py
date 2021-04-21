import json
import os
import blaise_dds
import pytest

from google.cloud.pubsub_v1 import PublisherClient
from unittest import mock
from main import publishMsg


@mock.patch.dict(
    os.environ,
    {
        "PROJECT_ID": "test_project_id",
        "ENV": "test",
        "TOPIC_NAME": "nifi-notify",
        "ON-PREM-SUBFOLDER": "DEV",
    },
)
@mock.patch.object(blaise_dds.Client, "update_state")
@mock.patch.object(PublisherClient, "publish")
@pytest.mark.parametrize(
    "instrument, expected_message",
    [
        ("LMC2102R", pytest.lazy_fixture("expected_pubsub_message_lmc")),
        ("OPN2102R", pytest.lazy_fixture("expected_pubsub_message_opn")),
        ("LMS2102R", pytest.lazy_fixture("expected_pubsub_message_lms")),
    ],
)
def test_publishMsg_dd(
    mock_pubsub, mock_update_state, dd_event, instrument, expected_message
):
    dd_event = dd_event(instrument)

    publishMsg(dd_event, None)
    assert mock_update_state.call_count == 2
    assert mock_update_state.call_args_list[0] == mock.call(
        dd_event["name"],
        "in_nifi_bucket",
        None,
    )
    assert mock_update_state.call_args_list[1] == mock.call(
        dd_event["name"],
        "nifi_notified",
        None,
    )
    assert len(mock_pubsub.call_args_list) == 1
    assert (
        mock_pubsub.call_args_list[0][0][0]
        == "projects/test_project_id/topics/nifi-notify"
    )
    pubsub_message = mock_pubsub.call_args_list[0][1]["data"]
    assert json.loads(pubsub_message) == expected_message


@mock.patch.dict(
    os.environ,
    {
        "PROJECT_ID": "test_project_id",
        "ENV": "test",
        "TOPIC_NAME": "nifi-notify",
        "ON-PREM-SUBFOLDER": "DEV",
    },
)
@mock.patch.object(blaise_dds.Client, "update_state")
@mock.patch.object(PublisherClient, "publish")
def test_publishMsg_mi(mock_pubsub, mock_update_state, mi_event):
    publishMsg(mi_event, None)
    assert mock_update_state.call_count == 2
    assert mock_update_state.call_args_list[0] == mock.call(
        mi_event["name"],
        "in_nifi_bucket",
        None,
    )
    assert mock_update_state.call_args_list[1] == mock.call(
        mi_event["name"],
        "nifi_notified",
        None,
    )

    assert (
        mock_pubsub.call_args_list[0][0][0]
        == "projects/test_project_id/topics/nifi-notify"
    )
    pubsub_message = mock_pubsub.call_args_list[0][1]["data"]
    assert json.loads(pubsub_message) == {
        "version": 3,
        "schemaVersion": 1,
        "files": [
            {
                "sizeBytes": "20",
                "name": "mi_foobar.zip:ons-blaise-v2-nifi",
                "md5sum": "d1ad7875be9ee3c6fde3b6f9efdf3c6b67fad78ebd7f6dbc",
                "relativePath": ".\\",
            }
        ],
        "sensitivity": "High",
        "sourceName": "gcp_blaise_test",
        "description": "Management Information files uploaded to GCP bucket from Blaise5",
        "dataset": "blaise_mi",
        "iterationL1": "DEV",
        "iterationL2": "",
        "iterationL3": "",
        "iterationL4": "",
        "manifestCreated": "0103202021_16428",
        "fullSizeMegabytes": "0.000020",
    }


@mock.patch.dict(
    os.environ,
    {"PROJECT_ID": "test_project_id", "ENV": "test", "TOPIC_NAME": "nifi-notify"},
)
@mock.patch.object(blaise_dds.Client, "update_state")
@mock.patch.object(PublisherClient, "publish")
@pytest.mark.parametrize(
    "instrument",
    [
        ("LMC2102R"),
        ("OPN2102R"),
        ("LMS2102R"),
    ],
)
def test_publishMsg_error(mock_pubsub, mock_update_state, dd_event, instrument):
    mock_pubsub.side_effect = Exception(
        "Explosions occurred when sending message to pubsub"
    )
    dd_event = dd_event(instrument)
    publishMsg(dd_event, None)
    assert mock_update_state.call_count == 2
    assert mock_update_state.call_args_list[0] == mock.call(
        dd_event["name"],
        "in_nifi_bucket",
        None,
    )
    assert mock_update_state.call_args_list[1] == mock.call(
        dd_event["name"],
        "errored",
        "Exception('Explosions occurred when sending message to pubsub')",
    )


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
def test_project_id_not_set(mock_update_state, dd_event, capsys, instrument):
    dd_event = dd_event(instrument)

    publishMsg(dd_event, None)
    assert mock_update_state.call_count == 1
    assert mock_update_state.call_args_list[0] == mock.call(
        dd_event["name"],
        "in_nifi_bucket",
        None,
    )
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
