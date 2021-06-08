import json
import os
from unittest import mock

import blaise_dds
import pytest
from google.cloud.pubsub_v1 import PublisherClient

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
        ("OPN2102R", pytest.lazy_fixture("expected_pubsub_message_dd_opn")),
        ("LMS2102R", pytest.lazy_fixture("expected_pubsub_message_dd_lms")),
        ("LMC2102R", pytest.lazy_fixture("expected_pubsub_message_dd_lmc")),
    ],
)
def test_publishMsg_for_data_delivery(
    mock_pubsub, _mock_update_state, dd_event, instrument, expected_message
):
    dd_event = dd_event(instrument)
    publishMsg(dd_event, None)

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
def test_publishMsg_for_management_information(
    mock_pubsub, _mock_update_state, mi_event, expected_pubsub_message_mi
):
    mi_event = mi_event("OPN2101A")
    publishMsg(mi_event, None)
    pubsub_message = mock_pubsub.call_args_list[0][1]["data"]
    assert json.loads(pubsub_message) == expected_pubsub_message_mi


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
    assert mock_update_state.call_args_list[1] == mock.call(
        dd_event["name"],
        "errored",
        "Exception('Explosions occurred when sending message to pubsub')",
    )
