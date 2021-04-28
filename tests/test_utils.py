from unittest import mock

import blaise_dds
import pytest

from utils import md5hash_to_md5sum, size_in_megabytes, update_data_delivery_state


def test_md5hash_to_md5sum(md5hash):
    assert (
        md5hash_to_md5sum(md5hash) == "d1ad7875be9ee3c6fde3b6f9efdf3c6b67fad78ebd7f6dbc"
    )


@pytest.mark.parametrize(
    "size_in_bytes,size_in_megs",
    [
        ("20", "0.000020"),
        ("320", "0.000320"),
        ("4783", "0.004783"),
        ("12004783", "12.004783"),
        ("3475231", "3.475231"),
    ],
)
def test_size_in_megabytes(size_in_bytes, size_in_megs):
    assert size_in_megabytes(size_in_bytes) == size_in_megs


@mock.patch.object(blaise_dds.Client, "update_state")
@pytest.mark.parametrize(
    "instrument,state",
    [
        ("LMC2102R", "in_nifi_bucket"),
        ("OPN2102R", "nifi_notified"),
        ("LMS2102R", "in_arc"),
    ],
)
def test_update_data_delivery_state(mock_update_state, dd_event, instrument, state):
    dd_event = dd_event(instrument)
    update_data_delivery_state(dd_event, state)
    assert mock_update_state.call_count == 1
    assert mock_update_state.call_args_list[0] == mock.call(
        dd_event["name"],
        state,
        None,
    )


@mock.patch.object(blaise_dds.Client, "update_state")
@pytest.mark.parametrize(
    "instrument,state",
    [
        ("LMC2102R", "in_nifi_bucket"),
        ("OPN2102R", "nifi_notified"),
        ("LMS2102R", "in_arc"),
    ],
)
def test_update_data_delivery_state_fail(
    mock_update_state, dd_event, capsys, instrument, state
):
    mock_update_state.side_effect = Exception(
        "Computer says no. Do not pass Go. Do not collect £200"
    )
    dd_event = dd_event(instrument)
    update_data_delivery_state(dd_event, state)
    captured = capsys.readouterr()
    assert (
        captured.out
        == "failed to update dds state: Computer says no. Do not pass Go. Do not collect £200\n"
    )
