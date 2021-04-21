import json
import pytest

from dataclasses import asdict
from unittest import mock

from models.message import File, Message, create_message, send_pub_sub_message
from utils import InvalidFileExtension, InvalidFileType

from google.cloud.pubsub_v1 import PublisherClient


def test_file_extension(file):
    assert file.extension() == ".zip"


def test_file_filename(file):
    assert file.filename() == "dd_file.zip"


@pytest.mark.parametrize(
    "file_name,file_type",
    [("dd_file.zip", "dd"), ("mi_file.zip", "mi")],
)
def test_file_type(file, file_name, file_type):
    file.name = f"{file_name}:my-bucket-name"
    assert file.type() == file_type


@pytest.mark.parametrize(
    "file_name, expected",
    [
        ("dd_opn2101a.zip", "OPN"),
        ("dd_lms2102_a1.zip", "LMS"),
        ("dd_lms2102_bk1.zip", "LMS"),
        ("dd_lmc2102_bk1.zip", "LMC"),
        ("dd_lmb21021_bk2.zip", "LMB"),
    ],
)
def test_file_survey_name(file, file_name, expected):
    file.name = f"{file_name}:my-bucket-name"
    assert file.survey_name() == expected


@pytest.mark.parametrize(
    "file_name, expected",
    [
        ("dd_opn2101a.zip", "OPN2101A"),
        ("dd_lms2102_a1.zip", "LMS2102_A1"),
        ("dd_lms2102_bk1.zip", "LMS2102_BK1"),
        ("dd_lmc2102_bk1.zip", "LMC2102_BK1"),
    ],
)
def test_file_instrument_name(file, file_name, expected):
    file.name = f"{file_name}:my-bucket-name"
    assert file.instrument_name() == expected


@pytest.mark.parametrize(
    "survey_name, expected",
    [
        ("OPN", False),
        ("OLS", False),
        ("LMS", True),
        ("LMB", True),
        ("IPS", False),
        ("LMC", True),
        ("LMO", True),
        ("QWERTY", False),
        ("LMNOP", True),
        ("LBS", False),
    ],
)
def test_file_is_lms(file, survey_name, expected):
    file.name = f"dd_{survey_name}2101a.zip:my-bucket-name"
    assert file.is_lms() is expected


@pytest.mark.parametrize(
    "survey_name, expected",
    [
        ("OPN", True),
        ("opn", True),
        ("LMS", False),
        ("LMB", False),
        ("IPS", False),
        ("LMC", False),
        ("LMO", False),
        ("QWERTY", False),
        ("LMNOP", False),
        ("OPNFOOBAR", True),
    ],
)
def test_file_is_opn(file, survey_name, expected):
    file.name = f"dd_{survey_name}2101a.zip:my-bucket-name"
    assert file.is_opn() is expected


def test_file_from_event(dd_event):
    file = File.from_event(dd_event("OPN2102R"))
    assert file.name == "dd_OPN2102R_0103202021_16428.zip:ons-blaise-v2-nifi"
    assert file.sizeBytes == "20"
    assert file.md5sum == "d1ad7875be9ee3c6fde3b6f9efdf3c6b67fad78ebd7f6dbc"
    assert file.relativePath == ".\\"


def test_message_json():
    file = File(name="a_file", sizeBytes="20", md5sum="dasdasd", relativePath="./")
    message = Message(
        files=[file],
        sourceName="blaise",
        description="blaise test",
        dataset="blaise dataset",
        manifestCreated="my_date",
        fullSizeMegabytes="1",
    )
    assert json.loads(message.json()) == {
        "version": 3,
        "schemaVersion": 1,
        "files": [
            {
                "sizeBytes": "20",
                "name": "a_file",
                "md5sum": "dasdasd",
                "relativePath": "./",
            }
        ],
        "sensitivity": "High",
        "sourceName": "blaise",
        "description": "blaise test",
        "dataset": "blaise dataset",
        "iterationL1": "",
        "iterationL2": "",
        "iterationL3": "",
        "iterationL4": "",
        "manifestCreated": "my_date",
        "fullSizeMegabytes": "1",
    }


def test_create_message_mi(mi_event, config):
    actual_message = create_message(mi_event, config)
    assert (
        actual_message.description
        == "Management Information files uploaded to GCP bucket from Blaise5"
    )
    assert actual_message.dataset == "blaise_mi"
    assert actual_message.iterationL1 == "OPN"
    assert actual_message.iterationL2 == ""


def test_create_message_dd_opn(dd_event, config, file):
    file.name = f"dd_OPN2101A.zip:my-bucket-name"

    dd_event = dd_event("OPN2101A")
    actual_message = create_message(dd_event, config)

    assert (
        actual_message.description
        == "Data Delivery files for OPN uploaded to GCP bucket from Blaise5"
    )
    assert actual_message.dataset == "blaise_dde"
    assert actual_message.iterationL1 == "SYSTEMS"
    assert actual_message.iterationL2 == config.on_prem_subfolder
    assert actual_message.iterationL3 == "OPN"
    assert actual_message.iterationL4 == "OPN2101A"


@pytest.mark.parametrize(
    "instrument,expected_survey_name",
    [
        ("LMS2102_A1", "LMS"),
        ("lms2102_bk1", "LMS"),
        ("lmc2102_bk1", "LMC"),
        ("lmb21021_bk2", "LMB"),
    ],
)
def test_create_message_dd_lms(
    instrument, expected_survey_name, dd_event, config, file
):
    file.name = f"dd_{instrument}.zip:my-bucket-name"
    dd_event = dd_event(instrument)
    actual_message = create_message(dd_event, config)

    assert (
        actual_message.description
        == f"Data Delivery files for {expected_survey_name} uploaded to GCP bucket from Blaise5"
    )
    assert actual_message.dataset == "blaise_dde"
    assert actual_message.iterationL1 == "LMS_Master"
    assert actual_message.iterationL2 == "CLOUD"
    assert actual_message.iterationL3 == config.env
    assert actual_message.iterationL4 == instrument.upper()


@pytest.mark.parametrize(
    "spicy_file_extension",
    [
        ("avi"),
        ("dat"),
        ("nth"),
        ("zoo"),
        ("qxd"),
    ],
)
def test_create_message_invalid_file_extension(spicy_file_extension, dd_event, config):
    dd_event = dd_event("OPN2101A")
    dd_event["name"] = f"dd_opn2101a.{spicy_file_extension}:my-bucket-name"

    with pytest.raises(InvalidFileExtension):
        create_message(dd_event, config)


@pytest.mark.parametrize(
    "spicy_file_types",
    [
        ("notMI"),
        ("notDD"),
        ("ddfoo"),
        ("mibar"),
        ("mmmm_spicy"),
    ],
)
def test_create_message_invalid_file_type(spicy_file_types, dd_event, config):
    dd_event = dd_event("OPN2101A")
    dd_event["name"] = f"{spicy_file_types}_opn2101a.zip:my-bucket-name"

    with pytest.raises(InvalidFileType):
        create_message(dd_event, config)


@mock.patch.object(PublisherClient, "publish")
def test_send_pub_sub_message(mock_pubsub, config, message):
    send_pub_sub_message(config, message)

    assert len(mock_pubsub.call_args_list) == 1
    assert mock_pubsub.call_args_list[0][0][0] == "projects/foobar/topics/barfoo"
    pubsub_message = mock_pubsub.call_args_list[0][1]["data"]
    assert json.loads(pubsub_message) == asdict(message)
