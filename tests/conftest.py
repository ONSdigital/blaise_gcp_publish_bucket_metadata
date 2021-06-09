import pytest

from models.config import Config
from models.message import File, Message


@pytest.fixture
def md5hash():
    return "0a14db6e48b947b57988a2f61469f228"


@pytest.fixture
def event(md5hash):
    def wrapper(filename):
        return {
            "name": f"{filename}.zip",
            "bucket": "ons-blaise-v2-nifi",
            "md5Hash": md5hash,
            "size": "20",
            "timeCreated": "0103202021_16428",
        }

    return wrapper


@pytest.fixture
def dd_event(md5hash):
    def wrapper(instrument):
        return {
            "name": f"dd_{instrument}_0103202021_16428.zip",
            "bucket": "ons-blaise-v2-nifi",
            "md5Hash": md5hash,
            "size": "20",
            "timeCreated": "0103202021_16428",
        }

    return wrapper


@pytest.fixture
def mi_event(md5hash):
    def wrapper(instrument):
        return {
            "name": f"mi_{instrument}_0103202021_16428.zip",
            "bucket": "ons-blaise-v2-nifi",
            "md5Hash": md5hash,
            "size": "20",
            "timeCreated": "0103202021_16428",
        }

    return wrapper


@pytest.fixture
def config():
    return Config(
        on_prem_subfolder="survey_on_prem_subfolder",
        project_id="survey_project_id",
        topic_name="topic_name",
        env="test",
    )


@pytest.fixture
def file():
    return File(
        name="dd_file.zip:my-bucket-name",
        sizeBytes="20",
        md5sum="dasdasd",
        relativePath="./",
    )


@pytest.fixture
def message(file, md5hash, config):
    return Message(
        files=file,
        sourceName="foo",
        manifestCreated="bar",
        fullSizeMegabytes="foobar",
        version=3,
        schemaVersion=1,
        description="barfoo",
        dataset="foobarfoo",
        sensitivity="High",
        iterationL1=config.on_prem_subfolder,
        iterationL2="",
        iterationL3="",
        iterationL4="",
    )


@pytest.fixture
def expected_pubsub_message_dd_opn():
    return {
        "version": 3,
        "schemaVersion": 1,
        "files": [
            {
                "sizeBytes": "20",
                "name": "dd_OPN2102R_0103202021_16428.zip:ons-blaise-v2-nifi",
                "md5sum": "d1ad7875be9ee3c6fde3b6f9efdf3c6b67fad78ebd7f6dbc",
                "relativePath": ".\\",
            }
        ],
        "sensitivity": "High",
        "sourceName": "gcp_blaise_test",
        "description": "Data Delivery files for OPN uploaded to GCP bucket from Blaise5",
        "dataset": "blaise_dde",
        "iterationL1": "SYSTEMS",
        "iterationL2": "DEV",
        "iterationL3": "OPN",
        "iterationL4": "OPN2102R",
        "manifestCreated": "0103202021_16428",
        "fullSizeMegabytes": "0.000020",
    }


@pytest.fixture
def expected_pubsub_message_dd_lms():
    return {
        "version": 3,
        "schemaVersion": 1,
        "files": [
            {
                "sizeBytes": "20",
                "name": "dd_LMS2102R_0103202021_16428.zip:ons-blaise-v2-nifi",
                "md5sum": "d1ad7875be9ee3c6fde3b6f9efdf3c6b67fad78ebd7f6dbc",
                "relativePath": ".\\",
            }
        ],
        "sensitivity": "High",
        "sourceName": "gcp_blaise_test",
        "description": "Data Delivery files for LMS uploaded to GCP bucket from Blaise5",
        "dataset": "blaise_dde_lms",
        "iterationL1": "CLOUD",
        "iterationL2": "test",
        "iterationL3": "LMS2102R",
        "iterationL4": "",
        "manifestCreated": "0103202021_16428",
        "fullSizeMegabytes": "0.000020",
    }


@pytest.fixture
def expected_pubsub_message_dd_lmc():
    return {
        "version": 3,
        "schemaVersion": 1,
        "files": [
            {
                "sizeBytes": "20",
                "name": "dd_LMC2102R_0103202021_16428.zip:ons-blaise-v2-nifi",
                "md5sum": "d1ad7875be9ee3c6fde3b6f9efdf3c6b67fad78ebd7f6dbc",
                "relativePath": ".\\",
            }
        ],
        "sensitivity": "High",
        "sourceName": "gcp_blaise_test",
        "description": "Data Delivery files for LMC uploaded to GCP bucket from Blaise5",
        "dataset": "blaise_dde_lms",
        "iterationL1": "CLOUD",
        "iterationL2": "test",
        "iterationL3": "LMC2102R",
        "iterationL4": "",
        "manifestCreated": "0103202021_16428",
        "fullSizeMegabytes": "0.000020",
    }


@pytest.fixture
def expected_pubsub_message_mi():
    return {
        "version": 3,
        "schemaVersion": 1,
        "files": [
            {
                "sizeBytes": "20",
                "name": "mi_OPN2101A_0103202021_16428.zip:ons-blaise-v2-nifi",
                "md5sum": "d1ad7875be9ee3c6fde3b6f9efdf3c6b67fad78ebd7f6dbc",
                "relativePath": ".\\",
            }
        ],
        "sensitivity": "High",
        "sourceName": "gcp_blaise_test",
        "description": "Management Information files uploaded to GCP bucket from Blaise5",
        "dataset": "blaise_mi",
        "iterationL1": "BL5-test",
        "iterationL2": "OPN",
        "iterationL3": "OPN2101A",
        "iterationL4": "",
        "manifestCreated": "0103202021_16428",
        "fullSizeMegabytes": "0.000020",
    }
