import pytest

from models.message import File


@pytest.fixture
def md5hash():
    return "0a14db6e48b947b57988a2f61469f228"


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
    return {
        "name": "mi_foobar.zip",
        "bucket": "ons-blaise-v2-nifi",
        "md5Hash": md5hash,
        "size": "20",
        "timeCreated": "0103202021_16428",
    }


@pytest.fixture
def file():
    return File(
        name="dd_file.zip:my-bucket-name",
        sizeBytes="20",
        md5sum="dasdasd",
        relativePath="./",
    )


@pytest.fixture
def expected_pubsub_message_opn():
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
def expected_pubsub_message_lms():
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
        "dataset": "blaise_dde",
        "iterationL1": "LMS_Master",
        "iterationL2": "CLOUD",
        "iterationL3": "test",
        "iterationL4": "LMS2102R",
        "manifestCreated": "0103202021_16428",
        "fullSizeMegabytes": "0.000020",
    }


@pytest.fixture
def expected_pubsub_message_lmc():
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
        "dataset": "blaise_dde",
        "iterationL1": "LMS_Master",
        "iterationL2": "CLOUD",
        "iterationL3": "test",
        "iterationL4": "LMC2102R",
        "manifestCreated": "0103202021_16428",
        "fullSizeMegabytes": "0.000020",
    }
