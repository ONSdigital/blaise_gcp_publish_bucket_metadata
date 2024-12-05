import json
import pathlib
from dataclasses import asdict, dataclass
from typing import List

from google.cloud import pubsub_v1

from utils import (
    InvalidFileExtension,
    InvalidFileType,
    md5hash_to_md5sum,
    size_in_megabytes,
)

SUPPORTED_FILE_EXTENSIONS = [".zip"]

SUPPORTED_FILE_TYPES = ["dd", "mi"]


@dataclass
class File:
    name: str
    sizeBytes: str
    md5sum: str
    relativePath: str = ".\\"

    def extension(self):
        return pathlib.Path(self.filename()).suffix

    def filename(self):
        return self.name.split(":")[0]

    def type(self):
        return self.name.split("_")[0]

    def survey_tla(self):
        return self.filename().split("_")[1][0:3].upper()

    def instrument_name(self):
        file_prefix = pathlib.Path(self.filename()).stem
        parsed_prefix = file_prefix.split("_")[1:]
        instrument_name = [
            instrument_name_part
            for instrument_name_part in parsed_prefix
            if not instrument_name_part.isnumeric()
        ]
        return "_".join(instrument_name).upper()

    def is_lms(self):
        return self.survey_tla().startswith("LM")
    
    def is_frs(self):
        return self.survey_tla().startswith("FRS")

    @classmethod
    def from_event(cls, event):
        return cls(
            name=f"{event['name']}:{event['bucket']}",
            sizeBytes=event["size"],
            md5sum=md5hash_to_md5sum(event["md5Hash"]),
        )


@dataclass
class Message:
    files: List[File]
    sourceName: str
    manifestCreated: str
    fullSizeMegabytes: str
    version: int = 3
    schemaVersion: int = 1
    description: str = ""
    dataset: str = ""
    sensitivity: str = "High"
    iterationL1: str = ""
    iterationL2: str = ""
    iterationL3: str = ""
    iterationL4: str = ""

    def json(self):
        return json.dumps(asdict(self))

    def first_file(self):
        return self.files[0]

    def management_information(self, config):
        file = self.first_file()
        self.description = (
            "Management Information files uploaded to GCP bucket from Blaise5"
        )
        self.dataset = "blaise_mi"
        self.iterationL1 = f"BL5-{config.env}"
        self.iterationL2 = file.survey_tla()
        self.iterationL3 = file.instrument_name()
        return self

    def data_delivery_default(self, config):
        file = self.first_file()
        survey_tla = file.survey_tla()
        self.description = (
            f"Data Delivery files for {survey_tla} uploaded to GCP bucket from Blaise5"
        )
        self.dataset = "blaise_dde"
        self.iterationL1 = "SYSTEMS"
        self.iterationL2 = config.on_prem_subfolder
        self.iterationL3 = survey_tla
        self.iterationL4 = file.instrument_name()
        return self

    def data_delivery_lms(self, config):
        file = self.first_file()
        survey_tla = file.survey_tla()
        environment = config.env
        self.description = (
            f"Data Delivery files for {survey_tla} uploaded to GCP bucket from Blaise5"
        )
        self.dataset = "blaise_dde_lms"
        self.iterationL1 = "CLOUD"
        self.iterationL2 = environment
        self.iterationL3 = file.instrument_name()
        return self
    
    def data_delivery_frs(self, config):
        file = self.first_file()
        survey_tla = file.survey_tla()
        environment = config.env
        self.description = (
            f"Data Delivery files for {survey_tla} uploaded to GCP bucket from Blaise5"
        )
        self.dataset = "blaise_dde_frs"
        self.iterationL1 = "ingress"
        self.iterationL2 = "survey_data"
        self.iterationL3 = f"bl5-{environment}"
        self.iterationL4 = file.instrument_name()
        return self


def create_message(event, config):
    file = File.from_event(event)

    msg = Message(
        sourceName=f"gcp_blaise_{config.env}",
        manifestCreated=event["timeCreated"],
        fullSizeMegabytes=size_in_megabytes(event["size"]),
        files=[file],
    )

    if file.extension() not in SUPPORTED_FILE_EXTENSIONS:
        raise InvalidFileExtension(
            f"File extension '{file.extension()}' is invalid, supported extensions: {SUPPORTED_FILE_EXTENSIONS}"  # noqa:E501
        )

    if file.type() == "mi":
        return msg.management_information(config)
    if file.type() == "dd" and file.is_lms():
        return msg.data_delivery_lms(config)
    if file.type() == "dd" and file.is_frs():
        return msg.data_delivery_frs(config)
    if file.type() == "dd":
        return msg.data_delivery_default(config)

    raise InvalidFileType(
        f"File type '{file.type()}' is invalid, supported extensions: {SUPPORTED_FILE_TYPES}"  # noqa:E501
    )


def send_pub_sub_message(config, message):
    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(config.project_id, config.topic_name)
    msg_bytes = bytes(message.json(), encoding="utf-8")
    client.publish(topic_path, data=msg_bytes)
    print("Message published")
