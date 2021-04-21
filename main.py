import blaise_dds
from google.cloud import pubsub_v1

from models.config import Config
from models.message import File, Message
from utils import InvalidFileExtension, InvalidFileType

SUPPORTED_FILE_EXTENSIONS = [".zip"]

SUPPORTED_FILE_TYPES = ["dd", "mi"]


def size_in_megabytes(size_in_bytes):
    return "{:.6f}".format(int(size_in_bytes) / 1000000)


def log_event(event):
    print(f"Configuration: File name: {event['name']}")
    print(f"Configuration: Bucket Name: {event['bucket']}")


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
    if file.type() == "dd" and file.is_opn():
        return msg.data_delivery_opn(config)
    if file.type() == "dd" and file.is_lms():
        return msg.data_delivery_lms(config)

    raise InvalidFileType(
        f"File type '{file.type()}' is invalid, supported extensions: {SUPPORTED_FILE_TYPES}"  # noqa:E501
    )


def send_pub_sub_message(config, message):
    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(config.project_id, config.topic_name)
    msg_bytes = bytes(message.json(), encoding="utf-8")
    client.publish(topic_path, data=msg_bytes)
    print("Message published")


def update_data_delivery_service(event, state, error=None):
    dds_client = blaise_dds.Client(blaise_dds.Config.from_env())
    try:
        dds_client.update_state(event["name"], state, error)
    except Exception as err:
        print(f"failed to establish dds client: {err}")
    return


def publishMsg(event, _context):
    config = Config.from_env()
    config.log()
    log_event(event)
    update_data_delivery_service(event, "in_nifi_bucket")

    if config.project_id is None:
        print("project_id not set, publish failed")
        return

    try:
        message = create_message(event, config)
        print(f"Message {message}")

        send_pub_sub_message(config, message)
        update_data_delivery_service(event, "nifi_notified")

    except Exception as error:
        print(repr(error))
        update_data_delivery_service(event, "errored", repr(error))
