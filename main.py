from models.config import Config
from models.message import create_message, send_pub_sub_message
from utils import log_event, update_data_delivery_state


def publishMsg(event, _context):
    config = Config.from_env()
    config.log()
    log_event(event)
    update_data_delivery_state(event, "in_nifi_bucket")

    if config.project_id is None:
        print("project_id not set, publish failed")
        return

    try:
        message = create_message(event, config)
        print(f"Message {message}")

        send_pub_sub_message(config, message)
        update_data_delivery_state(event, "nifi_notified")

    except Exception as error:
        print(repr(error))
        update_data_delivery_state(event, "errored", repr(error))
