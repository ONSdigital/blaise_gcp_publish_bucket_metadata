import base64
import binascii

import blaise_dds


def log_event(event):
    print(f"Configuration: File name: {event['name']}")
    print(f"Configuration: Bucket Name: {event['bucket']}")


def md5hash_to_md5sum(md5hash):
    decode_hash = base64.b64decode(md5hash)
    encoded_hash = binascii.hexlify(decode_hash)
    return str(encoded_hash, "utf-8")


def size_in_megabytes(size_in_bytes):
    return "{:.6f}".format(int(size_in_bytes) / 1000000)


def update_data_delivery_state(event, state, error=None):
    dds_client = blaise_dds.Client(blaise_dds.Config.from_env())
    try:
        dds_client.update_state(event["name"], state, error)
    except Exception as err:
        print(f"failed to update dds state: {err}")
    return


class InvalidFileExtension(Exception):
    pass


class InvalidFileType(Exception):
    pass
