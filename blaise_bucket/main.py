from cryptography.fernet import Fernet


def main():

    # load the key
    public_key_fromfile = load_key()
    encryptFile("blaise_bucket/Test Files/input_file.txt", public_key_fromfile)


def load_key():
    """
    Loads the key from the test public gpg file
    """
    return open("blaise_bucket/keys/ons_blaise5_gpg_key_a_test.gpg", "rb").read()


def encryptFile(input_file, public_key_fromfile):
    """
    Given a filename (str) and key (bytes), it encrypts the file and write it back out to a new file
    """
    output_file = "blaise_bucket/Test Files/output_file.txt"

    with open(input_file, "rb") as f:
        # read all input file data
        input_file_data = f.read()

    fernet = Fernet(public_key_fromfile)

    # encrypt data
    encrypted_data = fernet .encrypt(input_file_data)

    with open(output_file, 'wb') as f:
        f.write(encrypted_data)


def pubFileMetaData(data, context):
    import json
    import os

    from google.cloud import pubsub_v1
    project_id = os.environ['PROJECT_ID']

    filename = data['name']

    ext = data['name'].split(".")[1].lower()
    runPubSub = False
    if (ext == "csv"):
        runPubSub = True
        topic_name = "blaise-dev-258914-export-topic"
        metaTemplate = "mi-meta-template.json"
    elif ext == "asc" or ext == "rmk" or ext == "sps":
        runPubSub = True
        topic_name = "uploadedFile"
        metaTemplate = "dde-meta-template.json"
    else:
        runPubSub = False
        print("Error: Filetype {} not found for DDE or MI".format(ext))

    if (runPubSub):
        client = pubsub_v1.PublisherClient()
        topic_path = client.topic_path(project_id, topic_name)

        with open(metaTemplate) as json_file:
            msg = json.load(json_file)

        files = {}
        filename = data['name']
        sizeBytes = data['size']
        md5hash = data['md5Hash']
        manifestCreated = data['timeCreated']
        fullSizeMegabytes = int(data['size'])/1000000

        files["sizeBytes"] = f"{sizeBytes}"
        files["name"] = f"{data['name']}"
        files["md5sum"] = f"{md5hash}"
        files["relativePath"] = ".\\"
        msg['files'].append(files)
        msg["manifestCreated"] = f"{manifestCreated}"
        msg["fullSizeMegabytes"] = f"{fullSizeMegabytes}"
        msgbytes = bytes(json.dumps(msg), encoding='utf-8')
        client.publish(topic_path, data=msgbytes)