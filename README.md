# blaise-publish-bucket-metadata

Cloud function to create and publish metadata to a pub/sub topic about a zip file uploaded to a bucket.

Zip files uploaded to said bucket must be prefixed with "mi_" for Management Information or "dd_" for Data Delivery and be unique via a timestamp. Exampe:

`mi_opn2001a_01012020_1200.zip`

Appropriate zip file metadata messages are published to a Pub/Sub topic for MiNiFi to consume and transfer the zip files on-premises via NiFi.

<b>Please post refactored and new Survey messages as examples in Confluence: https://collaborate2.ons.gov.uk/confluence/display/QSS/Blaise+5+Publish+PubSub+Topic+for+NiFi<b>

##Local Setup

Clone the project locally:

```
git clone https://github.com/ONSdigital/blaise-publish-bucket-metadata.git
```

Install poetry:
```
pip install poetry
```

Run poetry install
```
poetry install
```

##Using Poetry
``` make format ``` will format your code to make it pretty which is the same as ```poetry run isort .```.

```make lint``` checks your coding standards and ```make test``` will run all tests.

###Troubleshooting

To give you the path to python for your virtual env run:
```
echo "$(poetry env info | grep Path | awk '{print $2}')/bin/python"
```

