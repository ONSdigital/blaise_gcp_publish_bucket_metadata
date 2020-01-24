## Publish a file's metadata onto PubSub queue when a file is uploaded to GCP bucket. **[LU-4496](https://collaborate2.ons.gov.uk/jira/browse/LU-4496)**

When following file types are created or updated on a specified bucket, the message (below) is published to the Pub/Sub queue with file metadata injected:
- *.sps
- *.asc
- *.rmk  
- *.csv (for MI)

This is based on dde/mi-meta-template.json where 'Files', 'iterationL2-4', 'manifestCreated' and 'fullSizeMegabytes' meta data substituted.

#### Message example: 

```
{
    "version": 1,
    "files": [{  // the following 4 items Updated by GCP storage trigger function pubFileMetaData
            "sizeBytes": "17",
            "name": "test.csv:blaise-dev-258914-results", // bucket name is appended to filename i.e. filename:bucketname
            "md5sum": "testmd5sumdfer34==",
            "relativePath": ".\\"
        }], 
    "sensitivity": "High",
    "sourceName": "gcp_blaise",
    "description": "Creation of Blaise Manifest for DDE files sent to GCP bucket",
    "iterationL1": "ldata12",
                "iterationL2": "opn", // Updated by GCP storage trigger function pubFileMetaData - first 3 letters of filename
                "iterationL3": "1911", // Updated by GCP storage trigger function pubFileMetaData - chars 4-8 of filename
                "iterationL4": "a", // Updated by GCP storage trigger function pubFileMetaData - 9 char of filename
    "dataset": "blaise_dde",
    "schemaVersion": 1,
    "manifestCreated": "",
    "fullSizeMegabytes": ""
}
```

### Cloud Build

The `cloudbuild.yaml` creates the Cloud Function. This assumes you have set up a Cloud Build trigger on GCP.
