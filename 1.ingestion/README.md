# Ingestion

We copy data from 4 types of sources:
1. Rest API's
2. SOAP API's
3. Flat files in SFTP servers
4. Flat files in BLOB's

### Proposed pattern

1. Compute:
    - Authenticates / Connects
    - Copies data
    - Contracts:
        1. Schema validation: validate we are receiving headers/keys we expect.
        2. Schema drift: track when we receive unexpected, new or changed headers/keys.
        3. Type validation: validate we are receiving the data types we are expected.
        4. Type drift: track when we receive unexpected, new or changed data types.
2. Storage:
    1. Staging
    2. Live records
    3. Archived records
    4. An archiving function
    5. A trigger for the archiving function
