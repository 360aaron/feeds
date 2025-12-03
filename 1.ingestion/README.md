# Ingestion

We copy data from 4 types of sources:
1. Rest API's
2. SOAP API's
3. Flat files in SFTP servers
4. Flat files in BLOB's

### Proposed pattern

This requires 6 elements / ~200 LOC + tests:
1. 1 serverless job running on a schedule, 
2. 3 tables:
    i. Staging
    ii. Live records
    iii. Archived records
3. 1 function
4. 1 trigger

Minimally viable with ~200 LOC:
```
--------------------------------------------------------------------
Language   Files        Lines        Blank      Comment      Code
--------------------------------------------------------------------
Python     1           72            4            5           63
 SQL       1           67            7            6           54
--------------------------------------------------------------------
Total      2          139           11           11          117
--------------------------------------------------------------------
```