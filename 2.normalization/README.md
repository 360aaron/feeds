# Conformity

It would be reasonable to:

1. Split data onto business relevant models
2. Validate split data: 
    - Resulting tuples should be what normalized tables expect
    - Table types and constaints disallow violations
    - Exceptions should be persisted and published to a DQL
3. Diff changes for publishing to avoid rebroacasting whole records

### Proposed pattern

Raw -> 
    Tabularly split and validated data -> 
        Domain outboxes -> 
            Avro Kakfka messages

**Outboxes**

1. Properties
2. Event spaces
3. Amenities
4. Restaurants
5. Translations
6. Room types
7. Availability
8. ImagesMetadata: Images, Image tags & Image captions

**Why Avro?**

- Reduce message sizes by 4-5x​
- Improve throughput and latency​
- Easier schema evolution

--  
https://www.diva-portal.org/smash/get/diva2:1878772/FULLTEXT01.pdf