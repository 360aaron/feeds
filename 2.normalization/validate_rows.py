import json
from fastavro.schema import parse_schema
from fastavro.validation import validate

with open("/Users/aaron/Documents/feeds/3.publishing/day_schema.avsc") as f:
    DAY_SCHEMA = parse_schema(json.load(f))

def is_day_valid(event: dict) -> bool:
    try:
        return validate(event, DAY_SCHEMA)
    except Exception as e:
        print(f"Avro validation failed: {e}")
        return False
