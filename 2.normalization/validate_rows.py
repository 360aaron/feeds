from fastavro.schema import parse_schema
from fastavro.validation import validate

DAY_SCHEMA_JSON = {
  "type": "record",
  "name": "NormalizedDaySparseOutbox",
  "namespace": "your.company.weather",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "source_unique_id", "type": ["null", "string"], "default": None},

    {"name": "maxtemp_c", "type": ["null", "double"], "default": None},
    {"name": "maxtemp_f", "type": ["null", "double"], "default": None},
    {"name": "mintemp_c", "type": ["null", "double"], "default": None},
    {"name": "mintemp_f", "type": ["null", "double"], "default": None},
    {"name": "avgtemp_c", "type": ["null", "double"], "default": None},
    {"name": "avgtemp_f", "type": ["null", "double"], "default": None},

    {"name": "maxwind_mph", "type": ["null", "double"], "default": None},
    {"name": "maxwind_kph", "type": ["null", "double"], "default": None},
    {"name": "totalprecip_mm", "type": ["null", "double"], "default": None},
    {"name": "totalprecip_in", "type": ["null", "double"], "default": None},
    {"name": "totalsnow_cm", "type": ["null", "double"], "default": None},
    {"name": "avgvis_km", "type": ["null", "double"], "default": None},
    {"name": "avgvis_miles", "type": ["null", "double"], "default": None},
    {"name": "avghumidity", "type": ["null", "double"], "default": None},

    {"name": "daily_will_it_rain", "type": ["null", "int"], "default": None},
    {"name": "daily_chance_of_rain", "type": ["null", "int"], "default": None},
    {"name": "daily_will_it_snow", "type": ["null", "int"], "default": None},
    {"name": "daily_chance_of_snow", "type": ["null", "int"], "default": None},

    {"name": "condition_text", "type": ["null", "string"], "default": None},
    {"name": "condition_icon", "type": ["null", "string"], "default": None},
    {"name": "condition_code", "type": ["null", "int"], "default": None},

    {"name": "uv", "type": ["null", "double"], "default": None}
  ]
}

DAY_SCHEMA = parse_schema(DAY_SCHEMA_JSON)

def is_day_valid(event: dict) -> bool:
    try:
        return validate(event, DAY_SCHEMA)
    except Exception as e:
        print(f"Avro validation failed: {e}")
        return False
