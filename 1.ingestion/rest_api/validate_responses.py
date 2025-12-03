from jsonschema import Draft202012Validator
import json

RESPONSE_SCHEMA = """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Weather Forecast Response",
  "type": "object",
  "required": ["location", "forecast"],
  "properties": {
    "location": {
      "type": "object",
      "required": [
        "name",
        "region",
        "country",
        "lat",
        "lon",
        "tz_id",
        "localtime_epoch",
        "localtime"
      ],
      "properties": {
        "name": { "type": "string" },
        "region": { "type": "string" },
        "country": { "type": "string" },
        "lat": { "type": "number" },
        "lon": { "type": "number" },
        "tz_id": { "type": "string" },
        "localtime_epoch": { "type": "integer" },
        "localtime": { "type": "string" }
      },
      "additionalProperties": false
    },
    "forecast": {
      "type": "object",
      "required": ["forecastday"],
      "properties": {
        "forecastday": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["date", "date_epoch", "day", "astro", "hour"],
            "properties": {
              "date": { "type": "string" },
              "date_epoch": { "type": "integer" },
              "day": {
                "type": "object",
                "required": [
                  "maxtemp_c",
                  "maxtemp_f",
                  "mintemp_c",
                  "mintemp_f",
                  "avgtemp_c",
                  "avgtemp_f",
                  "maxwind_mph",
                  "maxwind_kph",
                  "totalprecip_mm",
                  "totalprecip_in",
                  "totalsnow_cm",
                  "avgvis_km",
                  "avgvis_miles",
                  "avghumidity",
                  "daily_will_it_rain",
                  "daily_chance_of_rain",
                  "daily_will_it_snow",
                  "daily_chance_of_snow",
                  "condition",
                  "uv"
                ],
                "properties": {
                  "maxtemp_c": { "type": "number" },
                  "maxtemp_f": { "type": "number" },
                  "mintemp_c": { "type": "number" },
                  "mintemp_f": { "type": "number" },
                  "avgtemp_c": { "type": "number" },
                  "avgtemp_f": { "type": "number" },
                  "maxwind_mph": { "type": "number" },
                  "maxwind_kph": { "type": "number" },
                  "totalprecip_mm": { "type": "number" },
                  "totalprecip_in": { "type": "number" },
                  "totalsnow_cm": { "type": "number" },
                  "avgvis_km": { "type": "number" },
                  "avgvis_miles": { "type": "number" },
                  "avghumidity": { "type": "number" },
                  "daily_will_it_rain": { "type": "integer" },
                  "daily_chance_of_rain": { "type": "integer" },
                  "daily_will_it_snow": { "type": "integer" },
                  "daily_chance_of_snow": { "type": "integer" },
                  "condition": {
                    "type": "object",
                    "required": ["text", "icon", "code"],
                    "properties": {
                      "text": { "type": "string" },
                      "icon": { "type": "string" },
                      "code": { "type": "integer" }
                    },
                    "additionalProperties": false
                  },
                  "uv": { "type": "number" }
                },
                "additionalProperties": false
              },
              "astro": {
                "type": "object",
                "required": [
                  "sunrise",
                  "sunset",
                  "moonrise",
                  "moonset",
                  "moon_phase",
                  "moon_illumination"
                ],
                "properties": {
                  "sunrise": { "type": "string" },
                  "sunset": { "type": "string" },
                  "moonrise": { "type": "string" },
                  "moonset": { "type": "string" },
                  "moon_phase": { "type": "string" },
                  "moon_illumination": { "type": "integer" }
                },
                "additionalProperties": false
              },
              "hour": {
                "type": "array",
                "items": {
                  "type": "object",
                  "required": [
                    "time_epoch",
                    "time",
                    "temp_c",
                    "temp_f",
                    "is_day",
                    "condition",
                    "wind_mph",
                    "wind_kph",
                    "wind_degree",
                    "wind_dir",
                    "pressure_mb",
                    "pressure_in",
                    "precip_mm",
                    "precip_in",
                    "snow_cm",
                    "humidity",
                    "cloud",
                    "feelslike_c",
                    "feelslike_f",
                    "windchill_c",
                    "windchill_f",
                    "heatindex_c",
                    "heatindex_f",
                    "dewpoint_c",
                    "dewpoint_f",
                    "will_it_rain",
                    "chance_of_rain",
                    "will_it_snow",
                    "chance_of_snow",
                    "vis_km",
                    "vis_miles",
                    "gust_mph",
                    "gust_kph",
                    "uv"
                  ],
                  "properties": {
                    "time_epoch": { "type": "integer" },
                    "time": { "type": "string" },
                    "temp_c": { "type": "number" },
                    "temp_f": { "type": "number" },
                    "is_day": { "type": "integer" },
                    "condition": {
                      "type": "object",
                      "required": ["text", "icon", "code"],
                      "properties": {
                        "text": { "type": "string" },
                        "icon": { "type": "string" },
                        "code": { "type": "integer" }
                      },
                      "additionalProperties": false
                    },
                    "wind_mph": { "type": "number" },
                    "wind_kph": { "type": "number" },
                    "wind_degree": { "type": "integer" },
                    "wind_dir": { "type": "string" },
                    "pressure_mb": { "type": "number" },
                    "pressure_in": { "type": "number" },
                    "precip_mm": { "type": "number" },
                    "precip_in": { "type": "number" },
                    "snow_cm": { "type": "number" },
                    "humidity": { "type": "integer" },
                    "cloud": { "type": "integer" },
                    "feelslike_c": { "type": "number" },
                    "feelslike_f": { "type": "number" },
                    "windchill_c": { "type": "number" },
                    "windchill_f": { "type": "number" },
                    "heatindex_c": { "type": "number" },
                    "heatindex_f": { "type": "number" },
                    "dewpoint_c": { "type": "number" },
                    "dewpoint_f": { "type": "number" },
                    "will_it_rain": { "type": "integer" },
                    "chance_of_rain": { "type": "integer" },
                    "will_it_snow": { "type": "integer" },
                    "chance_of_snow": { "type": "integer" },
                    "vis_km": { "type": "number" },
                    "vis_miles": { "type": "number" },
                    "gust_mph": { "type": "number" },
                    "gust_kph": { "type": "number" },
                    "uv": { "type": "number" }
                  },
                  "additionalProperties": false
                }
              }
            },
            "additionalProperties": false
          }
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
"""

_schema = json.loads(RESPONSE_SCHEMA)
_validator = Draft202012Validator(_schema)

def validate_restapi_payload(payload):
    schema_missing = []
    schema_unexpected = []
    type_mismatch = []
    type_drift = []

    for error in _validator.iter_errors(payload):
        path = ".".join(str(p) for p in error.path) or "$"

        if error.validator == "required":
            missing = error.message.split("'")[1]
            schema_missing.append((f"{path}.{missing}", missing, error.message))

        elif error.validator == "additionalProperties":
            extra_key = error.message.split("'")[1]
            schema_unexpected.append((f"{path}.{extra_key}", extra_key, error.message))

        elif error.validator == "type":
            expected = error.validator_value
            actual = type(error.instance).__name__
            type_mismatch.append((path, expected, actual))
            type_drift.append((path, expected, actual))

    return {
        "schema_missing": schema_missing,
        "schema_unexpected": schema_unexpected,
        "type_mismatch": type_mismatch,
        "type_drift": type_drift,
    }
