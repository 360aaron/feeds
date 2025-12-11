from math import isinf, isnan

def is_day_valid(day_tuple) -> bool:
    # Must match the tuple shape in process_day
    if len(day_tuple) != 24:
        return False

    (
        day_hash,
        source_unique_id,
        maxtemp_c,
        maxtemp_f,
        mintemp_c,
        mintemp_f,
        avgtemp_c,
        avgtemp_f,
        maxwind_mph,
        maxwind_kph,
        totalprecip_mm,
        totalprecip_in,
        totalsnow_cm,
        avgvis_km,
        avgvis_miles,
        avghumidity,
        daily_will_it_rain,
        daily_chance_of_rain,
        daily_will_it_snow,
        daily_chance_of_snow,
        condition_text,
        condition_icon,
        condition_code,
        uv,
    ) = day_tuple

    # IDs
    if not isinstance(day_hash, str) or not isinstance(source_unique_id, str):
        return False

    # Floats (schema-type check, no domain logic)
    float_fields = [
        maxtemp_c, maxtemp_f, mintemp_c, mintemp_f,
        avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph,
        totalprecip_mm, totalprecip_in, totalsnow_cm,
        avgvis_km, avgvis_miles, avghumidity, uv,
    ]
    for v in float_fields:
        if not isinstance(v, float):
            return False
        if isnan(v) or isinf(v):
            return False

    # Ints
    int_fields = [
        daily_will_it_rain,
        daily_chance_of_rain,
        daily_will_it_snow,
        daily_chance_of_snow,
        condition_code,
    ]
    if not all(isinstance(v, int) for v in int_fields):
        return False

    # Text fields
    if not isinstance(condition_text, str) or not isinstance(condition_icon, str):
        return False

    return True

def is_astro_valid(astro_tuple) -> bool:
    # Must match build_astro shape
    if len(astro_tuple) != 9:
        return False

    (
        astro_hash,
        source_unique_id,
        parent_id,
        sunrise,
        sunset,
        moonrise,
        moonset,
        moon_phase,
        moon_illumination,
    ) = astro_tuple

    # IDs
    if not isinstance(astro_hash, str):
        return False
    if not isinstance(source_unique_id, str):
        return False
    if not isinstance(parent_id, str):
        return False

    # Text fields
    text_fields = [sunrise, sunset, moonrise, moonset, moon_phase]
    if not all(isinstance(v, str) for v in text_fields):
        return False

    # Int field
    if not isinstance(moon_illumination, int):
        return False

    return True


def is_hour_valid(hour_tuple) -> bool:
    # Must match each element in build_hours
    if len(hour_tuple) != 39:
        return False

    (
        hour_hash,
        source_unique_id,
        parent_id,
        time_epoch,
        time,
        temp_c,
        temp_f,
        is_day,
        condition_text,
        condition_icon,
        condition_code,
        wind_mph,
        wind_kph,
        wind_degree,
        wind_dir,
        pressure_mb,
        pressure_in,
        precip_mm,
        precip_in,
        snow_cm,
        humidity,
        cloud,
        feelslike_c,
        feelslike_f,
        windchill_c,
        windchill_f,
        heatindex_c,
        heatindex_f,
        dewpoint_c,
        dewpoint_f,
        will_it_rain,
        chance_of_rain,
        will_it_snow,
        chance_of_snow,
        vis_km,
        vis_miles,
        gust_mph,
        gust_kph,
        uv,
    ) = hour_tuple

    # IDs
    if not isinstance(hour_hash, str):
        return False
    if not isinstance(source_unique_id, str):
        return False
    if not isinstance(parent_id, str):
        return False

    # Ints
    int_fields = [
        time_epoch,
        is_day,
        condition_code,
        wind_degree,
        humidity,
        cloud,
        will_it_rain,
        chance_of_rain,
        will_it_snow,
        chance_of_snow,
    ]
    if not all(isinstance(v, int) for v in int_fields):
        return False

    # Floats (schema-type only, no domain rules)
    float_fields = [
        temp_c, temp_f,
        wind_mph, wind_kph,
        pressure_mb, pressure_in,
        precip_mm, precip_in,
        snow_cm,
        feelslike_c, feelslike_f,
        windchill_c, windchill_f,
        heatindex_c, heatindex_f,
        dewpoint_c, dewpoint_f,
        vis_km, vis_miles,
        gust_mph, gust_kph,
        uv,
    ]
    for v in float_fields:
        if not isinstance(v, float):
            return False
        if isnan(v) or isinf(v):
            return False

    # Text fields
    text_fields = [
        time,
        condition_text,
        condition_icon,
        wind_dir,
    ]
    if not all(isinstance(v, str) for v in text_fields):
        return False

    return True
