BAU_SELECT = """
select * from restapi_records
where normalized_on >= now() - interval '24 hours'; -- depends on job's frequency / schedule
"""

SINCE_SELECT = """
select * from restapi_records
where normalized_on::date >= %s;
"""

FULL_SELECT = """
select * from restapi_records;
"""

TARGETED_SELECT = """
select * from restapi_records
where source_unique_id = any(%s);
"""

UPSERT_DAY = """
insert into normalized_days (
    record_hash, source_unique_id,
    maxtemp_c, maxtemp_f, mintemp_c, mintemp_f,
    avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph,
    totalprecip_mm, totalprecip_in,
    totalsnow_cm, avgvis_km, avgvis_miles,
    avghumidity, daily_will_it_rain, 
    daily_chance_of_rain, daily_will_it_snow,
    daily_chance_of_snow, condition_text, 
    condition_icon, condition_code, uv
) values %s
on conflict (source_unique_id) do update set 
    record_hash          = excluded.record_hash,
    source_unique_id     = excluded.source_unique_id,
    maxtemp_c            = excluded.maxtemp_c,
    maxtemp_f            = excluded.maxtemp_f,
    mintemp_c            = excluded.mintemp_c,
    mintemp_f            = excluded.mintemp_f,
    avgtemp_c            = excluded.avgtemp_c,
    avgtemp_f            = excluded.avgtemp_f,
    maxwind_mph          = excluded.maxwind_mph,
    maxwind_kph          = excluded.maxwind_kph,
    totalprecip_mm       = excluded.totalprecip_mm,
    totalprecip_in       = excluded.totalprecip_in,
    totalsnow_cm         = excluded.totalsnow_cm,
    avgvis_km            = excluded.avgvis_km,
    avgvis_miles         = excluded.avgvis_miles,
    avghumidity          = excluded.avghumidity,
    daily_will_it_rain   = excluded.daily_will_it_rain,
    daily_chance_of_rain = excluded.daily_chance_of_rain,
    daily_will_it_snow   = excluded.daily_will_it_snow,
    daily_chance_of_snow = excluded.daily_chance_of_snow,
    condition_text       = excluded.condition_text,
    condition_icon       = excluded.condition_icon,
    condition_code       = excluded.condition_code,
    uv                   = excluded.uv
where normalized_days.record_hash is distinct from excluded.record_hash;
"""

UPSERT_ASTRO = """
insert into normalized_astrology (
    record_hash, source_unique_id, parent_id, sunrise, sunset,
    moonrise, moonset, moon_phase, moon_illumination
) values %s
on conflict (source_unique_id) do update set 
    record_hash         = excluded.record_hash,
    source_unique_id    = excluded.source_unique_id,
    parent_id           = excluded.parent_id,
    sunrise             = excluded.sunrise,
    sunset              = excluded.sunset,
    moonrise            = excluded.moonrise,
    moonset             = excluded.moonset,
    moon_phase          = excluded.moon_phase,
    moon_illumination   = excluded.moon_illumination
where normalized_astrology.record_hash is distinct from excluded.record_hash;
"""

UPSERT_HOUR = """
insert into normalized_hours (
    record_hash, source_unique_id, parent_id, time_epoch, time,
    temp_c, temp_f, is_day, condition_text, 
    condition_icon, condition_code, wind_mph, 
    wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in,
    precip_mm, precip_in, snow_cm, humidity, cloud,
    feelslike_c, feelslike_f, windchill_c, windchill_f,
    heatindex_c, heatindex_f, dewpoint_c, dewpoint_f,
    will_it_rain, chance_of_rain, will_it_snow, 
    chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv
) values %s
on conflict (source_unique_id) do update set 
    record_hash         = excluded.record_hash,
    source_unique_id    = excluded.source_unique_id,
    parent_id           = excluded.parent_id,
    time_epoch          = excluded.time_epoch,
    time                = excluded.time,
    temp_c              = excluded.temp_c,
    temp_f              = excluded.temp_f,
    is_day              = excluded.is_day,
    condition_text      = excluded.condition_text,
    condition_icon      = excluded.condition_icon,
    condition_code      = excluded.condition_code,
    wind_mph            = excluded.wind_mph,
    wind_kph            = excluded.wind_kph,
    wind_degree         = excluded.wind_degree,
    wind_dir            = excluded.wind_dir,
    pressure_mb         = excluded.pressure_mb,
    pressure_in         = excluded.pressure_in,
    precip_mm           = excluded.precip_mm,
    precip_in           = excluded.precip_in,
    snow_cm             = excluded.snow_cm,
    humidity            = excluded.humidity,
    cloud               = excluded.cloud,
    feelslike_c         = excluded.feelslike_c,
    feelslike_f         = excluded.feelslike_f,
    windchill_c         = excluded.windchill_c,
    windchill_f         = excluded.windchill_f,
    heatindex_c         = excluded.heatindex_c,
    heatindex_f         = excluded.heatindex_f,
    dewpoint_c          = excluded.dewpoint_c,
    dewpoint_f          = excluded.dewpoint_f,
    will_it_rain        = excluded.will_it_rain,
    chance_of_rain      = excluded.chance_of_rain,
    will_it_snow        = excluded.will_it_snow,
    chance_of_snow      = excluded.chance_of_snow,
    vis_km              = excluded.vis_km,
    vis_miles           = excluded.vis_miles,
    gust_mph            = excluded.gust_mph,
    gust_kph            = excluded.gust_kph,
    uv                  = excluded.uv
where normalized_hours.record_hash is distinct from excluded.record_hash;
"""