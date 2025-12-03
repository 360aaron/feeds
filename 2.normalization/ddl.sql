/* 

Stage valid ingested records
Upsert into live normalized tables; if there are changes:
    1. Archive current version of the record
    2. Outbox changes for downstream systems

TODO:
- Add outbox_archive(s)
- Add unique identifier from source for onconflict handling

*/
-- DROPS
drop table if exists normalized_days;
drop table if exists normalized_astrology;
drop table if exists normalized_hours;

drop table if exists normalized_days_archive;
drop table if exists normalized_astrology_archive;
drop table if exists normalized_hours_archive;

drop table if exists normalized_days_sparse_outbox;
drop table if exists normalized_astrology_sparse_outbox;
drop table if exists normalized_hours_sparse_outbox;

-- Live
create table if not exists normalized_days (
    --
    id                      serial,
    source_unique_id        text primary key,
    record_hash             text,
    normalized_on           timestamp default current_timestamp,
    archived_on             timestamp default null,
    --
    maxtemp_c               decimal,
    maxtemp_f               decimal,
    mintemp_c               decimal,
    mintemp_f               decimal,
    avgtemp_c               decimal,
    avgtemp_f               decimal,
    maxwind_mph             decimal,
    maxwind_kph             decimal,
    totalprecip_mm          decimal,
    totalprecip_in          decimal,
    totalsnow_cm            decimal,
    avgvis_km               decimal,
    avgvis_miles            decimal,
    avghumidity             decimal,
    daily_will_it_rain      int,
    daily_chance_of_rain    int,
    daily_will_it_snow      int,
    daily_chance_of_snow    int,
    condition_text          text,
    condition_icon          text,
    condition_code          int,
    uv                      decimal
);

create table if not exists normalized_astrology (
    --
    id                  serial,
    source_unique_id    text primary key,
    parent_id           text,
    record_hash         text,
    normalized_on       timestamp default current_timestamp,
    archived_on         timestamp default null,
    --
    sunrise             text,
    sunset              text,
    moonrise            text,
    moonset             text,
    moon_phase          text,
    moon_illumination   int
);

create table if not exists normalized_hours (
    --
    id                  serial,
    source_unique_id    text primary key,
    parent_id           text,
    record_hash         text,
    normalized_on       timestamp default current_timestamp,
    archived_on         timestamp default null,
    --
    time_epoch          int,
    time                text,
    temp_c              decimal,
    temp_f              decimal,
    is_day              int,
    condition_text      text,
    condition_icon      text,
    condition_code      int,
    wind_mph            decimal,
    wind_kph            decimal,
    wind_degree         int,
    wind_dir            text,
    pressure_mb         decimal,
    pressure_in         decimal,
    precip_mm           decimal,
    precip_in           decimal,
    snow_cm             decimal,
    humidity            int,
    cloud               int,
    feelslike_c         decimal,
    feelslike_f         decimal,
    windchill_c         decimal,
    windchill_f         decimal,
    heatindex_c         decimal,
    heatindex_f         decimal,
    dewpoint_c          decimal,
    dewpoint_f          decimal,
    will_it_rain        int,
    chance_of_rain      int,
    will_it_snow        int,
    chance_of_snow      int,
    vis_km              decimal,
    vis_miles           decimal,
    gust_mph            decimal,
    gust_kph            decimal,
    uv                  decimal
);

-- Archive
create table if not exists normalized_days_archive (
    --
    id                      serial primary key,
    source_unique_id        text,
    record_hash             text,
    normalized_on           timestamp,
    archived_on             timestamp default current_timestamp,
    --
    maxtemp_c               decimal,
    maxtemp_f               decimal,
    mintemp_c               decimal,
    mintemp_f               decimal,
    avgtemp_c               decimal,
    avgtemp_f               decimal,
    maxwind_mph             decimal,
    maxwind_kph             decimal,
    totalprecip_mm          decimal,
    totalprecip_in          decimal,
    totalsnow_cm            decimal,
    avgvis_km               decimal,
    avgvis_miles            decimal,
    avghumidity             decimal,
    daily_will_it_rain      int,
    daily_chance_of_rain    int,
    daily_will_it_snow      int,
    daily_chance_of_snow    int,
    condition_text          text,
    condition_icon          text,
    condition_code          int,
    uv                      decimal
);

create table if not exists normalized_astrology_archive (
    --
    id                  serial primary key,
    source_unique_id    text,
    parent_id           text,
    record_hash         text,
    normalized_on       timestamp,
    archived_on         timestamp default current_timestamp,
    --
    sunrise             text,
    sunset              text,
    moonrise            text,
    moonset             text,
    moon_phase          text,
    moon_illumination   int
);

create table if not exists normalized_hours_archive (
    --
    id                  serial primary key,
    source_unique_id    text,
    parent_id           text,
    record_hash         text,
    normalized_on       timestamp,
    archived_on         timestamp default current_timestamp,
    --
    time_epoch          int,
    time                text,
    temp_c              decimal,
    temp_f              decimal,
    is_day              int,
    condition_text      text,
    condition_icon      text,
    condition_code      int,
    wind_mph            decimal,
    wind_kph            decimal,
    wind_degree         int,
    wind_dir            text,
    pressure_mb         decimal,
    pressure_in         decimal,
    precip_mm           decimal,
    precip_in           decimal,
    snow_cm             decimal,
    humidity            int,
    cloud               int,
    feelslike_c         decimal,
    feelslike_f         decimal,
    windchill_c         decimal,
    windchill_f         decimal,
    heatindex_c         decimal,
    heatindex_f         decimal,
    dewpoint_c          decimal,
    dewpoint_f          decimal,
    will_it_rain        int,
    chance_of_rain      int,
    will_it_snow        int,
    chance_of_snow      int,
    vis_km              decimal,
    vis_miles           decimal,
    gust_mph            decimal,
    gust_kph            decimal,
    uv                  decimal
);

-- Outboxes
create table if not exists normalized_days_sparse_outbox (
    --
    id                      serial primary key,
    source_unique_id        text,
    outboxed_on             timestamp default current_timestamp,
    --
    maxtemp_c               decimal,
    maxtemp_f               decimal,
    mintemp_c               decimal,
    mintemp_f               decimal,
    avgtemp_c               decimal,
    avgtemp_f               decimal,
    maxwind_mph             decimal,
    maxwind_kph             decimal,
    totalprecip_mm          decimal,
    totalprecip_in          decimal,
    totalsnow_cm            decimal,
    avgvis_km               decimal,
    avgvis_miles            decimal,
    avghumidity             decimal,
    daily_will_it_rain      int,
    daily_chance_of_rain    int,
    daily_will_it_snow      int,
    daily_chance_of_snow    int,
    condition_text          text,
    condition_icon          text,
    condition_code          int,
    uv                      decimal
);

create table if not exists normalized_astrology_sparse_outbox (
    id                  serial,
    source_unique_id    text,
    parent_id           text,
    record_hash         text,
    outboxed_on         timestamp default current_timestamp,
    --
    sunrise             text,
    sunset              text,
    moonrise            text,
    moonset             text,
    moon_phase          text,
    moon_illumination   int
);

create table if not exists normalized_hours_sparse_outbox (
    --
    id                  serial primary key,
    source_unique_id    text,
    parent_id           text,
    outboxed_on         timestamp default current_timestamp,
    --
    time_epoch          int,
    time                text,
    temp_c              decimal,
    temp_f              decimal,
    is_day              int,
    condition_text      text,
    condition_icon      text,
    condition_code      int,
    wind_mph            decimal,
    wind_kph            decimal,
    wind_degree         int,
    wind_dir            text,
    pressure_mb         decimal,
    pressure_in         decimal,
    precip_mm           decimal,
    precip_in           decimal,
    snow_cm             decimal,
    humidity            int,
    cloud               int,
    feelslike_c         decimal,
    feelslike_f         decimal,
    windchill_c         decimal,
    windchill_f         decimal,
    heatindex_c         decimal,
    heatindex_f         decimal,
    dewpoint_c          decimal,
    dewpoint_f          decimal,
    will_it_rain        int,
    chance_of_rain      int,
    will_it_snow        int,
    chance_of_snow      int,
    vis_km              decimal,
    vis_miles           decimal,
    gust_mph            decimal,
    gust_kph            decimal,
    uv                  decimal
);

-- Triggers and Functions
create or replace function archive_normalized_days()
returns trigger
language plpgsql
as $$
begin
    if new.record_hash is distinct from old.record_hash then
        insert into normalized_days_archive (
            source_unique_id,
            record_hash,
            normalized_on,
            archived_on,
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
            uv
        )
        values (
            old.source_unique_id,
            old.record_hash,
            old.normalized_on,
            now(),
            old.maxtemp_c,
            old.maxtemp_f,
            old.mintemp_c,
            old.mintemp_f,
            old.avgtemp_c,
            old.avgtemp_f,
            old.maxwind_mph,
            old.maxwind_kph,
            old.totalprecip_mm,
            old.totalprecip_in,
            old.totalsnow_cm,
            old.avgvis_km,
            old.avgvis_miles,
            old.avghumidity,
            old.daily_will_it_rain,
            old.daily_chance_of_rain,
            old.daily_will_it_snow,
            old.daily_chance_of_snow,
            old.condition_text,
            old.condition_icon,
            old.condition_code,
            old.uv
        );
    end if;
    return new;
end;
$$;

create trigger archive_normalized_days_trigger
before update on normalized_days
for each row
execute function archive_normalized_days();

create or replace function outbox_day_changes()
returns trigger
language plpgsql
as $$
begin
    if tg_op = 'INSERT' then
        insert into normalized_days_sparse_outbox (
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
            uv
        )
        values (
            new.source_unique_id,
            new.maxtemp_c,
            new.maxtemp_f,
            new.mintemp_c,
            new.mintemp_f,
            new.avgtemp_c,
            new.avgtemp_f,
            new.maxwind_mph,
            new.maxwind_kph,
            new.totalprecip_mm,
            new.totalprecip_in,
            new.totalsnow_cm,
            new.avgvis_km,
            new.avgvis_miles,
            new.avghumidity,
            new.daily_will_it_rain,
            new.daily_chance_of_rain,
            new.daily_will_it_snow,
            new.daily_chance_of_snow,
            new.condition_text,
            new.condition_icon,
            new.condition_code,
            new.uv
        );
    
    elsif tg_op = 'UPDATE' then
        insert into normalized_days_sparse_outbox (
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
            uv
        )
        values (
            new.source_unique_id,
            case when new.maxtemp_c                   is distinct from old.maxtemp_c                   then new.maxtemp_c                   end,
            case when new.maxtemp_f                   is distinct from old.maxtemp_f                   then new.maxtemp_f                   end,
            case when new.mintemp_c                   is distinct from old.mintemp_c                   then new.mintemp_c                   end,
            case when new.mintemp_f                   is distinct from old.mintemp_f                   then new.mintemp_f                   end,
            case when new.avgtemp_c                   is distinct from old.avgtemp_c                   then new.avgtemp_c                   end,
            case when new.avgtemp_f                   is distinct from old.avgtemp_f                   then new.avgtemp_f                   end,
            case when new.maxwind_mph                 is distinct from old.maxwind_mph                 then new.maxwind_mph                 end,
            case when new.maxwind_kph                 is distinct from old.maxwind_kph                 then new.maxwind_kph                 end,
            case when new.totalprecip_mm              is distinct from old.totalprecip_mm              then new.totalprecip_mm              end,
            case when new.totalprecip_in              is distinct from old.totalprecip_in              then new.totalprecip_in              end,
            case when new.totalsnow_cm                is distinct from old.totalsnow_cm                then new.totalsnow_cm                end,
            case when new.avgvis_km                   is distinct from old.avgvis_km                   then new.avgvis_km                   end,
            case when new.avgvis_miles                is distinct from old.avgvis_miles                then new.avgvis_miles                end,
            case when new.avghumidity                 is distinct from old.avghumidity                 then new.avghumidity                 end,
            case when new.daily_will_it_rain          is distinct from old.daily_will_it_rain          then new.daily_will_it_rain          end,
            case when new.daily_chance_of_rain        is distinct from old.daily_chance_of_rain        then new.daily_chance_of_rain        end,
            case when new.daily_will_it_snow          is distinct from old.daily_will_it_snow          then new.daily_will_it_snow          end,
            case when new.daily_chance_of_snow        is distinct from old.daily_chance_of_snow        then new.daily_chance_of_snow        end,
            case when new.condition_text              is distinct from old.condition_text              then new.condition_text              end,
            case when new.condition_icon              is distinct from old.condition_icon              then new.condition_icon              end,
            case when new.condition_code              is distinct from old.condition_code              then new.condition_code              end,
            case when new.uv                          is distinct from old.uv                          then new.uv                          end
        );
    end if;

    return new;
end;
$$;

create trigger outbox_day_changes_trigger
after insert or update on normalized_days
for each row
execute function outbox_day_changes();

create or replace function archive_normalized_astrology()
returns trigger
language plpgsql
as $$
begin
    if new.record_hash is distinct from old.record_hash then
        insert into normalized_astrology_archive (
            source_unique_id,
            parent_id,
            record_hash,
            normalized_on,
            archived_on,
            sunrise,
            sunset,
            moonrise,
            moonset,
            moon_phase,
            moon_illumination
        )
        values (
            old.source_unique_id,
            old.parent_id,
            old.record_hash,
            old.normalized_on,
            old.archived_on,
            old.sunrise,
            old.sunset,
            old.moonrise,
            old.moonset,
            old.moon_phase,
            old.moon_illumination
        );
    end if;
    return new;
end;
$$;

create trigger archive_normalized_astrology_trigger
before update on normalized_astrology
for each row
execute function archive_normalized_astrology();

create or replace function outbox_astrology_changes()
returns trigger
language plpgsql
as $$
begin
    if tg_op = 'INSERT' then
        insert into normalized_astrology_sparse_outbox (
            parent_id,
            sunrise,
            sunset,
            moonrise,
            moonset,
            moon_phase,
            moon_illumination
        )
        values (
            new.parent_id,
            new.sunrise,
            new.sunset,
            new.moonrise,
            new.moonset,
            new.moon_phase,
            new.moon_illumination
        );

    elsif tg_op = 'UPDATE' then
        insert into normalized_astrology_sparse_outbox (
            parent_id,
            sunrise,
            sunset,
            moonrise,
            moonset,
            moon_phase,
            moon_illumination
        )
        values ( 
            new.parent_id,
            case when new.sunrise           is distinct from old.sunrise            then new.sunrise          end,
            case when new.sunset            is distinct from old.sunset             then new.sunset           end,
            case when new.moonrise          is distinct from old.moonrise           then new.moonrise         end,
            case when new.moonset           is distinct from old.moonset            then new.moonset          end,
            case when new.moon_phase        is distinct from old.moon_phase         then new.moon_phase       end,
            case when new.moon_illumination is distinct from old.moon_illumination  then new.moon_illumination end
        );
    end if;
    return new;
end;
$$;

create trigger outbox_astrology_changes_trigger
after insert or update on normalized_astrology
for each row
execute function outbox_astrology_changes();

create or replace function archive_normalized_hours()
returns trigger
language plpgsql
as $$
begin
    if new.record_hash is distinct from old.record_hash then
        insert into normalized_hours_archive (
            source_unique_id,
            parent_id,
            record_hash,
            normalized_on,
            archived_on,
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
            uv
        )
        values (
            old.source_unique_id,
            old.parent_id,
            old.record_hash,
            old.normalized_on,
            now(),
            old.time_epoch,
            old.time,
            old.temp_c,
            old.temp_f,
            old.is_day,
            old.condition_text,
            old.condition_icon,
            old.condition_code,
            old.wind_mph,
            old.wind_kph,
            old.wind_degree,
            old.wind_dir,
            old.pressure_mb,
            old.pressure_in,
            old.precip_mm,
            old.precip_in,
            old.snow_cm,
            old.humidity,
            old.cloud,
            old.feelslike_c,
            old.feelslike_f,
            old.windchill_c,
            old.windchill_f,
            old.heatindex_c,
            old.heatindex_f,
            old.dewpoint_c,
            old.dewpoint_f,
            old.will_it_rain,
            old.chance_of_rain,
            old.will_it_snow,
            old.chance_of_snow,
            old.vis_km,
            old.vis_miles,
            old.gust_mph,
            old.gust_kph,
            old.uv
        );
    end if;
    return new;
end;
$$;

create trigger archive_normalized_hours_trigger
before update on normalized_hours
for each row
execute function archive_normalized_hours();

create or replace function outbox_hour_changes()
returns trigger
language plpgsql
as $$
begin
    if tg_op = 'INSERT' then
        insert into normalized_hours_sparse_outbox (
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
            uv
        )
        values (
            new.source_unique_id,
            new.parent_id,
            new.time_epoch,
            new.time,
            new.temp_c,
            new.temp_f,
            new.is_day,
            new.condition_text,
            new.condition_icon,
            new.condition_code,
            new.wind_mph,
            new.wind_kph,
            new.wind_degree,
            new.wind_dir,
            new.pressure_mb,
            new.pressure_in,
            new.precip_mm,
            new.precip_in,
            new.snow_cm,
            new.humidity,
            new.cloud,
            new.feelslike_c,
            new.feelslike_f,
            new.windchill_c,
            new.windchill_f,
            new.heatindex_c,
            new.heatindex_f,
            new.dewpoint_c,
            new.dewpoint_f,
            new.will_it_rain,
            new.chance_of_rain,
            new.will_it_snow,
            new.chance_of_snow,
            new.vis_km,
            new.vis_miles,
            new.gust_mph,
            new.gust_kph,
            new.uv
        );

    elsif tg_op = 'UPDATE' then
        insert into normalized_hours_sparse_outbox (
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
            uv
        )
        values ( 
            case when new.source_unique_id  is distinct from old.source_unique_id   then new.source_unique_id   end,
            case when new.parent_id         is distinct from old.parent_id          then new.parent_id          end,
            case when new.time_epoch        is distinct from old.time_epoch         then new.time_epoch         end,
            case when new.time              is distinct from old.time               then new.time               end,
            case when new.temp_c            is distinct from old.temp_c             then new.temp_c             end,
            case when new.temp_f            is distinct from old.temp_f             then new.temp_f             end,
            case when new.is_day            is distinct from old.is_day             then new.is_day             end,
            case when new.condition_text    is distinct from old.condition_text     then new.condition_text     end,
            case when new.condition_icon    is distinct from old.condition_icon     then new.condition_icon     end,
            case when new.condition_code    is distinct from old.condition_code     then new.condition_code     end,
            case when new.wind_mph          is distinct from old.wind_mph           then new.wind_mph           end,
            case when new.wind_kph          is distinct from old.wind_kph           then new.wind_kph           end,
            case when new.wind_degree       is distinct from old.wind_degree        then new.wind_degree        end,
            case when new.wind_dir          is distinct from old.wind_dir           then new.wind_dir           end,
            case when new.pressure_mb       is distinct from old.pressure_mb        then new.pressure_mb        end,
            case when new.pressure_in       is distinct from old.pressure_in        then new.pressure_in        end,
            case when new.precip_mm         is distinct from old.precip_mm          then new.precip_mm          end,
            case when new.precip_in         is distinct from old.precip_in          then new.precip_in          end,
            case when new.snow_cm           is distinct from old.snow_cm            then new.snow_cm            end,
            case when new.humidity          is distinct from old.humidity           then new.humidity           end,
            case when new.cloud             is distinct from old.cloud              then new.cloud              end,
            case when new.feelslike_c       is distinct from old.feelslike_c        then new.feelslike_c        end,
            case when new.feelslike_f       is distinct from old.feelslike_f        then new.feelslike_f        end,
            case when new.windchill_c       is distinct from old.windchill_c        then new.windchill_c        end,
            case when new.windchill_f       is distinct from old.windchill_f        then new.windchill_f        end,
            case when new.heatindex_c       is distinct from old.heatindex_c        then new.heatindex_c        end,
            case when new.heatindex_f       is distinct from old.heatindex_f        then new.heatindex_f        end,
            case when new.dewpoint_c        is distinct from old.dewpoint_c         then new.dewpoint_c         end,
            case when new.dewpoint_f        is distinct from old.dewpoint_f         then new.dewpoint_f         end,
            case when new.will_it_rain      is distinct from old.will_it_rain       then new.will_it_rain       end,
            case when new.chance_of_rain    is distinct from old.chance_of_rain     then new.chance_of_rain     end,
            case when new.will_it_snow      is distinct from old.will_it_snow       then new.will_it_snow       end,
            case when new.chance_of_snow    is distinct from old.chance_of_snow     then new.chance_of_snow     end,
            case when new.vis_km            is distinct from old.vis_km             then new.vis_km             end,
            case when new.vis_miles         is distinct from old.vis_miles          then new.vis_miles          end,
            case when new.gust_mph          is distinct from old.gust_mph           then new.gust_mph           end,
            case when new.gust_kph          is distinct from old.gust_kph           then new.gust_kph           end,
            case when new.uv                is distinct from old.uv                 then new.uv                 end
        );
    end if;
    return new;
end;
$$;

create trigger outbox_hour_changes_trigger
after insert or update on normalized_hours
for each row
execute function outbox_hour_changes();

-- Tests
update normalized_days set maxtemp_f = 32, record_hash = '123test' where id = 1;
update normalized_astrology set sunrise = '07:21 AM', record_hash = '456test' where id = 1;
update normalized_hours set temp_c = 20, record_hash = '789test' where id = 1;