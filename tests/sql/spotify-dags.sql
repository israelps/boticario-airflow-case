DROP TABLE IF EXISTS raw_data_hackers_episodes CASCADE;
DROP TABLE IF EXISTS raw_data_hackers_podcasts CASCADE;

SELECT tablename FROM pg_catalog.pg_tables
where schemaname NOT IN ('pg_catalog', 'information_schema')
;

select * from pg_catalog.pg_views
where schemaname NOT IN ('pg_catalog', 'information_schema')
;


select * from raw_data_hackers_podcasts;

select * from raw_data_hackers_episodes;

select * from vw_data_hackers_boticario_episodes;


