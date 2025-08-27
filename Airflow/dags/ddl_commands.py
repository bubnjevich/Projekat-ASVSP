longest_sql = """
    CREATE SCHEMA IF NOT EXISTS curated;
    CREATE EXTENSION IF NOT EXISTS citus;

    CREATE TABLE IF NOT EXISTS curated.longest_episodes (
        airport_code text NOT NULL,
        type         text NOT NULL, 
        session_start timestamptz NOT NULL,
        session_end   timestamptz NOT NULL,
        session_duration_hours double precision NOT NULL,
        events_in_session int NOT NULL,
        PRIMARY KEY (airport_code, type, session_start)
    );

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_dist_partition
            WHERE logicalrelid = 'curated.longest_episodes'::regclass
        ) THEN
            PERFORM create_distributed_table('curated.longest_episodes', 'airport_code');
        END IF;
    END $$;
    """

exposure_sql = """
    CREATE SCHEMA IF NOT EXISTS curated;
    CREATE EXTENSION IF NOT EXISTS citus;
    
    CREATE TABLE IF NOT EXISTS curated.exposure_hours_daily (
        airport_code text NOT NULL,
        event_date date NOT NULL,
        exposure_hours_day double precision NOT NULL,
        exposure_hours_30d double precision,
        exposure_hours_90d double precision,
        PRIMARY KEY (airport_code, event_date)
    );
    
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_dist_partition
            WHERE logicalrelid = 'curated.exposure_hours_daily'::regclass
        ) THEN
            PERFORM create_distributed_table('curated.exposure_hours_daily', 'airport_code');
        END IF;
    END $$;
"""


