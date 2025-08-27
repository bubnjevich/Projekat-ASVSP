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

cooccurrence_sql = """
    CREATE SCHEMA IF NOT EXISTS curated;
    CREATE EXTENSION IF NOT EXISTS citus;

    -- Daily co-occurrence (minute overlap) per airport and pair of types
    CREATE TABLE IF NOT EXISTS curated.cooccurrence_minutes_daily (
        airport_code   text        NOT NULL,
        event_date     date        NOT NULL,
        type_i         text        NOT NULL,
        type_j         text        NOT NULL,
        overlap_minutes double precision NOT NULL,
        -- kanonizovan par za jedinstvenost (RAIN+FOG == FOG+RAIN)
        type_pair text GENERATED ALWAYS AS (
            CASE WHEN type_i <= type_j THEN type_i || '+' || type_j
                 ELSE type_j || '+' || type_i
            END
        ) STORED,
        PRIMARY KEY (airport_code, event_date, type_pair)
    );

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_dist_partition
            WHERE logicalrelid = 'curated.cooccurrence_minutes_daily'::regclass
        ) THEN
            PERFORM create_distributed_table('curated.cooccurrence_minutes_daily', 'airport_code');
        END IF;
    END $$;
"""
peak3h_sql = """
    CREATE SCHEMA IF NOT EXISTS curated;
    CREATE EXTENSION IF NOT EXISTS citus;
    
    CREATE TABLE IF NOT EXISTS curated.peak_exposure_3h_daily (
      airport_code   text        NOT NULL,
      event_date     date        NOT NULL,
      p3h_minutes    double precision NOT NULL,
      window_start   timestamptz NOT NULL,
      window_end     timestamptz NOT NULL,
      PRIMARY KEY (airport_code, event_date)
    );
    
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_dist_partition
        WHERE logicalrelid = 'curated.peak_exposure_3h_daily'::regclass
      ) THEN
        PERFORM create_distributed_table('curated.peak_exposure_3h_daily', 'airport_code');
      END IF;
END $$;
"""

monthly_trend_by_type_sql = """
    CREATE SCHEMA IF NOT EXISTS curated;
    CREATE EXTENSION IF NOT EXISTS citus;

    CREATE TABLE IF NOT EXISTS curated.monthly_risk_trend_by_type (
        airport_code      text    NOT NULL,
        year              int     NOT NULL,
        month             int     NOT NULL,   -- 1..12
        type              text    NOT NULL,
        avg_duration_min  double precision NOT NULL,
        total_events      bigint  NOT NULL,
        mom_change_pct    double precision,   -- % promene u odnosu na prethodni mesec (isti airport+type)
        PRIMARY KEY (airport_code, year, month, type)
    );

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_dist_partition
            WHERE logicalrelid = 'curated.monthly_risk_trend_by_type'::regclass
        ) THEN
            PERFORM create_distributed_table('curated.monthly_risk_trend_by_type', 'airport_code');
        END IF;
    END $$;
"""


