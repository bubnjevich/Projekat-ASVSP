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


winter_workload_index_sql = """
CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS curated.winter_workload_index (
    state             text    NOT NULL,
    county            text    NOT NULL,
    year              int     NOT NULL,
    month             int     NOT NULL,
    workload_index    double precision NOT NULL,
    mom_change_pct    double precision,
    rolling_3m_avg    double precision,
    rank_in_state     int,
    PRIMARY KEY (state, county, year, month)
);

-- Distribucija po state (da bi raspored bio na nivou države)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_dist_partition
        WHERE logicalrelid = 'curated.winter_workload_index'::regclass
    ) THEN
        PERFORM create_distributed_table('curated.winter_workload_index', 'state');
    END IF;
END $$;

"""

risky_sequences_sql = """

CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS curated.risky_sequences (
    state         text    NOT NULL,
    county        text    NOT NULL,
    seq_start_ts  timestamp NOT NULL,
    seq_end_ts    timestamp NOT NULL,
    duration_h    double precision NOT NULL,
    sequence      text    NOT NULL,   -- npr. "RAIN→FREEZE→SNOW"
    PRIMARY KEY (state, county, seq_start_ts)
);

-- Distribucija po state
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_dist_partition
        WHERE logicalrelid = 'curated.risky_sequences'::regclass
    ) THEN
        PERFORM create_distributed_table('curated.risky_sequences', 'state');
    END IF;
END $$;
"""

winter_burstiness_sql = """
CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS curated.winter_burstiness (
  state                       text NOT NULL,
  county                      text NOT NULL,
  season_year                 int  NOT NULL,  -- zimska sezona: DJF (decembar pripada narednoj year)
  total_episodes              int  NOT NULL,
  episodes_over_threshold     int  NOT NULL,
  avg_interepisode_gap_h      double precision,
  avg_episode_duration_h      double precision,
  max_episode_duration_h      double precision,
  PRIMARY KEY (state, county, season_year)
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_dist_partition
    WHERE logicalrelid = 'curated.winter_burstiness'::regclass
  ) THEN
    PERFORM create_distributed_table('curated.winter_burstiness','state');
  END IF;
END $$;
"""

night_thunderstorm_risk_sql = """
CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS curated.night_thunderstorm_risk (
    state               text NOT NULL,
    county              text NOT NULL,
    year                int  NOT NULL,
    month               int  NOT NULL,
    count_events        int  NOT NULL,         -- svi THUNDERSTORM događaji (dan + noć)
    count_night_events  int  NOT NULL,         -- samo noćni
    night_event_ratio   double precision,      -- procenat noćnih u odnosu na sve
    avg_duration_h      double precision,      -- prosečno trajanje noćnih oluja
    avg_severity_score  double precision,      -- prosečna ozbiljnost noćnih oluja
    rolling_3m_avg_duration double precision,  -- rolling 3M prosečno trajanje noćnih oluja
    PRIMARY KEY (state, county, year, month)
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_dist_partition
    WHERE logicalrelid = 'curated.night_thunderstorm_risk'::regclass
  ) THEN
    PERFORM create_distributed_table('curated.night_thunderstorm_risk','state');
  END IF;
END $$;

"""

winter_fenology_sql = """

CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS curated.winter_fenology (
    state                         text NOT NULL,
    county                        text NOT NULL,
    season_year                   int  NOT NULL, -- DJF: decembar ide u narednu godinu
    first_event_ts                timestamp NOT NULL, -- prvi zimski dogadjaj za taj okrug i tu godinu
    last_event_ts                 timestamp NOT NULL, -- poslednji zimski dogadjaj za taj okrug i tu godinu
    season_length_days            double precision NOT NULL,  -- (last-first) u danima
    start_shift_days              double precision,           -- pomeraj starta vs. prošla sezona
    end_shift_days                double precision,           -- pomeraj kraja vs. prošla sezona
    yoy_length_change_days        double precision,           -- promjena dužine sezone vs. prošla
    rolling_3y_length_avg_days    double precision,           -- 3-sezonski klizeći prosek dužine
    PRIMARY KEY (state, county, season_year)
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_dist_partition
    WHERE logicalrelid = 'curated.winter_fenology'::regclass
  ) THEN
    PERFORM create_distributed_table('curated.winter_fenology','state');
  END IF;
END $$;



"""