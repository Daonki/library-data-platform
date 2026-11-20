-- 원본(raw)과 마트(mart) 스키마. 여러 번 실행해도 안전하도록 IF NOT EXISTS 사용
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

-- ── raw: API 응답을 거의 그대로 보관 ──────────────────────────
CREATE TABLE IF NOT EXISTS raw.seat_snapshot (
    stdg_cd         TEXT        NOT NULL,      -- 지자체(법정동) 코드: 도서관 ID가 지자체마다 PLR001부터 다시 매겨져 함께 키로 사용
    pblib_id        TEXT        NOT NULL,
    rdrm_id         TEXT        NOT NULL,
    tot_dt          TIMESTAMP   NOT NULL,      -- 기관이 집계한 기준 시각
    pblib_nm        TEXT,
    rdrm_nm         TEXT,
    rdrm_type_nm    TEXT,
    floor_expln     TEXT,
    total_seats     INT,
    used_seats      INT,
    reserved_seats  INT,
    remain_seats    INT,
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (stdg_cd, pblib_id, rdrm_id, tot_dt)     -- 같은 기준 시각은 한 번만 (중복 적재 방지)
);

CREATE TABLE IF NOT EXISTS raw.library_info (
    stdg_cd         TEXT NOT NULL,
    pblib_id        TEXT NOT NULL,
    pblib_nm        TEXT,
    ctpv_nm         TEXT,           -- 시도
    sgg_nm          TEXT,           -- 시군구
    pblib_type_nm   TEXT,
    lat             NUMERIC,
    lon             NUMERIC,
    total_seats     INT,
    updated_at      TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (stdg_cd, pblib_id)
);

-- 검증에서 걸러진 행 (건너뛰고 사유 기록)
CREATE TABLE IF NOT EXISTS raw.load_rejects (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT,
    reason      TEXT,
    payload     JSONB,
    rejected_at TIMESTAMP NOT NULL DEFAULT now()
);

-- ── mart: 스타 스키마 ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_key    INT PRIMARY KEY,    -- YYYYMMDD
    full_date   DATE NOT NULL,
    year        INT,
    month       INT,
    day         INT,
    dow         INT,                -- 0=일 ~ 6=토
    is_weekend  BOOLEAN
);

CREATE TABLE IF NOT EXISTS mart.dim_library (
    library_key   SERIAL PRIMARY KEY,
    stdg_cd       TEXT NOT NULL,
    pblib_id      TEXT NOT NULL,
    pblib_nm      TEXT,
    ctpv_nm       TEXT,
    sgg_nm        TEXT,
    pblib_type_nm TEXT,
    lat           NUMERIC,
    lon           NUMERIC,
    UNIQUE (stdg_cd, pblib_id)
);

CREATE TABLE IF NOT EXISTS mart.dim_room (
    room_key      SERIAL PRIMARY KEY,
    stdg_cd       TEXT NOT NULL,
    pblib_id      TEXT NOT NULL,
    rdrm_id       TEXT NOT NULL,
    rdrm_nm       TEXT,
    rdrm_type_nm  TEXT,
    floor_expln   TEXT,
    UNIQUE (stdg_cd, pblib_id, rdrm_id)
);

CREATE TABLE IF NOT EXISTS mart.fact_seat_snapshot (
    library_key     INT NOT NULL REFERENCES mart.dim_library(library_key),
    room_key        INT NOT NULL REFERENCES mart.dim_room(room_key),
    date_key        INT NOT NULL REFERENCES mart.dim_date(date_key),
    hour            INT NOT NULL,
    tot_dt          TIMESTAMP NOT NULL,
    total_seats     INT,
    used_seats      INT,
    remain_seats    INT,
    occupancy_rate  NUMERIC(5,4),   -- 사용 좌석 / 전체 좌석
    PRIMARY KEY (room_key, tot_dt)
);
