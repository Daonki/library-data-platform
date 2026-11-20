-- raw → mart 증분 반영. 매 실행마다 돌려도 결과가 같도록(멱등) ON CONFLICT 사용
-- 도서관 ID(pblib_id)는 지자체마다 PLR001부터 다시 매겨지므로, 항상 지자체 코드(stdg_cd)와 함께 키로 씀

-- 1) 차원: 도서관 (기본정보 우선, 없으면 열람실 데이터의 도서관명으로)
INSERT INTO mart.dim_library (stdg_cd, pblib_id, pblib_nm, ctpv_nm, sgg_nm, pblib_type_nm, lat, lon)
SELECT DISTINCT ON (s.stdg_cd, s.pblib_id)
       s.stdg_cd, s.pblib_id, COALESCE(i.pblib_nm, s.pblib_nm), i.ctpv_nm, i.sgg_nm, i.pblib_type_nm, i.lat, i.lon
FROM raw.seat_snapshot s
LEFT JOIN raw.library_info i ON i.stdg_cd = s.stdg_cd AND i.pblib_id = s.pblib_id
ORDER BY s.stdg_cd, s.pblib_id, s.tot_dt DESC
ON CONFLICT (stdg_cd, pblib_id) DO UPDATE
SET pblib_nm = EXCLUDED.pblib_nm,
    ctpv_nm  = COALESCE(EXCLUDED.ctpv_nm, mart.dim_library.ctpv_nm),
    sgg_nm   = COALESCE(EXCLUDED.sgg_nm,  mart.dim_library.sgg_nm),
    pblib_type_nm = COALESCE(EXCLUDED.pblib_type_nm, mart.dim_library.pblib_type_nm),
    lat = COALESCE(EXCLUDED.lat, mart.dim_library.lat),
    lon = COALESCE(EXCLUDED.lon, mart.dim_library.lon);

-- 2) 차원: 열람실 (가장 최근 값 기준)
INSERT INTO mart.dim_room (stdg_cd, pblib_id, rdrm_id, rdrm_nm, rdrm_type_nm, floor_expln)
SELECT DISTINCT ON (stdg_cd, pblib_id, rdrm_id)
       stdg_cd, pblib_id, rdrm_id, rdrm_nm, NULLIF(rdrm_type_nm, ''), floor_expln
FROM raw.seat_snapshot
ORDER BY stdg_cd, pblib_id, rdrm_id, tot_dt DESC
ON CONFLICT (stdg_cd, pblib_id, rdrm_id) DO UPDATE
SET rdrm_nm = EXCLUDED.rdrm_nm,
    rdrm_type_nm = EXCLUDED.rdrm_type_nm,
    floor_expln = EXCLUDED.floor_expln;

-- 3) 차원: 날짜 (raw에 등장한 날짜만)
INSERT INTO mart.dim_date (date_key, full_date, year, month, day, dow, is_weekend)
SELECT DISTINCT
       TO_CHAR(tot_dt, 'YYYYMMDD')::INT, tot_dt::DATE,
       EXTRACT(YEAR FROM tot_dt)::INT, EXTRACT(MONTH FROM tot_dt)::INT, EXTRACT(DAY FROM tot_dt)::INT,
       EXTRACT(DOW FROM tot_dt)::INT, EXTRACT(DOW FROM tot_dt) IN (0, 6)
FROM raw.seat_snapshot
ON CONFLICT (date_key) DO NOTHING;

-- 4) 사실: 아직 마트에 없는 스냅샷만 추가 (증분)
INSERT INTO mart.fact_seat_snapshot
       (library_key, room_key, date_key, hour, tot_dt, total_seats, used_seats, remain_seats, occupancy_rate)
SELECT l.library_key, r.room_key,
       TO_CHAR(s.tot_dt, 'YYYYMMDD')::INT, EXTRACT(HOUR FROM s.tot_dt)::INT, s.tot_dt,
       s.total_seats, s.used_seats, s.remain_seats,
       CASE WHEN s.total_seats > 0 THEN ROUND(s.used_seats::NUMERIC / s.total_seats, 4) END
FROM raw.seat_snapshot s
JOIN mart.dim_library l ON l.stdg_cd = s.stdg_cd AND l.pblib_id = s.pblib_id
JOIN mart.dim_room    r ON r.stdg_cd = s.stdg_cd AND r.pblib_id = s.pblib_id AND r.rdrm_id = s.rdrm_id
-- 열람실별 워터마크: 마트에 이미 있는 가장 최근 기준 시각 이후만 가져옴
WHERE s.tot_dt > COALESCE((SELECT MAX(f.tot_dt) FROM mart.fact_seat_snapshot f
                           WHERE f.room_key = r.room_key), '-infinity'::TIMESTAMP)
ON CONFLICT (room_key, tot_dt) DO NOTHING;   -- 혹시 겹쳐도 중복 없음
