-- 마트(스타 스키마) 조회 예시. 데이터가 며칠 쌓인 뒤 실행하면 의미 있는 결과가 나옴

-- 1) 시도별 · 시간대별 평균 이용률
SELECT l.ctpv_nm, f.hour, ROUND(AVG(f.occupancy_rate) * 100, 1) AS avg_occupancy_pct, COUNT(*) AS snapshots
FROM mart.fact_seat_snapshot f
JOIN mart.dim_library l USING (library_key)
GROUP BY l.ctpv_nm, f.hour
ORDER BY l.ctpv_nm, f.hour;

-- 2) 평일 vs 주말 피크 시간 (이용률이 가장 높은 시간대)
SELECT is_weekend, hour, avg_pct
FROM (
  SELECT d.is_weekend, f.hour, ROUND(AVG(f.occupancy_rate) * 100, 1) AS avg_pct,
         RANK() OVER (PARTITION BY d.is_weekend ORDER BY AVG(f.occupancy_rate) DESC) AS rk
  FROM mart.fact_seat_snapshot f
  JOIN mart.dim_date d USING (date_key)
  GROUP BY d.is_weekend, f.hour
) t
WHERE rk <= 3
ORDER BY is_weekend, rk;

-- 3) 자주 꽉 차는 열람실 TOP 10 (이용률 90% 이상이었던 비율)
SELECT l.pblib_nm, r.rdrm_nm,
       ROUND(AVG(CASE WHEN f.occupancy_rate >= 0.9 THEN 1 ELSE 0 END) * 100, 1) AS full_ratio_pct,
       COUNT(*) AS snapshots
FROM mart.fact_seat_snapshot f
JOIN mart.dim_library l USING (library_key)
JOIN mart.dim_room r USING (room_key)
GROUP BY l.pblib_nm, r.rdrm_nm
HAVING COUNT(*) >= 10
ORDER BY full_ratio_pct DESC
LIMIT 10;

-- 4) 데이터 품질: 갱신이 멈춘 열람실 (마지막 기준 시각이 오래된 순)
SELECT l.pblib_nm, r.rdrm_nm, MAX(f.tot_dt) AS last_tot_dt
FROM mart.fact_seat_snapshot f
JOIN mart.dim_library l USING (library_key)
JOIN mart.dim_room r USING (room_key)
GROUP BY l.pblib_nm, r.rdrm_nm
ORDER BY last_tot_dt
LIMIT 20;

-- 5) 제외된 행 사유별 건수
SELECT reason, COUNT(*) FROM raw.load_rejects GROUP BY reason ORDER BY 2 DESC;
