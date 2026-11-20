"""
공공도서관 열람실 잔여석 ETL

30분마다:  추출(API) → 검증(불량 행은 rejects로) → raw 적재(중복 없이) → 마트 갱신 → 품질 점검
매일 1회:  도서관 기본정보 갱신 (library_info_refresh DAG)

회사에서 Python 스크립트로 직접 구현했던 것들을 Airflow 기능으로 옮긴 연습:
  - 실패 시 재시도      → default_args의 retries / retry_delay / 지수 백오프
  - 증분 적재          → 열람실별 마지막 기준 시각(워터마크) 이후만 적재
  - 다시 실행해도 안전  → PRIMARY KEY + ON CONFLICT DO NOTHING
  - 이상한 행 건너뛰기  → raw.load_rejects에 사유와 원본을 기록
"""
import json
import os
from datetime import datetime, timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

BASE_URL = "https://apis.data.go.kr/B551982/plr_v2"
SQL_DIR = "/opt/airflow/sql"
STALE_HOURS = 24          # 기준 시각이 이보다 오래되면 '갱신 멈춤'으로 봄

default_args = {
    "owner": "daon",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,     # 1분 → 2분 → 4분
}


def _api_key() -> str:
    key = os.environ.get("LIBRARY_API_KEY")
    if not key:
        raise AirflowFailException("LIBRARY_API_KEY 환경변수가 없습니다 (.env 확인)")
    return key


def _fetch_all(endpoint: str, page_size: int = 300) -> list[dict]:
    """페이지를 끝까지 돌며 전체 항목을 가져온다. 네트워크·서버 오류는 예외로 올려 Airflow가 재시도하게 둔다."""
    items, page = [], 1
    while True:
        res = requests.get(f"{BASE_URL}/{endpoint}", timeout=30, params={
            "serviceKey": _api_key(), "type": "json", "numOfRows": page_size, "pageNo": page})
        res.raise_for_status()
        body = res.json().get("body", {})
        batch = body.get("item") or []
        items += batch
        if len(items) >= int(body.get("totalCount", 0)) or not batch:
            return items
        page += 1


def _to_int(v):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id="dw")


@dag(
    dag_id="library_seat_etl",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 11, 20, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["library", "etl"],
)
def library_seat_etl():

    @task
    def create_tables():
        with open(f"{SQL_DIR}/01_schema.sql", encoding="utf-8") as f:
            _hook().run(f.read())

    @task
    def extract() -> list[dict]:
        rows = _fetch_all("rlt_rdrm_info_v2")
        print(f"API에서 {len(rows)}건 수신")
        return rows

    @task
    def validate(rows: list[dict]) -> list[dict]:
        """형식이 맞는 행만 통과시키고, 나머지는 사유와 함께 rejects 테이블에 남긴다."""
        run_id = get_current_context()["run_id"]
        good, rejects = [], []
        for r in rows:
            total, used = _to_int(r.get("tseatCnt")), _to_int(r.get("useSeatCnt"))
            try:
                tot_dt = datetime.strptime(str(r.get("totDt")), "%Y%m%d%H%M%S")
            except ValueError:
                tot_dt = None
            reason = None
            if not r.get("stdgCd") or not r.get("pblibId") or not r.get("rdrmId"):
                reason = "키 누락"
            elif tot_dt is None:
                reason = "기준 시각 형식 오류"
            elif total is None or used is None:
                reason = "좌석 수 형식 오류"
            elif total <= 0:
                reason = "전체 좌석 0 이하"
            elif used > total:
                reason = "사용 좌석이 전체보다 많음"
            if reason:
                rejects.append((run_id, reason, json.dumps(r, ensure_ascii=False)))
                continue
            good.append({
                "stdg_cd": r["stdgCd"], "pblib_id": r["pblibId"], "rdrm_id": str(r["rdrmId"]), "tot_dt": tot_dt.isoformat(),
                "pblib_nm": r.get("pblibNm"), "rdrm_nm": r.get("rdrmNm"),
                "rdrm_type_nm": (r.get("rdrmTypeNm") or "").strip(), "floor_expln": r.get("bldgFlrExpln"),
                "total_seats": total, "used_seats": used,
                "reserved_seats": _to_int(r.get("rsvtSeatCnt")), "remain_seats": _to_int(r.get("rmndSeatCnt")),
            })
        if rejects:
            _hook().insert_rows("raw.load_rejects", rejects, target_fields=["run_id", "reason", "payload"])
        print(f"통과 {len(good)}건 / 제외 {len(rejects)}건")
        return good

    @task
    def load_raw(rows: list[dict]) -> int:
        """열람실별 워터마크 이후 데이터만, 중복 없이 적재한다."""
        hook = _hook()
        # 열람실 키 = (지자체 코드, 도서관 ID, 열람실 ID). 도서관 ID만으로는 전국에서 유일하지 않음
        watermark = {(sc, p, r): t for sc, p, r, t in hook.get_records(
            "SELECT stdg_cd, pblib_id, rdrm_id, MAX(tot_dt) FROM raw.seat_snapshot GROUP BY 1, 2, 3")}
        def key(r): return (r["stdg_cd"], r["pblib_id"], r["rdrm_id"])
        new_rows = [r for r in rows
                    if key(r) not in watermark or datetime.fromisoformat(r["tot_dt"]) > watermark[key(r)]]
        if new_rows:
            cols = list(new_rows[0].keys())
            sql = (f"INSERT INTO raw.seat_snapshot ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
                   "ON CONFLICT (stdg_cd, pblib_id, rdrm_id, tot_dt) DO NOTHING")
            conn = hook.get_conn()
            with conn, conn.cursor() as cur:
                cur.executemany(sql, [tuple(r[c] for c in cols) for r in new_rows])
        print(f"수신 {len(rows)}건 중 새 기준 시각 {len(new_rows)}건 적재")
        return len(new_rows)

    @task
    def build_mart(_loaded: int):
        with open(f"{SQL_DIR}/02_build_mart.sql", encoding="utf-8") as f:
            _hook().run(f.read())

    @task
    def quality_check():
        """적재 결과를 점검해 로그로 남긴다. 데이터가 하나도 없으면 실패 처리."""
        hook = _hook()
        total = hook.get_first("SELECT COUNT(*) FROM raw.seat_snapshot")[0]
        if total == 0:
            raise AirflowFailException("raw.seat_snapshot이 비어 있습니다")
        stale = hook.get_first(f"""
            SELECT COUNT(*) FROM (
              SELECT stdg_cd, pblib_id, rdrm_id, MAX(tot_dt) AS last_dt FROM raw.seat_snapshot GROUP BY 1, 2, 3
            ) t WHERE last_dt < now() - INTERVAL '{STALE_HOURS} hours'""")[0]
        rejects_today = hook.get_first(
            "SELECT COUNT(*) FROM raw.load_rejects WHERE rejected_at >= CURRENT_DATE")[0]
        fact = hook.get_first("SELECT COUNT(*) FROM mart.fact_seat_snapshot")[0]
        print(f"raw {total}건 · fact {fact}건 · 갱신 멈춘 열람실 {stale}개 · 오늘 제외 {rejects_today}건")

    tables = create_tables()
    rows = extract()
    tables >> rows
    loaded = load_raw(validate(rows))
    build_mart(loaded) >> quality_check()


@dag(
    dag_id="library_info_refresh",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 20, tz="Asia/Seoul"),
    catchup=False,
    default_args=default_args,
    tags=["library", "etl"],
)
def library_info_refresh():

    @task
    def refresh():
        with open(f"{SQL_DIR}/01_schema.sql", encoding="utf-8") as f:
            _hook().run(f.read())
        rows = _fetch_all("info_v2")
        data = [(r["stdgCd"], r["pblibId"], r.get("pblibNm"), r.get("ctpvNm"), r.get("sggNm"), r.get("pblibTypeNm"),
                 r.get("lat") or None, r.get("lot") or None, _to_int(r.get("tseatCnt")))
                for r in rows if r.get("stdgCd") and r.get("pblibId")]
        sql = """INSERT INTO raw.library_info
                 (stdg_cd, pblib_id, pblib_nm, ctpv_nm, sgg_nm, pblib_type_nm, lat, lon, total_seats)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                 ON CONFLICT (stdg_cd, pblib_id) DO UPDATE SET
                   pblib_nm = EXCLUDED.pblib_nm, ctpv_nm = EXCLUDED.ctpv_nm, sgg_nm = EXCLUDED.sgg_nm,
                   pblib_type_nm = EXCLUDED.pblib_type_nm, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
                   total_seats = EXCLUDED.total_seats, updated_at = now()"""
        conn = _hook().get_conn()
        with conn, conn.cursor() as cur:
            cur.executemany(sql, data)
        print(f"도서관 기본정보 {len(data)}건 갱신")

    refresh()


library_seat_etl()
library_info_refresh()
