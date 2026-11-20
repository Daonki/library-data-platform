# 공공도서관 열람실 데이터 플랫폼

공공데이터포털의 전국 공공도서관 열람실 실시간 잔여석 API를 **Airflow로 30분마다 수집**해 PostgreSQL에 적재하고, **스타 스키마 데이터 마트**로 정리해 시간대별 이용률을 분석할 수 있게 만든 개인 실습 프로젝트입니다.

회사에서 Python 스크립트로 직접 구현했던 데이터 적재 패턴(등록 시각 기준 증분, 재시도, 불량 데이터 건너뛰기)을 **Airflow 워크플로**로 다시 구현해 보는 것이 목적입니다.

## 구조

```
공공데이터 API (열람실 288개 · 도서관 234개)
        │
        ▼
[Airflow · library_seat_etl · 30분마다]
  create_tables → extract → validate → load_raw → build_mart → quality_check
                              │
                              └─ 불량 행 → raw.load_rejects (사유 기록)
[Airflow · library_info_refresh · 매일 06시]  도서관 기본정보 갱신
        │
        ▼
PostgreSQL (dw)
  raw  : seat_snapshot · library_info · load_rejects
  mart : fact_seat_snapshot  ─┬─ dim_library
                              ├─ dim_room
                              └─ dim_date
```

## 설계에서 신경 쓴 것

| 문제 | 선택 |
|---|---|
| API 호출이 일시적으로 실패할 수 있음 | Airflow `retries=3` + 지수 백오프(1→2→4분). 네트워크·서버 오류는 예외로 올려 재시도에 맡김 |
| 30분마다 같은 데이터가 다시 들어옴 | 열람실별 **마지막 기준 시각(워터마크)** 이후만 적재 + `PRIMARY KEY` · `ON CONFLICT DO NOTHING`으로 다시 실행해도 중복 없음 |
| 형식이 깨진 행이 섞여 있음 | 키 누락 · 좌석 수 형식 오류 · 전체 좌석 0 이하 · 사용 좌석 > 전체 좌석은 적재하지 않고 `raw.load_rejects`에 사유와 원본을 기록 |
| 갱신이 멈춘 열람실이 있음 | 품질 점검 단계에서 기준 시각이 24시간 넘게 멈춘 열람실 수를 집계 |
| 분석 쿼리가 복잡해짐 | 원본(raw)과 분석용(mart)을 분리. 팩트(좌석 스냅샷) + 차원(도서관·열람실·날짜)의 스타 스키마로 정리 |

### 데이터를 보고 키를 다시 설계한 경험

처음에는 API의 `pblibId`(도서관 ID)를 도서관의 고유 키로 잡았습니다. 그런데 도서관 정보 234건을 넣었더니 **21건만 남았습니다.**

확인해 보니 `pblibId`는 **지자체마다 `PLR001`부터 다시 매겨지는 값**이었습니다. 예를 들어 `PLR001`은 광진구의 광진정보도서관이면서, 중랑구의 중랑구립정보도서관이기도 했습니다.

| 키 후보 | 고유한 값 개수 (도서관 234건 기준) |
|---|---|
| `pblibId` | 21 |
| `stdgCd`(지자체 코드) + `pblibId` | 233 |

그래서 도서관은 `(stdgCd, pblibId)`, 열람실은 `(stdgCd, pblibId, rdrmId)`를 키로 다시 설계했습니다. 필드 이름만 보고 키를 정하지 않고, **실제 데이터에서 유일성을 확인**해야 한다는 걸 배웠습니다.

## 첫 적재 결과 (2026-11-20)

| 테이블 | 건수 | 비고 |
|---|---|---|
| raw.seat_snapshot | 283 | API 288건 중 검증 통과분 |
| raw.load_rejects | 5 / 실행 | 키 누락 1 · 전체 좌석 0 이하 2 · 좌석 수 형식 오류 2 |
| mart.dim_library | 131 | 열람실이 있는 도서관, 모두 시도·시군구 연결 |
| mart.fact_seat_snapshot | 283 | 두 번째 실행에서는 새 기준 시각이 없어 0건 추가 (증분 확인) |

갱신이 멈춘 열람실은 8개였고, 가장 오래된 기준 시각은 2026-01-07이었습니다.

## 실행 방법

```bash
cp .env.example .env          # LIBRARY_API_KEY에 공공데이터포털 인증키 입력
docker compose up airflow-init
docker compose up -d
```

- Airflow: http://localhost:8080 (admin / admin)
- `library_info_refresh` → `library_seat_etl` 순서로 실행
- 분석 예시: `sql/03_analysis.sql` (PostgreSQL `localhost:5433`, DB `dw`, 계정 `dw/dw`)

## 기술

Python · Apache Airflow 2.10 (TaskFlow API, LocalExecutor) · PostgreSQL 16 · Docker Compose · SQL

## 알려진 한계 · 다음 단계

- 같은 불량 행이 실행마다 다시 기록됨 → 같은 행·같은 사유는 하루 한 번만 기록하도록 개선 예정
- 시간대별 분석은 며칠 이상 데이터가 쌓여야 의미가 있음
- 마트 위에 BI 대시보드(Metabase 등) 추가 예정
