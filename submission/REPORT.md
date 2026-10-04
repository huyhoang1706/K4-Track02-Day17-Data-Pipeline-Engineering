# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Mai Huy Hoàng / 2A202602685
**Repo hiện tại:** https://github.com/huyhoang1706/K4-Track02-Day17-Data-Pipeline-Engineering

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Baseline có 24 hàng/12 ticket; T-91 giữ cả ba trạng thái; rerun sai checksum. | u05 ngày 08-12 có 2 events/0 down; feature lệch full recompute. | T-97 chưa deleted; snapshot mới nhất còn 1 hàng, RAG còn 2 chunks. |
| **Nguyên nhân gốc** | Dedup trong batch rồi INSERT, không upsert giữa batch. | LOOKBACK_DAYS=0 nên bỏ partition cũ khi event đến muộn. | Staging chỉ đọc khoá từ after=null, rồi loại hàng delete. |
| **Cách sửa** | `pipeline/silver.py`: MERGE theo ticket_id, chỉ UPDATE khi LSN mới lớn hơn. | `pipeline/config.py`: lookback=ceil(P99)=3; giữ overwrite partition theo event time. | `pipeline/staging.py`: coalesce khoá after/before; trường dữ liệu vẫn lấy after để tombstone không giữ PII. |
| **Khái niệm trên slide** | Silver có khoá; idempotent; newest state wins. | Event time khác ingest time; đo lateness, lookback. | Debezium delete khác Kafka tombstone; xoá phải lan. |

## 2. Các con số

- Baseline: verify **8/18**, pytest **9 failed/25 passed**; log ở `submission/evidence/baseline-*.txt`.
- 43 records Bronze: P50=0, P95=2,90, **P99=3 ngày**, max=3 → **LOOKBACK_DAYS=3**.
- Sau sửa: verify **18/18**, pytest **34 passed**; u05 có **5 events, 3 clicks, 1 down**.
- `submission/checksums.txt`: **PASS**, C0=C1=C2=C3=`39e115c510ecdf526800eac227158a4f`.
- dbt fresh và incremental đều **PASS=19**, parity hai bảng chung đều **PARITY**.

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE ticket theo khoá vì mỗi thực thể chỉ có một trạng thái; overwrite partition feature vì phải tính lại tổng từ Silver khi có event muộn.
- Giữ tombstone và LSN để replay batch cũ không hồi sinh ticket; đổi lại phải lưu một hàng đánh dấu xoá.
- Snapshot dựng từ Bronze as-of và feedback đã nhận tới ngày đó để tránh leakage; không sửa version cũ khi feedback tới muộn.
- DuckDB đủ cho seed nhỏ và chạy local; dbt cho SQL có contract/test/microbatch; Spark tăng vận hành chưa cần thiết ở quy mô này.

## 4. Hai câu hỏi suy ngẫm

1. Trong lab, snapshot 08-12..08-14 còn T-97 có chủ đích. Production cần thu hồi bản chứa dữ liệu phải xoá, phát hành version sạch và lan xoá tới Bronze, transcript, cache, bản xuất, backup theo chính sách; audit giữ metadata, không giữ PII. Tính bất biến phục vụ tái lập không thay thế xử lý yêu cầu xoá.
2. Đặt cổng PII Bronze→Silver trước mọi Gold/LLM: regex kết hợp nhận diện tên/địa chỉ tiếng Việt, quarantine mẫu nghi ngờ để duyệt. Đo precision/recall từng loại PII trên tập gán nhãn và theo dõi rò rỉ downstream; tên Nguyễn Văn An cho thấy regex lab chưa đủ.

## 5. Output (thực tế)

Các lệnh chạy trong checkout này. Log gốc ở `submission/evidence/`; khi dán dbt bỏ mã màu ANSI và khoảng trắng cuối dòng, giữ nguyên nội dung; log gốc không sửa. Timestamp của công cụ là giờ hệ thống, không dùng để suy ra deadline lớp.

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

```text
$ make test
..................................                                       [100%]
34 passed in 1.40s
```

```text
$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

```text
$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

```text
$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. /home/huyhoangg/repo/vinai/K4-Track02-Day17-Data-Pipeline-Engineering/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
17:00:27  Running with dbt=1.12.5
17:00:27  Registered adapter: duckdb=1.11.0
17:00:27  Unable to do partial parsing because saved manifest not found. Starting full parse.
17:00:28  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
17:00:28
17:00:28  Concurrency: 1 threads (target='dev')
17:00:28
17:00:28  1 of 19 START sql view model main.stg_events ................................... [RUN]
17:00:28  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.05s]
17:00:28  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
17:00:28  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
17:00:28  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
17:00:28  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.06s]
17:00:28  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
17:00:28  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.07s]
17:00:28  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
17:00:28  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.06s]
17:00:28  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
17:00:28  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.02s]
17:00:28  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
17:00:28  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
17:00:28  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
17:00:28  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.01s]
17:00:28  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
17:00:28  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.01s]
17:00:28  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
17:00:28  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.01s]
17:00:28  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
17:00:28  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.01s]
17:00:28  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
17:00:28  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
17:00:28  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
17:00:29  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
17:00:29  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
17:00:29  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
17:00:29  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
17:00:29  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
17:00:29  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
17:00:29  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
17:00:29  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.02s]
17:00:29  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
17:00:29  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
17:00:29  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
17:00:29  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
17:00:29  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
17:00:29  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
17:00:29  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
17:00:29  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.02s]
17:00:29  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
17:00:29  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.02s]
17:00:29  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
17:00:29  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.02s]
17:00:29  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.16s]
17:00:29  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
17:00:29  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.01s]
17:00:29  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
17:00:29  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
17:00:29  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
17:00:29  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
17:00:29
17:00:29  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.71 seconds (0.71s).
17:00:29
17:00:29  Completed successfully
17:00:29
17:00:29  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```text
$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Kiểm tra dbt incremental

Chạy `make dbt` lần thứ hai để kiểm tra đường incremental/merge thực tế; tiếp theo chạy lại parity.

```text
$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. /home/huyhoangg/repo/vinai/K4-Track02-Day17-Data-Pipeline-Engineering/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
17:03:19  Running with dbt=1.12.5
17:03:19  Registered adapter: duckdb=1.11.0
17:03:19  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
17:03:19
17:03:19  Concurrency: 1 threads (target='dev')
17:03:19
17:03:19  1 of 19 START sql view model main.stg_events ................................... [RUN]
17:03:19  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.06s]
17:03:19  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
17:03:19  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
17:03:19  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
17:03:19  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.07s]
17:03:19  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
17:03:19  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.08s]
17:03:19  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
17:03:19  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.09s]
17:03:19  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
17:03:19  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.03s]
17:03:19  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
17:03:19  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
17:03:19  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
17:03:19  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.01s]
17:03:19  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
17:03:19  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
17:03:19  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
17:03:20  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
17:03:20  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
17:03:20  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
17:03:20  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
17:03:20  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
17:03:20  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
17:03:20  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
17:03:20  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
17:03:20  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
17:03:20  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
17:03:20  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
17:03:20  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
17:03:20  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
17:03:20  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.02s]
17:03:20  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
17:03:20  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.02s]
17:03:20  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
17:03:20  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
17:03:20  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
17:03:20  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
17:03:20  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
17:03:20  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.02s]
17:03:20  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
17:03:20  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.02s]
17:03:20  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
17:03:20  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.02s]
17:03:20  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.15s]
17:03:20  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
17:03:20  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
17:03:20  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
17:03:20  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
17:03:20  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
17:03:20  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
17:03:20
17:03:20  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.74 seconds (0.74s).
17:03:20
17:03:20  Completed successfully
17:03:20
17:03:20  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```text
$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Bonus B1 — LLM cache

Khoá cache gồm SHA-256 prompt chứa input, model và prompt version; cache cả phản hồi sai schema để replay không gọi lại. Gold chỉ nhận JSON đúng schema; quarantine không nhân bản; model/prompt version lưu trên từng hàng. Dự toán trước chạy dùng giá giả định của FakeLLM, không phải báo giá model thật.

```text
$ make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

Kiểm tra thêm thay input/model, xoá ticket, cache phản hồi lỗi và strict schema:

```text
PASS: failure responses cached; quarantine deduplicated; input/model changes invalidate cache; deleted tickets leave Gold; strict JSON validation.
```

### Bonus B2 — Thiết kế

Bài brainstorm: [bonus/DESIGN.md](../bonus/DESIGN.md), chọn flywheel chatbot CSKH tiếng Việt; có 5 câu hỏi với quyết định/đánh đổi, phương án loại và sơ đồ ASCII. Quy mô/SLA là giả định, không phải số đo production. Không chạy bonus Airflow.

### Mở rộng không chấm điểm

Đã chạy `make flywheel` và `make kg`; output đầy đủ tại [flywheel.txt](evidence/flywheel.txt) và [kg.txt](evidence/kg.txt). Demo có sẵn minh hoạ decontamination và ASOF, không nhận là prototype mới.
