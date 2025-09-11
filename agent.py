#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fleetcron agent v2
- 설정 파일(fleetcron.config.json) 기반
- 액션 체인(actions) + when 조건 + 리트라이 + 타임아웃
- 복수 스케줄 지원(jobs.schedules[] 또는 단일 hour/minute)
- 한 PC 1프로세스, 일련번호(1~N), 앞번호 온라인 감지, 단일 실행 보장
"""
import os, sys, time, json, uuid, socket, signal, threading, traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import requests
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# -------------------- 설정 로드 --------------------
APP_NAME = "fleetcron-agent"
CONFIG_BASENAME = "fleetcron.config.json"
HOME_DIR = Path.home() / ".fleetcron"
HOME_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_CONFIG = Path(sys.argv[0]).resolve().parent / CONFIG_BASENAME
HOME_CONFIG = HOME_DIR / CONFIG_BASENAME
MACHINE_FILE = HOME_DIR / "machine.json"
LOCK_FILE = HOME_DIR / "agent.lock"

def load_config() -> Dict[str, Any]:
    cfg_path = LOCAL_CONFIG if LOCAL_CONFIG.exists() else HOME_CONFIG
    if not cfg_path.exists():
        print(f"[config] 설정 파일이 없습니다: {cfg_path}\n"
              f"동일 폴더 또는 {HOME_CONFIG} 위치에 {CONFIG_BASENAME}를 만들어 주세요.", file=sys.stderr)
        sys.exit(1)
    with open(cfg_path, "r", encoding="utf-8") as fp:
        cfg = json.load(fp)

    # 기본값
    cfg.setdefault("db_name", "fleetcron")
    cfg.setdefault("tz", "Asia/Seoul")
    cfg.setdefault("max_serial", 10)
    cfg.setdefault("http_defaults", {"timeout_sec": 10, "retry": {"retries": 2, "delay_sec": 3, "backoff": 1.5}})
    cfg.setdefault("secrets", {})
    return cfg

CFG = load_config()

# -------------------- 타임존 --------------------
try:
    from zoneinfo import ZoneInfo
    CRON_TZ = ZoneInfo(CFG.get("tz", "Asia/Seoul"))
except Exception:
    CRON_TZ = timezone.utc

MAX_SERIAL = int(CFG.get("max_serial", 10))
OFFSET_STEP_SEC = 5
RESP_SAMPLE_MAX = 2000

# -------------------- 파일 락(한 PC 1프로세스) --------------------
if os.name == "nt":
    import msvcrt
    def acquire_lock():
        f = open(LOCK_FILE, "a+b")
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            return f
        except OSError:
            print("이미 실행 중입니다. (lock held)", file=sys.stderr); sys.exit(1)
else:
    import fcntl
    def acquire_lock():
        f = open(LOCK_FILE, "a+b")
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return f
        except OSError:
            print("이미 실행 중입니다. (lock held)", file=sys.stderr); sys.exit(1)

# -------------------- 머신 ID --------------------
def load_or_create_machine_id() -> str:
    if MACHINE_FILE.exists():
        with open(MACHINE_FILE, "r", encoding="utf-8") as fp:
            d = json.load(fp)
            if d.get("machine_id"): return d["machine_id"]
    mid = str(uuid.uuid4())
    with open(MACHINE_FILE, "w", encoding="utf-8") as fp:
        json.dump({"machine_id": mid}, fp)
    return mid

# -------------------- Mongo 연결/인덱스 --------------------
def get_db():
    uri = CFG.get("mongodb_uri", "")
    if not uri:
        print("config.mongodb_uri가 필요합니다.", file=sys.stderr); sys.exit(1)
    client = MongoClient(uri, appname=APP_NAME)
    db = client[CFG.get("db_name", "fleetcron")]
    db.machines.create_index("machine_id", unique=True)
    db.machines.create_index("serial")  # unique 제거
    db.machines.create_index("last_online_minute")
    db.serials.create_index("serial", unique=True)
    db.jobs.create_index([("enabled",1),("hour",1),("minute",1)])
    db.jobs.create_index([("enabled",1),("schedules.hour",1),("schedules.minute",1)])
    db.job_runs.create_index([("job_id",1),("scheduled_for",1)], unique=True)
    db.commands.create_index([("target",1),("created_at",1)])
    return db

# -------------------- 일련번호 --------------------
def get_or_assign_serial(db, machine_id: str, hostname: str) -> int:
    m = db.machines.find_one({"machine_id": machine_id})
    if m and "serial" in m: return int(m["serial"])
    now = datetime.now(timezone.utc)
    assigned = None
    for s in range(1, MAX_SERIAL+1):
        doc = db.serials.find_one_and_update(
            {"serial": s, "$or": [{"assigned_to": None}, {"assigned_to": machine_id}]},
            {"$setOnInsert": {"serial": s}, "$set": {"assigned_to": machine_id, "assigned_at": now}},
            upsert=True, return_document=ReturnDocument.AFTER
        )
        if doc and doc.get("assigned_to") == machine_id:
            assigned = s; break
    if not assigned:
        print(f"경고: 1~{MAX_SERIAL} 슬롯이 가득 참. 종료.", file=sys.stderr); sys.exit(2)
    db.machines.update_one(
        {"machine_id": machine_id},
        {"$set": {"machine_id": machine_id, "hostname": hostname, "serial": assigned, "last_seen": now}},
        upsert=True
    )
    return assigned

# -------------------- 템플릿 해석 --------------------
def resolve_templates(value):
    # 문자열의 {{KEY}}를 CFG.secrets[KEY]로 치환. dict/list는 재귀.
    if isinstance(value, str):
        out = value
        for k, v in CFG.get("secrets", {}).items():
            out = out.replace("{{"+k+"}}", str(v))
        return out
    if isinstance(value, dict):
        return {k: resolve_templates(v) for k,v in value.items()}
    if isinstance(value, list):
        return [resolve_templates(v) for v in value]
    return value

# -------------------- 잡 캐시 --------------------
class JobsCache:
    def __init__(self, db):
        self.db = db
        self._map = {}  # key=(hour, minute) -> List[job]
        self._lock = threading.Lock()
        self.reload()
    def _add_to(self, mp, j, hour, minute):
        # hour가 None이면 모든 시간(0-23)에 추가
        if hour is None:
            for h in range(24):
                key = (h, int(minute))
                mp.setdefault(key, []).append(j)
        else:
            key = (int(hour), int(minute))
            mp.setdefault(key, []).append(j)
    def reload(self):
        mp = {}
        for j in self.db.jobs.find({"enabled": True}):
            if "schedules" in j and j["schedules"]:
                for sch in j["schedules"]:
                    self._add_to(mp, j, sch.get("hour"), sch.get("minute", 0))
            else:
                self._add_to(mp, j, j.get("hour"), j.get("minute", 0))
        with self._lock:
            self._map = mp
        total = sum(len(v) for v in mp.values())
        print(f"[jobs] 로드: {total}개")
    def list_for(self, hour: int, minute: int) -> List[Dict[str,Any]]:
        with self._lock:
            return list(self._map.get((hour, minute), []))
    
    def get_next_schedule(self, from_time: datetime) -> Optional[datetime]:
        """다음 job 실행 시간 찾기"""
        with self._lock:
            if not self._map:
                return None
            
            current_hour = from_time.hour
            current_minute = from_time.minute
            
            # 오늘 남은 시간 체크
            for h in range(current_hour, 24):
                for m in range(60):
                    if h == current_hour and m <= current_minute:
                        continue
                    if (h, m) in self._map:
                        next_time = from_time.replace(hour=h, minute=m, second=0, microsecond=0)
                        return next_time
            
            # 내일 첫 스케줄
            for h in range(24):
                for m in range(60):
                    if (h, m) in self._map:
                        next_time = from_time.replace(hour=h, minute=m, second=0, microsecond=0)
                        next_time += timedelta(days=1)
                        return next_time
            
            return None

# -------------------- 명령 폴링 --------------------
def commands_watcher(db, machine_id: str, jobs_cache: JobsCache, stop_ev: threading.Event):
    last_seen = datetime.now(timezone.utc) - timedelta(seconds=1)
    while not stop_ev.is_set():
        try:
            cur = db.commands.find({
                "target": {"$in": [machine_id, "all"]},
                "created_at": {"$gt": last_seen}
            }).sort("created_at", 1)
            for cmd in cur:
                last_seen = cmd["created_at"]
                if cmd.get("type") == "reload_jobs":
                    jobs_cache.reload(); print("[cmd] jobs 갱신")
                elif cmd.get("type") == "reload_config":
                    global CFG, CRON_TZ
                    CFG = load_config()
                    try:
                        from zoneinfo import ZoneInfo
                        CRON_TZ = ZoneInfo(CFG.get("tz","Asia/Seoul"))
                    except Exception:
                        CRON_TZ = timezone.utc
                    jobs_cache.reload(); print("[cmd] config 갱신")
        except Exception as e:
            print("[cmd] watcher 오류:", e, file=sys.stderr)
        stop_ev.wait(5.0)

# -------------------- 시간 유틸/하트비트 --------------------
def floor_to_minute(dt: datetime) -> datetime: return dt.replace(second=0, microsecond=0)
def to_utc_minute(dt_local_minute: datetime) -> datetime: return dt_local_minute.astimezone(timezone.utc)
def update_heartbeat(db, machine_id: str, scheduled_minute_utc: datetime):
    now = datetime.now(timezone.utc)
    db.machines.update_one({"machine_id": machine_id},
        {"$set": {"last_online_minute": scheduled_minute_utc, "last_seen": now}})

def get_my_order(db, my_serial: int, machine_id: str, scheduled_minute_utc: datetime) -> int:
    # heartbeat 업데이트 (이 분에 내가 활동 중임을 기록)
    update_heartbeat(db, machine_id, scheduled_minute_utc)
    
    # 모든 머신을 serial 순으로 정렬해서 내 실제 순서 찾기  
    # last_online_minute가 현재 분인 머신들만 포함 (이 분에 활동 중인 머신들)
    machines = list(db.machines.find(
        {"last_online_minute": scheduled_minute_utc},
        {"serial": 1, "machine_id": 1}
    ).sort([("serial", 1), ("machine_id", 1)]))
    
    for idx, m in enumerate(machines, 1):
        if m["machine_id"] == machine_id:
            return idx
    return 1  # 못 찾으면 1번으로

def has_earlier_online(db, my_serial: int, scheduled_minute_utc: datetime) -> bool:
    if my_serial <= 1: return False
    # 내 serial보다 작은 번호 중 이번 분에 이미 실행한 머신이 있는지
    # last_online_minute로 이번 분에 활동했는지 확인
    exists = db.machines.find_one({
        "serial": {"$lt": my_serial},
        "last_online_minute": scheduled_minute_utc
    })
    return exists is not None

# -------------------- 클레임 --------------------
def claim_job_run(db, job, scheduled_minute_utc, machine_id, my_serial):
    now = datetime.now(timezone.utc)
    try:
        doc = db.job_runs.find_one_and_update(
            {
                "job_id": job["_id"], "scheduled_for": scheduled_minute_utc,
                "$or": [{"claimed_by": None}, {"claimed_by": machine_id}]
            },
            {"$setOnInsert":{"job_id":job["_id"],"scheduled_for":scheduled_minute_utc},
             "$set":{"claimed_by":machine_id,"claimed_at":now,"executed_by_serial":my_serial,"status":"running","steps":[]}},
            upsert=True, return_document=ReturnDocument.AFTER
        )
        return doc and doc.get("claimed_by")==machine_id
    except DuplicateKeyError:
        return False
    except PyMongoError as e:
        print("[claim] DB 오류:", e, file=sys.stderr); return False

# -------------------- HTTP 실행 + 리트라이 --------------------
def run_http_with_retry(step: Dict[str,Any], defaults: Dict[str,Any]) -> Tuple[str, Dict[str,Any]]:
    # 병합: defaults <- job defaults <- step overrides
    timeout = step.get("timeout_sec", defaults.get("timeout_sec", 10))
    rdef = defaults.get("retry", {}) or {}
    rcfg = step.get("retry", {}) or {}
    retries = int(rcfg.get("retries", rdef.get("retries", 0)))
    delay   = float(rcfg.get("delay_sec", rdef.get("delay_sec", 0)))
    backoff = float(rcfg.get("backoff", rdef.get("backoff", 1.0)))

    method = str(step.get("method","GET")).upper()
    url    = resolve_templates(step.get("url"))
    headers= resolve_templates(step.get("headers") or {})
    params = resolve_templates(step.get("params") or {})
    body   = resolve_templates(step.get("body"))

    attempts = 0
    last_info = {}
    while True:
        attempts += 1
        start = time.time()
        try:
            kwargs = dict(headers=headers, params=params, timeout=timeout)
            if method in ("POST","PUT","PATCH","DELETE"):
                if isinstance(body, (dict, list)): kwargs["json"]=body
                elif body is not None: kwargs["data"]=body
            resp = requests.request(method, url, **kwargs)
            info = {
                "status_code": resp.status_code,
                "elapsed_ms": int((time.time()-start)*1000),
                "response_sample": (resp.text or "")[:RESP_SAMPLE_MAX]
            }
            if 200 <= resp.status_code < 300:
                info["attempts"]=attempts
                return "ok", info
            else:
                last_info = {"error": f"http {resp.status_code}", **info}
        except Exception as e:
            last_info = {"error": str(e), "elapsed_ms": int((time.time()-start)*1000),
                         "trace": traceback.format_exc()[:2000]}
        # 실패 처리
        if attempts > retries:
            last_info["attempts"]=attempts
            return "error", last_info
        # 딜레이 후 재시도
        time.sleep(delay)
        delay = delay * backoff if backoff>1 else delay

# -------------------- when 조건 --------------------
def when_match(when: Optional[Dict[str,Any]], now_local: datetime) -> bool:
    if not when: return True
    if "hour_in" in when and now_local.hour not in set(when["hour_in"]): return False
    if "minute_in" in when and now_local.minute not in set(when["minute_in"]): return False
    return True  # 필요시 day_of_week 등 확장 가능

# -------------------- 액션 체인 실행 --------------------
def execute_actions(db, run_key: Dict[str,Any], job: Dict[str,Any], now_local: datetime,
                    job_defaults: Dict[str,Any]):
    steps_log = []
    status_overall = "ok"
    for idx, step in enumerate(job.get("actions", [])):
        if step.get("type","http") != "http":
            steps_log.append({"index":idx,"name":step.get("name","(http)"),"status":"skipped_unsupported"})
            continue
        if not when_match(step.get("when"), now_local):
            steps_log.append({"index":idx,"name":step.get("name","(http)"),"status":"skipped_when"})
            continue

        st, info = run_http_with_retry(step, {**CFG.get("http_defaults",{}),
                                              "timeout_sec": job.get("timeout_sec", CFG.get("http_defaults",{}).get("timeout_sec",10)),
                                              "retry": job.get("retry", CFG.get("http_defaults",{}).get("retry",{}))})
        s = {"index": idx, "name": step.get("name", step.get("url","(http)")), "status": st}
        if st=="ok":
            s.update({"status_code": info.get("status_code"), "elapsed_ms": info.get("elapsed_ms"),
                      "attempts": info.get("attempts"), "response_sample": info.get("response_sample")})
        else:
            s.update({"error": info.get("error"), "elapsed_ms": info.get("elapsed_ms"),
                      "attempts": info.get("attempts"), "status_code": info.get("status_code")})
        steps_log.append(s)

        db.job_runs.update_one(run_key, {"$push": {"steps": s}})

        if st!="ok" and not step.get("continue_on_failure", False):
            status_overall = "error"; break

    return status_overall, steps_log

# -------------------- 분 단위 처리 --------------------
def process_minute(db, jobs_cache: JobsCache, my_serial: int, machine_id: str, tick_minute_local: datetime, check_second: int):
    sched_minute_utc = to_utc_minute(tick_minute_local)
    
    # 0초에는 순서 확인 (heartbeat는 get_my_order 내부에서)
    if check_second == 0:
        # 실행 직전에 job 리스트 새로 갱신
        print(f"[{tick_minute_local:%H:%M}] Job 리스트 갱신 중...")
        jobs_cache.reload()
        
        my_order = get_my_order(db, my_serial, machine_id, sched_minute_utc)
        print(f"[{tick_minute_local:%H:%M}] 내 순서: {my_order}번")
        if my_order == 1:
            # 1번이면 바로 실행
            pass
        else:
            # 1번이 아니면 대기 (나중에 다시 체크)
            return my_order
    else:
        # (순서-1)*5초에 다시 체크 (heartbeat 없이 순서만 확인)
        # 이미 heartbeat를 0초에 업데이트했으므로 다시 조회만
        machines = list(db.machines.find(
            {"last_online_minute": sched_minute_utc},
            {"serial": 1, "machine_id": 1}
        ).sort([("serial", 1), ("machine_id", 1)]))
        
        my_order = 1
        for idx, m in enumerate(machines, 1):
            if m["machine_id"] == machine_id:
                my_order = idx
                break
        if has_earlier_online(db, my_serial, sched_minute_utc):
            print(f"[{tick_minute_local:%H:%M}:{check_second:02d}] 앞 순서 머신 온라인 → 패스")
            return

    hh, mm = tick_minute_local.hour, tick_minute_local.minute
    jobs = jobs_cache.list_for(hh, mm)
    if not jobs:
        print(f"[{tick_minute_local:%H:%M}] 이 시간에 실행할 job이 없음")
        return

    for j in jobs:
        # 최신화 (job이 삭제되었거나 비활성화되었을 수 있음)
        j2 = db.jobs.find_one({"_id": j["_id"], "enabled": True})
        if not j2:
            print(f"  - {j.get('name','(no name)')}: 삭제되었거나 비활성화됨 → 패스")
            continue
        # 단일 실행 클레임
        if not claim_job_run(db, j2, sched_minute_utc, machine_id, my_serial):
            print(f"  - {j2.get('name','(no name)')}: 다른 노드가 클레임 → 패스"); continue

        print(f"  - {j2.get('name','(no name)')}: 실행 시작")
        start_utc = datetime.now(timezone.utc)
        run_key = {"job_id": j2["_id"], "scheduled_for": sched_minute_utc}
        # 단순/체인 모두 지원: actions 없으면 단일 http로 간주
        if j2.get("actions"):
            status, steps = execute_actions(db, run_key, j2, tick_minute_local, {})
        else:
            # v1 하위호환(단일 HTTP)
            step = {
                "type":"http", "name": j2.get("name","(http)"),
                "method": j2.get("method","GET"), "url": j2.get("url"),
                "headers": j2.get("headers"), "params": j2.get("params"), "body": j2.get("body"),
                "timeout_sec": j2.get("timeout_sec"),
                "retry": j2.get("retry")
            }
            st, info = run_http_with_retry(step, {**CFG.get("http_defaults",{}),
                                                 "timeout_sec": j2.get("timeout_sec", CFG.get("http_defaults",{}).get("timeout_sec",10)),
                                                 "retry": j2.get("retry", CFG.get("http_defaults",{}).get("retry",{}))})
            s = {"index":0, "name": step["name"], "status": st}
            if st=="ok":
                s.update({"status_code": info.get("status_code"), "elapsed_ms": info.get("elapsed_ms"),
                          "attempts": info.get("attempts"), "response_sample": info.get("response_sample")})
            else:
                s.update({"error": info.get("error"), "elapsed_ms": info.get("elapsed_ms"),
                          "attempts": info.get("attempts"), "status_code": info.get("status_code")})
            db.job_runs.update_one(run_key, {"$push": {"steps": s}})
            status = "ok" if st=="ok" else "error"

        end_utc = datetime.now(timezone.utc)
        db.job_runs.update_one(run_key, {"$set": {"start_at": start_utc, "end_at": end_utc, "status": status}})
        print(f"  - {j2.get('name','(no name)')}: 실행 완료({status})")

# -------------------- 메인 --------------------
def agent_main():
    lock_handle = acquire_lock()
    machine_id = load_or_create_machine_id()
    hostname = socket.gethostname()
    db = get_db()
    my_serial = get_or_assign_serial(db, machine_id, hostname)
    print(f"시작: machine_id={machine_id}, host={hostname}, serial={my_serial}, tz={CFG.get('tz')}")

    jobs_cache = JobsCache(db)
    stop_ev = threading.Event()
    t = threading.Thread(target=commands_watcher, args=(db, machine_id, jobs_cache, stop_ev), daemon=True)
    t.start()

    def handle_sig(sig, frame):
        stop_ev.set(); print("종료 신호 수신."); 
        try: lock_handle.close()
        except Exception: pass
        sys.exit(0)

    if os.name != "nt":
        signal.signal(signal.SIGINT, handle_sig)
        signal.signal(signal.SIGTERM, handle_sig)

    while True:
        now_local = datetime.now(CRON_TZ)
        
        # 다음 job 시간 찾기
        next_schedule = jobs_cache.get_next_schedule(now_local)
        if not next_schedule:
            # job이 없으면 30분마다 체크 (매시 00분, 30분)
            next_check = now_local.replace(second=0, microsecond=0)
            if now_local.minute < 30:
                next_check = next_check.replace(minute=30)
            else:
                next_check = (next_check + timedelta(hours=1)).replace(minute=0)
            
            sleep_sec = (next_check - now_local).total_seconds()
            print(f"[scheduler] job 없음. {next_check:%H:%M}에 재확인 ({int(sleep_sec)}초 대기)")
            time.sleep(sleep_sec)
            jobs_cache.reload()
            continue
        
        # 다음 실행 시간까지 대기 (최대 30분)
        sleep_sec = (next_schedule - now_local).total_seconds()
        max_sleep = 30 * 60  # 30분
        
        if sleep_sec > max_sleep:
            # 30분 이상 대기해야 하면 30분만 대기 후 다시 체크
            print(f"[scheduler] 30분 대기 후 재확인 (다음 예정: {next_schedule:%H:%M})")
            time.sleep(max_sleep)
            jobs_cache.reload()  # 30분 후 reload해서 새 job 확인
            continue
        
        if sleep_sec > 0:
            print(f"[scheduler] 다음 실행: {next_schedule:%H:%M}, {int(sleep_sec)}초 대기")
            time.sleep(sleep_sec)
        
        try:
            # job 실행 시간에만 깨어남
            result = process_minute(db, jobs_cache, my_serial, machine_id, next_schedule, 0)
            
            if isinstance(result, int) and result > 1:
                # 내 순서가 2번 이상이면 (순서-1)*5초 후에 다시 체크
                my_order = result
                check_second = (my_order - 1) * OFFSET_STEP_SEC
                
                if check_second < 60:  # 60초를 넘지 않는 경우만
                    time.sleep(check_second)
                    process_minute(db, jobs_cache, my_serial, machine_id, next_schedule, check_second)
        except Exception as e:
            print("[loop] 오류:", e, file=sys.stderr)

def send_command(cmd_type: str, target: str = "all"):
    db = get_db()
    db.commands.insert_one({"type": cmd_type, "target": target, "created_at": datetime.now(timezone.utc)})
    print(f"{cmd_type} 명령 전송 완료 (target={target})")

if __name__ == "__main__":
    if len(sys.argv)>1 and sys.argv[1] in ("reload","refresh"):
        tgt = sys.argv[2] if len(sys.argv)>2 else "all"; send_command("reload_jobs", tgt)
    elif len(sys.argv)>1 and sys.argv[1] in ("reload-config","rc"):
        tgt = sys.argv[2] if len(sys.argv)>2 else "all"; send_command("reload_config", tgt)
    else:
        agent_main()
