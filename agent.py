#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fleetcron agent v2 - FIXED VERSION
- 설정 파일(fleetcron.config.json) 기반
- 액션 체인(actions) + when 조건 + 리트라이 + 타임아웃
- 복수 스케줄 지원(jobs.schedules[] 또는 단일 hour/minute)
- 한 PC 1프로세스, 일련번호(1~N), 앞번호 온라인 감지, 단일 실행 보장

주요 수정 사항:
1. 타임존 문제 해결 - pytz 우선 사용, 수동 UTC+9 폴백
2. SSL 인증서 문제 해결 - certifi 경로 명시
3. 머신 실행 순서 문제 해결 - 레이스 컨디션 방지
"""
import os, sys, time, json, uuid, socket, signal, threading, traceback, subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import shutil

import requests
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# 필수 패키지 설치 확인
MISSING_PACKAGES = []

# pytz 확인 (타임존 처리를 위해 필수)
try:
    import pytz
    HAS_PYTZ = True
except ImportError:
    HAS_PYTZ = False
    MISSING_PACKAGES.append("pytz")
    print("⚠️ WARNING: pytz not installed. Run: pip install pytz")

# certifi 확인 (SSL 인증서를 위해 필수)
try:
    import certifi
    HAS_CERTIFI = True
except ImportError:
    HAS_CERTIFI = False
    MISSING_PACKAGES.append("certifi")
    print("⚠️ WARNING: certifi not installed. Run: pip install certifi")

# Rich 라이브러리 (예쁜 출력용)
try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
    from rich.table import Table
    from rich.panel import Panel
    from rich.live import Live
    from rich.layout import Layout
    from rich.text import Text
    from rich import print as rprint
    USE_RICH = True
    console = Console()
except ImportError:
    USE_RICH = False
    console = None
    rprint = print

# cloudscraper 설치 시도 (Cloudflare 우회용)
try:
    import cloudscraper
    USE_CLOUDSCRAPER = True
except ImportError:
    USE_CLOUDSCRAPER = False

# 필수 패키지가 없으면 종료
if MISSING_PACKAGES:
    print(f"\n❌ CRITICAL: Missing required packages: {', '.join(MISSING_PACKAGES)}")
    print(f"Please install: pip install {' '.join(MISSING_PACKAGES)}")
    sys.exit(1)

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

# -------------------- PyInstaller 환경 수정 --------------------
def fix_pyinstaller_environment():
    """PyInstaller 빌드 환경에서 발생하는 문제 해결"""
    if getattr(sys, 'frozen', False):
        # PyInstaller로 빌드된 경우
        print("[INFO] Running in PyInstaller bundle")
        bundle_dir = sys._MEIPASS
        
        # SSL 인증서 경로 설정
        if HAS_CERTIFI:
            # 먼저 번들 내부의 certifi 확인
            bundled_ca = os.path.join(bundle_dir, 'certifi', 'cacert.pem')
            if os.path.exists(bundled_ca):
                os.environ['REQUESTS_CA_BUNDLE'] = bundled_ca
                os.environ['SSL_CERT_FILE'] = bundled_ca
                print(f"[INFO] Using bundled CA: {bundled_ca}")
            else:
                ca_bundle = certifi.where()
                if os.path.exists(ca_bundle):
                    os.environ['REQUESTS_CA_BUNDLE'] = ca_bundle
                    os.environ['SSL_CERT_FILE'] = ca_bundle
                    print(f"[INFO] Using certifi CA: {ca_bundle}")
                else:
                    # 시스템 CA 번들 시도
                    system_ca_paths = [
                        "/etc/ssl/cert.pem",  # macOS
                        "/etc/ssl/certs/ca-certificates.crt",  # Linux
                    ]
                    for path in system_ca_paths:
                        if os.path.exists(path):
                            os.environ['REQUESTS_CA_BUNDLE'] = path
                            os.environ['SSL_CERT_FILE'] = path
                            print(f"[INFO] Using system CA: {path}")
                            break
        
        # PATH 환경변수 보정 (curl 등을 찾기 위해)
        if '/usr/bin' not in os.environ.get('PATH', ''):
            os.environ['PATH'] = os.environ.get('PATH', '') + ':/usr/bin:/usr/local/bin'

fix_pyinstaller_environment()

# -------------------- 타임존 설정 (강화된 버전) --------------------
CRON_TZ = None
ACTUAL_TZ_NAME = None

def setup_timezone():
    """타임존 설정 - 여러 방법 시도"""
    global CRON_TZ, ACTUAL_TZ_NAME
    
    tz_config = CFG.get("tz", "Asia/Seoul")
    print(f"\n=== Timezone Setup ===")
    print(f"Requested timezone: {tz_config}")
    
    # 방법 1: pytz 사용 (가장 안정적)
    if HAS_PYTZ:
        try:
            CRON_TZ = pytz.timezone(tz_config)
            ACTUAL_TZ_NAME = tz_config
            
            # 테스트
            test_time = datetime.now(CRON_TZ)
            print(f"✓ Timezone loaded via pytz: {ACTUAL_TZ_NAME}")
            print(f"  Current time: {test_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            return
        except Exception as e:
            print(f"⚠️ pytz failed: {e}")
    
    # 방법 2: 수동 UTC 오프셋 설정 (한국 시간 특별 처리)
    if tz_config == "Asia/Seoul":
        CRON_TZ = timezone(timedelta(hours=9))
        ACTUAL_TZ_NAME = "Asia/Seoul (UTC+9)"
        print(f"✓ Using manual Korea timezone (UTC+9)")
        test_time = datetime.now(CRON_TZ)
        print(f"  Current time: {test_time.strftime('%Y-%m-%d %H:%M:%S')}")
        return
    
    # 방법 3: zoneinfo 시도 (Python 3.9+)
    try:
        from zoneinfo import ZoneInfo
        CRON_TZ = ZoneInfo(tz_config)
        ACTUAL_TZ_NAME = tz_config
        print(f"✓ Timezone loaded via ZoneInfo: {ACTUAL_TZ_NAME}")
        test_time = datetime.now(CRON_TZ)
        print(f"  Current time: {test_time.strftime('%Y-%m-%d %H:%M:%S')}")
        return
    except Exception as e:
        print(f"⚠️ ZoneInfo failed: {e}")
    
    # 방법 4: 시스템 로컬 타임존
    try:
        local_tz = datetime.now().astimezone().tzinfo
        CRON_TZ = local_tz
        ACTUAL_TZ_NAME = "System Local"
        print(f"⚠️ Using system local timezone")
        test_time = datetime.now(CRON_TZ)
        print(f"  Current time: {test_time.strftime('%Y-%m-%d %H:%M:%S')}")
        return
    except Exception as e:
        print(f"⚠️ System timezone failed: {e}")
    
    # 최종 폴백: UTC
    CRON_TZ = timezone.utc
    ACTUAL_TZ_NAME = "UTC (fallback)"
    print(f"⚠️ WARNING: Using UTC as fallback")
    test_time = datetime.now(CRON_TZ)
    print(f"  Current time: {test_time.strftime('%Y-%m-%d %H:%M:%S')}")

setup_timezone()

# 타임존 검증
print("\n=== Timezone Verification ===")
utc_now = datetime.now(timezone.utc)
local_now = datetime.now(CRON_TZ)
print(f"UTC time:   {utc_now.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Local time: {local_now.strftime('%Y-%m-%d %H:%M:%S')}")

if hasattr(local_now, 'utcoffset') and local_now.utcoffset():
    offset = local_now.utcoffset()
    hours = int(offset.total_seconds() // 3600)
    minutes = int((offset.total_seconds() % 3600) // 60)
    print(f"UTC offset: {hours:+03d}:{minutes:02d}")
print("=" * 30 + "\n")

MAX_SERIAL = int(CFG.get("max_serial", 10))
OFFSET_STEP_SEC = 10  # 5초에서 10초로 변경
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
    db.machines.create_index("serial")
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
        try:
            doc = db.serials.find_one_and_update(
                {"serial": s, "$or": [{"assigned_to": None}, {"assigned_to": machine_id}]},
                {"$set": {"assigned_to": machine_id, "assigned_at": now}},
                upsert=False,
                return_document=ReturnDocument.AFTER
            )
            
            if doc and doc.get("assigned_to") == machine_id:
                assigned = s
                break
            elif not doc:
                try:
                    db.serials.insert_one({
                        "serial": s,
                        "assigned_to": machine_id,
                        "assigned_at": now
                    })
                    assigned = s
                    break
                except DuplicateKeyError:
                    continue
                    
        except Exception as e:
            print(f"Serial {s} assignment attempt failed: {e}", file=sys.stderr)
            continue
    
    if not assigned:
        print(f"Warning: All serial slots 1~{MAX_SERIAL} are occupied. Exiting.", file=sys.stderr)
        sys.exit(2)
        
    db.machines.update_one(
        {"machine_id": machine_id},
        {"$set": {"machine_id": machine_id, "hostname": hostname, "serial": assigned, "last_seen": now}},
        upsert=True
    )
    return assigned

# -------------------- 템플릿 해석 --------------------
def resolve_templates(value):
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
        self._map = {}
        self._lock = threading.Lock()
        self.reload()
    
    def _add_to(self, mp, j, hour, minute):
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
        if USE_RICH:
            console.print(f"[green]✓[/green] Loaded {total} job schedules")
        else:
            print(f"✓ Loaded {total} job schedules")
    
    def list_for(self, hour: int, minute: int) -> List[Dict[str,Any]]:
        with self._lock:
            return list(self._map.get((hour, minute), []))
    
    def get_next_schedule(self, from_time: datetime) -> Optional[datetime]:
        with self._lock:
            if not self._map:
                return None
            
            current_hour = from_time.hour
            current_minute = from_time.minute
            
            for h in range(current_hour, 24):
                for m in range(60):
                    if h == current_hour and m <= current_minute:
                        continue
                    if (h, m) in self._map:
                        next_time = from_time.replace(hour=h, minute=m, second=0, microsecond=0)
                        return next_time
            
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
                    jobs_cache.reload()
                    if USE_RICH:
                        console.print("[green]✓[/green] Jobs reloaded")
                    else:
                        print("✓ Jobs reloaded")
                elif cmd.get("type") == "reload_config":
                    global CFG, CRON_TZ, ACTUAL_TZ_NAME
                    CFG = load_config()
                    setup_timezone()
                    jobs_cache.reload()
                    if USE_RICH:
                        console.print("[green]✓[/green] Config reloaded")
                    else:
                        print("✓ Config reloaded")
        except Exception as e:
            print("[cmd] watcher 오류:", e, file=sys.stderr)
        stop_ev.wait(5.0)

# -------------------- 시간 유틸/하트비트 --------------------
def floor_to_minute(dt: datetime) -> datetime: 
    return dt.replace(second=0, microsecond=0)

def to_utc_minute(dt_local_minute: datetime) -> datetime: 
    return dt_local_minute.astimezone(timezone.utc)

def update_heartbeat(db, machine_id: str, scheduled_minute_utc: datetime):
    now = datetime.now(timezone.utc)
    db.machines.update_one({"machine_id": machine_id},
        {"$set": {"last_online_minute": scheduled_minute_utc, "last_seen": now}})

def get_my_order(db, my_serial: int, machine_id: str, scheduled_minute_utc: datetime) -> int:
    update_heartbeat(db, machine_id, scheduled_minute_utc)
    
    # 다른 머신들의 heartbeat를 기다림 (레이스 컨디션 방지)
    if my_serial > 1:
        time.sleep(2)
    
    machines = list(db.machines.find(
        {"last_online_minute": scheduled_minute_utc},
        {"serial": 1, "machine_id": 1}
    ).sort([("serial", 1), ("machine_id", 1)]))
    
    for idx, m in enumerate(machines, 1):
        if m["machine_id"] == machine_id:
            return idx
    return 1

def has_earlier_online(db, my_serial: int, scheduled_minute_utc: datetime) -> bool:
    if my_serial <= 1: return False
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
def get_ssl_verify_path():
    """SSL 인증서 경로 결정"""
    if HAS_CERTIFI:
        ca_path = certifi.where()
        if os.path.exists(ca_path):
            return ca_path
    
    # 시스템 CA 경로들
    system_paths = ["/etc/ssl/cert.pem", "/etc/ssl/certs/ca-certificates.crt"]
    for path in system_paths:
        if os.path.exists(path):
            return path
    
    # 환경변수 확인
    env_ca = os.environ.get('REQUESTS_CA_BUNDLE')
    if env_ca and os.path.exists(env_ca):
        return env_ca
    
    return True  # 기본값

def execute_curl_request(url: str, headers: Dict, params: Dict, timeout: int, retries: int, delay: float):
    """curl 명령 실행 헬퍼 함수"""
    curl_paths = [
        shutil.which("curl"),
        "/usr/bin/curl",
        "/opt/homebrew/bin/curl",
        "/usr/local/bin/curl",
    ]
    
    curl_cmd = None
    for path in curl_paths:
        if path and os.path.exists(path):
            curl_cmd = path
            break
    
    if not curl_cmd:
        raise Exception("curl not found in system")
    
    cmd = [curl_cmd, "-v", "-i"]
    cmd.append(url)
    
    cmd.extend([
        "-H", f"User-Agent: {headers.get('User-Agent', 'Mozilla/5.0')}",
        "-H", f"Accept: {headers.get('Accept', '*/*')}",
        "-H", f"Accept-Language: {headers.get('Accept-Language', 'en-US,en;q=0.9')}",
        "-H", f"Upgrade-Insecure-Requests: {headers.get('Upgrade-Insecure-Requests', '1')}"
    ])
    
    # SSL 인증서 경로 추가
    ca_path = get_ssl_verify_path()
    if ca_path and ca_path != True and os.path.exists(str(ca_path)):
        cmd.extend(["--cacert", str(ca_path)])
    
    cmd.extend([
        "-m", str(int(timeout)),
        "--retry", str(retries),
        "--retry-delay", str(int(delay)),
        "--retry-all-errors"
    ])
    
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout*(retries+1)+30)
    output = result.stdout + result.stderr
    
    import re
    match = re.search(r'< HTTP/[\d\.]+ (\d+)', output)
    status_code = int(match.group(1)) if match else 0
    
    class FakeResponse:
        def __init__(self, status, text):
            self.status_code = status
            self.text = text
    
    return FakeResponse(status_code, output)

def run_http_with_retry_with_progress(step: Dict[str,Any], defaults: Dict[str,Any]) -> Tuple[str, Dict[str,Any]]:
    """진행 상황을 표시하는 HTTP 실행 함수"""
    action_name = step.get("name", step.get("url", "HTTP Request"))
    
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
    total_start = time.time()
    
    while True:
        attempts += 1
        start = time.time()
        
        if attempts > 1:
            print_action_progress(action_name, "retrying", int((time.time()-total_start)*1000), attempts, retries+1)
        else:
            print_action_progress(action_name, "running", 0)
        
        try:
            use_curl = step.get("use_curl", False)
            use_cloudscraper = step.get("use_cloudscraper", False) or "render.com" in url.lower()
            
            if use_curl and method == "GET":
                resp = execute_curl_request(url, headers, params, timeout, retries, delay)
            elif USE_CLOUDSCRAPER and use_cloudscraper:
                scraper = cloudscraper.create_scraper()
                for k, v in headers.items():
                    scraper.headers[k] = v
                kwargs = dict(params=params, timeout=timeout, verify=get_ssl_verify_path())
                if method in ("POST","PUT","PATCH","DELETE"):
                    if isinstance(body, (dict, list)): kwargs["json"]=body
                    elif body is not None: kwargs["data"]=body
                resp = scraper.request(method, url, **kwargs)
            else:
                kwargs = dict(headers=headers, params=params, timeout=timeout, verify=get_ssl_verify_path())
                if method in ("POST","PUT","PATCH","DELETE"):
                    if isinstance(body, (dict, list)): kwargs["json"]=body
                    elif body is not None: kwargs["data"]=body
                resp = requests.request(method, url, **kwargs)
                
            elapsed = int((time.time()-start)*1000)
            
            info = {
                "status_code": resp.status_code,
                "elapsed_ms": elapsed,
                "response_sample": (resp.text or "")[:RESP_SAMPLE_MAX]
            }
            
            if 200 <= resp.status_code < 300:
                info["attempts"] = attempts
                return "ok", info
            else:
                last_info = {"error": f"HTTP {resp.status_code}", **info}
                
        except Exception as e:
            elapsed = int((time.time()-start)*1000)
            error_msg = str(e)
            
            # SSL 오류 상세 정보
            if "SSL" in error_msg or "certificate" in error_msg.lower():
                error_msg += f"\n  SSL Verify Path: {get_ssl_verify_path()}"
                if HAS_CERTIFI:
                    error_msg += f"\n  Certifi Location: {certifi.where()}"
            
            last_info = {"error": error_msg, "elapsed_ms": elapsed,
                         "trace": traceback.format_exc()[:2000]}
        
        if attempts > retries:
            last_info["attempts"] = attempts
            return "error", last_info
        
        if delay > 0:
            for i in range(int(delay)):
                time.sleep(1)
                remaining = int(delay - i - 1)
                if USE_RICH and remaining > 0:
                    console.print(f"    ⏱️ [dim]Waiting {remaining}s before retry...[/dim]", end="\r")
            if delay % 1 > 0:
                time.sleep(delay % 1)
        
        delay = delay * backoff if backoff > 1 else delay

def run_http_with_retry(step: Dict[str,Any], defaults: Dict[str,Any]) -> Tuple[str, Dict[str,Any]]:
    # 진행 표시 없는 버전 (이전 버전과 동일하게 동작)
    return run_http_with_retry_with_progress(step, defaults)

# -------------------- when 조건 --------------------
def when_match(when: Optional[Dict[str,Any]], now_local: datetime) -> bool:
    if not when: return True
    if "hour_in" in when and now_local.hour not in set(when["hour_in"]): return False
    if "minute_in" in when and now_local.minute not in set(when["minute_in"]): return False
    return True

# -------------------- 액션 체인 실행 --------------------
def execute_actions(db, run_key: Dict[str,Any], job: Dict[str,Any], now_local: datetime,
                    job_defaults: Dict[str,Any]):
    steps_log = []
    status_overall = "ok"
    actions = job.get("actions", [])
    total_actions = len(actions)
    successful_actions = 0
    
    for idx, step in enumerate(actions):
        action_name = step.get("name", step.get("url", "(unnamed action)"))
        
        print_action_start(action_name, idx, total_actions, step)
        
        if step.get("type","http") != "http":
            print_action_progress(action_name, "skipped")
            steps_log.append({"index":idx,"name":action_name,"status":"skipped_unsupported"})
            continue
            
        if not when_match(step.get("when"), now_local):
            print_action_progress(action_name, "skipped")
            steps_log.append({"index":idx,"name":action_name,"status":"skipped_when"})
            continue

        st, info = run_http_with_retry_with_progress(step, {**CFG.get("http_defaults",{}),
                                              "timeout_sec": job.get("timeout_sec", CFG.get("http_defaults",{}).get("timeout_sec",10)),
                                              "retry": job.get("retry", CFG.get("http_defaults",{}).get("retry",{}))})
        
        if st == "ok":
            print_action_progress(action_name, "ok", info.get("elapsed_ms", 0), info.get("attempts", 1))
            successful_actions += 1
        else:
            print_action_progress(action_name, "error", info.get("elapsed_ms", 0), info.get("attempts", 1))
            
        s = {"index": idx, "name": action_name, "status": st}
        if st=="ok":
            s.update({"status_code": info.get("status_code"), "elapsed_ms": info.get("elapsed_ms"),
                      "attempts": info.get("attempts"), "response_sample": info.get("response_sample")})
        else:
            s.update({"error": info.get("error"), "elapsed_ms": info.get("elapsed_ms"),
                      "attempts": info.get("attempts"), "status_code": info.get("status_code")})
        steps_log.append(s)

        db.job_runs.update_one(run_key, {"$push": {"steps": s}})

        if st!="ok" and not step.get("continue_on_failure", False):
            status_overall = "error"
            break

    return status_overall, steps_log, successful_actions

# -------------------- 분 단위 처리 --------------------
def process_minute(db, jobs_cache: JobsCache, my_serial: int, machine_id: str, tick_minute_local: datetime, check_second: int):
    sched_minute_utc = to_utc_minute(tick_minute_local)
    
    if check_second == 0:
        # Serial > 1은 무조건 대기 (레이스 컨디션 방지)
        if my_serial > 1:
            if USE_RICH:
                console.print(f"\n[bold cyan]⏰ {tick_minute_local:%H:%M}[/bold cyan] - Serial #{my_serial}, waiting {(my_serial-1)*OFFSET_STEP_SEC}s...")
            else:
                print(f"\n⏰ {tick_minute_local:%H:%M} - Serial #{my_serial}, waiting {(my_serial-1)*OFFSET_STEP_SEC}s...")
            return my_serial
        
        # Serial 1만 즉시 실행 가능
        jobs_cache.reload()
        my_order = get_my_order(db, my_serial, machine_id, sched_minute_utc)
        
        if USE_RICH:
            console.print(f"\n[bold cyan]⏰ {tick_minute_local:%H:%M}[/bold cyan] - Serial #1, Order: [yellow]#{my_order}[/yellow]")
        else:
            print(f"\n⏰ {tick_minute_local:%H:%M} - Serial #1, Order #{my_order}")
        
        if my_serial == 1 and my_order == 1:
            pass  # 즉시 실행
        else:
            return my_order
    else:
        # 재확인 시점
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
            if USE_RICH:
                console.print(f"[dim]Earlier machine online, skipping[/dim]")
            else:
                print(f"Earlier machine online, skipping")
            return

    hh, mm = tick_minute_local.hour, tick_minute_local.minute
    jobs = jobs_cache.list_for(hh, mm)
    if not jobs:
        if USE_RICH:
            console.print(f"[yellow]No jobs scheduled for {tick_minute_local:%H:%M}[/yellow]")
        else:
            print(f"No jobs at {tick_minute_local:%H:%M}")
        return

    for j in jobs:
        j2 = db.jobs.find_one({"_id": j["_id"], "enabled": True})
        if not j2:
            if USE_RICH:
                console.print(f"  [dim]• {j.get('name','Unknown')}: Disabled/deleted[/dim]")
            else:
                print(f"  • {j.get('name','Unknown')}: Disabled/deleted")
            continue
            
        if not claim_job_run(db, j2, sched_minute_utc, machine_id, my_serial):
            if USE_RICH:
                console.print(f"  [dim]• {j2.get('name','Unknown')}: Already claimed[/dim]")
            else:
                print(f"  • {j2.get('name','Unknown')}: Already claimed")
            continue

        print_job_start(j2.get('name', 'Unknown'), my_serial, j2)
        start_utc = datetime.now(timezone.utc)
        run_key = {"job_id": j2["_id"], "scheduled_for": sched_minute_utc}
        
        if j2.get("actions"):
            status, steps, successful_actions = execute_actions(db, run_key, j2, tick_minute_local, {})
            total_actions = len(j2.get("actions", []))
        else:
            # v1 하위호환
            step = {
                "type":"http", "name": j2.get("name","(http)"),
                "method": j2.get("method","GET"), "url": j2.get("url"),
                "headers": j2.get("headers"), "params": j2.get("params"), "body": j2.get("body"),
                "timeout_sec": j2.get("timeout_sec"),
                "retry": j2.get("retry"),
                "use_curl": j2.get("use_curl", False),
                "use_cloudscraper": j2.get("use_cloudscraper", False)
            }
            
            print_action_start(step["name"], 0, 1, step)
            
            st, info = run_http_with_retry_with_progress(step, {**CFG.get("http_defaults",{}),
                                                 "timeout_sec": j2.get("timeout_sec", CFG.get("http_defaults",{}).get("timeout_sec",10)),
                                                 "retry": j2.get("retry", CFG.get("http_defaults",{}).get("retry",{}))})
            
            print_action_progress(step["name"], st, info.get("elapsed_ms", 0), info.get("attempts", 1))
            
            s = {"index":0, "name": step["name"], "status": st}
            if st=="ok":
                s.update({"status_code": info.get("status_code"), "elapsed_ms": info.get("elapsed_ms"),
                          "attempts": info.get("attempts"), "response_sample": info.get("response_sample")})
                successful_actions = 1
            else:
                s.update({"error": info.get("error"), "elapsed_ms": info.get("elapsed_ms"),
                          "attempts": info.get("attempts"), "status_code": info.get("status_code")})
                successful_actions = 0
            db.job_runs.update_one(run_key, {"$push": {"steps": s}})
            status = "ok" if st=="ok" else "error"
            total_actions = 1

        end_utc = datetime.now(timezone.utc)
        db.job_runs.update_one(run_key, {"$set": {"start_at": start_utc, "end_at": end_utc, "status": status}})
        elapsed = int((end_utc - start_utc).total_seconds() * 1000)
        print_job_result(j2.get('name', 'Unknown'), status, elapsed, total_actions, successful_actions)

# -------------------- 예쁜 출력 함수들 --------------------
def show_countdown(target_time: datetime, next_job_name: str = "Next job", machine_id: str = "", hostname: str = "", serial: int = 0):
    """실시간 카운트다운 타이머 표시"""
    if not USE_RICH:
        sleep_sec = (target_time - datetime.now(target_time.tzinfo)).total_seconds()
        if sleep_sec > 0:
            print(f"⏰ Waiting {int(sleep_sec)}s until {target_time:%H:%M:%S} for {next_job_name}")
            time.sleep(sleep_sec)
        return
    
    with Live(console=console, refresh_per_second=1) as live:
        while True:
            now = datetime.now(target_time.tzinfo)
            remaining = (target_time - now).total_seconds()
            
            if remaining <= 0:
                break
                
            layout = Layout()
            layout.split_column(
                Layout(name="header", size=3),
                Layout(name="timer", size=7),
                Layout(name="info", size=5)
            )
            
            header_text = Text()
            header_text.append("🤖 FleetCron Agent ", style="bold cyan")
            header_text.append(f"[{hostname}]", style="yellow")
            header_text.append(f" Serial #{serial}", style="green")
            layout["header"].update(Panel(header_text, title="[bold blue]System Info[/bold blue]"))
            
            timer_text = Text(justify="center")
            mins, secs = divmod(int(remaining), 60)
            hours, mins = divmod(mins, 60)
            timer_text.append(f"\n⏳ Time Remaining: ", style="white")
            timer_text.append(f"{hours:02d}:{mins:02d}:{secs:02d}\n", style="bold cyan")
            
            if remaining < 3600:
                total_wait = 3600
                progress = max(0, min(100, (1 - remaining/total_wait) * 100))
                bar_length = 40
                filled = int(bar_length * progress / 100)
                bar = "█" * filled + "░" * (bar_length - filled)
                timer_text.append(f"\n[{bar}] {progress:.1f}%\n", style="green")
            
            layout["timer"].update(Panel(timer_text, title=f"[bold yellow]⏰ Next: {target_time:%H:%M:%S}[/bold yellow]"))
            
            info_table = Table.grid(padding=1)
            info_table.add_column(style="cyan", justify="right")
            info_table.add_column(style="white")
            info_table.add_row("📋 Next Job:", f"[bold yellow]{next_job_name}[/bold yellow]")
            info_table.add_row("🆔 Machine ID:", f"[dim]{machine_id[:8]}...[/dim]")
            info_table.add_row("🌐 Timezone:", f"{ACTUAL_TZ_NAME}")
            
            layout["info"].update(Panel(info_table, title="[bold magenta]Job Info[/bold magenta]"))
            
            live.update(layout)
            time.sleep(0.5)

def print_job_start(job_name: str, my_order: int, job_config: Dict[str, Any] = None):
    """작업 시작 시 상세 정보 출력"""
    if USE_RICH:
        table = Table(show_header=True, header_style="bold cyan", box=None)
        table.add_column("Property", style="yellow", width=20)
        table.add_column("Value", style="white")
        
        table.add_row("📋 Job Name", f"[bold]{job_name}[/bold]")
        table.add_row("🎯 Execution Order", f"#{my_order}")
        
        if job_config:
            timeout = job_config.get("timeout_sec", CFG.get("http_defaults", {}).get("timeout_sec", 30))
            table.add_row("⏱️ Timeout", f"{timeout}s")
            
            retry_config = job_config.get("retry", CFG.get("http_defaults", {}).get("retry", {}))
            retries = retry_config.get("retries", 3)
            delay = retry_config.get("delay_sec", 1)
            backoff = retry_config.get("backoff", 1.0)
            table.add_row("🔄 Retry Policy", f"{retries} times, {delay}s delay" + (f", {backoff}x backoff" if backoff > 1 else ""))
            
            actions = job_config.get("actions", [])
            if actions:
                table.add_row("🎬 Actions", f"{len(actions)} actions")
            elif job_config.get("url"):
                table.add_row("🌐 Type", "Single HTTP request")
        
        console.print("\n")
        console.print(Panel(table, title="[bold green]🚀 Starting Job[/bold green]", border_style="green"))
    else:
        print(f"\n🚀 Starting: {job_name} (Order #{my_order})")
        if job_config:
            timeout = job_config.get("timeout_sec", 30)
            retry = job_config.get("retry", {})
            print(f"   ⏱️ Timeout: {timeout}s | 🔄 Retries: {retry.get('retries', 0)}")

def print_action_start(action_name: str, action_index: int, total_actions: int, action_config: Dict[str, Any] = None):
    """액션 시작 시 상세 정보 출력"""
    if USE_RICH:
        progress_text = f"[cyan]Action {action_index + 1}/{total_actions}[/cyan]"
        
        details = []
        if action_config:
            method = action_config.get("method", "GET")
            url = action_config.get("url", "")
            if url:
                display_url = url if len(url) <= 60 else url[:57] + "..."
                details.append(f"[yellow]{method}[/yellow] {display_url}")
            
            retry = action_config.get("retry", {})
            if retry:
                details.append(f"🔄 {retry.get('retries', 0)} retries")
            
            if action_config.get("when"):
                details.append("⚡ Conditional")
        
        console.print(f"\n  {progress_text} [bold]{action_name}[/bold]")
        if details:
            console.print(f"    {' | '.join(details)}")
    else:
        print(f"\n  ▶ Action {action_index + 1}/{total_actions}: {action_name}")

def print_action_progress(action_name: str, status: str, elapsed_ms: int = 0, attempt: int = 1, max_attempts: int = 1):
    """액션 진행 상황 출력"""
    if USE_RICH:
        if status == "running":
            console.print(f"    ⏳ [yellow]Running...[/yellow] ({elapsed_ms}ms elapsed)", end="\r")
        elif status == "retrying":
            console.print(f"    🔄 [yellow]Retry {attempt}/{max_attempts}[/yellow] ({elapsed_ms}ms)", end="\r")
        elif status == "ok":
            console.print(f"    ✅ [green]Success[/green] ({elapsed_ms}ms, {attempt} attempt{'s' if attempt > 1 else ''})")
        elif status == "error":
            console.print(f"    ❌ [red]Failed[/red] ({elapsed_ms}ms, {attempt} attempt{'s' if attempt > 1 else ''})")
        elif status == "skipped":
            console.print(f"    ⏭️ [dim]Skipped[/dim]")
    else:
        symbols = {"running": "⏳", "retrying": "🔄", "ok": "✅", "error": "❌", "skipped": "⏭️"}
        symbol = symbols.get(status, "•")
        if status == "retrying":
            print(f"    {symbol} Retry {attempt}/{max_attempts} ({elapsed_ms}ms)")
        else:
            print(f"    {symbol} {status.capitalize()} ({elapsed_ms}ms)")

def print_job_result(job_name: str, status: str, elapsed_ms: int = 0, total_actions: int = 0, successful_actions: int = 0):
    """작업 결과 출력"""
    if USE_RICH:
        if status == "ok":
            console.print(f"\n[bold green]✅ Job Completed Successfully[/bold green]")
            console.print(f"   [dim]{job_name} - {elapsed_ms}ms total[/dim]")
            if total_actions > 0:
                console.print(f"   [dim]{successful_actions}/{total_actions} actions succeeded[/dim]")
        else:
            console.print(f"\n[bold red]❌ Job Failed[/bold red]")
            console.print(f"   [dim]{job_name} - {elapsed_ms}ms total[/dim]")
            if total_actions > 0:
                console.print(f"   [dim]{successful_actions}/{total_actions} actions succeeded[/dim]")
    else:
        symbol = "✅" if status == "ok" else "❌"
        print(f"\n{symbol} {job_name}: {status} ({elapsed_ms}ms)")
        if total_actions > 0:
            print(f"   {successful_actions}/{total_actions} actions completed")

# -------------------- 메인 --------------------
def agent_main():
    lock_handle = acquire_lock()
    machine_id = load_or_create_machine_id()
    hostname = socket.gethostname()
    db = get_db()
    my_serial = get_or_assign_serial(db, machine_id, hostname)
    
    # 실제 타임존 정보 표시
    tz_display = f"{CFG.get('tz')} (configured)"
    if ACTUAL_TZ_NAME != CFG.get('tz'):
        tz_display = f"{CFG.get('tz')} → {ACTUAL_TZ_NAME} (actual)"
    
    if USE_RICH:
        start_panel = Panel.fit(
            f"[bold cyan]🚀 FleetCron Agent Started[/bold cyan]\n\n"
            f"[yellow]Machine ID:[/yellow] {machine_id[:12]}...\n"
            f"[yellow]Hostname:[/yellow] {hostname}\n"
            f"[yellow]Serial:[/yellow] #{my_serial}\n"
            f"[yellow]Timezone:[/yellow] {tz_display}\n"
            f"[yellow]Offset Step:[/yellow] {OFFSET_STEP_SEC}s\n"
            f"[yellow]SSL:[/yellow] {'✓ certifi' if HAS_CERTIFI else '⚠️ system CA'}",
            title="[bold green]System Ready[/bold green]",
            border_style="green"
        )
        console.print(start_panel)
    else:
        print(f"🚀 Started: machine_id={machine_id}, host={hostname}, serial={my_serial}, tz={tz_display}, offset={OFFSET_STEP_SEC}s")

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
        
        next_schedule = jobs_cache.get_next_schedule(now_local)
        if not next_schedule:
            next_check = now_local.replace(second=0, microsecond=0)
            if now_local.minute < 30:
                next_check = next_check.replace(minute=30)
            else:
                next_check = (next_check + timedelta(hours=1)).replace(minute=0)
            
            sleep_sec = (next_check - now_local).total_seconds()
            if USE_RICH:
                console.print(f"[yellow]⏸  No jobs scheduled. Checking again at {next_check:%H:%M}[/yellow]")
            else:
                print(f"⏸  No jobs. Check at {next_check:%H:%M} ({int(sleep_sec)}s)")
            time.sleep(sleep_sec)
            jobs_cache.reload()
            continue
        
        sleep_sec = (next_schedule - now_local).total_seconds()
        max_sleep = 30 * 60
        
        if sleep_sec > max_sleep:
            if USE_RICH:
                console.print(f"[dim]⏸  Long wait. Next job at {next_schedule:%H:%M} (waiting 30min)[/dim]")
            else:
                print(f"⏸  Long wait. Next: {next_schedule:%H:%M} (30min)")
            time.sleep(max_sleep)
            jobs_cache.reload()
            continue
        
        if sleep_sec > 0:
            next_jobs = jobs_cache.list_for(next_schedule.hour, next_schedule.minute)
            next_job_name = next_jobs[0].get('name', 'Unknown Job') if next_jobs else 'Unknown Job'
            
            show_countdown(next_schedule, next_job_name, machine_id, hostname, my_serial)
        
        try:
            result = process_minute(db, jobs_cache, my_serial, machine_id, next_schedule, 0)
            
            if isinstance(result, int) and result > 1:
                my_order = result
                check_second = (my_order - 1) * OFFSET_STEP_SEC
                
                if check_second < 60:
                    time.sleep(check_second)
                    process_minute(db, jobs_cache, my_serial, machine_id, next_schedule, check_second)
        except Exception as e:
            print("[loop] 오류:", e, file=sys.stderr)
            traceback.print_exc()

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