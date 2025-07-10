# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Enhanced property sync script with structured logging, retries, and metrics.
"""
import csv, io, json, logging, os, sys, threading, time, uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from PIL import Image
from io import BytesIO

import requests
from google.auth.exceptions import RefreshError
from google.cloud import storage, logging as cloud_logging

def ensure_csv_blob_exists(blob_name: str, cols: List[str]) -> None:
    """
    If gs://<BUCKET>/<blob_name> doesn’t exist, create it with only the header row.
    """
    gcs_blob = bucket.blob(blob_name)
    if not gcs_blob.exists():
        tmp = f"/tmp/{os.path.basename(blob_name)}"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
        gcs_blob.upload_from_filename(tmp, content_type="text/csv")
        log_struct("csv_blob_created", blob=blob_name, headers=",".join(cols))

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


# ────────────────────────────────────────────────────────────
# Run ID & Metrics
# ────────────────────────────────────────────────────────────
run_id = uuid.uuid4().hex
metrics = {
    "records_processed": 0,
    "records_failed": 0,
    "images_downloaded": 0,
    "image_failures": 0,
    "api_errors": 0,
    "geocode_failures": 0,
}

# ────────────────────────────────────────────────────────────
# Logging Setup (with run_id)
# ────────────────────────────────────────────────────────────
for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

cloud_client = cloud_logging.Client()
cloud_client.setup_logging(log_level=logging.INFO)

# ────────────────────────────────────────────────────────────
# Console handler so Cloud Run’s stdout/stderr viewer shows detail
# ────────────────────────────────────────────────────────────
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)          # or INFO if you prefer
console_fmt = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console_handler.setFormatter(console_fmt)
logging.getLogger().addHandler(console_handler)

def log_phase(phase: str, msg: str | None = None, **fields) -> None:
    extra = {"run_id": run_id, "phase": phase, **fields}
    logging.getLogger().info(msg or phase, extra=extra)

log_struct = log_phase  # alias

# ────────────────────────────────────────────────────────────
# Retry decorator
# ────────────────────────────────────────────────────────────
def retry(attempts: int = 3, initial_delay: float = 1.0, backoff: float = 2.0):
    def decorator(func):
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    metrics["api_errors"] += 1
                    logging.exception(
                        f"[{func.__name__}] attempt {attempt}/{attempts} failed: {e}",
                        extra={"run_id": run_id, "attempt": attempt}
                    )
                    if attempt == attempts:
                        raise
                    time.sleep(delay)
                    delay *= backoff
        return wrapper
    return decorator

# ────────────────────────────────────────────────────────────
# Helpers with retry and structured logging
# ────────────────────────────────────────────────────────────
@retry(attempts=3, initial_delay=1.0, backoff=2.0)
def safe_request(method: str, url: str, **kwargs) -> requests.Response:
    """
    Abort on non-2xx, with retry and full error dumps.
    """
    r = requests.request(method, url, **kwargs)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        # 1) log the snippet
        body = getattr(r, "text", "")[:500]
        logging.error(
            "HTTP error %s %s: %s",
            method, url, body,
            extra={"run_id": run_id, "status": r.status_code}
        )
        # 2) now dump the full JSON or full text
        try:
            full = r.json()
            logging.error(
                "Full error JSON: %s",
                json.dumps(full),
                extra={"run_id": run_id, "status": r.status_code}
            )
        except Exception:
            logging.error(
                "Full error text: %s",
                r.text,
                extra={"run_id": run_id, "status": r.status_code}
            )
        raise
    return r

@retry(attempts=2, initial_delay=0.5, backoff=2.0)
def geocode_address(addr: str) -> Tuple[str, str]:
    """Geocode via Google Maps API, count failures."""
    if not addr:
        return "", ""
    try:
        r = safe_request(
            "GET",
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": addr, "key": os.getenv("MAPS_API_KEY")},
            timeout=10
        )
        data = r.json().get("results", [])
        if data and isinstance(data, list):
            loc = data[0]["geometry"]["location"]
            return str(loc.get("lng", "")), str(loc.get("lat", ""))
    except Exception:
        metrics["geocode_failures"] += 1
        logging.exception("Geocode exception for address: %s", addr, extra={"run_id": run_id})
    return "", ""

@retry(attempts=2, initial_delay=1.0, backoff=2.0)
def get_exchange_rate() -> Dict[str, float]:
    """
    Fetch live EUR⇄HUF rates (used for rounding rent/sale prices).
    Falls back to 350 HUF/EUR on error.
    """
    url = "https://api.exchangerate-api.com/v4/latest/EUR"
    try:
        # use safe_request so you get retries & full error dumps
        r = safe_request("GET", url, timeout=10)
        data = r.json()
        huf = float(data["rates"].get("HUF", 0))
        if huf <= 0:
            raise ValueError(f"Invalid HUF rate: {huf}")
        return {"EUR_to_HUF": huf, "HUF_to_EUR": 1.0 / huf}
    except Exception:
        # log and fallback
        logging.exception("Error fetching FX rates; using fallback 350 HUF/EUR",
                          extra={"run_id": run_id})
        fallback = 350.0
        return {"EUR_to_HUF": fallback, "HUF_to_EUR": 1.0 / fallback}

# ────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────
PROJECT_ID     = "prime-estate-kft"
BUCKET_NAME    = "property_images_web"
HU_BLOB        = "property_data_tables/property_web_hu.csv"
ENG_BLOB       = "property_data_tables/property_web_eng.csv"
WATERMARK_BLOB = "property_data_tables/last_run_date.txt"
TMP_HU         = "/tmp/property_web_hu.csv"
TMP_ENG        = "/tmp/property_web_eng.csv"
DELTA_HU_BLOB  = "property_data_tables/property_delta_hu.csv"
DELTA_ENG_BLOB = "property_data_tables/property_delta_eng.csv"
TMP_DELTA_HU   = "/tmp/property_delta_hu.csv"
TMP_DELTA_ENG  = "/tmp/property_delta_eng.csv"

# ────────────────────────────────────────────────────────────
# Exchange rates placeholder (will be populated at runtime)
# ────────────────────────────────────────────────────────────
EXCHANGE_RATES: Dict[str, float] = {}


BASE_API_URL           = "https://www.zohoapis.eu/crm/v7"
API_URL_FILES_DOWNLOAD = "https://www.zohoapis.eu/crm/v7/files"
MODULE_API_NAME        = "Properties"

REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
CLIENT_ID     = os.getenv("ZOHO_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
MAPS_API_KEY  = os.getenv("MAPS_API_KEY")

PER_PAGE     = 200
IMAGE_FIELDS = ["Images","Image_2","Image_3","Image_4","Image_5","Image_6","Image_8","Image_9"]
EXTRACT_FIELDS = [
    "id","Name","Meret","HalokSzama","FurdokSzama","ZuhanyzokSzama",
    "Butorozott_multi","legkondi_lista","lift_lista","GarazsokSzama",
    "TeraszokSzama","kilitas_dropdown","status","internetre_lista",
    "images_data1","images_data2","images_data3","Varos",
    "varosresz_dropdown","MegjegyzesPublikusMagyar",
    "MegjegyzesPublikusAngol","BerletiDijBrutto","BerletiDijPenznem",
    "EladasiAr","EladasiArPenznem","Kerulet",
    "tipus_dropdown"   # ← newly added
]
SUBFORM_FIELD, SUBFORM_EXTRACT_FIELD = "Teraszok","terasz_meret"

CSV_COLUMNS_HU = [
    "id","Ingatlan ID","Varos","varosresz_dropdown","Méret","Hálók","Furdok",
    "Butorozott","Legkondi","Lift","Garazs","Kilatas","Statusz","Internet",
    "Tipus","Terasz","Leírás",
    "Bérleti Díj","Bérleti Díj Pénznem","Gross Rent EUR","Gross Rent HUF",
    "Eladási Ár","Eladási Ár Pénznem","Sale Price EUR","Sale Price HUF",
    "Kerulet","KeruletCategory","Cím","Longitude","Latitude","ingatlan_kepek"
]

CSV_COLUMNS_ENG = [
    "id","Property ID","City","District","Size","Bedrooms","Bathrooms",
    "Furnished","Air Conditioning","Elevator","Garage","View","Status",
    "Internet","Type","Terrace","Description","Gross Rent EUR","Gross Rent HUF",
    "Rental Currency","Sale Price EUR","Sale Price HUF","Sale Currency",
    "District No.","Area Category","Cím","Longitude","Latitude"
]

# ────────────────────────────────────────────────────────────
# Globals / GCS Client
# ────────────────────────────────────────────────────────────
storage_client = storage.Client(project=PROJECT_ID)
bucket         = storage_client.bucket(BUCKET_NAME)
HEADERS: Dict[str,str]  = {}
LAST_TOKEN_REFRESH      = 0.0
TOKEN_LOCK              = threading.Lock()

# ────────────────────────────────────────────────────────────
# GCS & Watermark Helpers
# ────────────────────────────────────────────────────────────
def read_gcs_text(blob: str) -> Optional[str]:
    try:
        return bucket.blob(blob).download_as_text()
    except Exception:
        logging.exception("Failed to read GCS blob: %s", blob, extra={"run_id": run_id})
        return None

def write_gcs_text(blob: str, text: str) -> None:
    try:
        bucket.blob(blob).upload_from_string(text, "text/plain")
    except Exception:
        logging.exception("Failed to write GCS blob: %s", blob, extra={"run_id": run_id})

def read_watermark() -> str:
    txt = read_gcs_text(WATERMARK_BLOB)
    wm  = txt.strip() if txt else "2025-05-25T00:00:00+00:00"
    log_struct("watermark_read", watermark=wm)
    return wm

def write_watermark_now() -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    write_gcs_text(WATERMARK_BLOB, now)
    log_struct("watermark_written", watermark=now)

# ────────────────────────────────────────────────────────────
# Sanitizers & Converters
# ────────────────────────────────────────────────────────────
def safe_text(v: Any) -> str:
    """
    Clean scalar or list values:
    - None, "", [], {} → ""
    - Python lists → join items with ", "
    - String repr of lists ("['X','Y']") → parse JSON or split → join
    - Otherwise → str(v).strip()
    """
    if v in (None, "", [], {}):
        return ""

    # If it's already a real list, join its items
    if isinstance(v, list):
        items = [
            str(item).strip()
            for item in v
            if str(item).strip() and str(item).strip().lower() != "none"
        ]
        return ", ".join(items)

    s = str(v).strip()
    # unwrap simple list-encoded strings
    if s.startswith("[") and s.endswith("]"):
        inner = s
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                items = [
                    str(item).strip()
                    for item in parsed
                    if str(item).strip() and str(item).strip().lower() != "none"
                ]
                return ", ".join(items)
        except Exception:
            # fallback: split on commas inside the brackets
            inner = s[1:-1]
        parts = [
            part.strip(" '\"")
            for part in inner.split(",")
            if part.strip(" '\"") and part.strip(" '\"").lower() != "none"
        ]
        return ", ".join(parts)

    return s

def safe_numeric(v: Any) -> str:
    s = safe_text(v)
    return s if s.replace(".","",1).isdigit() else ""

def normalize_bool(v: Any, yes: str="Igen", no: str="Nem") -> str:
    t = safe_text(v).lower()
    return yes if t==yes.lower() else (no if t==no.lower() else "")

def int_to_roman(n: int) -> str:
    table = [(1000,"M"),(900,"CM"),(500,"D"),(400,"CD"),
             (100,"C"),(90,"XC"),(50,"L"),(40,"XL"),
             (10,"X"),(9,"IX"),(5,"V"),(4,"IV"),(1,"I")]
    res = ""
    for arab,rom in table:
        while n>=arab:
            res+=rom; n-=arab
    return res

# ────────────────────────────────────────────────────────────
# Zoho OAuth & CRUD Helpers
# ────────────────────────────────────────────────────────────
@retry(attempts=3, initial_delay=1.0)
def refresh_access_token(if_mod: str) -> None:
    global ACCESS_TOKEN, HEADERS, LAST_TOKEN_REFRESH
    with TOKEN_LOCK:
        now = time.time()
        if now - LAST_TOKEN_REFRESH < 30:
            return
        payload = {
            "refresh_token": REFRESH_TOKEN,
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type":    "refresh_token"
        }
        r = safe_request("POST", "https://accounts.zoho.eu/oauth/v2/token",
                         data=payload, timeout=15)
        data = r.json()
        ACCESS_TOKEN = data["access_token"]
        LAST_TOKEN_REFRESH = now
        HEADERS = {
            "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}",
            "If-Modified-Since": if_mod
        }
        log_struct("oauth_refreshed", expires_in=data.get("expires_in"))

@retry(attempts=3, initial_delay=1.0)
def get_deleted_record_ids(api_module: str) -> Set[str]:
    deleted: Set[str] = set()
    page = 1
    while True:
        resp = safe_request(
            "GET",
            f"{BASE_API_URL}/{api_module}/deleted",
            headers=HEADERS,
            params={"type":"all","page":page,"per_page":PER_PAGE},
            timeout=30
        )
        if resp.status_code in (204,304):
            log_struct("fetch_deleted_no_change",
                       api_module=api_module,
                       page=page,
                       status=resp.status_code)
            break
        payload = resp.json()
        batch = payload.get("data", [])
        more  = payload.get("info", {}).get("more_records", False)
        deleted.update(str(d.get("id")) for d in batch if d.get("id"))
        log_struct("fetch_deleted_page",
                   api_module=api_module,
                   page=page,
                   returned=len(batch),
                   more_records=more)
        if not more:
            break
        page += 1
    log_struct("deleted_records_fetched",
               api_module=api_module,
               total_deleted=len(deleted))
    return deleted

@retry(attempts=3, initial_delay=1.0)
def get_all_records(api_module: str) -> List[Dict[str,Any]]:
    """
    Fetch all (modified) records since watermark, including raw image fields.
    """
    out: List[Dict[str,Any]] = []
    page = 1

    # ← include the IMAGE_FIELDS alongside your EXTRACT_FIELDS
    fields = ",".join(EXTRACT_FIELDS + IMAGE_FIELDS)

    while True:
        resp = safe_request(
            "GET",
            f"{BASE_API_URL}/{api_module}",
            headers=HEADERS,
            params={"page": page, "per_page": PER_PAGE, "fields": fields},
            timeout=30
        )
        if resp.status_code in (204, 304):
            log_struct("fetch_no_change",
                       api_module=api_module,
                       page=page,
                       status=resp.status_code)
            break

        payload = resp.json()
        batch   = payload.get("data", [])
        more    = payload.get("info", {}).get("more_records", False)
        out.extend(batch)

        log_struct("fetch_page",
                   api_module=api_module,
                   page=page,
                   returned=len(batch),
                   more_records=more)

        if not more:
            break
        page += 1

    log_struct("all_records_fetched",
               api_module=api_module,
               total_records=len(out))
    return out

@retry(attempts=2, initial_delay=0.5)
def get_single_record(rid: str) -> Optional[Dict[str,Any]]:
    """
    Fetch one full record (used for sub-form or image lookups).
    """
    fields = "id," + ",".join(EXTRACT_FIELDS + IMAGE_FIELDS)
    resp = safe_request(
        "GET",
        f"{BASE_API_URL}/{MODULE_API_NAME}/{rid}",
        headers=HEADERS,
        params={"fields":fields},
        timeout=15,
    )
    payload = resp.json()
    records = payload.get("data", [])
    return records[0] if records else None

# ────────────────────────────────────────────────────────────
# Image Extraction & Sync
# ────────────────────────────────────────────────────────────

@retry(attempts=3, initial_delay=1.0)
def extract_upload_ids(rec: Dict[str, Any]) -> List[Tuple[str, int]]:
    """
    Pull every Encrypted_Id (or File_Id__s) from IMAGE_FIELDS in order,
    assigning one continuous sequence number across all fields.
    If the initial record fetch (with If-Modified-Since) omitted images,
    do one extra GET for the full record (no If-Modified-Since header)
    so you always see the current Image Upload fields.
    """
    def _extract_from(raw_obj) -> List[Tuple[str,int]]:
        out, seq = [], 0
        # normalize into list of dicts
        if isinstance(raw_obj, list):
            data = raw_obj
        elif isinstance(raw_obj, dict) and isinstance(raw_obj.get("data"), list):
            data = raw_obj["data"]
        elif isinstance(raw_obj, str) and raw_obj.strip().startswith("["):
            try:
                data = json.loads(raw_obj)
            except:
                data = []
        else:
            data = []

        for item in data:
            if not isinstance(item, dict):
                continue
            fid = item.get("Encrypted_Id") or item.get("File_Id__s")
            if fid:
                seq += 1
                out.append((fid, seq))
        return out

    # 1) try on the modified record we already fetched
    combined: List[Tuple[str,int]] = []
    for fld in IMAGE_FIELDS:
        combined += _extract_from(rec.get(fld))
    if combined:
        return combined

    # 2) fallback: re-fetch the full record *without* If-Modified-Since
    rid = rec.get("id")
    if not rid:
        return []

    fields = "id," + ",".join(EXTRACT_FIELDS + IMAGE_FIELDS)
    resp = safe_request(
        "GET",
        f"{BASE_API_URL}/{MODULE_API_NAME}/{rid}",
        headers={"Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}"},
        params={"fields": fields},
        timeout=15
    )
    data = resp.json().get("data", [])
    if not data:
        return []

    full = data[0]
    combined = []
    for fld in IMAGE_FIELDS:
        combined += _extract_from(full.get(fld))
    return combined

@retry(attempts=3, initial_delay=1.0)
def patch_images_data(pid: str, raw_ids: List[Tuple[str, int]]) -> None:
    """
    Write your extracted <fid>-<seq> list back into Zoho's images_data1/2/3.
    Only runs for records modified since If-Modified-Since (i.e. those passed in).
    """
    count = len(raw_ids)
    log_struct("images_data_updating", property_id=pid, image_count=count)

    # join then chunk at Zoho's 2000-char limit
    txt = ",".join(f"{fid}-{seq}" for fid, seq in raw_ids)
    chunks = [txt[i : i + 2000] for i in range(0, len(txt), 2000)]

    payload = {
        "data": [{
            "id": pid,
            "images_data1": chunks[0] if len(chunks) > 0 else "",
            "images_data2": chunks[1] if len(chunks) > 1 else "",
            "images_data3": chunks[2] if len(chunks) > 2 else "",
        }]
    }

    safe_request(
        "PUT",
        f"{BASE_API_URL}/{MODULE_API_NAME}/{pid}",
        headers={**HEADERS, "Content-Type": "application/json"},
        json=payload,
        timeout=30
    )

    log_struct("images_data_updated", property_id=pid, image_count=count)
    
@retry(attempts=3, initial_delay=1.0)
def list_blobs_for_property(iid: str) -> Set[str]:
    """
    List all blobs under the folder named by the Ingatlan ID (iid).
    """
    try:
        return {b.name for b in bucket.list_blobs(prefix=f"{iid}/")}
    except RefreshError:
        metrics["api_errors"] += 1
        logging.exception(
            "GCS refresh error for Ingatlan ID: %s", iid,
            extra={"run_id": run_id}
        )
        return set()


@retry(attempts=3, initial_delay=1.0)
def download_and_upload_image(
    fid: str,
    iid: str,
    seq: int,
    existing: Set[str]
) -> Optional[str]:
    """
    Download from Zoho by file-ID, compress/resize, then upload to
    gs://<BUCKET>/<IngatlanID>/<fid>-<seq>.jpg.

    On a 401 or INVALID_TOKEN, automatically refreshes the token and retries once.
    """
    new_name   = f"{iid}/{fid}-{seq}.jpg"
    public_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{new_name}"

    # delete stale duplicates
    dup_prefix = f"{iid}/{fid}-"
    for dup in [n for n in existing if n.startswith(dup_prefix) and n != new_name]:
        try:
            bucket.blob(dup).delete()
            existing.remove(dup)
        except Exception:
            logging.exception("Failed to delete duplicate blob %s", dup,
                              extra={"run_id": run_id})

    # skip if already small enough
    if new_name in existing:
        blob = bucket.blob(new_name)
        if (blob.size or 0) <= 300_000:
            return public_url
        blob.delete()
        existing.remove(new_name)

    # attempt download, refreshing token on invalid
    for attempt in range(2):
        resp = requests.get(
            API_URL_FILES_DOWNLOAD,
            headers=HEADERS,
            params={"id": fid},
            timeout=(5, 60)
        )

        # if Zoho says token invalid, refresh and retry
        if resp.status_code == 401:
            logging.warning("Token expired during image download, refreshing…",
                            extra={"run_id": run_id})
            refresh_access_token(HEADERS.get("If-Modified-Since","1970-01-01T00:00:00+00:00"))
            continue

        # Zoho sometimes returns 200 but with { code: "INVALID_TOKEN" }
        try:
            j = resp.json()
            if j.get("code") == "INVALID_TOKEN":
                logging.warning("Received INVALID_TOKEN payload, refreshing…",
                                extra={"run_id": run_id})
                refresh_access_token(HEADERS.get("If-Modified-Since","1970-01-01T00:00:00+00:00"))
                continue
        except ValueError:
            pass

        # finally, ensure it's a good download
        try:
            resp.raise_for_status()
        except Exception:
            # let safe_request decorator capture/log this
            raise

        # success
        r = resp
        break
    else:
        # if we exhausted retries, raise
        resp.raise_for_status()

    # compress & upload
    img = Image.open(BytesIO(r.content))
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    img.thumbnail((1024,1024), Image.LANCZOS)
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=85, optimize=True)

    bucket.blob(new_name).upload_from_string(buf.getvalue(), "image/jpeg")
    existing.add(new_name)
    return public_url



# ────────────────────────────────────────────────────────────
# CSV Helpers
# ────────────────────────────────────────────────────────────

def load_csv_to_dict(blob: str, cols: List[str]) -> Dict[str,Dict[str,str]]:
    out: Dict[str,Dict[str,str]] = {}
    txt = read_gcs_text(blob)
    if txt:
        for row in csv.DictReader(io.StringIO(txt)):
            out[row["id"]] = {c: row.get(c, "") for c in cols}
    return out

def save_dict_to_csv(path: str, rows: Dict[str, Dict[str, Any]], cols: List[str]) -> None:
    """
    Write out rows to CSV, ensuring no None/"None" values appear:
    they get coerced to empty strings.
    """
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        for row in rows.values():
            clean_row = {}
            for c in cols:
                v = row.get(c, "")
                # drop actual None or the string "None"
                if v is None or str(v).lower() == "none":
                    clean_row[c] = ""
                else:
                    clean_row[c] = v
            writer.writerow(clean_row)
            
def calculate_furdok(rec: Dict[str, Any]) -> str:
    """
    Compute the total number of bathrooms:
      - If both ZuhanyzokSzama and FurdokSzama are present, sum them.
      - If only one is present, use that value.
      - If neither is present, return empty string.
    """
    z_str = safe_numeric(rec.get("ZuhanyzokSzama"))
    f_str = safe_numeric(rec.get("FurdokSzama"))

    # If neither field has a value, return blank
    if not z_str and not f_str:
        return ""

    # Parse each (missing → 0.0)
    try:
        z = float(z_str) if z_str else 0.0
        f = float(f_str) if f_str else 0.0
    except ValueError:
        # fallback in case of unexpected non-numeric
        return ""

    total = z + f
    # If one was zero, this just returns the other; if both present, returns sum.
    return str(int(round(total)))

def calculate_garazs(rec: Dict[str, Any]) -> str:
    """
    Determine if a property has a garage:
      - If GarazsokSzama parses to a number > 0 → return "Igen"
      - Otherwise → return ""
    """
    g = safe_numeric(rec.get("GarazsokSzama"))
    if not g:
        return ""
    try:
        return "Igen" if float(g) > 0 else ""
    except ValueError:
        return ""

# ────────────────────────────────────────────────────────────
# Record Transform
# ────────────────────────────────────────────────────────────
def process_property(
    rec: Dict[str, Any],
    image_urls: List[str],
    *,
    skip_live_filter: bool = False
) -> Optional[Dict[str, str]]:
    """
    Build one CSV row (Hungarian) with:
     • No None values (empty instead)
     • Money fields rounded to integers
     • New 'Tipus' category based on tipus_dropdown
     • Boolean flags as Igen/Nem, N/A if missing
     • Internet column from Zoho’s internetre_lista

    If skip_live_filter=True, we never drop by internetre/status.
    """
    rid = safe_text(rec.get("id"))
    # pull subform terrace sizes if needed
    if rid and not safe_text(rec.get("TeraszokSzama")):
        full = get_single_record(rid)
        rec[SUBFORM_EXTRACT_FIELD] = ",".join(
            str(e.get(SUBFORM_EXTRACT_FIELD, "")).strip()
            for e in ((full or {}).get(SUBFORM_FIELD, []))
            if e.get(SUBFORM_EXTRACT_FIELD)
        )

    out: Dict[str, str] = {}

    # Basic fields
    out["id"]                 = rid
    out["Ingatlan ID"]        = safe_text(rec.get("Name"))
    out["Varos"]              = (
        "Budapest"
        if safe_text(rec.get("Varos")).lower() == "bp"
        else safe_text(rec.get("Varos")) or ""
    )
    out["varosresz_dropdown"] = safe_text(rec.get("varosresz_dropdown"))
    out["Méret"]              = safe_numeric(rec.get("Meret"))
    out["Hálók"]              = safe_numeric(rec.get("HalokSzama"))
    out["Furdok"]             = calculate_furdok(rec)

    # Boolean flags
    val = safe_text(rec.get("Butorozott_multi"))
    out["Butorozott"] = val if val else "N/A"
    val = normalize_bool(rec.get("legkondi_lista"))
    out["Legkondi"]   = val if val else "N/A"
    val = normalize_bool(rec.get("lift_lista"))
    out["Lift"]       = val if val else "N/A"
    val = calculate_garazs(rec)
    out["Garazs"]     = val if val else "N/A"

    out["Kilatas"] = safe_text(rec.get("kilitas_dropdown"))

    # Status
    st = safe_text(rec.get("status")).lower()
    if st == "kiadó":
        out["Statusz"] = "Kiadó"
    elif st == "eladó":
        out["Statusz"] = "Eladó"
    else:
        out["Statusz"] = safe_text(rec.get("status"))

    # Internet (new)
    out["Internet"] = safe_text(rec.get("internetre_lista"))

    # Type
    tipus_raw = safe_text(rec.get("tipus_dropdown")).lower()
    if tipus_raw in ("ház", "ikerház", "sorház", "villa"):
        out["Tipus"] = "Ház"
    elif tipus_raw in (
        "társasházi lakás",
        "lakóparki lakás",
        "villalakás",
        "penthouse",
        "iroda",
        "tetőtéri lakás",
    ):
        out["Tipus"] = "Lakás"
    else:
        out["Tipus"] = ""

    # Terrace
    tb = safe_numeric(rec.get("TeraszokSzama"))
    ts = safe_text(rec.get(SUBFORM_EXTRACT_FIELD))
    out["Terasz"] = "Igen" if tb != "0" or ts else "Nem"

    # Descriptions
    out["Leírás"] = safe_text(rec.get("MegjegyzesPublikusMagyar"))
    out["MegjegyzesPublikusAngol"] = safe_text(rec.get("MegjegyzesPublikusAngol"))

    # ── RENT ──
    rent_raw, rent_curr = (
        safe_numeric(rec.get("BerletiDijBrutto")),
        safe_text(rec.get("BerletiDijPenznem")).upper(),
    )
    if rent_raw:
        try:
            rv = float(rent_raw)
            if rent_curr == "HUF":
                out["Gross Rent HUF"] = str(int(round(rv)))
                out["Gross Rent EUR"] = str(
                    int(round(rv * EXCHANGE_RATES["HUF_to_EUR"]))
                )
            elif rent_curr == "EUR":
                out["Gross Rent EUR"] = str(int(round(rv)))
                out["Gross Rent HUF"] = str(
                    int(round(rv * EXCHANGE_RATES["EUR_to_HUF"]))
                )
            else:
                out["Gross Rent EUR"] = out["Gross Rent HUF"] = ""
        except:
            out["Gross Rent EUR"] = out["Gross Rent HUF"] = ""
        out["Bérleti Díj"] = str(int(round(rv)))
        out["Bérleti Díj Pénznem"] = rent_curr
    else:
        out.update(
            {
                "Gross Rent EUR": "",
                "Gross Rent HUF": "",
                "Bérleti Díj": "",
                "Bérleti Díj Pénznem": "",
            }
        )

    # ── SALE ──
    sale_raw, sale_curr = (
        safe_numeric(rec.get("EladasiAr")),
        safe_text(rec.get("EladasiArPenznem")).upper(),
    )
    if sale_raw:
        try:
            sv = float(sale_raw)
            if sale_curr == "HUF":
                out["Sale Price HUF"] = str(int(round(sv)))
                out["Sale Price EUR"] = str(
                    int(round(sv * EXCHANGE_RATES["HUF_to_EUR"]))
                )
            elif sale_curr == "EUR":
                out["Sale Price EUR"] = str(int(round(sv)))
                out["Sale Price HUF"] = str(
                    int(round(sv * EXCHANGE_RATES["EUR_to_HUF"]))
                )
            else:
                out["Sale Price EUR"] = out["Sale Price HUF"] = ""
        except:
            out["Sale Price EUR"] = out["Sale Price HUF"] = ""
        out["Eladási Ár"] = str(int(round(sv)))
        out["Eladási Ár Pénznem"] = sale_curr
    else:
        out.update(
            {
                "Sale Price EUR": "",
                "Sale Price HUF": "",
                "Eladási Ár": "",
                "Eladási Ár Pénznem": "",
            }
        )

    # ── District / Address (supports “2A” style values) ──
    raw = safe_text(rec.get("Kerulet"))
    digits = "".join(ch for ch in raw if ch.isdigit())
    ki = int(digits) if digits else 0
    if ki > 0:
        rom = int_to_roman(ki)
        out["Kerulet"] = f"{rom}. kerület"
        out["KeruletCategory"] = "Buda" if ki in {1, 2, 3, 11, 12, 22} else "Pest"
    else:
        out["Kerulet"] = out["KeruletCategory"] = ""

    # ── Images & Geocode ──
    out["ingatlan_kepek"] = ",".join(image_urls)
    addr_parts = [out["Varos"], out["Kerulet"], out["varosresz_dropdown"]]
    addr = ", ".join(p for p in addr_parts if p)
    out["Cím"] = addr
    out["Longitude"], out["Latitude"] = geocode_address(addr)

    # ── Live‐filter ──
    if not skip_live_filter:
        if (
            safe_text(rec.get("internetre_lista")).lower() != "igen"
            or out["Statusz"] not in {"Kiadó", "Eladó"}
        ):
            return None

    return out

@retry(attempts=3, initial_delay=1.0)
def translate_record_to_english(hu: Dict[str, str]) -> Dict[str, str]:
    """
    Map HU→ENG columns, translate booleans/status/types, and
    replace any trailing " kerület"→" district" in the Cím.
    Includes the new "Internet" column.
    """
    mapping = {
        "id": "id",
        "Ingatlan ID": "Property ID",
        "Varos": "City",
        "varosresz_dropdown": "District",
        "Méret": "Size",
        "Hálók": "Bedrooms",
        "Furdok": "Bathrooms",
        "Butorozott": "Furnished",
        "Legkondi": "Air Conditioning",
        "Lift": "Elevator",
        "Garazs": "Garage",
        "Kilatas": "View",
        "Statusz": "Status",
        "Internet": "Internet",
        "Tipus": "Type",
        "Terasz": "Terrace",
        "MegjegyzesPublikusAngol": "Description",
        "Gross Rent EUR": "Gross Rent EUR",
        "Gross Rent HUF": "Gross Rent HUF",
        "Bérleti Díj Pénznem": "Rental Currency",
        "Sale Price EUR": "Sale Price EUR",
        "Sale Price HUF": "Sale Price HUF",
        "Eladási Ár Pénznem": "Sale Currency",
        "Kerulet": "District No.",
        "KeruletCategory": "Area Category",
        "Cím": "Cím",
        "Longitude": "Longitude",
        "Latitude": "Latitude",
    }

    status_map = {"Kiadó": "For Rent", "Eladó": "For Sale"}
    bool_map   = {"Igen": "Yes", "Nem": "No", "N/A": "N/A"}
    type_map   = {"Ház": "House", "Lakás": "Apartment"}
    furnishing_map = {
        "Igen": "Yes",
        "Nem": "No",
        "Részben": "Partially",
        "Bútorral/bútor nélkül": "Furnished/Unfurnished",
        "Beépített": "Built-in",
        "Bútor nélkül/részben": "Unfurnished/Partially",
        "Részben bútorozva": "Partially",
        "Bútorral": "Yes",
        "Bútor nélkül": "No",
        "Bútorral/Bútor nélkül": "Furnished/Unfurnished",
    }

    out: Dict[str, str] = {}
    for hu_key, eng_col in mapping.items():
        val = hu.get(hu_key, "").strip()
        if hu_key == "Tipus":
            out[eng_col] = type_map.get(val, "")
        elif hu_key == "Statusz":
            out[eng_col] = status_map.get(val, val)
        elif hu_key == "Butorozott":
            out[eng_col] = furnishing_map.get(val, val if val else "N/A")
        elif hu_key in ("Legkondi", "Lift", "Terasz", "Garazs"):
            out[eng_col] = bool_map.get(val, "N/A")
        else:
            out[eng_col] = val

    # address tweak
    if out.get("Cím"):
        out["Cím"] = out["Cím"].replace(" kerület", " district")

    return out

def upload_csv_no_cache(local_path: str, blob_name: str) -> None:
    """
    Uploads a CSV to GCS and forces no caching on the public URL by setting
    Cache-Control: private, max-age=0, no-transform
    """
    blob = bucket.blob(blob_name)
    # set cache-control before upload so the upload operation honors it
    blob.cache_control = "private, max-age=0, no-transform"
    blob.upload_from_filename(local_path, content_type="text/csv")
    # patch metadata so GCS frontend picks up the new cache-control header immediately
    blob.patch()
    log_struct("csv_uploaded_nocache", blob=blob_name, cache_control=blob.cache_control)


# ────────────────────────────────────────────────────────────
# Main
def main() -> None:
    start = time.time()

    # 1) Read watermark & refresh Zoho token
    wm = read_watermark()
    refresh_access_token(wm)

    # 2) Fetch FX rates before any processing
    global EXCHANGE_RATES
    EXCHANGE_RATES = get_exchange_rate()

    # 3) Ensure both Full-CSV and Delta-CSV blobs exist (headers only)
    ensure_csv_blob_exists(HU_BLOB,       CSV_COLUMNS_HU)
    ensure_csv_blob_exists(ENG_BLOB,      CSV_COLUMNS_ENG)
    ensure_csv_blob_exists(DELTA_HU_BLOB, CSV_COLUMNS_HU)
    ensure_csv_blob_exists(DELTA_ENG_BLOB,CSV_COLUMNS_ENG)

    # ───── FETCH ONCE: MODIFIED + DELETED ─────
    modified = get_all_records(MODULE_API_NAME)
    deleted  = get_deleted_record_ids(MODULE_API_NAME)

    # ───── DELTA PASS ─────
    delta_hu:  Dict[str, Dict[str, str]] = {}
    delta_eng: Dict[str, Dict[str, str]] = {}
    image_synced: Set[str] = set()

    # 3.a) build Hungarian delta rows
    for rec in modified:
        base = process_property(rec, [], skip_live_filter=True)
        if not base:
            continue
        pid         = base["id"]
        orig_status = base.get("Statusz", "")
        has_sale    = bool(base.get("Sale Price EUR"))

        delta_hu[pid] = base
        if orig_status == "Kiadó" and has_sale:
            dup = base.copy()
            dup["id"]            = f"{pid}-1"
            dup["Ingatlan ID"]   = f"{base['Ingatlan ID']}-1"
            dup["Statusz"]       = "Eladó"
            delta_hu[dup["id"]]  = dup

    # 3.b) tombstones for deletions
    for did in deleted:
        blank = {c: "" for c in CSV_COLUMNS_HU}
        blank["id"] = did
        delta_hu[did] = blank

    # 3.c) translate delta to English
    for eid, hu_row in delta_hu.items():
        delta_eng[eid] = translate_record_to_english(hu_row)

    # 3.d) delta image-sync for live properties
    for eid, hu_row in list(delta_hu.items()):
        base_id = eid[:-2] if eid.endswith("-1") else eid
        rec     = next((r for r in modified if r["id"] == base_id), None)
        if not rec:
            continue
        online = safe_text(rec.get("internetre_lista")).lower()
        status = hu_row.get("Statusz", "")
        if online == "igen" and status in {"Kiadó", "Eladó"}:
            raw_ids = extract_upload_ids(rec)
            patch_images_data(base_id, raw_ids)
            existing = list_blobs_for_property(rec["Name"])
            urls: List[str] = []
            for fid, seq in raw_ids:
                url = download_and_upload_image(fid, rec["Name"], seq, existing)
                if url:
                    urls.append(url)
            hu_row["ingatlan_kepek"] = ",".join(urls)
            image_synced.add(base_id)
        else:
            hu_row["ingatlan_kepek"] = ""

    # 3.e) write & publish Delta CSVs (no-cache)
    save_dict_to_csv(TMP_DELTA_HU,  delta_hu,  CSV_COLUMNS_HU)
    save_dict_to_csv(TMP_DELTA_ENG, delta_eng, CSV_COLUMNS_ENG)
    upload_csv_no_cache(TMP_DELTA_HU, DELTA_HU_BLOB)
    upload_csv_no_cache(TMP_DELTA_ENG, DELTA_ENG_BLOB)
    log_struct("delta_csv_uploaded", hu_count=len(delta_hu), eng_count=len(delta_eng))

    # ───── FULL PASS ─────
    hu_rows  = load_csv_to_dict(HU_BLOB,  CSV_COLUMNS_HU)
    eng_rows = load_csv_to_dict(ENG_BLOB, CSV_COLUMNS_ENG)

    # apply deletions to full
    for did in deleted:
        hu_rows.pop(did, None)
        eng_rows.pop(did, None)
        for dup in [eid for eid in list(hu_rows) if eid.startswith(f"{did}-")]:
            hu_rows.pop(dup, None)
            eng_rows.pop(dup, None)
    log_struct("deletions_applied", count=len(deleted))

    # full image-sync for any remaining properties
    image_urls_by_id: Dict[str, List[str]] = {}
    for rec in get_all_records(MODULE_API_NAME):
        pid = rec["id"]
        if pid in image_synced:
            continue
        raw_ids = extract_upload_ids(rec)
        patch_images_data(pid, raw_ids)
        existing = list_blobs_for_property(safe_text(rec.get("Name","")))
        urls: List[str] = []
        for fid, seq in raw_ids:
            url = download_and_upload_image(fid, safe_text(rec.get("Name","")), seq, existing)
            if url:
                urls.append(url)
        image_urls_by_id[pid] = urls
    log_struct("images_synced", properties=len(image_urls_by_id))

    # re-fetch & full transform
    recs = get_all_records(MODULE_API_NAME)
    added = updated = removed = skipped = 0
    for rec in recs:
        pid = rec["id"]
        base = process_property(rec, image_urls_by_id.get(pid, []))
        if not base:
            if pid in hu_rows:
                removed += 1
                hu_rows.pop(pid, None)
                eng_rows.pop(pid, None)
            else:
                skipped += 1
            continue

        has_rent = bool(base.get("Gross Rent EUR"))
        has_sale = bool(base.get("Sale Price EUR"))
        orig_status = base.get("Statusz","")

        # rent entry
        if has_rent:
            rent_ent = base.copy()
            rent_ent["Statusz"] = "Kiadó"
            rent_ent["id"]       = pid
            if pid not in hu_rows:
                added += 1
            elif rent_ent != hu_rows[pid]:
                updated += 1
            hu_rows[pid]  = rent_ent
            eng_rows[pid] = translate_record_to_english(rent_ent)

        # sale entry
        if has_sale:
            sale_id = f"{pid}-1" if orig_status == "Kiadó" else pid
            sale_ent = base.copy()
            sale_ent["Statusz"]     = "Eladó"
            sale_ent["id"]           = sale_id
            sale_ent["Ingatlan ID"] = f"{base['Ingatlan ID']}{'-1' if orig_status=='Kiadó' else ''}"
            if sale_id not in hu_rows:
                added += 1
            elif sale_ent != hu_rows.get(sale_id, {}):
                updated += 1
            hu_rows[sale_id]  = sale_ent
            eng_rows[sale_id] = translate_record_to_english(sale_ent)

        metrics["records_processed"] += 1

    log_struct("records_merged",
               total=len(recs), added=added, updated=updated,
               removed=removed, skipped=skipped)

    # write & publish Full CSVs (no-cache)
    save_dict_to_csv(TMP_HU,  hu_rows,  CSV_COLUMNS_HU)
    save_dict_to_csv(TMP_ENG, eng_rows, CSV_COLUMNS_ENG)
    upload_csv_no_cache(TMP_HU, HU_BLOB)
    upload_csv_no_cache(TMP_ENG, ENG_BLOB)
    log_struct("csv_uploaded", hu_count=len(hu_rows), eng_count=len(eng_rows))

    # update watermark & metrics
    write_watermark_now()
    log_struct("run_complete", duration_s=round(time.time() - start, 2))
    log_struct("metrics", **metrics)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Fatal error; aborting", extra={"run_id": run_id})
        sys.exit(1)