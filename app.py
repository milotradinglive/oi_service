# app.py — Milo OI Service (Flask, anti-429, snapshots con cache, lecturas minimizadas)
# ----------------------------------------------------------------------------
# Cambios clave anti-429:
# 1) Snapshots 5m/15m/1h/1d SIN LECTURAS por ciclo: cache en memoria por hoja de snapshot.
#    El cache se inicializa una sola vez por hoja y luego solo se escribe.
# 2) Rate limiter de lecturas (_safe_read) con bucket ~30 lecturas/min.
# 3) "ACCESOS" se procesa cada 30 minutos (tracked en hoja META) para evitar spam.
# 4) Mantiene la estructura Flask original: /healthz, /update (async), /run (sync), /apply_access.
# 5) Misma lógica de cálculo que venías usando y mismas fórmulas H..AA.
# 6) Nombres de snapshots: SNAP_5min__, SNAP__, SNAP_H1__, SNAP_dia__.
# ----------------------------------------------------------------------------

import os
import time
import json
import re
import fcntl
import traceback
import threading
from collections import defaultdict as _dd, deque
from datetime import datetime, timedelta, timezone

import pytz
import gspread
import requests
from flask import Flask, jsonify, request
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from gspread.exceptions import WorksheetNotFound, APIError as GAPIError

# ========= Flask app =========
app = Flask(__name__)
OI_SECRET = os.getenv("OI_SECRET", "").strip()
APP_VERSION = os.getenv("RENDER_GIT_COMMIT", "")[:7] or "dev"
# ========= Logging (simple) =========
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()
_LEVELS = {"ERROR": 40, "WARN": 30, "WARNING": 30, "INFO": 20, "DEBUG": 10}

def log(msg: str, level: str = "INFO"):
    if _LEVELS.get(level, 20) >= _LEVELS.get(LOG_LEVEL, 20):
        print(f"[{level}] {msg}", flush=True)

log(f"oi-updater v{APP_VERSION} | LOG_LEVEL={LOG_LEVEL}", "INFO")

@app.get("/")
def root():
    return jsonify({
        "status": "ok",
        "service": "oi-updater",
        "version": APP_VERSION,
        "time": datetime.utcnow().isoformat() + "Z",
    })

# ========= Config =========
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

MAIN_FILE_ID = os.getenv("MAIN_FILE_ID", "18sxVJ-8ChEt09DR9HxyteK4fWdbyxEJ12fkSDIbvtDA")
ACCESS_FILE_ID = os.getenv("ACCESS_FILE_ID", "1CY06Lw1QYZQEXuMO02EPe8vUOipizuhbWypffPETPyk")
ACCESS_SHEET_TITLE = os.getenv("ACCESS_SHEET_TITLE", "AUTORIZADOS")
TRADIER_TOKEN = os.getenv("TRADIER_TOKEN", "")

LOCK_FILE = "/tmp/oi-updater.lock"

# ========= Auth =========
def make_gspread_and_creds():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("Falta variable de entorno GOOGLE_CREDENTIALS_JSON")
    creds_info = json.loads(creds_json)
    legacy_creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, SCOPES)
    return gspread.authorize(legacy_creds), legacy_creds, creds_info

client, google_api_creds, _creds_info = make_gspread_and_creds()
drive = build("drive", "v3", credentials=google_api_creds)

# ========= Datos base =========
TICKERS = [
    "AAPL","AMD","AMZN","BA","BAC","DIA","GLD","GOOG","IBM","INTC",
    "IWM","JPM","META","MRNA","MSFT","NFLX","NVDA","NVTS","ORCL","UBER",
    "PLTR","QQQ","SLV","SNAP","SPY","TNA","TSLA","USO","WFC","WMT","XOM","V",
    "URA","CVX","PYPL",
]

# ========= Tradier =========
BASE_TRADIER = "https://api.tradier.com/v1"
TIMEOUT = 20
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept": "application/json"})

def get_json(url, params=None, max_retries=5):
    for intento in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 429:
                espera = 2 * intento
                log(f"Tradier 429. retry in {espera}s", "WARN")
                time.sleep(espera)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if intento == max_retries:
                raise
            espera = 1.5 * intento
            log(f"Tradier error: {e} | retry {intento}/{max_retries} in {espera:.1f}s", "WARN")
            time.sleep(espera)

# ========= Backoff + Rate limiter LECTURAS Sheets =========
def _retry(call, tries=6, base=0.9, max_sleep=60):
    import random
    for k in range(tries):
        try:
            return call()
        except GAPIError as e:
            s = str(e)
            if ("Internal error encountered" in s or "Quota exceeded" in s or "429" in s) and k < tries - 1:
                sleep_s = min(max_sleep, base * (2 ** k)) + random.uniform(0, 0.6)
                time.sleep(sleep_s); continue
            raise
        except Exception as e:
            s = str(e)
            if ("rate limit" in s.lower() or "quota" in s.lower()) and k < tries - 1:
                time.sleep(min(max_sleep, base * (2 ** k))); continue
            raise

class ReadRateLimiter:
    def __init__(self, max_reads_per_min=30):
        self.max = max_reads_per_min
        self.q = deque()
    def wait(self):
        now = time.time()
        while self.q and now - self.q[0] > 60:
            self.q.popleft()
        if len(self.q) >= self.max:
            sleep_s = 60 - (now - self.q[0]) + 0.05
            if sleep_s > 0:
                time.sleep(sleep_s)
        self.q.append(time.time())

READ_LIMITER = ReadRateLimiter(max_reads_per_min=30)

def _safe_read(call):
    READ_LIMITER.wait()
    return _retry(call)

def _update_values(ws, range_name, values, user_entered=True):
    opt = "USER_ENTERED" if user_entered else "RAW"
    _retry(lambda: ws.update(range_name=range_name, values=values, value_input_option=opt))
    time.sleep(0.3)
    
def _reset_cf_for_columns(ws, start_row_idx, end_row_idx, cols_0idx, ws_meta, sheet_title):
    """
    Milo — Borra reglas de Conditional Formatting SOLO en columnas específicas (0-index),
    y lo marca en META para ejecutarlo solo 1 vez por hoja.
    """
    key = f"cf_reset_pct_v1__{sheet_title}"
    if _meta_read(ws_meta, key, "") == "1":
        return

    sheet_id = ws.id
    # Traer TODAS las reglas actuales del sheet
    rules = _safe_read(lambda: ws.spreadsheet.fetch_sheet_metadata()).get("sheets", [])
    target = None
    for sh in rules:
        if sh.get("properties", {}).get("sheetId") == sheet_id:
            target = sh
            break

    cf_rules = (target or {}).get("conditionalFormats", []) or []
    if not cf_rules:
        _meta_write(ws_meta, key, "1")
        return

    # Construimos requests para borrar reglas que toquen esas columnas
    req = []
    # Se borran por índice (de atrás hacia adelante)
    for idx in range(len(cf_rules) - 1, -1, -1):
        rule = cf_rules[idx]
        ranges = rule.get("ranges", []) or []
        hit = False
        for r in ranges:
            sc = r.get("startColumnIndex", 0)
            ec = r.get("endColumnIndex", 0)
            sr = r.get("startRowIndex", 0)
            er = r.get("endRowIndex", 0)

            # Intersección de filas
            if er <= start_row_idx or sr >= end_row_idx:
                continue

            # Intersección con cualquiera de las columnas objetivo
            for c in cols_0idx:
                if sc <= c < ec:
                    hit = True
                    break
            if hit:
                break

        if hit:
            req.append({"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": idx}})

    if req:
        _retry(lambda: ws.spreadsheet.batch_update({"requests": req}))

    _meta_write(ws_meta, key, "1")

# ========= Utilidades =========
def S(v) -> str:
    if v is None: return ""
    try: return str(v).strip()
    except Exception: return ""

def fmt_millones(x):
    s = f"{x:,.1f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_entero_miles(x):
    s = f"{int(x):,}"
    return s.replace(",", ".")

# ========= Hora NY / cortes =========
NY_TZ = pytz.timezone("America/New_York")
def _now_ny(): return datetime.now(timezone.utc).astimezone(NY_TZ)

def _es_corte_5m(dt=None):
    ny = dt or _now_ny()
    return (ny.minute % 5) == 0

def _es_corte_15m(dt=None):
    ny = dt or _now_ny()
    return (ny.minute % 15) == 0

def _es_corte_1h(dt=None):
    ny = dt or _now_ny()
    return ny.minute == 0

def _es_corte_1hConVentana(dt=None, ventana_min=3):
    ny = dt or _now_ny()
    return ny.minute < ventana_min  # true de :00 a :02 NY

def _floor_hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)

def _should_run_h1_once(ws_meta, ny_now, scope_key: str):
    key = f"last_h1_hour_iso__{scope_key}"  # clave por hoja
    last_iso = _meta_read(ws_meta, key, "")
    current_hour = _floor_hour(ny_now).isoformat()
    if last_iso == current_hour:
        return False
    _meta_write(ws_meta, key, current_hour)
    return True

def _es_corte_1553(dt=None):
    ny = dt or _now_ny()
    return ny.hour == 15 and ny.minute == 53

# === Helpers diarios (08:00 / 15:50) ===
# ---- helper: after time NY ----
def _after_time(h, m, dt=None):
    ny = dt or _now_ny()
    return (ny.hour, ny.minute) >= (h, m)

def _es_snap_0800(dt=None, window_s=90):
    ny = dt or _now_ny()
    target = ny.replace(hour=8, minute=0, second=0, microsecond=0)
    return 0 <= (ny - target).total_seconds() < window_s

def _es_snap_1550(dt=None, window_s=90):
    ny = dt or _now_ny()
    target = ny.replace(hour=15, minute=50, second=0, microsecond=0)
    return 0 <= (ny - target).total_seconds() < window_s

def _is_any_cut(ny):
    return (
        _es_corte_5m(ny)
        or _es_corte_15m(ny)
        or _es_corte_1hConVentana(ny, 3)
        or _es_snap_0800(ny)
        or _es_snap_1550(ny)
    )

# Memo de snapshots diarios realizados hoy (por hoja)
DAILY_SNAP_DONE = {}  # {(sheet_title, 'YYYY-MM-DD'): True}

def _daily_snapshot_done_today(ws_snap):
    hoy = _now_ny().strftime("%Y-%m-%d")
    key = (ws_snap.title, hoy)
    if DAILY_SNAP_DONE.get(key) is True:
        return True
    try:
        tss = _safe_read(lambda: ws_snap.get_values("D2:D200"))
        done = any(row and str(row[0]).startswith(hoy) for row in tss)
        if done:
            DAILY_SNAP_DONE[key] = True
        return done
    except Exception:
        return False

# ========= Estado por hoja (colores / L) =========
def _ensure_estado_sheet(doc, nombre_estado: str):
    try:
        ws = _retry(lambda: doc.worksheet(nombre_estado))
    except WorksheetNotFound:
        ws = _retry(lambda: doc.add_worksheet(title=nombre_estado, rows=600, cols=6))
        _update_values(ws, "A1", [["Ticker","ColorOI","ColorVol","EstadoL","PrevH","PrevI"]])
        return ws
    headers = _safe_read(lambda: ws.get_values("A1:F1"))
    if not headers or len(headers[0]) < 6:
        _update_values(ws, "A1", [["Ticker","ColorOI","ColorVol","EstadoL","PrevH","PrevI"]])
    return ws

def _leer_estado(ws_estado):
    rows = _safe_read(lambda: ws_estado.get_all_values())
    d = {}
    for r in rows[1:]:
        if not r: continue
        t = (r[0] or "").strip().upper()
        if not t: continue
        c_oi = (r[1] if len(r)>1 else "").strip()
        c_v  = (r[2] if len(r)>2 else "").strip()
        e_l  = (r[3] if len(r)>3 else "").strip()
        try: prev_h = float(r[4]) if len(r)>4 and r[4] != "" else None
        except: prev_h = None
        try: prev_i = float(r[5]) if len(r)>5 and r[5] != "" else None
        except: prev_i = None
        d[t] = (c_oi, c_v, e_l, prev_h, prev_i)
    return d

def _escribir_estado(ws_estado, mapa):
    data = [["Ticker","ColorOI","ColorVol","EstadoL","PrevH","PrevI"]]
    for tk in sorted(mapa.keys()):
        c_oi, c_v, e_l, prev_h, prev_i = mapa[tk]
        data.append([tk, c_oi, c_v, e_l, "" if prev_h is None else prev_h, "" if prev_i is None else prev_i])
    _retry(lambda: ws_estado.batch_clear(["A2:F10000"]))
    if len(data) > 1:
        _update_values(ws_estado, f"A1:F{len(data)}", data)

# ========= Helpers hojas =========
def _ensure_sheet_generic(doc, title, rows=200, cols=20):
    try:
        return _retry(lambda: doc.worksheet(title))
    except WorksheetNotFound:
        return _retry(lambda: doc.add_worksheet(title=title, rows=rows, cols=cols))

def _ensure_snapshot_sheet(doc, nombre_snap: str):
    try:
        ws = _retry(lambda: doc.worksheet(nombre_snap))
    except WorksheetNotFound:
        ws = _retry(lambda: doc.add_worksheet(title=nombre_snap, rows=1000, cols=4))
        _update_values(ws, "A1", [["Ticker","N_prev","N_curr","ts"]], user_entered=False)
        return ws
    headers = _safe_read(lambda: ws.get_values("A1:D1"))
    if not headers or len(headers[0]) < 4:
        _update_values(ws, "A1", [["Ticker","N_prev","N_curr","ts"]], user_entered=False)
    return ws

# ========= Clasificador de filtro institucional (col. K) =========
def _clasificar_filtro_institucional(val_h: float, val_i: float) -> str:
    if (val_h > 0.5) and (val_i > 0.4): return "CALLS"
    if (val_h < 0)   and (val_i < 0):   return "PUTS"
    return ""

# ========= Expiraciones y OI/dinero =========
from datetime import datetime as _dt, timedelta as _td
def elegir_expiracion_viernes(expiraciones, posicion_fecha):
    hoy = _dt.now().date()
    dias_a_viernes = (4 - hoy.weekday()) % 7
    if dias_a_viernes == 0: dias_a_viernes = 7
    proximo_viernes = hoy + _td(days=dias_a_viernes)
    fechas_viernes = []
    for d in expiraciones or []:
        try:
            dt = _dt.strptime(d, "%Y-%m-%d").date()
            if dt.weekday() == 4 and dt >= proximo_viernes:
                fechas_viernes.append(dt)
        except: continue
    fechas_viernes.sort()
    if len(fechas_viernes) <= posicion_fecha: return None
    return fechas_viernes[posicion_fecha].strftime("%Y-%m-%d")

def obtener_dinero(ticker, posicion_fecha=0):
    try:
        q = get_json(f"{BASE_TRADIER}/markets/quotes", params={"symbols": ticker})
        qq = q.get("quotes", {}).get("quote", {})
        quote = qq[0] if isinstance(qq, list) else qq
        last = float(quote.get("last") or 0)
        precio = last if last > 0 else float(quote.get("close") or 0)

        expj = get_json(
            f"{BASE_TRADIER}/markets/options/expirations",
            params={"symbol": ticker, "includeAllRoots": "true", "strikes": "false"},
        )
        expiraciones = expj.get("expirations", {}).get("date", [])

        viernes_ref_str = elegir_expiracion_viernes(expiraciones, posicion_fecha)
        if not viernes_ref_str:
            return 0, 0, 0.0, 0.0, 0, 0, None

        viernes_ref = _dt.strptime(viernes_ref_str, "%Y-%m-%d").date()
        lunes_ref = viernes_ref - _td(days=4)

        fechas_semana = []
        for d in expiraciones:
            try:
                dt = _dt.strptime(d, "%Y-%m-%d").date()
                if lunes_ref <= dt <= viernes_ref:
                    fechas_semana.append(dt)
            except: continue
        fechas_semana.sort()
        fechas_a_sumar = [viernes_ref] if len(fechas_semana) == 1 else fechas_semana

        oi_call_total = oi_put_total = 0
        dinero_call_total = dinero_put_total = 0.0
        vol_call_total = vol_put_total = 0

        for fecha_vto in fechas_a_sumar:
            fecha_str = fecha_vto.strftime("%Y-%m-%d")
            sj = get_json(f"{BASE_TRADIER}/markets/options/strikes", params={"symbol": ticker, "expiration": fecha_str})
            raw_strikes = sj.get("strikes", {}).get("strike", []) or []
            strikes = [float(s) for s in raw_strikes]

            cj = get_json(
                f"{BASE_TRADIER}/markets/options/chains",
                params={"symbol": ticker, "expiration": fecha_str, "greeks": "false"},
            )
            opciones = cj.get("options", {}).get("option", []) or []
            if isinstance(opciones, dict): opciones = [opciones]

            if precio <= 0 and opciones:
                try:
                    under_px = float(opciones[0].get("underlying_price") or 0)
                    if under_px > 0: precio = under_px
                except: pass

            strikes_itm_call = sorted([s for s in strikes if s < precio], reverse=True)[:10]
            strikes_otm_call = sorted([s for s in strikes if s > precio])[:10]
            set_call = set(strikes_itm_call + strikes_otm_call)

            strikes_itm_put = sorted([s for s in strikes if s > precio])[:10]
            strikes_otm_put = sorted([s for s in strikes if s < precio], reverse=True)[:10]
            set_put = set(strikes_itm_put + strikes_otm_put)

            for op in opciones:
                try: strike = float(op.get("strike", 0))
                except: continue
                typ = op.get("option_type")
                oi = int(op.get("open_interest") or 0)
                vol = int(op.get("volume") or 0)
                bid = float(op.get("bid") or 0.0)
                ask = float(op.get("ask") or 0.0)
                last_opt = float(op.get("last") or 0.0)

                mid = 0.0
                if bid > 0 and ask > 0: mid = (bid + ask) / 2
                elif ask > 0:           mid = ask
                elif last_opt > 0:      mid = last_opt

                if typ == "call" and strike in set_call:
                    oi_call_total += oi
                    dinero_call_total += oi * mid * 100
                    vol_call_total += vol
                elif typ == "put" and strike in set_put:
                    oi_put_total += oi
                    dinero_put_total += oi * mid * 100
                    vol_put_total += vol

        return (
            oi_call_total, oi_put_total,
            round(dinero_call_total / 1_000_000, 1),
            round(dinero_put_total / 1_000_000, 1),
            vol_call_total, vol_put_total,
            viernes_ref_str
        )
    except Exception as e:
        print(f"❌ Error con {ticker}: {e}")
        return 0, 0, 0.0, 0.0, 0, 0, None

# ========= SNAPSHOT CACHE (memoria) =========
# Mapa: sheet_title -> { ticker: last_N_curr }
CACHE_SNAP = {}

def _cargar_cache_snapshot(ws_snap, cache_dict):
    rows = _safe_read(lambda: ws_snap.get_values("A2:C"))
    cache_dict.clear()
    for r in rows:
        if not r or not r[0]: continue
        tk = r[0].strip().upper()
        try:
            n_curr = float(str(r[2]).replace(",", ".")) if len(r) > 2 and str(r[2]).strip() != "" else None
        except Exception:
            n_curr = None
        if n_curr is not None:
            cache_dict[tk] = n_curr

def _get_cache_for(ws_snap):
    key = ws_snap.title
    if key not in CACHE_SNAP:
        CACHE_SNAP[key] = {}
        _cargar_cache_snapshot(ws_snap, CACHE_SNAP[key])
    return CACHE_SNAP[key]

# ========= META helpers (para throttling ACCESOS) =========
META_CACHE = {}  # {ws_title: {key: value}}

def _meta_get_map(ws_meta):
    k = ws_meta.title
    if k not in META_CACHE:
        vals = _safe_read(lambda: ws_meta.get_all_values())
        d = {}
        for row in vals[1:]:
            if len(row) >= 2 and row[0]:
                d[row[0]] = row[1]
        META_CACHE[k] = d
    return META_CACHE[k]

def _meta_read(ws_meta, key, default=""):
    return _meta_get_map(ws_meta).get(key, default)

def _meta_write(ws_meta, key, val):
    d = _meta_get_map(ws_meta)

    vals = _safe_read(lambda: ws_meta.get_all_values())
    if not vals:
        _update_values(ws_meta, "A1", [["key","value"]], user_entered=False)
        vals = [["key","value"]]

    found_row = None
    for i, row in enumerate(vals[1:], start=2):
        if len(row) >= 1 and row[0] == key:
            found_row = i
            break

    if found_row is None:
        next_row = len(vals) + 1
        _update_values(ws_meta, f"A{next_row}:B{next_row}", [[key, val]], user_entered=False)
    else:
        _update_values(ws_meta, f"A{found_row}:B{found_row}", [[key, val]], user_entered=False)

    d[key] = val  # ✅ actualiza cache

# ========= ACCESOS (Drive) — igual que antes, pero ejecutado cada 30 min =========
def _col_indexes(ws):
    headers = [S(h).lower() for h in _safe_read(lambda: ws.row_values(1))]
    def col(name):
        name = name.strip().lower()
        try: return headers.index(name) + 1
        except ValueError: raise RuntimeError(f"Encabezado '{name}' no encontrado en {ws.title}.")
    return {"email": col("email"), "duracion": col("duracion"), "rol": col("rol"),
            "creado_utc": col("creado_utc"), "expira_utc": col("expira_utc"),
            "estado": col("estado"), "perm_id": col("perm_id"), "nota": col("nota")}

def _parse_duration_drive(s: str) -> timedelta:
    s = S(s).lower()
    if not s: return timedelta(hours=24)
    total_h = 0.0
    for num, unit in re.findall(r"(\d+(?:\.\d+)?)([dh])", s):
        n = float(num); total_h += n * (24 if unit == "d" else 1)
    if total_h == 0:
        try: total_h = float(s)
        except: total_h = 24.0
    return timedelta(hours=total_h)

def _get_sa_email_from_env_info() -> str:
    try: return S(_creds_info.get("client_email", "")).lower()
    except Exception:
        try: return S(json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON", "")).get("client_email", "")).lower()
        except Exception: return ""

def _grant_with_optional_exp(email: str, role: str, exp_dt: datetime, send_mail=True, email_message=None):
    base = {"type": "user", "role": role, "emailAddress": email}
    if email_message: base["emailMessage"] = email_message
    try:
        body = {**base, "expirationTime": exp_dt.replace(microsecond=0).isoformat()}
        created = drive.permissions().create(fileId=MAIN_FILE_ID, body=body, fields="id",
                                             sendNotificationEmail=send_mail).execute()
        return created["id"], "OK"
    except HttpError as e:
        msg = str(e)
        if "cannotSetExpiration" in msg:
            created = drive.permissions().create(fileId=MAIN_FILE_ID, body=base, fields="id",
                                                 sendNotificationEmail=send_mail).execute()
            return created["id"], "NO_EXP"
        if "invalidSharingRequest" in msg:
            created = drive.permissions().create(fileId=MAIN_FILE_ID, body=base, fields="id",
                                                 sendNotificationEmail=True).execute()
            return created["id"], "NOTIFIED"
        raise

def _revoke_by_id(perm_id: str):
    drive.permissions().delete(fileId=MAIN_FILE_ID, permissionId=perm_id).execute()

def procesar_autorizados_throttled(doc_main, accesos_doc, main_file_url):
    ws_meta = _ensure_sheet_generic(doc_main, "META", rows=50, cols=2)
    last = _meta_read(ws_meta, "last_accesses_check_iso", "")
    now = datetime.utcnow()
    should = True
    if last:
        try:
            last_dt = datetime.fromisoformat(last.replace("Z","")).replace(tzinfo=timezone.utc)
            should = (now - last_dt) >= timedelta(minutes=30)
        except: should = True
    if not should:
        return {"activados": 0, "revocados": 0, "skipped": True}

    hoja_aut = accesos_doc.worksheet(ACCESS_SHEET_TITLE)
    cols = _col_indexes(hoja_aut)
    rows = hoja_aut.get_all_records(default_blank="")
    now_utc = datetime.now(timezone.utc)
    sa_email = _get_sa_email_from_env_info()
    send_mail = (os.getenv("SEND_SHARE_EMAIL", "true").strip().lower() != "false")

    perms = drive.permissions().list(fileId=MAIN_FILE_ID, fields="permissions(id,emailAddress,role,type)").execute().get("permissions", [])
    by_email = {(S(p.get("emailAddress")).lower()): p for p in perms if S(p.get("type")).lower() == "user"}

    activados = revocados = sincronizados = 0

    for idx, r in enumerate(rows, start=2):
        email = S(r.get("email")).lower()
        dur_txt = S(r.get("duracion"))
        rol_in = S(r.get("rol")).lower() or "reader"
        estado = S(r.get("estado")).upper()
        perm_id = S(r.get("perm_id"))
        expira = S(r.get("expira_utc"))
        nota = S(r.get("nota"))
        if not email: continue

        role = rol_in if rol_in in ("reader", "commenter", "writer") else "reader"

        if email == sa_email:
            hoja_aut.update_cell(idx, cols["nota"], "IGNORADO (service account)"); continue

        if estado == "REVOCADO":
            pid = perm_id or (by_email.get(email) or {}).get("id")
            try:
                if pid:
                    try: _revoke_by_id(pid)
                    except HttpError as e:
                        if getattr(e, "resp", None) and getattr(e.resp, "status", None) in (404, 400): pass
                        else: raise
                hoja_aut.update_cell(idx, cols["perm_id"], "")
                hoja_aut.update_cell(idx, cols["nota"], "Revocado (manual o ya no existía)")
                by_email.pop(email, None); revocados += 1
            except Exception as e:
                hoja_aut.update_cell(idx, cols["nota"], f"ERROR revoke: {e}")
            continue

        if estado in ("", "PENDIENTE"):
            try:
                dur_td = _parse_duration_drive(dur_txt)
                exp_dt = now_utc + dur_td
                pid, modo = _grant_with_optional_exp(email, role, exp_dt, send_mail=send_mail)
                hoja_aut.update_cell(idx, cols["creado_utc"], now_utc.replace(microsecond=0).isoformat() + "Z")
                hoja_aut.update_cell(idx, cols["expira_utc"], exp_dt.replace(microsecond=0).isoformat() + "Z")
                hoja_aut.update_cell(idx, cols["estado"], "ACTIVO")
                hoja_aut.update_cell(idx, cols["perm_id"], pid)
                hoja_aut.update_cell(idx, cols["nota"], f"Concedido ({modo})")
                by_email[email] = {"id": pid}; activados += 1
            except Exception as e:
                hoja_aut.update_cell(idx, cols["nota"], f"ERROR grant: {e}")
            continue

        if estado == "ACTIVO":
            if not perm_id and email in by_email:
                pid = by_email[email]["id"]
                hoja_aut.update_cell(idx, cols["perm_id"], pid)
                if not nota:
                    hoja_aut.update_cell(idx, cols["nota"], "Sincronizado (existía en Drive)")
                sincronizados += 1

            if expira:
                try:
                    iso = expira.rstrip("Z")
                    exp_dt = datetime.fromisoformat(iso)
                    if exp_dt.tzinfo is None: exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    hoja_aut.update_cell(idx, cols["nota"], f"ERROR_PARSE_EXP: {expira}"); continue

                if datetime.now(timezone.utc) >= exp_dt:
                    try:
                        pid = (by_email.get(email) or {}).get("id") or perm_id
                        if pid: _revoke_by_id(pid)
                        hoja_aut.update_cell(idx, cols["estado"], "REVOCADO")
                        hoja_aut.update_cell(idx, cols["perm_id"], "")
                        hoja_aut.update_cell(idx, cols["nota"], "Vencimiento automático")
                        by_email.pop(email, None); revocados += 1
                    except Exception as e:
                        hoja_aut.update_cell(idx, cols["nota"], f"ERROR_REVOKE: {e}")

    _meta_write(ws_meta, "last_accesses_check_iso", datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    print(f"✅ AUTORIZADOS (drive) → activados: {activados} | sincronizados: {sincronizados} | revocados: {revocados}")
    return {"activados": activados, "revocados": revocados, "skipped": False}

# ========= Actualización de una hoja objetivo (anti-429) =========
def _apply_cf_inflow_thresholds(ws, sheet_title, ws_meta):
    """
    Milo — Señales por 'entrada de dinero' (Δ en millones) en columnas:
    N(5m), R(15m), V(1h), Z(día).
    Verde si >= umbral, Rojo si <= -umbral.
    """
    key = f"cf_inflow_v3__{sheet_title}"
    if _meta_read(ws_meta, key, "") == "1":
        return

    sheet_id = ws.id
    verde = {"red": 0.35, "green": 0.85, "blue": 0.35}   # VERDE fuerte (dinero entrando)
    rojo  = {"red": 0.90, "green": 0.35, "blue": 0.35}   # ROJO fuerte (dinero saliendo)

    start_row = 2
    end_row = 2000

    cfg = [
        (13, 5),   # N 5m
        (17, 10),  # R 15m
        (21, 15),  # V 1h
        (25, 20),  # Z día
    ]

    req = []
    for col0, thr in cfg:
        rng = {
            "sheetId": sheet_id,
            "startRowIndex": start_row,
            "endRowIndex": end_row,
            "startColumnIndex": col0,
            "endColumnIndex": col0 + 1
        }

        req.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [rng],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER_THAN_EQ", "values": [{"userEnteredValue": str(thr)}]},
                        "format": {"backgroundColor": verde}
                    }
                },
                "index": 0
            }
        })

        req.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [rng],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_LESS_THAN_EQ", "values": [{"userEnteredValue": str(-thr)}]},
                        "format": {"backgroundColor": rojo}
                    }
                },
                "index": 0
            }
        })

    _retry(lambda: ws.spreadsheet.batch_update({"requests": req}))
    _meta_write(ws_meta, key, "1")
    
def actualizar_hoja(doc, sheet_title, posicion_fecha, now_ny_base=None):
    try:
        ws = _retry(lambda: doc.worksheet(sheet_title))
    except WorksheetNotFound:
        ws = _retry(lambda: doc.add_worksheet(title=sheet_title, rows=2000, cols=27))

    ws_meta = _ensure_sheet_generic(doc, "META", rows=50, cols=2)

    # 1) Limpiar SOLO CF viejo de % (O,S,W,AA) — 1 sola vez por hoja
    _reset_cf_for_columns(
        ws,
        start_row_idx=2,
        end_row_idx=2000,
        cols_0idx=[14, 18, 22, 26],  # O, S, W, AA (0-index)
        ws_meta=ws_meta,
        sheet_title=sheet_title
    )

    # 2) Aplicar CF nuevo por entrada de dinero (Δ) en N/R/V/Z — 1 sola vez por hoja
    _apply_cf_inflow_thresholds(ws, sheet_title, ws_meta)

    ny = now_ny_base or _now_ny()
    fecha_txt = f"{ny:%Y-%m-%d}"
    hora_txt = ny.strftime("%H:%M:%S")
    log(f"Actualizando: {sheet_title} (vto #{posicion_fecha+1}) NY {fecha_txt} {hora_txt}", "INFO")

    # Estado previo
    nombre_estado = f"ESTADO__{sheet_title}"
    ws_estado = _ensure_estado_sheet(doc, nombre_estado)
    estado_prev = _leer_estado(ws_estado)
    estado_nuevo = {}
    cambios_por_ticker = {}

    ws_snap5m = _ensure_snapshot_sheet(doc, f"SNAP_5min__{sheet_title}")
    ws_snap15 = _ensure_snapshot_sheet(doc, f"SNAP__{sheet_title}")
    ws_snap1h = _ensure_snapshot_sheet(doc, f"SNAP_H1__{sheet_title}")
    ws_snap_d0800 = _ensure_snapshot_sheet(doc, f"SNAP_d0800__{sheet_title}")  # 1d @ 08:00 NY
    ws_snap_d1550 = _ensure_snapshot_sheet(doc, f"SNAP_d1550__{sheet_title}")  # 1d @ 15:50 NY

    cache_5m  = _get_cache_for(ws_snap5m)
    cache_15m = _get_cache_for(ws_snap15)
    cache_h1  = _get_cache_for(ws_snap1h)
    cache_d0800 = _get_cache_for(ws_snap_d0800)
    cache_d1550 = _get_cache_for(ws_snap_d1550)

    actualiza_d0800 = _es_snap_0800(ny)
    actualiza_d1550 = _es_snap_1550(ny)

    need_seed_0800 = _after_time(8, 0, ny)   and not _daily_snapshot_done_today(ws_snap_d0800)
    need_seed_1550 = _after_time(15, 50, ny) and not _daily_snapshot_done_today(ws_snap_d1550)

# ===== Recolección base (snapshots diarios se manejan abajo) =====
   
    # Recolecta datos
    datos = []
    for tk in TICKERS:
        oi_c, oi_p, m_c, m_p, v_c, v_p, exp = obtener_dinero(tk, posicion_fecha)
        datos.append([tk, "CALL", m_c, v_c, exp, oi_c])
        datos.append([tk, "PUT",  m_p, v_p, exp, oi_p])
        time.sleep(0.15)

    # Agregado por ticker
    agg = _dd(lambda: {"CALL": [0.0, 0], "PUT": [0.0, 0], "EXP": None})
    for tk, side, m_usd, vol, exp, _oi in datos:
        if not agg[tk]["EXP"] and exp:
            agg[tk]["EXP"] = exp
        agg[tk][side][0] += m_usd
        agg[tk][side][1] += vol

    # ========= Métricas por ticker y estado =========
    stats = {}

    for tk in agg.keys():
        m_call, v_call = agg[tk]["CALL"]
        m_put,  v_put  = agg[tk]["PUT"]

        val_h_num = (m_call - m_put) / max(m_call, m_put) if max(m_call, m_put) > 0 else 0.0
        val_i_num = (v_call - v_put) / max(v_call, v_put) if max(v_call, v_put) > 0 else 0.0

        clasif = _clasificar_filtro_institucional(val_h_num, val_i_num)

        color_oi  = "🟢" if val_h_num > 0 else "🔴" if val_h_num < 0 else "⚪"
        color_vol = "🟢" if val_i_num > 0 else "🔴" if val_i_num < 0 else "⚪"

        prev_oi, prev_vol, prev_l, _ph, _pi = estado_prev.get(tk, ("", "", "", None, None))

        cambio_oi  = (prev_oi  != "") and (color_oi  != prev_oi)
        cambio_vol = (prev_vol != "") and (color_vol != prev_vol)

        es_alineado = clasif in ("CALLS", "PUTS")
        cambio_L = es_alineado and (clasif != prev_l)

        cambios_por_ticker[tk] = (cambio_oi, cambio_vol, cambio_L)
        estado_nuevo[tk] = (color_oi, color_vol, clasif, val_h_num, val_i_num)

        stats[tk] = {
            "m_call": m_call, "m_put": m_put,
            "v_call": v_call, "v_put": v_put,
            "val_h": val_h_num, "val_i": val_i_num,
            "clasif": clasif,
        }

    # Ordenar UNA sola vez
    filas_sorted = sorted(stats.keys(), key=lambda t: stats[t]["val_h"], reverse=True)

    # Encabezado — UNA sola vez
    from gspread.utils import rowcol_to_a1
    encabezado = [[
        "Fecha","Hora","Ticker",
        "Trade Cnt VERDE","Trade Cnt ROJO",
        "VOLUMEN ENTRA","VOLUMEN SALE",
        "TENDENCIA Trade Cnt.","VOLUMEN.",
        "N (5m SNAP)","O (5m SNAP)","5m","5m %",
        "N (15m SNAP)","O (15m SNAP)","15m","15m %",
        "N (1h SNAP)","O (1h SNAP)","1h","1h %",
        "N (1d 08:00)","N (1d 15:50)","día","día %"
    ]]
    end_a1 = rowcol_to_a1(2, len(encabezado[0]))
    _update_values(ws, f"A2:{end_a1}", encabezado)

    # Tabla con fórmulas (incluye Y con fallback a X y AA con vacío si Y es blanco/0)
    s5      = f"SNAP_5min__{sheet_title}"
    s15     = f"SNAP__{sheet_title}"
    s1h     = f"SNAP_H1__{sheet_title}"
    sd0800  = f"SNAP_d0800__{sheet_title}"
    sd1550  = f"SNAP_d1550__{sheet_title}"

    tabla = []
    for i, tk in enumerate(filas_sorted, start=3):
        m_call = stats[tk]["m_call"]; m_put = stats[tk]["m_put"]
        v_call = stats[tk]["v_call"]; v_put = stats[tk]["v_put"]

        H  = f"=SI.ERROR((D{i}-E{i})/MAX(D{i};E{i});0)"
        I  = f"=SI.ERROR((F{i}-G{i})/MAX(F{i};G{i});0)"

        L  = f"=SI.ERROR(BUSCARV($C{i};'{s5}'!$A:$C;3;FALSO);)"
        M  = f"=SI.ERROR(BUSCARV($C{i};'{s5}'!$A:$B;2;FALSO);)"
        N  = f"=SI.ERROR(L{i}-M{i};0)"
        O  = f"=SI.ERROR(N{i}/MAX(ABS(M{i});0,000001);0)"

        P  = f"=SI.ERROR(BUSCARV($C{i};'{s15}'!$A:$C;3;FALSO);)"
        Q  = f"=SI.ERROR(BUSCARV($C{i};'{s15}'!$A:$B;2;FALSO);)"
        R  = f"=SI.ERROR(P{i}-Q{i};0)"
        S  = f"=SI.ERROR(R{i}/MAX(ABS(Q{i});0,000001);0)"

        T  = f"=SI.ERROR(BUSCARV($C{i};'{s1h}'!$A:$C;3;FALSO);)"
        U  = f"=LET(_u;SI.ERROR(BUSCARV($C{i};'{s1h}'!$A:$B;2;FALSO););SI(ESBLANCO(_u); T{i}; _u))"
        V  = f"=SI.ERROR(T{i}-U{i};0)"
        W  = f"=SI.ERROR(V{i}/MAX(ABS(U{i});0,000001);0)"

        # ======== 1D con fallback en Y y AA en blanco si Y vacío/0 ========
        X  = f"=SI.ERROR(BUSCARV($C{i};'{sd0800}'!$A:$C;3;FALSO);)"   # N_curr @ 08:00
        Y  = f"=SI.ERROR(BUSCARV($C{i};'{sd1550}'!$A:$C;3;FALSO);)"   # N_curr @ 15:50
        Z  = f"=SI.ERROR(Y{i}-X{i};0)"                                # Δ = Y − X
        AA = f"=SI( O(ESBLANCO(X{i}); ABS(X{i})=0 ); \"\"; SI.ERROR(Z{i}/ABS(X{i});0) )"  # % = Δ/|X|

        tabla.append([
            agg[tk]["EXP"] or fecha_txt, hora_txt, tk,
            fmt_millones(m_call), fmt_millones(m_put),
            fmt_entero_miles(v_call), fmt_entero_miles(v_put),
            H, I,
            L, M, N, O,
            P, Q, R, S,
            T, U, V, W,
            X, Y, Z, AA
        ])
    hay_corte = (
        _es_corte_5m(ny)
        or _es_corte_15m(ny)
        or (_es_corte_1hConVentana(ny, 3) and _should_run_h1_once(ws_meta, ny, sheet_title))
        or actualiza_d0800
        or actualiza_d1550
        or need_seed_0800
        or need_seed_1550
    )

    if hay_corte and tabla:
        _retry(lambda: ws.batch_clear(["A3:AA2000"]))
        _update_values(ws, f"A3:AA{len(tabla)+2}", tabla, user_entered=True)

        # Formatos %
        sheet_id = ws.id
        start_row = 2
        total_rows = len(tabla)
        req = []
        # H, I, J -> 0.0%
        # H, I -> 0.0%
        for col in (7, 8):
            req.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": start_row,
                                                 "endRowIndex": start_row + total_rows,
                                                 "startColumnIndex": col, "endColumnIndex": col+1},
                                       "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT","pattern": "0.0%"}}},
                                       "fields": "userEnteredFormat.numberFormat"}})
        # O, S, W, AA -> 0%
        for col in (14, 18, 22, 26):
            req.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": start_row,
                                                 "endRowIndex": start_row + total_rows,
                                                 "startColumnIndex": col, "endColumnIndex": col+1},
                                       "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT","pattern": "0%"}}},
                                       "fields": "userEnteredFormat.numberFormat"}})

        # Colores (sin “amarillo por cambio” si no lo quieres; mantengo tu lógica actual)
        verde    = {"red": 0.80, "green": 1.00, "blue": 0.80}
        rojo     = {"red": 1.00, "green": 0.80, "blue": 0.80}
        amarillo = {"red": 1.00, "green": 1.00, "blue": 0.60}
        blanco   = {"red": 1.00, "green": 1.00, "blue": 1.00}

        for idx, row in enumerate(tabla):
            tk = str(row[2]).strip().upper()
            ch_oi, ch_vol, ch_L = cambios_por_ticker.get(tk, (False, False, False))
            clasif = estado_nuevo[tk][2]

            bg_h = amarillo if ch_oi else blanco
            bg_i = amarillo if ch_vol else blanco
            req += [
                {"repeatCell": {"range": {"sheetId": sheet_id,
                                          "startRowIndex": start_row + idx, "endRowIndex": start_row + idx + 1,
                                          "startColumnIndex": 7, "endColumnIndex": 8},
                                "cell": {"userEnteredFormat": {"backgroundColor": bg_h}},
                                "fields": "userEnteredFormat.backgroundColor"}},
                {"repeatCell": {"range": {"sheetId": sheet_id,
                                          "startRowIndex": start_row + idx, "endRowIndex": start_row + idx + 1,
                                          "startColumnIndex": 8, "endColumnIndex": 9},
                                "cell": {"userEnteredFormat": {"backgroundColor": bg_i}},
                                "fields": "userEnteredFormat.backgroundColor"}}
            ]
        if req:
            _retry(lambda: ws.spreadsheet.batch_update({"requests": req}))

    # ====== SNAPSHOTS (solo escrituras, usando cache) ======
    n_map = {}
    for tk in filas_sorted:
        m_call = agg[tk]["CALL"][0]
        m_put  = agg[tk]["PUT"][0]
        n_map[tk] = round(m_call - m_put, 1)

    ts = ny.strftime("%Y-%m-%d %H:%M:%S")

    # 5m — solo en corte 5m (evita "update fantasma" en minutos raros como 9:31)
    if _es_corte_5m(ny):
        data_5m = [["Ticker","N_prev","N_curr","ts"]]
        for tk in sorted(n_map.keys()):
            n_prev = cache_5m.get(tk, "")
            n_curr = n_map[tk]
            data_5m.append([tk, n_prev, n_curr, ts])
            cache_5m[tk] = n_curr
        _retry(lambda: ws_snap5m.batch_clear(["A2:D10000"]))
        _update_values(ws_snap5m, f"A1:D{len(data_5m)}", data_5m, user_entered=False)

    # 15m — cortes :00/:15/:30/:45
    if _es_corte_15m(ny):
        data_15 = [["Ticker","N_prev","N_curr","ts"]]
        for tk in sorted(n_map.keys()):
            n_prev = cache_15m.get(tk, "")
            n_curr = n_map[tk]
            data_15.append([tk, n_prev, n_curr, ts])
            cache_15m[tk] = n_curr
        _retry(lambda: ws_snap15.batch_clear(["A2:D10000"]))
        _update_values(ws_snap15, f"A1:D{len(data_15)}", data_15, user_entered=False)

    # 1h — con ventana de gracia
    if _es_corte_1hConVentana(ny, ventana_min=3) and _should_run_h1_once(ws_meta, ny, sheet_title):
        data_h1 = [["Ticker","N_prev","N_curr","ts"]]
        for tk in sorted(n_map.keys()):
            n_curr = n_map[tk]
            n_prev = cache_h1.get(tk, n_curr)
            data_h1.append([tk, n_prev, n_curr, ts])
            cache_h1[tk] = n_curr
        _retry(lambda: ws_snap1h.batch_clear(["A2:D10000"]))
        _update_values(ws_snap1h, f"A1:D{len(data_h1)}", data_h1, user_entered=False)

    # ======= SNAP 1d @ 08:00 NY =======
    if actualiza_d0800 or need_seed_0800:
        ts_now_d0800 = ny.strftime("%Y-%m-%d %H:%M:%S")
        data_d0800 = [["Ticker","N_prev","N_curr","ts"]]
        for tk in sorted(n_map.keys()):
            n_curr = n_map[tk]
            n_prev = cache_d0800.get(tk, n_curr)   # si no hay previo, usa curr
            data_d0800.append([tk, n_prev, n_curr, ts_now_d0800])
            cache_d0800[tk] = n_curr
        _retry(lambda: ws_snap_d0800.batch_clear(["A2:D10000"]))
        _update_values(ws_snap_d0800, f"A1:D{len(data_d0800)}", data_d0800, user_entered=False)

    # ======= SNAP 1d @ 15:50 NY =======
    if actualiza_d1550 or need_seed_1550:
        ts_now_d1550 = ny.strftime("%Y-%m-%d %H:%M:%S")
        data_d1550 = [["Ticker","N_prev","N_curr","ts"]]
        for tk in sorted(n_map.keys()):
            n_curr = n_map[tk]
            n_prev = cache_d1550.get(tk, n_curr)   # si no hay previo, usa curr
            data_d1550.append([tk, n_prev, n_curr, ts_now_d1550])
            cache_d1550[tk] = n_curr
        _retry(lambda: ws_snap_d1550.batch_clear(["A2:D10000"]))
        _update_values(ws_snap_d1550, f"A1:D{len(data_d1550)}", data_d1550, user_entered=False)
    # Persistir estado
    _escribir_estado(ws_estado, estado_nuevo)

    # Retorno mapeo K
    return {tk: "" for tk in filas_sorted}

# ========= Runner de UNA corrida =========
def run_once(skip_oi: bool = False):
    doc_main = client.open_by_key(MAIN_FILE_ID)
    accesos = client.open_by_key(ACCESS_FILE_ID)
    main_url = f"https://docs.google.com/spreadsheets/d/{MAIN_FILE_ID}/edit"

    l_vto1 = l_vto2 = {}
    if not skip_oi:
        now_ny_base = _now_ny()

        # ✅ Gate: solo ejecuta OI si estamos en corte real
        if _is_any_cut(now_ny_base):
            l_vto1 = actualizar_hoja(doc_main, "Semana actual", posicion_fecha=0, now_ny_base=now_ny_base)
            l_vto2 = actualizar_hoja(doc_main, "Semana siguiente", posicion_fecha=1, now_ny_base=now_ny_base)
            try:
                print("SERVICIO_IO::K_SEMANA_ACTUAL=", json.dumps(l_vto1, ensure_ascii=False), flush=True)
                print("SERVICIO_IO::K_SEMANA_SIGUIENTE=", json.dumps(l_vto2, ensure_ascii=False), flush=True)
            except Exception:
                pass
        else:
            print(f"⏭️ No es corte (NY {now_ny_base:%H:%M:%S}) → se omite OI para evitar updates fuera de ventana.", flush=True)

    doc_main = client.open_by_key(MAIN_FILE_ID)
    acc = procesar_autorizados_throttled(doc_main, accesos, main_url)

    return {
        "ok": True,
        "main_title": doc_main.title,
        "access_title": accesos.title,
        "activados": acc.get("activados", 0),
        "revocados": acc.get("revocados", 0),
        "access_skipped": acc.get("skipped", False),
        "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "mode": os.getenv("ACCESS_MODE", "drive"),
        "skipped_oi": skip_oi,
        "K_semana_actual": l_vto1,
        "K_semana_siguiente": l_vto2,
    }

# ========= Flask async guards =========
def _acquire_lock():
    f = open(LOCK_FILE, "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            f.seek(0); f.truncate(0); f.write(str(os.getpid())); f.flush()
        except Exception: pass
        return f
    except BlockingIOError:
        f.close(); return None

def _authorized(req: request) -> bool:
    if not OI_SECRET: return True
    return req.headers.get("X-Auth-Token", "") == OI_SECRET

def _run_guarded():
    file_lock = _acquire_lock()
    if not file_lock:
        log("/update skipped: already running (lock)", "WARN")
        return
    try:
        log("[update] start", "INFO")
        run_once()
        log("[update] done", "INFO")
    except Exception as e:
        log(f"[update] error: {repr(e)}", "ERROR")
        traceback.print_exc()
    finally:
        fcntl.flock(file_lock, fcntl.LOCK_UN)
        file_lock.close()
        log(f"[update] thread end @ {datetime.utcnow().isoformat()}Z", "DEBUG")

@app.get("/healthz")
def healthz():
    return "ok", 200

@app.route("/update", methods=["GET", "POST"])
def update():
    if not _authorized(request):
        return jsonify({"error": "unauthorized"}), 401

    print(
        "🌐 /update caller:",
        request.headers.get("X-Forwarded-For", request.remote_addr),
        "| ua:", request.headers.get("User-Agent", ""),
        flush=True
    )

    t = threading.Thread(target=_run_guarded, daemon=True)
    t.start()
    return jsonify({"accepted": True, "started_at": datetime.utcnow().isoformat() + "Z"}), 202

@app.get("/run")
def http_run():
    try:
        skip = request.args.get("skip_oi", "").strip().lower() in ("1", "true", "yes")
        print(f"➡️ [/run] inicio (skip_oi={skip})", flush=True)
        file_lock = _acquire_lock()
        if not file_lock:
            msg = "ya hay una ejecución en curso"
            print(f"⏳ [/run] {msg}", flush=True)
            return jsonify({"ok": False, "running": True, "msg": msg}), 409
        try:
            result = run_once(skip_oi=skip)
            print(f"✅ [/run] ok: {result}", flush=True)
            return jsonify(result), 200
        finally:
            fcntl.flock(file_lock, fcntl.LOCK_UN); file_lock.close()
    except Exception as e:
        traceback.print_exc()
        print(f"❌ [/run] error: {e}", flush=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/apply_access")
def http_apply_access():
    if not _authorized(request):
        return jsonify({"error": "unauthorized"}), 401

    file_lock = _acquire_lock()
    if not file_lock:
        msg = "ya hay una ejecución en curso"
        print(f"⏳ [/apply_access] {msg}", flush=True)
        return jsonify({"ok": False, "running": True, "msg": msg}), 409

    try:
        print("➡️ [/apply_access] inicio", flush=True)
        doc_main = client.open_by_key(MAIN_FILE_ID)
        accesos = client.open_by_key(ACCESS_FILE_ID)
        main_url = f"https://docs.google.com/spreadsheets/d/{MAIN_FILE_ID}/edit"

        acc = procesar_autorizados_throttled(doc_main, accesos, main_url)
        return jsonify({"ok": True, **acc}), 200
    finally:
        fcntl.flock(file_lock, fcntl.LOCK_UN)
        file_lock.close()
        
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))




