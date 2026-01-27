# app_milo_anti429.py — Milo OI Service (anti-429, snapshots con cache, lecturas minimizadas)
# ----------------------------------------------------------------------------
# Cambios clave para evitar 429 (Read requests/min per user):
# 1) Snapshots 5m/15m/1h/1d SIN LECTURAS por ciclo: usamos un cache en memoria
#    por hoja de snapshot: N_prev = último N_curr del cache. Al iniciar, el cache
#    se carga UNA vez desde cada hoja de snapshot y no se vuelve a leer.
# 2) Rate limiter de lecturas (_safe_read) con bucket ~30 lecturas/min.
# 3) "ACCESOS" se procesa cada 30 minutos (no en cada ciclo de 5m).
# 4) Mantiene toda la lógica de cálculo y escritura de la tabla principal.
# ----------------------------------------------------------------------------

# === 1) Auth Google Sheets ===
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
import time
import traceback
import requests
import pytz  # <-- para manejar hora de Nueva York
import json
import statistics

# --- PegAR JUSTO DESPUÉS de: import statistics ---
NY_TZ = pytz.timezone("America/New_York")

# Modo de sesión para filtros intradía: 'rth' | 'extended' | 'auto'
SESSION_MODE = "auto"

def _is_rth_now_ny():
    """True si AHORA está dentro de RTH (09:30–16:00 NY, lun–vie)."""
    ny = datetime.now(NY_TZ)
    if ny.weekday() > 4:
        return False
    t = ny.time()
    return (t >= datetime.strptime("09:30", "%H:%M").time() and
            t <  datetime.strptime("16:00", "%H:%M").time())

def _drop_if_incomplete(points, interval_minutes):
    if not points: 
        return points
    # si la última barra “toca” el bucket en curso, descártala
    last_dt = points[-1]["_dt"]
    now = now_ny()
    bucket_start = now.replace(minute=(now.minute//interval_minutes)*interval_minutes, second=0, microsecond=0)
    if last_dt >= bucket_start:
        return points[:-1]
    return points

from collections import defaultdict, deque
def _sample_sd(vals, poblacional=False):
    vals = [float(x) for x in vals if x is not None]
    n = len(vals)
    if n == 0:
        return 0.0
    avg = sum(vals) / n
    if poblacional or n < 2:
        var = sum((x - avg) ** 2 for x in vals) / max(n, 1)
    else:
        var = sum((x - avg) ** 2 for x in vals) / (n - 1)
    sd = var ** 0.5
    return 0.0 if sd == 0.0 else sd

# alias simple para defaultdict con factory
_dd_rel = lambda f: defaultdict(f)

# z-score: comparar la ÚLTIMA vela CERRADA contra las 10 ANTERIORES

# Umbrales 1:1 con tu TKS (ajusta si quieres)
# Mínimo de velas CERRADAS por TF para calcular el token (puedes ajustar)
# Umbrales de RelVol (simétricos a tu TKS/Watchlist)
# z-score: comparar la ÚLTIMA vela CERRADA contra las 10 ANTERIORES (ventana=10)
REL_MIN_LEN = {
    "5m": 10,
    "15m": 10,
    "1h": 10,
    "1d": 10,
}
REL_LEN = max(REL_MIN_LEN.values())  # deques guardan (última + base) => maxlen = REL_LEN + 1

# Umbral único para señal en las 4 columnas AB..AE
REL_THRESH = {"5m": 0.0, "15m": 0.0, "1h": 0.0, "1d": 0.0}

def _min_len(tf_key: str) -> int:
    return int(REL_MIN_LEN.get(tf_key, 10))
# Pequeña pausa por ticker al cargar caches intradía
PAUSA_REL_TK = 0.03

def relvol_from_last10(vol_last):
    vols = [v for v in vol_last if v is not None and v > 0]
    n = len(vols)
    if n < 3:
        return 0.0  # no hay base suficiente todavía
    avg = sum(vols) / n
    if avg == 0:
        return 0.0
    # desviación estándar manual (más tolerante)
    sd = (sum((v - avg)**2 for v in vols) / n) ** 0.5
    if sd == 0:
        return 0.0
    # usa el último valor como muestra actual
    return (vols[-1] - avg) / sd

# Último OPEN/CLOSE CERRADO por TF (para dirección CALL/PUT)
REL_OC = {
    "5m": _dd_rel(dict),
    "15m": _dd_rel(dict),
    "1h": _dd_rel(dict),
    "1d": _dd_rel(dict),
}
# Volúmenes recientes por TF (guardamos 11: 1 evaluada + 10 base)
REL_VOL = {
    "5m":  _dd_rel(lambda: deque(maxlen=REL_LEN + 1)),
    "15m": _dd_rel(lambda: deque(maxlen=REL_LEN + 1)),
    "1h":  _dd_rel(lambda: deque(maxlen=REL_LEN + 1)),
    "1d":  _dd_rel(lambda: deque(maxlen=REL_LEN + 1)),
}

def _fmt_rel_num(x):
    # 2 decimales con coma (tu locale Sheets)
    s = f"{x:.2f}"
    return s.replace(".", ",")

def paint_token(rel, is_calls, threshold=None):
    # siempre mostrar el número; si tenemos dirección, anteponer ▲/▼
    num = _fmt_rel_num(rel)
    if is_calls is None:
        return num
    return f"{'▲' if is_calls else '▼'} {num}"

from gspread.exceptions import APIError as GAPIError, WorksheetNotFound
from gspread.utils import rowcol_to_a1
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Modo de sesión para RelVol (AB–AE)
# 'rth'      -> siempre horario regular
# 'extended' -> siempre 24h (incluye pre/after)
# 'auto'     -> RTH en vivo, extendido fuera (recomendado para tu 24/7)
# --- FIX: modo de sesión por defecto para filtros RTH/extended ---

def _attach_dt(points):
    out = []
    from datetime import datetime
    for p in points or []:
        ts = str(p.get("t") or p.get("time") or p.get("timestamp") or p.get("date") or "")
        ts = ts.replace("T"," ")
        dt = None
        try:
            dt = datetime.strptime(ts[:16], "%Y-%m-%d %H:%M")
        except Exception:
            try:
                dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
        # normaliza a zona NY
        if dt.tzinfo is None:
            dt = NY_TZ.localize(dt)
        else:
            dt = dt.astimezone(NY_TZ)
        q = dict(p)
        q["_dt"] = dt
        out.append(q)
    return out

def _filter_by_session(points):
    """
    Aplica el filtro según SESSION_MODE:
    - 'rth'      : filtra a 09:30–16:00 NY (lun–vie)
    - 'extended' : no filtra (incluye pre/after)
    - 'auto'     : RTH si mercado abierto, sino extendido
    """
    mode = SESSION_MODE
    if mode == "auto":
        mode = "rth" if _is_rth_now_ny() else "extended"

    pts = _attach_dt(points)
    if mode == "rth":
        only_rth = []
        for p in pts:
            dt = p["_dt"]
            if dt.weekday() > 4:
                continue
            tm = dt.time()
            if datetime.strptime("09:30","%H:%M").time() <= tm < datetime.strptime("16:00","%H:%M").time():
                only_rth.append(p)
        return only_rth
    return pts  # extended (intraday)

# === Utilidades de tiempo NY ===
def now_ny():
    return datetime.now(NY_TZ)

def es_ciclo_5m():
    # === Bucle: cada 5 minutos; "ACCESOS" cada 30 min; alarmas 08:00 y 15:50 NY ===
    ny = now_ny()
    return (ny.minute % 5) == 0

def es_ciclo_15m():
    ny = now_ny()
    return (ny.minute % 15) == 0

def es_ciclo_1h():
    ny = now_ny()
    return ny.minute == 0

def es_snap_0800(window_s=90):
    ny = now_ny()
    target = ny.replace(hour=8, minute=0, second=0, microsecond=0)
    return 0 <= (ny - target).total_seconds() < window_s

def es_snap_1550(window_s=90):
    ny = now_ny()
    target = ny.replace(hour=15, minute=50, second=0, microsecond=0)
    return 0 <= (ny - target).total_seconds() < window_s

def _daily_snapshot_done_today(ws_snap):
    """True si ya existe snapshot HOY. Lee como máximo 1 vez al día por hoja."""
    hoy = now_ny().strftime("%Y-%m-%d")
    key = (ws_snap.title, hoy)
    if DAILY_SNAP_DONE.get(key) is True:
        return True
    # Sólo si no está memorizado, hacemos UNA lectura y memo:
    try:
        tss = _safe_read(lambda: ws_snap.get_values("D2:D200"))
        done = any(row and str(row[0]).startswith(hoy) for row in tss)
        if done:
            DAILY_SNAP_DONE[key] = True
        return done
    except Exception:
        return False

def _after_time(h, m, grace_min=2):
    """True si estamos al menos 'grace_min' minutos después de h:m NY."""
    ny = now_ny()
    t  = ny.replace(hour=h, minute=m, second=0, microsecond=0)
    return ny >= (t + timedelta(minutes=grace_min))


# === Credenciales / cliente ===
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
archivo_credenciales = "credenciales_milo.json.json"
creds = ServiceAccountCredentials.from_json_keyfile_name(archivo_credenciales, scope)
client = gspread.authorize(creds)

drive = build('drive', 'v3', credentials=creds)

sheets = build('sheets', 'v4', credentials=creds)

def borrar_formatos_condicionales_hoja(doc, ws):
    """
    Elimina TODAS las reglas de formato condicional de una hoja específica.
    doc: objeto gspread.Spreadsheet (en tu caso 'documento')
    ws : objeto gspread.Worksheet (hoja_objetivo)
    """
    ssid     = doc.id
    sheet_id = ws.id

    # Trae solo sheetId + reglas condicionales
    meta = _retry(lambda: sheets.spreadsheets().get(
        spreadsheetId=ssid,
        # 👇 OJO: el nombre correcto es `conditionalFormats`
        fields="sheets(properties(sheetId),conditionalFormats)"
    ).execute())

    total_borrados = 0

    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("sheetId") != sheet_id:
            continue

        # 👇 También aquí: `conditionalFormats`
        rules = s.get("conditionalFormats", []) or []
        n = len(rules)
        if n == 0:
            break

        # Borra de atrás hacia adelante: index n-1 ... 0
        reqs = []
        for idx in reversed(range(n)):
            reqs.append({
                "deleteConditionalFormatRule": {
                    "sheetId": sheet_id,
                    "index": idx
                }
            })

        _retry(lambda: sheets.spreadsheets().batchUpdate(
            spreadsheetId=ssid,
            body={"requests": reqs}
        ).execute())

        total_borrados = n
        break

    print(f"🧹 Borrados {total_borrados} formatos condicionales en '{ws.title}'")
    return total_borrados

# === Backoff general para llamadas a Sheets ===
def _retry(call, tries=6, base=0.9, max_sleep=60):
    import random
    for k in range(tries):
        try:
            return call()
        except GAPIError as e:
            s = str(e)
            # 500 intermitente / 429 cuota / errores internos
            if ("Internal error encountered" in s or "Quota exceeded" in s or "429" in s) and k < tries - 1:
                sleep_s = min(max_sleep, base * (2 ** k)) + random.uniform(0, 0.6)
                time.sleep(sleep_s)
                continue
            raise
        except Exception as e:
            s = str(e)
            if ("rate limit" in s.lower() or "quota" in s.lower()) and k < tries - 1:
                time.sleep(min(max_sleep, base * (2 ** k)))
                continue
            raise

# === Rate Limiter para LECTURAS (evita [429] Read requests/min per user) ===
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

# === Helper estable para updates (WRITES) ===
def _update_values(ws, range_name, values, user_entered=True):
    opt = "USER_ENTERED" if user_entered else "RAW"
    _retry(lambda: ws.update(range_name=range_name, values=values, value_input_option=opt))
    time.sleep(0.5)  # pequeño respiro

# === Creación/aseguramiento de hojas auxiliares ===

def _ensure_snapshot_sheet(doc, nombre_snap):
    try:
        ws = _retry(lambda: doc.worksheet(nombre_snap))
    except WorksheetNotFound:
        ws = _retry(lambda: doc.add_worksheet(title=nombre_snap, rows=1000, cols=4))
        _update_values(ws, "A1", [["Ticker","N_prev","N_curr","ts"]], user_entered=False)
        return ws
    # Asegura encabezado A:D sin leer (evita 1 read por ciclo/hoja)
    if ws.title not in ENSURED_HEADERS:
        _update_values(ws, "A1", [["Ticker","N_prev","N_curr","ts"]], user_entered=False)
        ENSURED_HEADERS.add(ws.title)
    return ws


def _ensure_estado_sheet(doc, nombre_estado):
    """Crea/abre hoja de estado con columnas: Ticker, ColorOI, ColorVol, EstadoL."""
    try:
        ws = _retry(lambda: doc.worksheet(nombre_estado))
    except WorksheetNotFound:
        ws = _retry(lambda: doc.add_worksheet(title=nombre_estado, rows=600, cols=4))
        _update_values(ws, "A1", [["Ticker","ColorOI","ColorVol","EstadoL"]])
        return ws
    if ws.title not in ENSURED_HEADERS:
        _update_values(ws, "A1", [["Ticker","ColorOI","ColorVol","EstadoL"]])
        ENSURED_HEADERS.add(ws.title)
    return ws


def _leer_estado(ws_estado):
    """Devuelve dict {ticker: (color_oi, color_vol, estado_l)} usando cache para evitar lecturas."""
    key = ws_estado.title
    if key in ESTADO_CACHE:
        return ESTADO_CACHE[key]
    rows = _safe_read(lambda: ws_estado.get_values("A1:D"))
    d = {}
    for r in rows[1:]:
        if not r:
            continue
        t = (r[0] or "").strip().upper()
        if not t:
            continue
        c_oi = (r[1] if len(r) > 1 else "").strip()
        c_v  = (r[2] if len(r) > 2 else "").strip()
        e_l  = (r[3] if len(r) > 3 else "").strip()
        d[t] = (c_oi, c_v, e_l)
    ESTADO_CACHE[key] = d
    return d

def _escribir_estado(ws_estado, mapa):
    data = [["Ticker","ColorOI","ColorVol","EstadoL"]]
    for tk in sorted(mapa.keys()):
        c_oi, c_v, e_l = mapa[tk]
        data.append([tk, c_oi, c_v, e_l])
    _retry(lambda: ws_estado.batch_clear(["A2:D10000"]))
    if len(data) > 1:
        _update_values(ws_estado, f"A1:D{len(data)}", data)
    # Actualiza cache en memoria para no volver a leer la próxima vez
    ESTADO_CACHE[ws_estado.title] = dict(mapa)

# --- Abrir hoja principal (por ID) ---
MAIN_FILE_ID = "1MNoTuFYZp3cp7vk0O3Aw0DC8CnALXyTDMfV5mKxtgw0"  # <-- REEMPLAZA CON TU ID
MAIN_FILE_URL = f"https://docs.google.com/spreadsheets/d/{MAIN_FILE_ID}/edit"
with open(archivo_credenciales, "r", encoding="utf-8") as f:
    sa_email = json.load(f)["client_email"]
print("🔐 Service Account:", sa_email)

import gspread as _gsp
try:
    documento = client.open_by_key(MAIN_FILE_ID)
    print("✅ Principal abierto correctamente.")
except _gsp.exceptions.SpreadsheetNotFound:
    try:
        documento = client.open("Tabla institucional - milo.trading.live")
        print("✅ Principal abierto por nombre.")
    except _gsp.exceptions.SpreadsheetNotFound:
        raise SystemExit(
            "❌ No puedo abrir la hoja principal.\n"
            f"- Revisa que el ID sea correcto: {MAIN_FILE_ID}\n"
            f"- Asegúrate de compartir la hoja con: {sa_email} (Editor)."
        )

# === ACCESOS (archivo y hoja externos) ===
ACCESS_FILE_ID = "11yZiO6skOKwCzvRsVU56SpVOkiYclAY2x3MqQDtzTok"
ACCESS_SHEET_TITLE = "AUTORIZADOS"

try:
    accesos_doc = client.open_by_key(ACCESS_FILE_ID)
    print("✅ ACCESOS abierto correctamente.")
except gspread.exceptions.SpreadsheetNotFound:
    raise SystemExit(
        "❌ No puedo abrir ACCESOS (404).\n"
        f"- Verifica ID: {ACCESS_FILE_ID}\n"
        f"- Comparte ACCESOS con el service account (Editor)."
    )

try:
    hoja_aut = accesos_doc.worksheet(ACCESS_SHEET_TITLE)
except gspread.exceptions.WorksheetNotFound:
    hoja_aut = accesos_doc.add_worksheet(title=ACCESS_SHEET_TITLE, rows=500, cols=8)
    _update_values(hoja_aut, "A1", [["email","duracion","rol","creado_utc","expira_utc","estado","perm_id","nota"]])
print("✅ ACCESOS listo: llena ACCESOS → AUTORIZADOS y se gestiona solo.")

# === Utilidad: parsear duración tipo "24h"/"2d" ===
import re

def _parse_duration(s: str) -> timedelta:
    if not s:
        return timedelta(hours=24)
    s = s.strip().lower()
    total_h = 0.0
    for num, unit in re.findall(r'(\d+(?:\.\d+)?)([dh])', s):
        num = float(num); total_h += num * (24 if unit == 'd' else 1)
    if total_h == 0:
        try: total_h = float(s)
        except: total_h = 24.0
    return timedelta(hours=total_h)

FILE_ID_MAIN = documento.id

def compartir_temporal_email(email, duracion_texto="24h", rol="reader", send_mail=True, email_message=None):
    now = datetime.now(timezone.utc)
    exp_dt = now + _parse_duration(duracion_texto)
    base = {"type": "user", "role": rol, "emailAddress": email}
    if email_message:
        base["emailMessage"] = email_message

    try:
        body = {**base, "expirationTime": exp_dt.isoformat(timespec="seconds")}
        created = drive.permissions().create(
            fileId=FILE_ID_MAIN,
            body=body,
            fields="id",
            sendNotificationEmail=send_mail,
            supportsAllDrives=True
        ).execute()
        return created["id"], exp_dt, "OK"

    except HttpError as e:
        msg = str(e)
        if "do not have permission to share" in msg.lower():
            raise PermissionError("NO_SHARE_RIGHTS")
        if "cannotSetExpiration" in msg:
            created = drive.permissions().create(
                fileId=FILE_ID_MAIN,
                body=base,
                fields="id",
                sendNotificationEmail=send_mail,
                supportsAllDrives=True
            ).execute()
            return created["id"], exp_dt, "NO_EXP"
        if "invalidSharingRequest" in msg:
            created = drive.permissions().create(
                fileId=FILE_ID_MAIN,
                body=base,
                fields="id",
                sendNotificationEmail=True,
                supportsAllDrives=True
            ).execute()
            return created["id"], exp_dt, "NOTIFIED"
        raise


def procesar_autorizados():
    rows = _safe_read(lambda: hoja_aut.get_all_values())
    if len(rows) <= 1:
        print("ℹ️  AUTORIZADOS: sin filas.")
        return

    now = datetime.now(timezone.utc)
    with open(archivo_credenciales, "r", encoding="utf-8") as f:
        sa_email_local = json.load(f)["client_email"]

    activados = 0
    revocados = 0

    for i, raw in enumerate(rows[1:], start=2):
        row = (raw + [''] * 8)[:8]
        email, dur_txt, rol, creado, expira, estado, perm_id, nota = [(c or '').strip() for c in row]

        if not any([email, dur_txt, rol, creado, expira, estado, perm_id, nota]):
            continue

        if email.lower() == sa_email_local.lower():
            if nota != "IGNORADO (service account)":
                _retry(lambda: hoja_aut.update(range_name=f"H{i}", values=[["IGNORADO (service account)"]]))
                continue

        r = (rol or "reader").lower()
        if r not in ("reader","commenter"):
            r = "reader"

        if estado in ("", "PENDIENTE"):
            try:
                pid, exp_dt, modo = compartir_temporal_email(email, dur_txt or "24h", r, send_mail=True)
                now_iso = now.isoformat(timespec="seconds")
                _retry(lambda: hoja_aut.update(range_name=f"D{i}:G{i}", values=[[now_iso, exp_dt.isoformat(timespec="seconds"), "ACTIVO", pid]]))
                _retry(lambda: hoja_aut.update(range_name=f"H{i}", values=[[f"Invitación enviada ({modo}). Link: {MAIN_FILE_URL}"]]))
                activados += 1
                print(f"✅ ACCESO ACTIVO → {email} ({r}) hasta {exp_dt} UTC [{modo}]")
            except PermissionError:
                _retry(lambda: hoja_aut.update(range_name=f"H{i}", values=[["ERROR_PERMISO: el service account no puede compartir este archivo"]]))
                _retry(lambda: hoja_aut.update(range_name=f"F{i}", values=[["RECHAZADO"]]))
                print(f"🚫 Sin permisos para compartir → {email}. Marcado y no se reintenta.")
            except Exception as e:
                _retry(lambda: hoja_aut.update(range_name=f"H{i}", values=[[f"ERROR: {e}"]]))
            continue

        if estado == "ACTIVO" and expira:
            try:
                exp_dt = datetime.fromisoformat(expira.replace("Z", ""))
                if exp_dt.tzinfo is None:
                    exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            except Exception:
                _retry(lambda: hoja_aut.update(range_name=f"H{i}", values=[[f"ERROR_PARSE_EXP: {expira}"]]))
                continue

            if now >= exp_dt:
                try:
                    if perm_id:
                        drive.permissions().delete(fileId=FILE_ID_MAIN, permissionId=perm_id, supportsAllDrives=True).execute()
                    _retry(lambda: hoja_aut.update(range_name=f"F{i}", values=[["REVOCADO"]]))
                    _retry(lambda: hoja_aut.update(range_name=f"H{i}", values=[["Vencimiento automático"]]))
                    revocados += 1
                    print(f"🗑️  ACCESO REVOCADO → {email}")
                except Exception as e:
                    _retry(lambda: hoja_aut.update(range_name=f"H{i}", values=[[f"ERROR_REVOKE: {e}"]]))
    print(f"🔎 AUTORIZADOS: {activados} activados, {revocados} revocados.")

# Ejecuta una vez de inmediato (además de cada ciclo programado)
procesar_autorizados()

# =======================================================================
# Hojas objetivo
hoja_datos = documento.worksheet("DATOS")
hoja_resumen = documento.worksheet("Semana actual")
hoja_vto2 = documento.worksheet("Semana siguiente")

# === Tickers trabajados ===
tickers = [
    "AAPL","AMD","AMZN","BA","BAC","DIA","GLD","GOOG","IBM","INTC",
    "IWM","JPM","META","MRNA","MSFT","NFLX","NVDA","NVTS","ORCL","UBER",
    "PLTR","QQQ","SLV","SNAP","SPY","TNA","TSLA","USO","WFC","WMT","XOM",
    "PYPL","URA","CVX",
]

# === Tradier API ===
import os, pathlib
print("📂 Directorio actual:", os.getcwd())
print("🔍 Archivo tradier_token.txt existe?", pathlib.Path("tradier_token.txt").exists())
print("🧭 Ruta completa esperada:", pathlib.Path(__file__).with_name("tradier_token.txt"))


# 1) Intenta variable de entorno
TRADIER_TOKEN = os.getenv("TRADIER_TOKEN", "").strip()

# 2) Fallback: archivo plano "tradier_token.txt" junto al .py
if not TRADIER_TOKEN:
    TOKEN_PATH = pathlib.Path(__file__).with_name("tradier_token.txt")
    if TOKEN_PATH.exists():
        TRADIER_TOKEN = TOKEN_PATH.read_text(encoding="utf-8").strip()

# 3) Si aún no hay token, error claro
if not TRADIER_TOKEN:
    raise SystemExit("❌ Falta TRADIER_TOKEN. Define la variable de entorno o crea tradier_token.txt (una sola línea con el token).")

BASE = "https://api.tradier.com/v1"

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {TRADIER_TOKEN}",
    "Accept": "application/json",
    "Connection": "keep-alive"
})
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

TIMEOUT = (5, 25)
_retry_strategy = Retry(
    total=6, connect=3, read=3, status=6,
    status_forcelist=[429, 502, 503, 504],
    allowed_methods=frozenset(["GET"]),
    backoff_factor=0.8, raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry_strategy, pool_connections=100, pool_maxsize=100)
session.mount("https://", _adapter)
session.mount("http://", _adapter)

# === Tradier OHLCV → historial por intervalo (últimas ~12 velas cerradas) ===
# Intervals válidos: 5min, 15min, hour, daily

def _parse_timesales_points(j):
    """Normaliza respuesta de /markets/timesales a lista de puntos {t,o,c,v}.
    Para interval=5min o 15min, Tradier entrega buckets ya agregados.
    Campos esperados: time, open, close, volume (según doc).
    """
    series = (j or {}).get("series", {}).get("data", []) or []
    if not isinstance(series, list):
        series = [series]
    out = []
    for p in series:
        try:
            # claves típicas: 'time','open','close','volume'
            t = p.get("time") or p.get("timestamp") or p.get("date") or ""
            o = float(p.get("open", p.get("price", 0)) or 0)
            c = float(p.get("close", p.get("price", 0)) or 0)
            v = int(p.get("volume", p.get("size", 0)) or 0)
            out.append({"t": t, "o": o, "c": c, "v": v})
        except Exception:
            continue
    return out

# --- Helpers para filtrar horario regular y quitar la vela aún en formación ---
from datetime import datetime as _dt

def _aggregate_to_hour(points):
    """Agrega buckets 5m/15m a velas H1 (CERRADAS).
    Devuelve lista de barras {t,o,c,v} por cada hora completa.
    """
    if not points:
        return []
    # Asegura orden ascendente por tiempo
    def _key(p):
        return p["t"]
    pts = sorted(points, key=_key)

    buckets = []
    curr = []
    curr_hour = None
    from datetime import datetime
    for p in pts:
        # parse YYYY-MM-DD HH:MM (acepta con o sin segundos)
        ts = p["t"]
        ts = ts.replace("T", " ")  # <— normaliza ISO
        try:
            dt = datetime.strptime(ts[:16], "%Y-%m-%d %H:%M")
        except Exception:
            # intento con segundos
            try:
                dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
        hkey = dt.strftime("%Y-%m-%d %H:00")
        if curr_hour is None:
            curr_hour = hkey
        if hkey != curr_hour:
            if curr:
                o = curr[0]["o"]
                c = curr[-1]["c"]
                v = sum(x["v"] for x in curr)
                buckets.append({"t": curr_hour, "o": o, "c": c, "v": v})
            curr = [p]
            curr_hour = hkey
        else:
            curr.append(p)
    # Cierra el último bucket (solo si la hora está completa: al menos 60m/12×5m)
    if curr and len(curr) >= 12:  # 12×5m ≈ 1 hora; suficiente criterio cierre
        o = curr[0]["o"]; c = curr[-1]["c"]; v = sum(x["v"] for x in curr)
        buckets.append({"t": curr_hour, "o": o, "c": c, "v": v})
    return buckets

# Siempre trabajar con velas CERRADAS en producción
USE_RT_BAR = False  # NO cambiar: espejo en vivo solo para pruebas puntuales

# --- RelVol simétrico a WATCHLIST TOS (incluye la vela evaluada y SD poblacional) ---
def relvol_watchlist_token(vols_closed, length, threshold):
    """
    Usa SOLO velas CERRADAS.
    Calcula z-score sobre las últimas 'length' velas (incluye la última cerrada).
    SD poblacional (TOS). Devuelve el valor con signo y coma si |z| >= threshold;
    si no, '•'.
    """
    n = len(vols_closed)
    if n < length:
        return "•"

    window = vols_closed[-length:]      # incluye última cerrada
    v_last = window[-1]

    # media y sd poblacional (TOS)
    avg = sum(window) / float(length)
    sd  = _sample_sd(window, poblacional=True)
    if sd == 0:
        return "•"

    z = (v_last - avg) / sd
    if abs(z) < float(threshold):
        return "•"

    # muestra el número con signo y coma decimal
    return f"{z:+.2f}".replace(".", ",")

def _tradier_bars(symbol, interval="5min", limit=12):
    """
    Devuelve lista de barras normalizadas [{t,o,c,v}] ya CERRADAS.
    - daily  -> /markets/history
    - 5min   -> /markets/timesales interval=5min
    - 15min  -> /markets/timesales interval=15min
    - hour   -> /markets/timesales interval=5min (y se agrega a H1)
    """
    hoy_ny = now_ny()

    if interval == "daily":
        start = (hoy_ny.date() - timedelta(days=120)).isoformat()
        j = get_json(f"{BASE}/markets/history", params={
            "symbol": symbol,
            "interval": "daily",
            "start": start
        })
        days = (j or {}).get("history", {}).get("day", []) or []
        if not isinstance(days, list):
            days = [days]
        bars = []
        for b in days[-(limit+2):]:
            try:
                o = float(b.get("open", 0) or 0)
                c = float(b.get("close", 0) or 0)
                v = int(b.get("volume", 0) or 0)
                t = b.get("time") or b.get("date")
                bars.append({"t": t, "o": o, "c": c, "v": v})
            except Exception:
                continue
        return bars[-limit:]  # últimos 'limit' cerrados
    
    # Siempre trabajar con velas CERRADAS en producción
    # --- Intradía vía /markets/timesales ---
    start_dt = hoy_ny - timedelta(days=7)
    start = start_dt.strftime("%Y-%m-%d %H:%M")
    end   = hoy_ny.strftime("%Y-%m-%d %H:%M")

    if interval in ("5min", "15min"):
        j = get_json(f"{BASE}/markets/timesales", params={
            "symbol": symbol,
            "interval": interval,        # 5min | 15min
            "start": start,
            "end": end,
            "session_filter": "all"      # filtramos nosotros RTH
        })
        pts = _parse_timesales_points(j)
        pts = _filter_by_session(pts)  # auto: RTH en vivo, extendido fuera
        pts.sort(key=lambda x: x["_dt"])
        pts = _drop_if_incomplete(pts, 5 if interval == "5min" else 15)
        pts = pts[-limit:]  # últimos cerrados
        return [{"t": p["t"], "o": p["o"], "c": p["c"], "v": p["v"]} for p in pts]

    if interval == "hour":
        # Traemos 5m y agregamos a H1
        j = get_json(f"{BASE}/markets/timesales", params={
            "symbol": symbol,
            "interval": "5min",
            "start": start,
            "end": end,
            "session_filter": "all"
        })
        pts = _parse_timesales_points(j)
        pts = _filter_by_session(pts)  # auto: RTH en vivo, extendido fuera
        pts.sort(key=lambda x: x["_dt"])
        pts = _drop_if_incomplete(pts, 5)
        h1 = _aggregate_to_hour([{"t": p["t"], "o": p["o"], "c": p["c"], "v": p["v"]} for p in pts])
        return h1[-limit:]

    # fallback
    return []

def _update_rel_cache_for_tf(symbols, tf, interval):
    # Actualiza caches de VOL[10] y OC último CERRADO para cada ticker en el TF dado
    for tk in symbols:
        bars = _tradier_bars(tk, interval=interval, limit=REL_LEN+2)
        if not bars:
            time.sleep(PAUSA_REL_TK); continue
        # Últimos cerrados: tomamos desde el final (ya cerrados)
        # Armamos deque con el más reciente CERRADO primero
        vols = [b["v"] for b in bars][-(REL_LEN + 1):][::-1]
        REL_VOL[tf][tk].clear()
        for v in vols:
            REL_VOL[tf][tk].append(v)
        # Último OC CERRADO (el último bar de la lista es cerrado)
        last = bars[-1]
        REL_OC[tf][tk] = {"o": last["o"], "c": last["c"]}
        time.sleep(PAUSA_REL_TK)

def maybe_update_relvol_caches(symbols, do_5m, do_15m, do_1h, do_1d):
    if do_5m:
        _update_rel_cache_for_tf(symbols, "5m", "5min")
    if do_15m:
        _update_rel_cache_for_tf(symbols, "15m", "15min")
    if do_1h:
        _update_rel_cache_for_tf(symbols, "1h", "hour")
    if do_1d:
        _update_rel_cache_for_tf(symbols, "1d", "daily")
try:
    tk_dbg = "NVDA"  # o el que tengas en pantalla
    print(f"🧩 {tk_dbg}: deque 5m len={len(REL_VOL['5m'][tk_dbg])} (debe ser ≥ 10 para AB)")
except Exception as _e:
    pass

# --- Compose tokens with 10-bar minimum (v3 final sync TKS) ---
# === NUEVA FUNCIÓN: cálculo de z-score crudo + dirección CALL/PUT ===
def compose_rel_values_for_ticker(tk):
    """Devuelve z-scores crudos (float) y la dirección CALL/PUT de la última vela cerrada por TF."""
    out = {}
    dirs = {}
    for tf_key in ("5m","15m","1h","1d"):
        vols_closed = list(REL_VOL[tf_key][tk])[::-1]
        if len(vols_closed) >= 10:
            window = vols_closed[-10:]
            avg = sum(window)/10.0
            sd  = _sample_sd(window, poblacional=True)
            z   = 0.0 if sd==0 else (window[-1]-avg)/sd
        else:
            z = 0.0
        out[tf_key] = z            # z-score crudo con signo
        oc = REL_OC[tf_key].get(tk, {"o":None,"c":None})
        dirs[tf_key] = (None if oc["o"] is None else (oc["c"] > oc["o"]))  # True=CALL, False=PUT
    return out, dirs

# === DEBUG: Comparación RelVol Sheet vs Watchlist ===
def _z_muestral(vals):
    n = len(vals)
    if n < 2:
        return 0.0
    avg = sum(vals) / n
    var = sum((x - avg) ** 2 for x in vals) / (n - 1)
    sd = var ** 0.5
    return 0.0 if sd == 0 else (vals[-1] - avg) / sd

def debug_rel_ticker(tk="NVDA"):
    """
    Muestra los 10 volúmenes cerrados usados en cada temporalidad,
    el z-score muestral y el token final (idéntico al que ve la hoja).
    Sirve para comparar con Watchlist ThinkorSwim.
    """
    def _dump(tf_key, deque_obj, thr):
        vols_closed = list(deque_obj)[::-1]  # más vieja → más nueva
        print(f"\n[{tk}] TF={tf_key}  len={len(vols_closed)}  (usa últimas 10 cerradas)")
        if len(vols_closed) < 10:
            print("  ⚠️ Aún no hay 10 velas cerradas → hoja mostrará '•'")
            return
        w = vols_closed[-10:]
        z = _z_tos(w)  # ya con signo
        token = (f"{z:+.2f}".replace('.', ',') if abs(z) >= thr else '•')
        print("  Vols (últimas 10, cerradas):", w)
        print(f"  Z-poblacional={z:.4f}  | umbral={thr}  → token={token}")

    _dump("5m",  REL_VOL["5m"][tk],  REL_THRESH["5m"])
    _dump("15m", REL_VOL["15m"][tk], REL_THRESH["15m"])
    _dump("1h",  REL_VOL["1h"][tk],  REL_THRESH["1h"])
    _dump("1d",  REL_VOL["1d"][tk],  REL_THRESH["1d"])

def _z_tos(vals):  # TOS usa sd poblacional
    n = len(vals)
    if n < 2:
        return 0.0
    avg = sum(vals) / n
    sd = _sample_sd(vals, poblacional=True)
    return 0.0 if sd == 0 else (vals[-1] - avg) / sd

def _ensure_rel_debug_sheet(doc, ticker):
    title = f"REL_DEBUG__{ticker}"
    try:
        return doc.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = doc.add_worksheet(title=title, rows=1000, cols=40)  # antes 200
        header = ["TF","thr","vol[-9]","vol[-8]","vol[-7]","vol[-6]","vol[-5]",
                  "vol[-4]","vol[-3]","vol[-2]","vol[-1]","vol[0](última cerrada)",
                  "media","sd_muestral","z_raw","z_trunc(≥0)","TOKEN(AB..AE)"]
        _update_values(ws, "A1:Q1", [header])
        return ws

def _emit_rel_debug_row(ws, tf_key, thr, vols_closed):
    # Construye la fila
    row = [tf_key, thr]
    if len(vols_closed) >= 10:
        w = vols_closed[-10:]
        avg = sum(w)/10.0
        sd  = _sample_sd(w, poblacional=True)
        z   = 0.0 if sd == 0 else (w[-1] - avg) / sd
        zt = z  # dejamos el signo para inspección en la hoja de debug
        token = str(round(max(0.0, z), 2)).replace(".", ",")  # sólo como referencia visual
        row += w[:-1] + [w[-1]]
        row += [round(avg,2), round(sd,2), round(z,4), round(zt,4), token]
    else:
        row += [""]*10 + ["", "", "", "", "•"]

    # --- Calcular siguiente fila disponible y ampliar si es necesario ---
    rows_now = ws.row_count                      # filas actuales de la hoja
    used_colA = _safe_read(lambda: ws.get_values(f"A1:A{rows_now}"))
    next_idx = len(used_colA) + 1                # primera fila vacía

    if next_idx > rows_now:
        # añade un bloque generoso para evitar ampliaciones frecuentes
        add_n = max(100, next_idx - rows_now + 50)
        _retry(lambda: ws.add_rows(add_n))
        rows_now = ws.row_count  # refresca contador

    # Escribe la fila
    a1 = f"A{next_idx}:Q{next_idx}"
    _update_values(ws, a1, [row], user_entered=True)

def emitir_rel_debug_a_sheets(doc, ticker):
    """
    Vuelca una fila por TF con:
    TF, umbral, 10 vols cerradas, media, sd(n-1), z, z_trunc, token.
    """
    ws = _ensure_rel_debug_sheet(doc, ticker.upper())

    def _closed_list(tf_key):
        # nuestros deques guardan MÁS RECIENTE primero → invertir
        return list(REL_VOL[tf_key][ticker])[::-1]

    for tf_key, thr in [("5m", REL_THRESH["5m"]),
                        ("15m", REL_THRESH["15m"]),
                        ("1h", REL_THRESH["1h"]),
                        ("1d", REL_THRESH["1d"])]:
        vols_closed = _closed_list(tf_key)
        _emit_rel_debug_row(ws, tf_key, thr, vols_closed)

    print(f"📄 REL_DEBUG__{ticker} actualizado.")

# === Utilidades de formato (formato CO: coma decimal, puntos miles) ===
def fmt_millones(x):
    s = f"{x:,.1f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_entero_miles(x):
    s = f"{int(x):,}"
    return s.replace(",", ".")

def pct_str(p):
    return f"{p:.1f}%".replace(".", ",")

# === Lógica columna L (Filtro institucional) ===
def clasificar_estado_L(val_h, val_i):
    if (val_h > 0 and val_i > 0):  # sin 0.5 / 0.4
        return "CALLS"
    if (val_h < 0 and val_i < 0):
        return "PUTS"
    return ""

# —— Monitor simple de tormenta 5xx para “pausar” una corrida cuando el upstream está mal
_LAST_5XX_TS = [0.0]   # usar lista para mutabilidad en cierre
_5XX_COUNT   = [0]

def _mark_5xx():
    import time as _t
    _5XX_COUNT[0] += 1
    _LAST_5XX_TS[0] = _t.time()

def _should_cooldown(cooldown_s=120, threshold=8):
    import time as _t
    # Si hubo >= threshold errores 5xx en ~2 minutos, mejor saltarse la corrida
    return _5XX_COUNT[0] >= threshold and (_t.time() - _LAST_5XX_TS[0]) < cooldown_s

def get_json(url, params=None, max_retries=6):
    for intento in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)

            # Manejo explícito de estados inestables
            if r.status_code in (429, 502, 503, 504):
                espera = min(30, 0.8 * (2 ** (intento - 1)))
                tag = "429" if r.status_code == 429 else f"{r.status_code}"
                print(f"⏳ {tag} Tradier en {url}. Reintento {intento}/{max_retries} en {espera:.1f}s...")
                if r.status_code in (502, 503, 504):
                    _mark_5xx()
                time.sleep(espera)
                continue

            r.raise_for_status()

            # Algunas veces 200 con cuerpo vacío o HTML
            try:
                return r.json()
            except Exception:
                txt = (r.text or "").strip()
                if not txt:
                    return {}
                # Si vino HTML o algo raro, no rompas el ciclo
                return {}

        except requests.exceptions.RequestException as e:
            if intento < max_retries:
                espera = min(30, 0.8 * (2 ** (intento - 1)))
                print(f"⚠️ get_json error: {e}. Reintento {intento}/{max_retries} en {espera:.1f}s…")
                time.sleep(espera)
                continue
            raise

# === Elegir viernes válido (pos 0 = próximo, pos 1 = el siguiente) ===
def elegir_expiracion_viernes(expiraciones, posicion_fecha):
    hoy = datetime.now().date()
    dias_a_viernes = (4 - hoy.weekday()) % 7
    if dias_a_viernes == 0:
        dias_a_viernes = 7
    proximo_viernes = hoy + timedelta(days=dias_a_viernes)

    fechas_viernes = []
    for d in expiraciones or []:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
            if dt.weekday() == 4 and dt >= proximo_viernes:
                fechas_viernes.append(dt)
        except:
            continue
    fechas_viernes.sort()
    if len(fechas_viernes) <= posicion_fecha:
        return None
    return fechas_viernes[posicion_fecha].strftime("%Y-%m-%d")

# === DIAGNÓSTICO RÁPIDO TRADIER (pegar justo DESPUÉS de crear `session` y `get_json`) ===
def _test_tradier_basico(sym="AAPL"):
    try:
        q = get_json(f"{BASE}/markets/quotes", params={"symbols": sym}) or {}
        expj = get_json(f"{BASE}/markets/options/expirations", params={"symbol": sym, "includeAllRoots":"true","strikes":"false"}) or {}
        exps = expj.get("expirations", {}).get("date", [])
        print(f"⚙️  TEST {sym}: quotes={'OK' if q else 'VACÍO'} | expirations={len(exps)} fechas")

        if not exps:
            print("   → Sin expiraciones: probable falta de 'market data' de opciones o token de paper.")
            return

        # toma el primer viernes válido con nuestra misma lógica
        vref = elegir_expiracion_viernes(exps, 0)
        print(f"   Viernes elegido: {vref}")
        cj = get_json(f"{BASE}/markets/options/chains", params={"symbol": sym, "expiration": vref, "greeks":"false"}) or {}
        ops = cj.get("options", {}).get("option", [])
        if isinstance(ops, dict): ops = [ops]
        print(f"   Chains: {len(ops)} contratos")

        if not ops:
            print("   → Chains vacías: cuenta sin habilitar ‘OPCIONES’ en datos o token no tiene acceso a chains.")
    except Exception as e:
        print(f"   EXCEPCIÓN TEST: {e}")

# Llamar una vez al inicio:
for s in ["AAPL","NVDA","SPY"]:
    _test_tradier_basico(s)


# === Obtener dinero/volumen para un ticker ===
def obtener_dinero(ticker, posicion_fecha=0):
    try:
        q = get_json(f"{BASE}/markets/quotes", params={"symbols": ticker})
        qq = q.get("quotes", {}).get("quote", {})
        quote = qq[0] if isinstance(qq, list) else qq
        last = float(quote.get("last") or 0)
        precio = last if last > 0 else float(quote.get("close") or 0)

        expj = get_json(f"{BASE}/markets/options/expirations", params={
            "symbol": ticker, "includeAllRoots": "true", "strikes": "false"
        })
        expiraciones = expj.get("expirations", {}).get("date", [])

        viernes_ref_str = elegir_expiracion_viernes(expiraciones, posicion_fecha)
        if not viernes_ref_str:
            return 0, 0, 0.0, 0.0, 0, 0, None
        viernes_ref = datetime.strptime(viernes_ref_str, "%Y-%m-%d").date()
        lunes_ref = viernes_ref - timedelta(days=4)

        fechas_semana = []
        for d in expiraciones:
            try:
                dt = datetime.strptime(d, "%Y-%m-%d").date()
                if lunes_ref <= dt <= viernes_ref:
                    fechas_semana.append(dt)
            except:
                continue
        fechas_semana.sort()
        fechas_a_sumar = [viernes_ref] if len(fechas_semana) == 1 else fechas_semana

        oi_call_total = oi_put_total = 0
        dinero_call_total = dinero_put_total = 0.0
        vol_call_total = vol_put_total = 0

        for fecha_vto in fechas_a_sumar:
            fecha_str = fecha_vto.strftime("%Y-%m-%d")

            sj = get_json(f"{BASE}/markets/options/strikes", params={
                "symbol": ticker, "expiration": fecha_str
            })
            raw_strikes = sj.get("strikes", {}).get("strike", []) or []
            strikes = [float(s) for s in raw_strikes]

            cj = get_json(f"{BASE}/markets/options/chains", params={
                "symbol": ticker, "expiration": fecha_str, "greeks": "false"
            })
            opciones = cj.get("options", {}).get("option", []) or []
            if isinstance(opciones, dict):
                opciones = [opciones]

            if precio <= 0 and opciones:
                try:
                    under_px = float(opciones[0].get("underlying_price") or 0)
                    if under_px > 0:
                        precio = under_px
                except:
                    pass

            strikes_itm_call = sorted([s for s in strikes if s < precio], reverse=True)[:10]
            strikes_otm_call = sorted([s for s in strikes if s > precio])[:10]
            set_call = set(strikes_itm_call + strikes_otm_call)

            strikes_itm_put = sorted([s for s in strikes if s > precio])[:10]
            strikes_otm_put = sorted([s for s in strikes if s < precio], reverse=True)[:10]
            set_put = set(strikes_itm_put + strikes_otm_put)

            for op in opciones:
                try:
                    strike = float(op.get("strike", 0))
                except:
                    continue
                typ = op.get("option_type")
                oi = int(op.get("open_interest") or 0)
                vol = int(op.get("volume") or 0)
                bid = float(op.get("bid") or 0.0)
                ask = float(op.get("ask") or 0.0)
                last_opt = float(op.get("last") or 0.0)

                mid = 0.0
                if bid > 0 and ask > 0:
                    mid = (bid + ask) / 2
                elif ask > 0:
                    mid = ask
                elif last_opt > 0:
                    mid = last_opt

                if typ == "call" and strike in set_call:
                    oi_call_total += oi
                    dinero_call_total += oi * mid * 100
                    vol_call_total += vol
                elif typ == "put" and strike in set_put:
                    oi_put_total += oi
                    dinero_put_total += oi * mid * 100
                    vol_put_total += vol

        return (
            oi_call_total,
            oi_put_total,
            round(dinero_call_total / 1_000_000, 1),
            round(dinero_put_total / 1_000_000, 1),
            vol_call_total,
            vol_put_total,
            viernes_ref_str
        )

    except Exception as e:
        print(f"❌ Error con {ticker}: {e}")
        return 0, 0, 0.0, 0.0, 0, 0, None

# === CACHE de snapshots en memoria por hoja ===
# Mapa: titulo_worksheet -> { ticker: last_N_curr }
CACHE_SNAP = {}
# Cache de estado por hoja (evita leer cada ciclo)
ESTADO_CACHE = {}
ENSURED_HEADERS = set()
# Memo: por hoja y por fecha 'YYYY-MM-DD' si ya confirmamos snapshot hecho
DAILY_SNAP_DONE = {}  # {(sheet_title, '2025-10-26'): True}

def _cargar_cache_snapshot(ws_snap, cache_dict):
    """Lee 1 sola vez un snapshot para inicializar cache: Ticker -> N_curr."""
    rows = _safe_read(lambda: ws_snap.get_values("A2:C"))  # A:Ticker, B:N_prev, C:N_curr
    cache_dict.clear()
    for r in rows:
        if not r or not r[0]:
            continue
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

# === Actualizar hoja objetivo ===
def actualizar_hoja(hoja_objetivo, posicion_fecha):
    print(f"⏳ Actualizando: {hoja_objetivo.title} (venc. #{posicion_fecha+1})")
    actualiza_5m = True

    # Si hubo tormenta de 5xx recientemente, saltamos esta corrida para no spamear
    if _should_cooldown():
        print("🌩️ Tradier con 5xx recientes. Hago cooldown esta corrida (2 min) y omito esta vuelta…")
        time.sleep(120)
        return  # <-- salgo SOLO si hay cooldown

    actualiza_15m   = es_ciclo_15m()
    actualiza_1h    = es_ciclo_1h()
    actualiza_d0800 = es_snap_0800()   # 08:00 NY
    actualiza_d1550 = es_snap_1550()   # 15:50 NY
    # --- Anti-deriva de TF + actualización de caches RelVol (con daily correcto) ---
    def _ny_seconds():
        n = now_ny()
        return int(n.timestamp())

    # timestamps en memoria por TF (atributo de la función global para no reiniciar)
    if not hasattr(maybe_update_relvol_caches, "_last_ts"):
        maybe_update_relvol_caches._last_ts = {"15m": 0, "1h": 0, "1d": 0}

    now_s = _ny_seconds()

    # Si el reloj se atrasó (no cayó justo en :00/:15/:30/:45), fuerza actualización
    if now_s - maybe_update_relvol_caches._last_ts["15m"] > 18 * 60:
        actualiza_15m = True
    if now_s - maybe_update_relvol_caches._last_ts["1h"] > 70 * 60:
        actualiza_1h = True

    # ¿Toca daily por ventana fija o por seed pendiente?
    need_seed_0800 = _after_time(8, 0)   and not _daily_snapshot_done_today(
        _ensure_snapshot_sheet(documento, f"SNAP_d0800__{hoja_objetivo.title}")
    )
    need_seed_1550 = _after_time(15, 50) and not _daily_snapshot_done_today(
        _ensure_snapshot_sheet(documento, f"SNAP_d1550__{hoja_objetivo.title}")
    )
    do_daily = (actualiza_d0800 or actualiza_d1550 or need_seed_0800 or need_seed_1550)

    try:
        # 5m: siempre; 15m/1h: según reloj/anti-deriva; 1d: cuando corresponda
        maybe_update_relvol_caches(
            symbols=tickers,
            do_5m=True,
            do_15m=actualiza_15m,
            do_1h=actualiza_1h,
            do_1d=do_daily
        )
        if actualiza_15m:
            maybe_update_relvol_caches._last_ts["15m"] = now_s
        if actualiza_1h:
            maybe_update_relvol_caches._last_ts["1h"] = now_s
        if do_daily:
            maybe_update_relvol_caches._last_ts["1d"] = now_s
    except Exception as e:
        print(f"⚠️ RELVOL cache update error: {e}")

    try:
        tk_dbg = "NVDA"
        print(f"🧩 DEBUG REL: NVDA 5m={len(REL_VOL['5m'][tk_dbg])} | 15m={len(REL_VOL['15m'][tk_dbg])} | 1h={len(REL_VOL['1h'][tk_dbg])} | 1d={len(REL_VOL['1d'][tk_dbg])}")
        zvals, _dirs = compose_rel_values_for_ticker(tk_dbg)

        def _tok(z, thr):
            z = float(z)
            return (f"{z:+.2f}".replace('.', ',')) if abs(z) >= float(thr) else "•"

        print(
            "   TOKENS NVDA → "
            f"AB={_tok(zvals['5m'],  REL_THRESH['5m'])}  "
            f"AC={_tok(zvals['15m'], REL_THRESH['15m'])}  "
            f"AD={_tok(zvals['1h'],  REL_THRESH['1h'])}  "
            f"AE={_tok(zvals['1d'],  REL_THRESH['1d'])}"
        )
        # 🔁 Actualiza hoja REL_DEBUG__NVDA en cada corrida
        emitir_rel_debug_a_sheets(documento, tk_dbg)
    except Exception as _e:
        print(f"⚠️ Error al actualizar REL_DEBUG__NVDA: {_e}")
        pass

    datos = []
    hora = now_ny().strftime("%H:%M:%S")

    # Estado previo (persiste entre corridas)
    nombre_estado = f"ESTADO__{hoja_objetivo.title}"
    ws_estado = _ensure_estado_sheet(documento, nombre_estado)
    # --- FIX: cargar estado previo y preparar mapa nuevo ---
    ultimo = _leer_estado(ws_estado)  # dict {ticker: (color_oi, color_vol, estadoL)}
    nuevo_estado = {}                 # se llenará y se escribirá al final si cambia

    # Snapshots (asegurar hojas)
    nombre_snap_5m  = f"SNAP_5min__{hoja_objetivo.title}"  # 5 min
    ws_snap_5m      = _ensure_snapshot_sheet(documento, nombre_snap_5m)

    nombre_snap_15m = f"SNAP__{hoja_objetivo.title}"       # 15 min (legacy)
    ws_snap_15m     = _ensure_snapshot_sheet(documento, nombre_snap_15m)

    nombre_snap_1h  = f"SNAP_H1__{hoja_objetivo.title}"    # 1h
    ws_snap_1h      = _ensure_snapshot_sheet(documento, nombre_snap_1h)

    nombre_snap_d0800 = f"SNAP_d0800__{hoja_objetivo.title}"   # 1d @ 08:00 NY
    ws_snap_d0800     = _ensure_snapshot_sheet(documento, nombre_snap_d0800)

    nombre_snap_d1550 = f"SNAP_d1550__{hoja_objetivo.title}"   # 1d @ 15:50 NY
    ws_snap_d1550     = _ensure_snapshot_sheet(documento, nombre_snap_d1550)

    # Cargar caches si es primera vez en esta corrida (por hoja de snapshot)
    cache_5m  = _get_cache_for(ws_snap_5m)
    cache_15m = _get_cache_for(ws_snap_15m)
    cache_h1  = _get_cache_for(ws_snap_1h)
    cache_d0800 = _get_cache_for(ws_snap_d0800)
    cache_d1550 = _get_cache_for(ws_snap_d1550)

    # 1) Recolección
    PAUSA_TICKER = 0.10 if not _should_cooldown() else 0.50
    for ticker in tickers:
        oi_c, oi_p, m_call, m_put, v_c, v_p, exp = obtener_dinero(ticker, posicion_fecha)
        datos.append([ticker, "CALL", m_call, v_c, exp, oi_c])
        datos.append([ticker, "PUT",  m_put,  v_p, exp, oi_p])
        time.sleep(PAUSA_TICKER)

    # 2) Agregación por ticker
    agg = defaultdict(lambda: {"CALL": [0.0, 0], "PUT": [0.0, 0], "EXP": None})
    for tk, side, m_usd, vol, exp, _oi in datos:
        agg[tk]["EXP"] = agg[tk]["EXP"] or exp
        agg[tk][side][0] += m_usd
        agg[tk][side][1] += vol

    # 3) Cálculos y flags
    filas_con_flags = []
    for tk in sorted(agg.keys()):
        m_call, v_call = agg[tk]["CALL"]
        m_put,  v_put  = agg[tk]["PUT"]
        exp = agg[tk]["EXP"] or "-"

        total_m = m_call + m_put
        fuerza = 0.0 if total_m == 0 else (round(100*m_call/total_m,1) if m_call>=m_put else -round(100*m_put/total_m,1))
        total_vol = v_call + v_put
        fuerza_vol = 0.0 if total_vol == 0 else (round(100*v_call/total_vol,1) if v_call>=v_put else -round(100*v_put/total_vol,1))

        color_oi  = "🟢" if fuerza >= 20 else "🔴" if fuerza <= -20 else "⚪"
        color_vol = "🟢" if fuerza_vol >= 20 else "🔴" if fuerza_vol <= -20 else "⚪"

        val_h     = (m_call - m_put) / max(m_call, m_put) if max(m_call, m_put) > 0 else 0.0
        val_i_vol = (v_call - v_put) / max(v_call, v_put) if max(v_call, v_put) > 0 else 0.0

        estado_l_actual = clasificar_estado_L(val_h, val_i_vol)
        ult_oi, ult_vol, ult_l = ultimo.get(tk, ("", "", ""))
        es_alineado = estado_l_actual in ("CALLS","PUTS")
        cambio_L   = es_alineado and (estado_l_actual != ult_l)
        cambio_oi  = (ult_oi  != "") and (ult_oi  != color_oi)
        cambio_vol = (ult_vol != "") and (ult_vol != color_vol)

        filas_con_flags.append({
            "tk": tk, "exp": exp, "hora": hora,
            "m_call": m_call, "m_put": m_put,
            "v_call": v_call, "v_put": v_put,
            "color_oi": color_oi, "color_vol": color_vol,
            "fuerza": fuerza, "val_h": val_h, "val_i_vol": val_i_vol,
            "estado_l": estado_l_actual, "cambio_L": cambio_L,
            "cambio_oi": cambio_oi, "cambio_vol": cambio_vol
        })
        nuevo_estado[tk] = (color_oi, color_vol, estado_l_actual)

    # Orden por fuerza desc
    filas_con_flags.sort(key=lambda r: -float(r["fuerza"]))

    # 4) Encabezado (A..AE)
    encabezado = [[
        "Fecha","Hora","Ticker",
        "Trade Cnt VERDE","Trade Cnt ROJO",
        "VOLUMEN ENTRA","VOLUMEN SALE",
        "TENDENCIA trade Cnt","VOLUMEN",
        "Fuerza",                # J
        "Filtro institucional",  # K
        # ---- 5 min (L-O)
        "N (5m SNAP)",           # L
        "O (5m SNAP)",           # M
        "5m",                    # N
        "5m %",                  # O
        # ---- 15 min (P-S)
        "N (15m SNAP)",          # P
        "O (15m SNAP)",          # Q
        "15m",                   # R
        "15m %",                 # S
        # ---- 1 hora (T-W)
        "N (1h SNAP)",           # T
        "O (1h SNAP)",           # U
        "1h",                    # V
        "1h %",                  # W
        # ---- 1 día (X-AA)
        "N (1d 08:00)",          # X
        "N (1d 15:50)",          # Y
        "día",                   # Z
        "día %",                 # AA
        # ---- Relative Volume (última vela CERRADA) — visibles (texto con +/− y coma)
        "Rel 5m","Rel 15m","Rel 1h","Rel 1d",     # AB..AE
        "RUN_TS_NY",                               # AF
        # Z crudos (numéricos) para fórmulas
        "z5m","z15m","z1h","z1d",                 # AG..AJ
        # Dirección última vela cerrada por TF (TRUE=CALL, FALSE=PUT)
        "dir5m","dir15m","dir1h","dir1d",         # AK..AN
    ]]
    end_a1 = rowcol_to_a1(1, len(encabezado[0]))
    _update_values(hoja_objetivo, f"A1:{end_a1}", encabezado)

    # 5) Cuerpo completo A..AE en una sola actualización
    tabla = []
    for idx, r in enumerate(filas_con_flags, start=2):
        # === REL_VOL SYNC PATCH (TOS ↔ Sheets) ===
        zvals, dirmap = compose_rel_values_for_ticker(r["tk"])
        # === Direcciones última vela cerrada por TF ===
        d5  = dirmap.get("5m",  None)
        d15 = dirmap.get("15m", None)
        d1  = dirmap.get("1h",  None)
        dD  = dirmap.get("1d",  None)

        # Z-scores numéricos (última vela CERRADA) y dirección
        z5  = float(zvals["5m"]);   z15 = float(zvals["15m"]);   z1 = float(zvals["1h"]);   zD = float(zvals["1d"])

        # --- Helper: token solo numérico (±), filtra por |z| >= umbral ---
        def _tok(rel, thr):
            rel = float(rel)
            if abs(rel) < float(thr):
                return "•"
            # negativo con signo "−", positivo sin "+" y con coma decimal
            return f"{rel:.2f}".replace('.', ',')

        # --- Tokens listos para AB..AE (texto) usando solo el z con signo ---
        tAB = _tok(z5,  REL_THRESH["5m"])
        tAC = _tok(z15, REL_THRESH["15m"])
        tAD = _tok(z1,  REL_THRESH["1h"])
        tAE = _tok(zD,  REL_THRESH["1d"])


        # RUN_TS_NY (AF)
        run_ts = now_ny().strftime("%Y-%m-%d %H:%M:%S")

        # H..K
        formula_h = r["val_h"]
        formula_i = r["val_i_vol"]
        formula_j = r["fuerza"]/100.0
        formula_k = r["estado_l"]

        fila = [
            r['exp'], r['hora'], r['tk'],                          # A..C
            fmt_millones(r['m_call']),                             # D
            fmt_millones(r['m_put']),                              # E
            fmt_entero_miles(r['v_call']),                         # F
            fmt_entero_miles(r['v_put']),                          # G
            formula_h, formula_i, formula_j, formula_k,            # H..K
            # 5m (SNAP_5min__)
            f"=SI.ERROR(BUSCARV($C{idx};'{nombre_snap_5m}'!$A:$C;3;FALSO);)",   # L
            f"=SI.ERROR(BUSCARV($C{idx};'{nombre_snap_5m}'!$A:$B;2;FALSO);)",   # M
            f"=SI.ERROR(L{idx}-M{idx};0)",                                      # N
            f"=SI.ERROR(N{idx}/MAX(ABS(M{idx});0,000001);0)",                   # O
            # 15m (SNAP__)
            f"=SI.ERROR(BUSCARV($C{idx};'{nombre_snap_15m}'!$A:$C;3;FALSO);)",  # P
            f"=SI.ERROR(BUSCARV($C{idx};'{nombre_snap_15m}'!$A:$B;2;FALSO);)",  # Q
            f"=SI.ERROR(P{idx}-Q{idx};0)",                                      # R
            f"=SI.ERROR(R{idx}/MAX(ABS(Q{idx});0,000001);0)",                   # S
            # 1h (SNAP_H1__)
            f"=SI.ERROR(BUSCARV($C{idx};'{nombre_snap_1h}'!$A:$C;3;FALSO);)",   # T
            f"=LET(_u;SI.ERROR(BUSCARV($C{idx};'{nombre_snap_1h}'!$A:$B;2;FALSO););SI(ESBLANCO(_u); T{idx}; _u))",  # U
            f"=SI.ERROR(T{idx}-U{idx};0)",                                      # V
            f"=SI.ERROR(V{idx}/MAX(ABS(U{idx});0,000001);0)",                   # W
            # 1d (08:00 y 15:50)
            f"=SI.ERROR(BUSCARV($C{idx};'{nombre_snap_d0800}'!$A:$C;3;FALSO);)", # X
            f"=SI.ERROR(BUSCARV($C{idx};'{nombre_snap_d1550}'!$A:$C;3;FALSO);)", # Y
            f"=SI.ERROR(Y{idx}-X{idx};0)",                                       # Z
            f"=SI( O(ESBLANCO(X{idx}); ABS(X{idx})=0 ); \"\"; SI.ERROR(Z{idx}/ABS(X{idx});0) )",  # AA
            # RelVol visibles + RUN_TS_NY
            tAB, tAC, tAD, tAE,       # AB..AE (texto)
            run_ts,                   # AF
            # Z crudos (AG..AJ)
            z5, z15, z1, zD,

            # Direcciones (AK..AN)
            d5 if d5 is not None else "",
            d15 if d15 is not None else "",
            d1 if d1 is not None else "",
            dD if dD is not None else "",
        ]
        tabla.append(fila)

    # ⬇️ Escribe UNA sola vez todas las filas
    if tabla:
        _update_values(hoja_objetivo, f"A2:AN{len(tabla)+1}", tabla)
      
    # === Formatos (batch_update) ===
    sheet_id   = hoja_objetivo.id
    start_row  = 1                # fila 2 en Sheets (0-indexed -> 1)
    total_rows = len(filas_con_flags)
    
    # 🧹 Antes de aplicar NUEVOS formatos condicionales,
    # borro TODOS los existentes en esta hoja para que no se acumulen.
    try:
        borrar_formatos_condicionales_hoja(documento, hoja_objetivo)
    except Exception as e:
        print(f"⚠️ No se pudieron borrar formatos condicionales de '{hoja_objetivo.title}': {e}")

    

    fmt_reqs = [
        # Limpiar fondo H:J
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": start_row+total_rows,
                  "startColumnIndex": 7, "endColumnIndex": 10},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red":1,"green":1,"blue":1}}},
            "fields": "userEnteredFormat.backgroundColor"
        }},
        # % en H:I:J
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": start_row+total_rows,
                      "startColumnIndex": 7, "endColumnIndex": 10},
            "cell": {"userEnteredFormat": {"numberFormat": {"type":"PERCENT","pattern":"0.0%"}}},
            "fields": "userEnteredFormat.numberFormat"
        }},
        # % en O (Δ% 5m)
       {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": start_row+total_rows,
                      "startColumnIndex": 14, "endColumnIndex": 15},
            "cell": {"userEnteredFormat": {"numberFormat": {"type":"PERCENT","pattern":"0%"}}},
            "fields": "userEnteredFormat.numberFormat"
        }},
        # % en S (Δ% 15m)
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": start_row+total_rows,
                      "startColumnIndex": 18, "endColumnIndex": 19},
            "cell": {"userEnteredFormat": {"numberFormat": {"type":"PERCENT","pattern":"0%"}}},
            "fields": "userEnteredFormat.numberFormat"
        }},
        # % en W (Δ% 1h)
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": start_row+total_rows,
                      "startColumnIndex": 22, "endColumnIndex": 23},
            "cell": {"userEnteredFormat": {"numberFormat": {"type":"PERCENT","pattern":"0%"}}},
            "fields": "userEnteredFormat.numberFormat"
        }},
        # % en AA (Δ% 1d)
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": start_row+total_rows,
                      "startColumnIndex": 26, "endColumnIndex": 27},
            "cell": {"userEnteredFormat": {"numberFormat": {"type":"PERCENT","pattern":"0%"}}},
            "fields": "userEnteredFormat.numberFormat"
        }},
    ]

    verde    = {"red": 0.80, "green": 1.00, "blue": 0.80}
    rojo     = {"red": 1.00, "green": 0.80, "blue": 0.80}
    blanco   = {"red": 1.00, "green": 1.00, "blue": 1.00}

    for i, r in enumerate(filas_con_flags):
        row0 = start_row + i

        # H (tendencia trade cnt)
        bg_h = (verde if r["val_h"] > 0 else rojo if r["val_h"] < 0 else blanco)
        fmt_reqs.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id,
                          "startRowIndex": row0, "endRowIndex": row0+1,
                          "startColumnIndex": 7, "endColumnIndex": 8},  # H
                "cell": {"userEnteredFormat": {"backgroundColor": bg_h}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

        # I (volumen)
        bg_i = (verde if r["val_i_vol"] > 0 else rojo if r["val_i_vol"] < 0 else blanco)
        fmt_reqs.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id,
                          "startRowIndex": row0, "endRowIndex": row0+1,
                          "startColumnIndex": 8, "endColumnIndex": 9},  # I
                "cell": {"userEnteredFormat": {"backgroundColor": bg_i}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

        # K (Filtro institucional)
        if   r["estado_l"] == "CALLS": bg_k = verde
        elif r["estado_l"] == "PUTS":  bg_k = rojo
        else:                          bg_k = blanco
        fmt_reqs.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id,
                          "startRowIndex": row0, "endRowIndex": row0+1,
                          "startColumnIndex": 10, "endColumnIndex": 11},  # K
                "cell": {"userEnteredFormat": {"backgroundColor": bg_k}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    # === Helpers para reglas condicionales O/S/W/AA ==================
    def _a1_col_to_index(a1_col: str) -> int:
        s = a1_col.strip().upper()
        idx = 0
        for ch in s:
            idx = idx * 26 + (ord(ch) - ord('A') + 1)
        return idx - 1

    def _fmt_co(v: float) -> str:
        # coma decimal
        return str(v).replace('.', ',')

    # Δ% (O, S, W, AA). Ajusta en vivo:
    PCT_POS = {"5m": 0.10,  "15m": 0.10,  "1h": 0.10,  "1d": 0.10}     # ≥ +100%
    PCT_NEG = {"5m": -0.10, "15m": -0.10, "1h": -0.10, "1d": -0.10}    # ≤ −100%

    # RelVol (z) — mismo umbral ± para todas las TF (ajustable)
    REL_POS = {"5m": 0.10,  "15m": 0.10,  "1h": 0.10,  "1d": 0.10}
    REL_NEG = {"5m": -0.10, "15m": -0.10, "1h": -0.10, "1d": -0.10}

    def add_fmt_condicional_OSWAA(documento, hoja_objetivo, total_rows):
        """Colores en O/S/W/AA según Δ% + RelVol AG..AJ."""
        sheet_id = hoja_objetivo.id
        start_row = 1
        end_row   = start_row + max(0, total_rows)

        thr = {
            # Usa AG..AJ (z numéricos) en lugar de AB..AE (texto)
            "5m":  {"pct_pos": PCT_POS["5m"],  "pct_neg": PCT_NEG["5m"],  "rel_pos": REL_POS["5m"],  "rel_neg": REL_NEG["5m"],  "col_pct": "O",  "col_rel": "AG"},
            "15m": {"pct_pos": PCT_POS["15m"], "pct_neg": PCT_NEG["15m"], "rel_pos": REL_POS["15m"], "rel_neg": REL_NEG["15m"], "col_pct": "S",  "col_rel": "AH"},
            "1h":  {"pct_pos": PCT_POS["1h"],  "pct_neg": PCT_NEG["1h"],  "rel_pos": REL_POS["1h"],  "rel_neg": REL_NEG["1h"],  "col_pct": "W",  "col_rel": "AI"},
            "1d":  {"pct_pos": PCT_POS["1d"],  "pct_neg": PCT_NEG["1d"],  "rel_pos": REL_POS["1d"],  "rel_neg": REL_NEG["1d"],  "col_pct": "AA", "col_rel": "AJ"},
        }

        verde = {"red": 0.80, "green": 1.00, "blue": 0.80}
        rojo  = {"red": 1.00, "green": 0.80, "blue": 0.80}
        blanco= {"red": 1.00, "green": 1.00, "blue": 1.00}

        requests = []
        for tf_key in ["5m","15m","1h","1d"]:
            c_pct = thr[tf_key]["col_pct"]   # O | S | W | AA
            c_rel = thr[tf_key]["col_rel"]   # AG | AH | AI | AJ

            c0 = _a1_col_to_index(c_pct); c1 = c0 + 1

            f_verde = f'=Y(${c_pct}2>{_fmt_co(thr[tf_key]["pct_pos"])}; N(${c_rel}2)>{_fmt_co(thr[tf_key]["rel_pos"])})'
            f_rojo  = f'=Y(${c_pct}2<{_fmt_co(thr[tf_key]["pct_neg"])}; N(${c_rel}2)<{_fmt_co(thr[tf_key]["rel_neg"])})'

            # Fondo verde
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": start_row, "endRowIndex": end_row,
                            "startColumnIndex": c0, "endColumnIndex": c1
                        }],
                        "booleanRule": {
                            "condition": {"type": "CUSTOM_FORMULA",
                                          "values": [{"userEnteredValue": f_verde}]},
                            "format": {"backgroundColor": verde}
                        }
                    },
                    "index": 0
                }
            })
            # Fondo rojo
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": start_row, "endRowIndex": end_row,
                            "startColumnIndex": c0, "endColumnIndex": c1
                        }],
                        "booleanRule": {
                            "condition": {"type": "CUSTOM_FORMULA",
                                          "values": [{"userEnteredValue": f_rojo}]},
                            "format": {"backgroundColor": rojo}
                        }
                    },
                    "index": 1
                }
            })
            # Fondo blanco + % por defecto
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row, "endRowIndex": end_row,
                        "startColumnIndex": c0, "endColumnIndex": c1
                    },
                    "cell": {"userEnteredFormat": {
                        "backgroundColor": blanco,
                        "numberFormat": {"type":"PERCENT","pattern":"0%"},
                        "horizontalAlignment": "CENTER"
                    }},
                    "fields": "userEnteredFormat(backgroundColor,numberFormat,horizontalAlignment)"
                }
            })

        _retry(lambda: documento.batch_update({"requests": requests}))
        print("🎨 Formatos condicionales aplicados a O, S, W y AA (Δ% + RelVol).")

    def add_fmt_condicional_ABAE(documento, hoja_objetivo, total_rows):
        """
       Versión simplificada:
        - NO crea formatos condicionales sobre AB, AC, AD y AE.
        - Solo aplica formatos condicionales a O, S, W y AA (Δ% + RelVol).
        """
        try:
            # Sólo pintamos O, S, W y AA usando AG..AJ
            add_fmt_condicional_OSWAA(documento, hoja_objetivo, total_rows)
            print("🎨 Formatos condicionales aplicados SOLO a O, S, W y AA (AB–AE sin reglas).")
        except Exception as e:
            print(f"⚠️ Error en formatos O/S/W/AA: {e}")

    # --- 1) Formatos condicionales de RelVol y Δ% ---
    try:
        add_fmt_condicional_ABAE(documento, hoja_objetivo, total_rows)
    except Exception as e:
        print(f"⚠️ Error aplicando formatos AB–AE/O/S/W/AA: {e}")

    # --- 2) Sube el resto de formatos (H, I, K, % en O/S/W/AA) ---
    if fmt_reqs:
        try:
            _retry(lambda: documento.batch_update({"requests": fmt_reqs}))
        except Exception as e:
            print(f"⚠️ Error aplicando fmt_reqs (H/I/K y %): {e}")

        # ============= SNAPSHOTS (sin lecturas por ciclo) =============

    # 7) SNAP 5m — SIEMPRE (cada corrida)
    if actualiza_5m:
        ts_now_5m = now_ny().strftime("%Y-%m-%d %H:%M:%S")
        data_5m = [["Ticker","N_prev","N_curr","ts"]]
        for tk, rmap in [(r["tk"], r) for r in filas_con_flags]:
            n_prev = cache_5m.get(tk, "")
            n_curr = round(rmap["m_call"] - rmap["m_put"], 1)
            data_5m.append([tk, n_prev, n_curr, ts_now_5m])
            cache_5m[tk] = n_curr
        _retry(lambda: ws_snap_5m.batch_clear(["A2:D10000"]))
        _update_values(ws_snap_5m, f"A1:D{len(data_5m)}", data_5m, user_entered=False)

    # 7-bis) SNAP 15m — :00/:15/:30/:45 NY
    if actualiza_15m:
        ts_now_15 = now_ny().strftime("%Y-%m-%d %H:%M:%S")
        data_15 = [["Ticker","N_prev","N_curr","ts"]]
        for tk, rmap in [(r["tk"], r) for r in filas_con_flags]:
            n_prev = cache_15m.get(tk, "")
            n_curr = round(rmap["m_call"] - rmap["m_put"], 1)
            data_15.append([tk, n_prev, n_curr, ts_now_15])
            cache_15m[tk] = n_curr
        _update_values(ws_snap_15m, f"A1:D{len(data_15)}", data_15, user_entered=False)

    # 7-ter) SNAP 1h — :00 NY
    if actualiza_1h:
        ts_now_h1 = now_ny().strftime("%Y-%m-%d %H:%M:%S")
        data_h1 = [["Ticker","N_prev","N_curr","ts"]]
        for tk, rmap in [(r["tk"], r) for r in filas_con_flags]:
            n_curr = round(rmap["m_call"] - rmap["m_put"], 1)
            n_prev = cache_h1.get(tk, n_curr)
            data_h1.append([tk, n_prev, n_curr, ts_now_h1])
            cache_h1[tk] = n_curr
        _retry(lambda: ws_snap_1h.batch_clear(["A2:D10000"]))
        _update_values(ws_snap_1h, f"A1:D{len(data_h1)}", data_h1, user_entered=False)

    # 7-quater) SNAP 1d @ 08:00 NY
    if actualiza_d0800 or need_seed_0800:
        ts_now_d0800 = now_ny().strftime("%Y-%m-%d %H:%M:%S")
        data_d0800 = [["Ticker","N_prev","N_curr","ts"]]
        for tk, rmap in [(r["tk"], r) for r in filas_con_flags]:
            n_curr = round(rmap["m_call"] - rmap["m_put"], 1)
            n_prev = cache_d0800.get(tk, n_curr)
            data_d0800.append([tk, n_prev, n_curr, ts_now_d0800])
            cache_d0800[tk] = n_curr
        _retry(lambda: ws_snap_d0800.batch_clear(["A2:D10000"]))
        _update_values(ws_snap_d0800, f"A1:D{len(data_d0800)}", data_d0800, user_entered=False)


    # 7-quinquies) SNAP 1d @ 15:50 NY
    if actualiza_d1550 or need_seed_1550:
        ts_now_d1550 = now_ny().strftime("%Y-%m-%d %H:%M:%S")
        data_d1550 = [["Ticker","N_prev","N_curr","ts"]]
        for tk, rmap in [(r["tk"], r) for r in filas_con_flags]:
            n_curr = round(rmap["m_call"] - rmap["m_put"], 1)
            n_prev = cache_d1550.get(tk, n_curr)
            data_d1550.append([tk, n_prev, n_curr, ts_now_d1550])
            cache_d1550[tk] = n_curr
        _retry(lambda: ws_snap_d1550.batch_clear(["A2:D10000"]))
        _update_values(ws_snap_d1550, f"A1:D{len(data_d1550)}", data_d1550, user_entered=False)

    # 8) Guardar estado (colores / estado L)
    if nuevo_estado != ultimo:
        _escribir_estado(ws_estado, nuevo_estado)

# === NOTAS: crear/actualizar y devolver la hoja ===
from datetime import datetime as _dt

def crear_o_actualizar_hoja_notas(doc):
    try:
        ws = doc.worksheet("Datos Thinkorswim")
    except gspread.exceptions.WorksheetNotFound:
        ws = doc.add_worksheet(title="Datos Thinkorswim", rows=200, cols=2)

    codigo_ts = """# === Label combinado: Volumen + Relative Volume ===
input length = 10;  # Velas para promedio
def vol = volume;
def avgVol = Average(volume, length);
def relVol = if avgVol != 0 then vol / avgVol else 0;
def umbralInstitucional = 1.5;  # Ajusta a tu preferencia
def fuerzaInstitucional = relVol >= umbralInstitucional;
AddLabel(yes,
    "Vol: " + AsText(Round(vol, 0)) +
    " | RelVol: " + AsText(Round(relVol, 2)),
    if fuerzaInstitucional then Color.GREEN else Color.GRAY
);"""

    _update_values(ws, "A1", [["Notas técnicas (ThinkScript)"],
                               ["Actualizado:", _dt.now().strftime("%Y-%m-%d %H:%M:%S")]])
    _update_values(ws, "A4", [[codigo_ts]], user_entered=True)

    _retry(lambda: doc.batch_update({
        "requests": [
            { "updateSheetProperties": {
                "properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }},
            { "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 3, "endRowIndex": 4,
                          "startColumnIndex": 0, "endColumnIndex": 1},
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat.wrapStrategy"
            }},
            { "updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 820},
                "fields": "pixelSize"
            }}
        ]
    }))
    return ws

# Llamar una vez ANTES del while
hoja_notas = crear_o_actualizar_hoja_notas(documento)

# === Bucle: cada 5 minutos; "ACCESOS" cada 30 min; despierta justo 15:53 NY ===

# --- WARM-UP RELVOL (prefill una sola vez al inicio) ---
print("🔥 Warm-up inicial de RELVOL (llenando deques con velas cerradas)...")
for intento in range(1, 4):
    try:
        maybe_update_relvol_caches(
            symbols=tickers,
            do_5m=True,
            do_15m=True,
            do_1h=True,
            do_1d=True
        )

        for tk_dbg in ["AAPL","NVDA","SPY"]:
            vols5  = len(REL_VOL["5m"][tk_dbg])
            vols15 = len(REL_VOL["15m"][tk_dbg])
            vols1h = len(REL_VOL["1h"][tk_dbg])
            vols1d = len(REL_VOL["1d"][tk_dbg])
            zvals,_dirs = compose_rel_values_for_ticker(tk_dbg)
            print(f"   z NVDA: 5m={zvals['5m']:.2f} 15m={zvals['15m']:.2f} 1h={zvals['1h']:.2f} 1d={zvals['1d']:.2f}")
            def _tok(z,thr):
                z = max(0.0, float(z))
                return (str(round(z,2)).replace('.',',')) if z >= thr else "•"
            print(f"✅ {tk_dbg}: 5m={vols5} 15m={vols15} 1h={vols1h} 1d={vols1d} | "
                  f"AB={_tok(zvals['5m'],  REL_THRESH['5m'])} "
                  f"AC={_tok(zvals['15m'], REL_THRESH['15m'])} "
                  f"AD={_tok(zvals['1h'],  REL_THRESH['1h'])} "
                  f"AE={_tok(zvals['1d'],  REL_THRESH['1d'])}")

        # si llegó aquí, warm-up correcto
        break

    except Exception as e:
        print(f"⚠️ Warm-up intento {intento} falló: {e}")
        time.sleep(2)
else:
    print("❌ Warm-up falló en los 3 intentos; REL_VOL vacío. Revisa conexión o token Tradier.")

# Ejecuta este healthcheck una vez por ciclo antes de actualizar las hojas
def ciclo_diagnostico():
    for tk_dbg in ["AAPL", "NVDA", "SPY"]:
        healthcheck_relvol(tk_dbg)

INTERVALO_MIN = 5
ULTIMA_REVISION_ACCESOS = 0
PERIODO_ACCESOS_S = 30 * 60  # 30 minutos


def _seconds_until(dt_target):
    now = datetime.now(pytz.utc)
    return max(1, int((dt_target.astimezone(pytz.utc) - now).total_seconds()))


def _next_5min_boundary():
    ahora = now_ny()
    resto = ahora.minute % INTERVALO_MIN
    minutos_para_esperar = (INTERVALO_MIN - resto) % INTERVALO_MIN
    segundos_para_esperar = (minutos_para_esperar * 60) - ahora.second
    if segundos_para_esperar <= 0:
        segundos_para_esperar += INTERVALO_MIN * 60
    return segundos_para_esperar


def _next_fixed_time_seconds(hour:int, minute:int):
    ny = now_ny()
    target = ny.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if ny >= target:
        target = (target + timedelta(days=1)).replace(hour=hour, minute=minute, second=0, microsecond=0)
    return _seconds_until(target)

def _next_0800_seconds():
    return _next_fixed_time_seconds(8, 0)

def _next_1550_seconds():
    return _next_fixed_time_seconds(15, 50)

def esperar_hasta_siguiente_intervalo(mins=INTERVALO_MIN):
    s5   = _next_5min_boundary()
    s8   = _next_0800_seconds()
    s155 = _next_1550_seconds()
    sleep_s = min(s5, s8, s155)
    print(f"⏳ Próxima corrida en {sleep_s//60}m {sleep_s%60}s… (5m={s5}s | 08:00NY={s8}s | 15:50NY={s155}s)\n")
    time.sleep(sleep_s)

def healthcheck_relvol(symbol="NVDA"):
    print(f"\n🧩 [HEALTH-CHECK] {symbol} @ {now_ny().strftime('%Y-%m-%d %H:%M:%S')} NY")
    for tf, interval in [("5m","5min"),("15m","15min"),("1h","hour"),("1d","daily")]:
        try:
            bars = _tradier_bars(symbol, interval=interval, limit=12)
            n = len(bars); last_t = bars[-1]["t"] if n>0 else "-"
            vols = [b["v"] for b in bars[-10:]]
            z = 0.0
            if len(vols)>=3:
                avg = sum(vols)/len(vols)
                sd  = (sum((v-avg)**2 for v in vols)/len(vols))**0.5
                z   = 0.0 if sd==0 else (vols[-1]-avg)/sd
            thr = REL_THRESH[tf]; tok = (f"{z:+.2f}".replace(".",",") if abs(z)>=thr else "•")
            print(f"   TF={tf:<3} bars={n:<2} last={last_t:<19} z={z:>5.2f} → token={tok}")
        except Exception as e:
            print(f"   ⚠️ {tf} error: {e}")

while True:
    ciclo_diagnostico()
    try:
        actualizar_hoja(hoja_resumen, posicion_fecha=0)
        actualizar_hoja(hoja_vto2, posicion_fecha=1)

        now_epoch = time.time()
        if now_epoch - ULTIMA_REVISION_ACCESOS > PERIODO_ACCESOS_S:
            procesar_autorizados()
            ULTIMA_REVISION_ACCESOS = now_epoch

        esperar_hasta_siguiente_intervalo()

    except GAPIError as e:
        msg = str(e)
        if "[503]" in msg or "The service is currently unavailable" in msg:
            print("⛑️ Google Sheets 503 (Service Unavailable). Reintentando pronto...")
            time.sleep(15)
            continue
        traceback.print_exc()
        time.sleep(60)
    except Exception:
        traceback.print_exc()
        time.sleep(60)
