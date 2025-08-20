# app.py
import os, time, json, traceback
from datetime import datetime, timedelta, timezone
import pytz
import threading
from collections import defaultdict as _dd
import gspread
import requests
import re
import fcntl
from flask import Flask, jsonify, request
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= Flask app =========
app = Flask(__name__)
OI_SECRET = os.getenv("OI_SECRET", "").strip()
APP_VERSION = os.getenv("RENDER_GIT_COMMIT", "")[:7] or "dev"
print(f"🚀 Iniciando oi-updater versión {APP_VERSION}", flush=True)

@app.get("/")
def root():
    return jsonify({
        "status": "ok",
        "service": "oi-updater",
        "version": APP_VERSION,
        "time": datetime.utcnow().isoformat() + "Z"
    })

# ========= Config =========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# IDs por entorno (defaults para STAGING)
MAIN_FILE_ID = os.getenv("MAIN_FILE_ID", "18sxVJ-8ChEt09DR9HxyteK4fWdbyxEJ12fkSDIbvtDA")
ACCESS_FILE_ID = os.getenv("ACCESS_FILE_ID", "1CY06Lw1QYZQEXuMO02EPe8vUOipizuhbWypffPETPyk")
ACCESS_SHEET_TITLE = "AUTORIZADOS"
TRADIER_TOKEN = os.getenv("TRADIER_TOKEN", "")

# ====== Lock inter-proceso (para evitar /run y /update simultáneos) ======
LOCK_FILE = "/tmp/oi-updater.lock"
def _acquire_lock():
    """ Intenta tomar un candado de archivo no bloqueante.
        Devuelve el file handle si lo obtiene, o None si ya hay otra ejecución corriendo.
    """
    f = open(LOCK_FILE, "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            f.seek(0); f.truncate(0)
            f.write(str(os.getpid()))
            f.flush()
        except Exception:
            pass
        return f
    except BlockingIOError:
        f.close()
        return None

# ========= Auth desde variable de entorno =========
def make_gspread_and_creds():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("Falta variable de entorno GOOGLE_CREDENTIALS_JSON")
    creds_info = json.loads(creds_json)
    legacy_scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    legacy_creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, legacy_scopes)
    return gspread.authorize(legacy_creds), legacy_creds, creds_info

client, google_api_creds, _creds_info = make_gspread_and_creds()
drive = build("drive", "v3", credentials=google_api_creds)

# ========= Datos base =========
TICKERS = [
    "AAPL","AMD","AMZN","BA","BAC","DIA","GLD","GOOG","IBM","INTC",
    "IWM","JPM","META","MRNA","MSFT","NFLX","NVDA","NVTS","ORCL",
    "PLTR","QQQ","SLV","SNAP","SPY","TNA","TSLA","TSLL","USO","WFC","WMT","XOM","V"
]

BASE_TRADIER = "https://api.tradier.com/v1"
TIMEOUT = 12

# ========= Sesión HTTP Tradier =========
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept": "application/json"})

def get_json(url, params=None, max_retries=3):
    for intento in range(1, max_retries+1):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 429:
                espera = 2 * intento
                print(f"⏳ 429 rate limit. Reintentando en {espera}s…")
                time.sleep(espera); continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if intento == max_retries:
                raise
            espera = 1.5 * intento
            print(f"⚠️ Error {e}. Reintento {intento}/{max_retries} en {espera:.1f}s")
            time.sleep(espera)

# ========= Utilidades de formato =========
def fmt_millones(x):
    s = f"{x:,.1f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_entero_miles(x):
    s = f"{int(x):,}"
    return s.replace(",", ".")

def pct_str(p):
    return f"{p:.1f}%".replace(".", ",")

# ========= Lógica de expiraciones / datos (OI) =========
from datetime import datetime as _dt, timedelta as _td

def elegir_expiracion_viernes(expiraciones, posicion_fecha):
    hoy = _dt.now().date()
    dias_a_viernes = (4 - hoy.weekday()) % 7
    if dias_a_viernes == 0:
        dias_a_viernes = 7
    proximo_viernes = hoy + _td(days=dias_a_viernes)
    fechas_viernes = []
    for d in expiraciones or []:
        try:
            dt = _dt.strptime(d, "%Y-%m-%d").date()
            if dt.weekday() == 4 and dt >= proximo_viernes:
                fechas_viernes.append(dt)
        except:
            continue
    fechas_viernes.sort()
    if len(fechas_viernes) <= posicion_fecha:
        return None
    return fechas_viernes[posicion_fecha].strftime("%Y-%m-%d")

# ... [código completo sigue igual hasta el final] ...

if __name__ == "__main__":
    # Para pruebas locales: http://127.0.0.1:8080/run
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
