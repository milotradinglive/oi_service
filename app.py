# app.py
import os, json, time, traceback
from datetime import datetime, timezone
import threading

import pytz
import gspread
import requests

from flask import Flask, jsonify, request
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ===================== Config / Entorno =====================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
MAIN_FILE_ID   = os.getenv("MAIN_FILE_ID", "").strip()   # <-- tu hoja principal
ACCESS_FILE_ID = os.getenv("ACCESS_FILE_ID", "").strip() # <-- si usas hoja secundaria
TRADIER_TOKEN  = os.getenv("TRADIER_TOKEN", "").strip()  # opcional
USE_TRADIER    = os.getenv("USE_TRADIER", "0").strip() == "1"  # por defecto OFF


def make_gspread_and_creds():
    """Crea cliente gspread con credenciales desde GOOGLE_CREDENTIALS_JSON (env)."""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("Falta GOOGLE_CREDENTIALS_JSON en Environment.")

    creds_info = json.loads(creds_json)
    legacy_scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    legacy_creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, legacy_scopes)
    client = gspread.authorize(legacy_creds)

    drive = build("drive", "v3", credentials=legacy_creds)
    return client, drive


gclient, gdrive = make_gspread_and_creds()


# ===================== Utilidad de hora =====================
def now_utc_and_ny():
    utc = datetime.now(timezone.utc)
    ny  = utc.astimezone(pytz.timezone("America/New_York"))
    return utc, ny


# ===================== Diagnóstico (escribir horas) =====================
def write_debug_times(doc, sheet_title):
    ws = doc.worksheet(sheet_title)
    utc, ny = now_utc_and_ny()
    utc_s = utc.strftime("%Y-%m-%d %H:%M:%S")
    ny_s  = ny.strftime("%Y-%m-%d %H:%M:%S")

    ws.update(values=[["DEBUG_UTC", utc_s], ["DEBUG_NY", ny_s]], range_name="N1:O2")
    ws.update(values=[["'" + ny.strftime("%H:%M:%S") + " ET"]], range_name="B2")


# ===================== (Opcional) Tradier – desactivado por defecto =====================
session = requests.Session()
if TRADIER_TOKEN:
    session.headers.update({"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept": "application/json"})

def run_full_update(doc):
    # Aquí pondrás tu lógica completa cuando el diagnóstico funcione.
    write_debug_times(doc, "Semana actual")
    write_debug_times(doc, "Semana siguiente")


# ===================== Runner de UNA corrida =====================
def run_once():
    if not MAIN_FILE_ID:
        raise RuntimeError("Falta MAIN_FILE_ID en Environment.")

    doc_main = gclient.open_by_key(MAIN_FILE_ID)

    write_debug_times(doc_main, "Semana actual")
    write_debug_times(doc_main, "Semana siguiente")

    if USE_TRADIER:
        run_full_update(doc_main)

    utc, ny = now_utc_and_ny()
    return {
        "ok": True,
        "mode": "full" if USE_TRADIER else "diagnostic",
        "main_title": doc_main.title,
        "debug_utc": utc.strftime("%Y-%m-%d %H:%M:%S"),
        "debug_ny":  ny.strftime("%Y-%m-%d %H:%M:%S"),
    }


# ===================== Flask app =====================
app = Flask(__name__)
OI_SECRET = os.getenv("OI_SECRET", "").strip()

def _authorized(req: request) -> bool:
    return (not OI_SECRET) or (req.headers.get("X-Auth-Token", "") == OI_SECRET)

_update_lock = threading.Lock()
_is_running = False

def _run_guarded():
    global _is_running
    try:
        with _update_lock:
            if _is_running:
                print("⏳ [/update] Ya hay una ejecución en curso; se omite.", flush=True)
                return
            _is_running = True

        print("🚀 [update] Inicio actualización", flush=True)
        r = run_once()
        print(f"✅ [update] Fin actualización :: {r}", flush=True)

    except Exception as e:
        traceback.print_exc()
        print(f"❌ [/update] Error: {e}", flush=True)
    finally:
        _is_running = False
        print(f"🟣 [/update] Hilo terminado @ {datetime.now(timezone.utc).isoformat()}", flush=True)

@app.get("/")
def root():
    utc, ny = now_utc_and_ny()
    return jsonify({
        "status": "ok",
        "service": "oi-updater",
        "utc": utc.isoformat(),
        "ny":  ny.isoformat(),
        "mode": "full" if USE_TRADIER else "diagnostic"
    })

@app.get("/healthz")
def healthz():
    return "ok", 200

@app.route("/update", methods=["GET", "POST"])
def update():
    if not _authorized(request):
        return jsonify({"error": "unauthorized"}), 401
    t = threading.Thread(target=_run_guarded, daemon=True)
    t.start()
    return jsonify({"accepted": True}), 202

@app.get("/run")
def http_run():
    try:
        result = run_once()
        return jsonify(result), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
