# === 🚪 app_audit.py – Gateway de acceso con logging de aperturas ===
# - Genera tokens únicos por email (desde la hoja ACCESOS o lista directa).
# - Registra cada apertura (fecha/hora UTC, email, token, IP, user-agent).
# - Redirige 302 a la hoja real (MAIN_FILE_ID).
#
# Requisitos: pip install flask gspread oauth2client google-api-python-client

import os, json, uuid, traceback
from datetime import datetime, timezone
from flask import Flask, request, redirect, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ========= Config =========
ACCESS_FILE_ID = os.getenv("ACCESS_FILE_ID", "").strip()
MAIN_FILE_ID   = os.getenv("MAIN_FILE_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
OI_SECRET = os.getenv("OI_SECRET", "").strip()

LOG_SHEET_TAB    = os.getenv("LOG_SHEET_TAB", "APERTURAS").strip()
TOKENS_SHEET_TAB = os.getenv("TOKENS_SHEET_TAB", "ACC_TOKENS").strip()
GATEWAY_BASE     = os.getenv("GATEWAY_BASE", "").rstrip("/")

# Hoja ACCESOS (de donde leemos emails activos)
ACCESOS_TAB_NAME = os.getenv("ACCESOS_TAB_NAME", "ACCESOS").strip()

# ========= Google Sheets auth =========
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def _get_client():
    if GOOGLE_CREDENTIALS_JSON.startswith("{"):
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    else:
        # path en disco (opcional)
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_JSON, SCOPE)
    return gspread.authorize(creds)

client = _get_client()
book  = client.open_by_key(ACCESS_FILE_ID)

def _get_or_create_ws(title):
    try:
        return book.worksheet(title)
    except gspread.WorksheetNotFound:
        return book.add_worksheet(title=title, rows=1000, cols=12)

def _ensure_headers(ws, headers):
    try:
        current = ws.row_values(1)
        if current != headers:
            ws.update("A1", [headers])
    except Exception:
        pass

# ========= App Flask =========
app = Flask(__name__)

@app.get("/audit_healthz")
def audit_healthz():
    if OI_SECRET and request.headers.get("X-Auth-Token") != OI_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    return jsonify({"ok": True, "service": "milo-oi-audit"})

@app.post("/gen_tokens")
def gen_tokens():
    # Protegido por OI_SECRET (para evitar abuso)
    if OI_SECRET and request.headers.get("X-Auth-Token") != OI_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    # 1) Leemos emails ACTIVO de ACCESOS
    try:
        ws_acc = book.worksheet(ACCESOS_TAB_NAME)
    except gspread.WorksheetNotFound:
        return jsonify({"ok": False, "error": f"Worksheet {ACCESOS_TAB_NAME} not found"}), 400

    headers = ws_acc.row_values(1)
    idx = {h.lower(): i for i, h in enumerate(headers, start=1)}
    needed = ["email", "estado"]
    for k in needed:
        if k not in idx:
            return jsonify({"ok": False, "error": f"Missing column '{k}' in {ACCESOS_TAB_NAME}"}), 400

    records = ws_acc.get_all_records()  # as dicts by header
    activos = [r["email"].strip().lower() for r in records if str(r.get("estado", "")).upper() == "ACTIVO" and r.get("email")]

    # 2) Preparamos hoja TOKENS
    ws_tokens = _get_or_create_ws(TOKENS_SHEET_TAB)
    _ensure_headers(ws_tokens, ["email", "token", "created_utc", "link_gateway"])

    # Leemos existentes (para no duplicar)
    existing = {}
    rows = ws_tokens.get_all_records()
    for r in rows:
        em = r.get("email", "").strip().lower()
        tk = r.get("token", "").strip()
        if em and tk:
            existing[em] = r

    # 3) Insertamos nuevos tokens (si no existen)
    created = 0
    updates = []
    for email in activos:
        if email in existing:
            continue
        token = uuid.uuid4().hex
        link = f"{GATEWAY_BASE}/l/{token}"
        ts = datetime.now(timezone.utc).isoformat()
        updates.append([email, token, ts, link])
        created += 1

    if updates:
        ws_tokens.append_rows(updates, value_input_option="RAW")

    return jsonify({"ok": True, "activos": len(activos), "creados": created})

@app.get("/l/<token>")
def open_with_log(token):
    # 1) Resolver email a partir del token
    try:
        ws_tokens = book.worksheet(TOKENS_SHEET_TAB)
    except gspread.WorksheetNotFound:
        return jsonify({"ok": False, "error": f"Worksheet {TOKENS_SHEET_TAB} not found"}), 400

    # map token->email (se puede optimizar con un índice en memoria)
    rows = ws_tokens.get_all_records()
    email = None
    for r in rows:
        if r.get("token", "").strip() == token:
            email = r.get("email", "").strip().lower()
            break

    if not email:
        return jsonify({"ok": False, "error": "token_not_found"}), 404

    # 2) Registrar apertura
    ws_log = _get_or_create_ws(LOG_SHEET_TAB)
    _ensure_headers(ws_log, ["ts_utc", "email", "token", "sheet_id", "ip", "user_agent", "referrer"])

    ts = datetime.now(timezone.utc).isoformat()
    ip = request.headers.get("X-Forwarded-For", request.remote_addr) or ""
    ua = request.headers.get("User-Agent", "")
    ref = request.referrer or ""
    ws_log.append_row([ts, email, token, MAIN_FILE_ID, ip, ua, ref], value_input_option="RAW")

    # 3) Redirigir 302 a la hoja real
    dest = f"https://docs.google.com/spreadsheets/d/{MAIN_FILE_ID}/edit"
    return redirect(dest, code=302)

# Ejecución local:
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=False)
