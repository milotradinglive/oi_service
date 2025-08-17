# app.py
import os, time, json, traceback
from datetime import datetime, timedelta, timezone
import pytz
import threading
from collections import defaultdict as _dd
import gspread
import requests

from flask import Flask, jsonify, request
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= Config =========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

# IDs por entorno (defaults para STAGING)
MAIN_FILE_ID   = os.getenv("MAIN_FILE_ID",   "1DlwiPxbgDWAQmM_7n5MRi2Ms4YRas5SYKsteXYHD3Ks")
ACCESS_FILE_ID = os.getenv("ACCESS_FILE_ID", "1ZwLVuinFA1sBprPMWVliu_nwdr1mlmav6FJ-zQm2FlE")  # usa otro ID si tu hoja de accesos es distinta
ACCESS_SHEET_TITLE = "AUTORIZADOS"

TRADIER_TOKEN  = os.getenv("TRADIER_TOKEN", "")

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
drive = build("drive", "v3", credentials=google_api_creds)  # (opcional; útil para debug/links)

# ========= Datos base =========
TICKERS = [
    "AAPL","AMD","AMZN","BA","BAC","DIA","GLD","GOOGL","IBM","INTC",
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

# ========= Lógica de expiraciones / datos =========
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

        expj = get_json(f"{BASE_TRADIER}/markets/options/expirations",
                        params={"symbol": ticker, "includeAllRoots": "true", "strikes": "false"})
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
            sj = get_json(f"{BASE_TRADIER}/markets/options/strikes",
                          params={"symbol": ticker, "expiration": fecha_str})
            raw_strikes = sj.get("strikes", {}).get("strike", []) or []
            strikes = [float(s) for s in raw_strikes]

            cj = get_json(f"{BASE_TRADIER}/markets/options/chains",
                          params={"symbol": ticker, "expiration": fecha_str, "greeks": "false"})
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
                oi  = int(op.get("open_interest") or 0)
                vol = int(op.get("volume") or 0)
                bid = float(op.get("bid") or 0.0)
                ask = float(op.get("ask") or 0.0)
                last_opt = float(op.get("last") or 0.0)

                mid = 0.0
                if bid > 0 and ask > 0:   mid = (bid + ask) / 2
                elif ask > 0:             mid = ask
                elif last_opt > 0:        mid = last_opt

                if typ == "call" and strike in set_call:
                    oi_call_total     += oi
                    dinero_call_total += oi * mid * 100
                    vol_call_total    += vol
                elif typ == "put" and strike in set_put:
                    oi_put_total      += oi
                    dinero_put_total  += oi * mid * 100
                    vol_put_total     += vol

        return (oi_call_total, oi_put_total,
                round(dinero_call_total / 1_000_000, 1),
                round(dinero_put_total  / 1_000_000, 1),
                vol_call_total, vol_put_total, viernes_ref_str)
    except Exception as e:
        print(f"❌ Error con {ticker}: {e}")
        return 0, 0, 0.0, 0.0, 0, 0, None

# ========= Escritura en Google Sheets =========
def actualizar_hoja(doc, sheet_title, posicion_fecha):
    ws = doc.worksheet(sheet_title)

    # Hora NY (derivada de UTC) inequívoca
    now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
    ny_tz   = pytz.timezone("America/New_York")
    now_ny  = now_utc.astimezone(ny_tz)

    fecha_txt = f"{now_ny:%Y-%m-%d}"
    hora_txt  = now_ny.strftime("%H:%M:%S")   # texto para que Sheets no convierta

    # Debug UTC/NY en hoja (N1:O2) y logs
    print(f"[debug] UTC={now_utc:%Y-%m-%d %H:%M:%S} | NY={now_ny:%Y-%m-%d %H:%M:%S}", flush=True)
    ws.update(values=[["DEBUG_UTC", f"{now_utc:%Y-%m-%d %H:%M:%S}"],
                      ["DEBUG_NY",  f"{now_ny:%Y-%m-%d %H:%M:%S}"]],
              range_name="N1:O2")

    print(f"⏳ Actualizando: {sheet_title} (venc. #{posicion_fecha+1})")
    datos, resumen = [], []

    # Recolecta datos por ticker
    for tk in TICKERS:
        oi_c, oi_p, m_c, m_p, v_c, v_p, exp = obtener_dinero(tk, posicion_fecha)
        datos.append([tk, "CALL", m_c, v_c, exp, oi_c])
        datos.append([tk, "PUT",  m_p, v_p, exp, oi_p])
        time.sleep(0.15)

    # Agrega por ticker
    agg = _dd(lambda: {"CALL": [0.0, 0], "PUT": [0.0, 0], "EXP": None})
    for tk, side, m_usd, vol, exp, _oi in datos:
        agg[tk]["EXP"] = agg[tk]["EXP"] or exp
        agg[tk][side][0] += m_usd
        agg[tk][side][1] += vol

    # Construye filas finales
    for tk in sorted(agg.keys()):
        m_call, v_call = agg[tk]["CALL"]
        m_put,  v_put  = agg[tk]["PUT"]
        exp = agg[tk]["EXP"] or "-"

        total_m = m_call + m_put
        if total_m == 0:
            pct_c = pct_p = fuerza = 0.0
        else:
            pct_c = round(100 * m_call / total_m, 1)
            pct_p = round(100 - pct_c, 1)
            fuerza = pct_c if pct_c > pct_p else -pct_p

        total_vol = v_call + v_put
        if total_vol == 0:
            pct_vc = pct_vp = fuerza_vol = 0.0
        else:
            pct_vc = round(100 * v_call / total_vol, 1)
            pct_vp = round(100 - pct_vc, 1)
            fuerza_vol = pct_vc if pct_vc > pct_vp else -pct_vp

        color_oi  = "🟢" if fuerza >= 20 else "🔴" if fuerza <= -20 else "⚪"
        color_vol = "🟢" if fuerza_vol >= 20 else "🔴" if fuerza_vol <= -20 else "⚪"
        if color_oi == "🟢" and color_vol == "🟢":   color_final = "🟢🟢"
        elif color_oi == "🔴" and color_vol == "🔴": color_final = "🔴🔴"
        elif (color_oi, color_vol) in (("🟢","🔴"),("🔴","🟢")): color_final = "🟢🔴"
        else: color_final = "⚪"

        resumen.append([
            fecha_txt,      # A: Fecha (NY)
            hora_txt,       # B: Hora (NY, texto)
            tk,
            fmt_millones(m_call), fmt_millones(m_put),
            fmt_entero_miles(v_call), fmt_entero_miles(v_put),
            pct_str(pct_c), pct_str(pct_p),
            color_oi, color_vol, pct_str(fuerza), color_final
        ])

    # Escribe encabezado y cuerpo
    encabezado = [[
        "Fecha","Hora","Ticker",
        "RELATIVE VERDE","RELATIVE ROJO",
        "VOLUMEN ENTRA","VOLUMEN SALE",
        "%SUBIDA","%BAJADA",
        "INTENCION","VOLUMEN","Fuerza","Relación"
    ]]
    ws.update(values=encabezado, range_name="A1:M1")
    ws.batch_clear(["A2:M1000"])

    def fuerza_to_float(s):
        try:    return float(s.replace("%","").replace(",", "."))
        except: return -9999.0

    resumen.sort(key=lambda row: -fuerza_to_float(row[11]))
    ws.update(values=resumen, range_name=f"A2:M{len(resumen)+1}")

# ===== AUTORIZADOS (idempotente básico con grupos) =====
def _parse_duration(txt):
    txt = (txt or "").strip().lower()
    if txt.endswith("h"): return timedelta(hours=int(txt[:-1] or 24))
    if txt.endswith("d"): return timedelta(days=int(txt[:-1] or 1))
    if txt.endswith("w"): return timedelta(weeks=int(txt[:-1] or 1))
    return timedelta(hours=24)

def procesar_autorizados(accesos_doc, main_file_url):
    try:
        hoja_aut = accesos_doc.worksheet(ACCESS_SHEET_TITLE)
    except gspread.exceptions.WorksheetNotFound:
        hoja_aut = accesos_doc.add_worksheet(title=ACCESS_SHEET_TITLE, rows=500, cols=8)
        hoja_aut.update(values=[["email","duracion","rol","creado_utc","expira_utc","estado","perm_id","nota"]], range_name="A1")

    rows = hoja_aut.get_all_values()
    if not rows or len(rows) == 1:
        print("ℹ️ AUTORIZADOS vacío.")
        return {"activados": 0, "revocados": 0}

    # Directory API solo si hay ADMIN_SUBJECT definido
    try:
        from google.oauth2.service_account import Credentials as SACreds
        ADMIN_SUBJECT = os.getenv("ADMIN_SUBJECT", "").strip()
        SCOPES_DIR = [
            "https://www.googleapis.com/auth/admin.directory.group.member",
            "https://www.googleapis.com/auth/apps.groups.settings",
        ]

        directory = None
        if ADMIN_SUBJECT:
            creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
            if not creds_json:
                raise RuntimeError("Falta GOOGLE_CREDENTIALS_JSON para Directory API")
            creds_info = json.loads(creds_json)

            creds_delegated = SACreds.from_service_account_info(
                creds_info, scopes=SCOPES_DIR, subject=ADMIN_SUBJECT
            )
            directory = build("admin", "directory_v1", credentials=creds_delegated)

        GROUP_READER_EMAIL = os.getenv("GROUP_READER_EMAIL", "accesos-lectores@milotradinglive.com")
        GROUP_COMMENTER_EMAIL = os.getenv("GROUP_COMMENTER_EMAIL", "accesos-comentadores@milotradinglive.com")
        def grupo_para_rol(rol): return GROUP_COMMENTER_EMAIL if (rol or "reader").lower()=="commenter" else GROUP_READER_EMAIL

        def add_member(group_email, user_email):
            if not directory:
                return "skip"
            try:
                directory.members().insert(groupKey=group_email, body={"email": user_email, "role": "MEMBER"}).execute()
                return "ok"
            except HttpError as e:
                msg = str(e).lower()
                if any(s in msg for s in ("duplicate","memberexists","already exists")):
                    return "ya_miembro"
                raise

        activados = revocados = 0
        now_utc = datetime.now(timezone.utc)

                # CORRECCIÓN: tomar el email de la service account correctamente
        sa_email = ""
        if hasattr(_creds_info, "service_account_email"):
            sa_email = _creds_info.service_account_email
        else:
            try:
                sa_email = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON","")).get("client_email","").lower()
            except:
                sa_email = ""

        for i, raw in enumerate(rows[1:], start=2):
            row = (raw + [""]*8)[:8]
            email, dur_txt, rol, creado, expira, estado, perm_id, nota = [(c or "").strip() for c in row]
            if not any([email, dur_txt, rol, creado, expira, estado, perm_id, nota]): 
                continue
            if sa_email and email.lower() == sa_email:
                if nota != "IGNORADO (service account)":
                    hoja_aut.update(values=[["IGNORADO (service account)"]], range_name=f"H{i}")
                continue

            r = (rol or "reader").lower()
            if r not in ("reader","commenter"): r = "reader"
            est = (estado or "").lower()
            grupo = grupo_para_rol(r)

            if est in ("", "pendiente"):
                result = add_member(grupo, email)
                exp_dt = now_utc + _parse_duration(dur_txt or "24h")
                now_iso = now_utc.isoformat(timespec="seconds")
                hoja_aut.update(values=[[now_iso, exp_dt.isoformat(timespec="seconds"), "ACTIVO", f"group:{grupo}"]], range_name=f"D{i}:G{i}")
                hoja_aut.update(values=[[f"Miembro en {grupo}. Link: {main_file_url}"]], range_name=f"H{i}")
                activados += 1
                print(("✅ ACTIVADO " if result=="ok" else "ℹ️ (Idempotente) ") + f"{email} en {grupo} hasta {exp_dt} UTC")
                time.sleep(1.0)

            elif est == "activo" and expira:
                try:
                    exp_dt = datetime.fromisoformat(expira.replace("Z",""))
                    if exp_dt.tzinfo is None:
                        exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    hoja_aut.update(values=[[f"ERROR_PARSE_EXP: {expira}"]], range_name=f"H{i}")
                    continue
                if now_utc >= exp_dt:
                    hoja_aut.update(values=[["REVOCADO"]], range_name=f"F{i}")
                    hoja_aut.update(values=[["Salida del grupo por vencimiento"]], range_name=f"H{i}")
                    revocados += 1
                    print(f"🗑️ REVOCADO {email} (vencido)")

        return {"activados": activados, "revocados": revocados}

    except Exception as e:
        print(f"⚠️ procesar_autorizados sin Directory API: {e}")
        return {"activados": 0, "revocados": 0}

# ========= Runner de UNA corrida =========
def run_once():
    doc_main  = client.open_by_key(MAIN_FILE_ID)
    accesos   = client.open_by_key(ACCESS_FILE_ID)
    main_url  = f"https://docs.google.com/spreadsheets/d/{MAIN_FILE_ID}/edit"  # CORREGIDO

    actualizar_hoja(doc_main, "Semana actual", posicion_fecha=0)
    actualizar_hoja(doc_main, "Semana siguiente", posicion_fecha=1)
    acc = procesar_autorizados(accesos, main_url)

    return {
        "ok": True,
        "main_title": doc_main.title,
        "access_title": accesos.title,
        "activados": acc.get("activados", 0),
        "revocados": acc.get("revocados", 0),
        "when": datetime.utcnow().isoformat(timespec="seconds") + "Z"
    }

# ========= Flask app =========
app = Flask(__name__)
OI_SECRET = os.getenv("OI_SECRET", "").strip()

def _authorized(req: request) -> bool:
    if not OI_SECRET: return True
    return req.headers.get("X-Auth-Token", "") == OI_SECRET

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

        print("🚀 [update] Inicio actualización OI", flush=True)
        run_once()
        print("✅ [update] Fin actualización OI", flush=True)

    except Exception as e:
        print(f"❌ [/update] Error: {repr(e)}", flush=True)
    finally:
        _is_running = False
        print(f"🟣 [/update] Hilo terminado @ {datetime.utcnow().isoformat()}Z", flush=True)

@app.get("/")
def root():
    return jsonify({"status": "ok", "service": "oi-updater", "time": datetime.utcnow().isoformat() + "Z"})

@app.get("/healthz")
def healthz():
    return "ok", 200

@app.route("/update", methods=["GET", "POST"])
def update():
    if not _authorized(request):
        return jsonify({"error": "unauthorized"}), 401
    t = threading.Thread(target=_run_guarded, daemon=True)
    t.start()
    return jsonify({"accepted": True, "started_at": datetime.utcnow().isoformat() + "Z"}), 202

@app.get("/run")
def http_run():
    try:
        result = run_once()
        return jsonify(result), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    # Para pruebas locales: http://127.0.0.1:8080/run
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
