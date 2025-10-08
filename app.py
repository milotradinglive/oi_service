# app.py — milo-oi-service (unificado con snapshot + columnas L, M, N, O, P y Q)
# - Mantiene toda la estructura/funciones originales.
# - Implementa exactamente la lógica de L, M, N, O, P y Q del script de Sheets:
#     L: "CALLS" si H>0.5 e I>0.4 ; "PUTS" si H<0 e I<0 ; "" en otro caso
#     N: SNAP!N_curr   (m_call - m_put de la corrida de corte anterior)
#     O: SNAP!N_prev
#     P: N - O
#     Q: P / O   (si O==0 → 0)
#     M: "🔥🔥🔥" si |Q|>=0.5  y el FONDO de M: VERDE si P>0, ROJO si P<0
# - El snapshot (SNAP__<hoja>) se actualiza SOLO al cierre de cada hora (minuto 00 NY).

import os
import time
import json
import traceback
import re
import fcntl
import threading
from collections import defaultdict as _dd
from datetime import datetime, timedelta, timezone

import pytz
import gspread
import requests
from flask import Flask, jsonify, request
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from gspread.exceptions import WorksheetNotFound

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
        "time": datetime.utcnow().isoformat() + "Z",
    })

# ========= Config =========
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# IDs por entorno (defaults para STAGING)
MAIN_FILE_ID = os.getenv("MAIN_FILE_ID", "18sxVJ-8ChEt09DR9HxyteK4fWdbyxEJ12fkSDIbvtDA")
ACCESS_FILE_ID = os.getenv("ACCESS_FILE_ID", "1CY06Lw1QYZQEXuMO02EPe8vUOipizuhbWypffPETPyk")
ACCESS_SHEET_TITLE = "AUTORIZADOS"
TRADIER_TOKEN = os.getenv("TRADIER_TOKEN", "")

# ====== Lock inter-proceso ======
LOCK_FILE = "/tmp/oi-updater.lock"

def _acquire_lock():
    f = open(LOCK_FILE, "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            f.seek(0)
            f.truncate(0)
            f.write(str(os.getpid()))
            f.flush()
        except Exception:
            pass
        return f
    except BlockingIOError:
        f.close()
        return None

# ========= Auth =========
def make_gspread_and_creds():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("Falta variable de entorno GOOGLE_CREDENTIALS_JSON")
    creds_info = json.loads(creds_json)

    legacy_scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    legacy_creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, legacy_scopes)
    return gspread.authorize(legacy_creds), legacy_creds, creds_info

client, google_api_creds, _creds_info = make_gspread_and_creds()
drive = build("drive", "v3", credentials=google_api_creds)

# ========= Datos base =========
TICKERS = [
    "AAPL", "AMD", "AMZN", "BA", "BAC", "DIA", "GLD", "GOOG", "IBM", "INTC",
    "IWM", "JPM", "META", "MRNA", "MSFT", "NFLX", "NVDA", "NVTS", "ORCL", "UBER",
    "PLTR", "QQQ", "SLV", "SNAP", "SPY", "TNA", "TSLA", "TSLL", "USO", "WFC", "WMT", "XOM", "V",
]

BASE_TRADIER = "https://api.tradier.com/v1"
TIMEOUT = 12

# ========= Sesión HTTP Tradier =========
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept": "application/json"})

def get_json(url, params=None, max_retries=3):
    for intento in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 429:
                espera = 2 * intento
                print(f"⏳ 429 rate limit. Reintentando en {espera}s…")
                time.sleep(espera)
                continue
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

# === Clasificador L (igual al script original) ===
def _clasificar_filtro_institucional(val_h: float, val_i: float) -> str:
    """
    Columna L minimal del script:
    - 'CALLS' si val_h > 0.5 e val_i > 0.4
    - 'PUTS'  si val_h < 0   e val_i < 0
    - ''      en otro caso
    """
    if (val_h > 0.5) and (val_i > 0.4):
        return "CALLS"
    if (val_h < 0) and (val_i < 0):
        return "PUTS"
    return ""

# ========= Estado (se conserva estructura con PrevH/PrevI) =========
def _ensure_estado_sheet(doc, nombre_estado: str):
    try:
        ws = doc.worksheet(nombre_estado)
    except gspread.exceptions.WorksheetNotFound:
        ws = doc.add_worksheet(title=nombre_estado, rows=600, cols=6)
        ws.update(values=[["Ticker","ColorOI","ColorVol","EstadoL","PrevH","PrevI"]], range_name="A1")
        return ws
    headers = ws.get_values("A1:F1")
    if not headers or len(headers[0]) < 6:
        ws.update(values=[["Ticker","ColorOI","ColorVol","EstadoL","PrevH","PrevI"]], range_name="A1")
    return ws

def _leer_estado(ws_estado):
    rows = ws_estado.get_all_values()
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
        try:
            prev_h = float(r[4]) if len(r) > 4 and r[4] != "" else None
        except:
            prev_h = None
        try:
            prev_i = float(r[5]) if len(r) > 5 and r[5] != "" else None
        except:
            prev_i = None
        d[t] = (c_oi, c_v, e_l, prev_h, prev_i)
    return d

def _escribir_estado(ws_estado, mapa):
    data = [["Ticker","ColorOI","ColorVol","EstadoL","PrevH","PrevI"]]
    for tk in sorted(mapa.keys()):
        c_oi, c_v, e_l, prev_h, prev_i = mapa[tk]
        data.append([tk, c_oi, c_v, e_l,
                     "" if prev_h is None else prev_h,
                     "" if prev_i is None else prev_i])
    ws_estado.batch_clear(["A2:F10000"])
    if len(data) > 1:
        ws_estado.update(values=data, range_name=f"A1:F{len(data)}")

# ========= Lógica expiraciones / datos (OI) =========
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
        except Exception:
            continue
    fechas_viernes.sort()
    if len(fechas_viernes) <= posicion_fecha:
        return None
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
            except Exception:
                continue
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
            if isinstance(opciones, dict):
                opciones = [opciones]

            if precio <= 0 and opciones:
                try:
                    under_px = float(opciones[0].get("underlying_price") or 0)
                    if under_px > 0:
                        precio = under_px
                except Exception:
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
                except Exception:
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
            viernes_ref_str,
        )
    except Exception as e:
        print(f"❌ Error con {ticker}: {e}")
        return 0, 0, 0.0, 0.0, 0, 0, None

# ========= SNAPSHOT "dia anterior" (se conserva) =========
NYSE_HOLIDAYS_2025 = {
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26",
    "2025-06-19", "2025-07-04", "2025-09-01", "2025-11-27", "2025-12-25",
}
_last_closed_log = None  # para no spamear

def _now_ny():
    ny = pytz.timezone("America/New_York")
    return datetime.now(timezone.utc).astimezone(ny)

def _is_trading_day(dt_ny):
    if dt_ny.weekday() >= 5:
        return False
    return dt_ny.strftime("%Y-%m-%d") not in NYSE_HOLIDAYS_2025

def _es_cierre_hora(dt_ny=None):
    """True exactamente cuando cierra la vela 1H (minuto == 0, zona NY)."""
    if dt_ny is None:
        dt_ny = _now_ny()
    return dt_ny.minute == 0

def _is_rth_open(dt_ny):
    t = dt_ny.time()
    return (t >= datetime.strptime("09:30:00", "%H:%M:%S").time()
            and t <= datetime.strptime("16:00:00", "%H:%M:%S").time())

def _ensure_sheet_generic(doc, title, rows=200, cols=20):
    try:
        return doc.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return doc.add_worksheet(title=title, rows=rows, cols=cols)

def _get_used_size(ws):
    vals = ws.get_all_values()
    if not vals:
        return 0, 0
    rows = len(vals)
    cols = max((len(r) for r in vals), default=0)
    return rows, cols

def _meta_read(ws_meta, key, default=""):
    vals = ws_meta.get_all_values()
    kv = {}
    for row in vals[1:]:
        if len(row) >= 2 and row[0]:
            kv[row[0]] = row[1]
    return kv.get(key, default)

def _meta_write(ws_meta, key, val):
    vals = ws_meta.get_all_values()
    if not vals:
        ws_meta.update([["key","value"]], "A1")
        vals = [["key","value"]]
    found_row = None
    for i, row in enumerate(vals[1:], start=2):
        if len(row) >= 1 and row[0] == key:
            found_row = i
            break
    if found_row is None:
        next_row = len(vals) + 1
        ws_meta.update([[key, val]], f"A{next_row}:B{next_row}")
    else:
        ws_meta.update([[key, val]], f"A{found_row}:B{found_row}")

def _batch_update(doc, body):
    doc.batch_update(body)

def _clear_range_format_and_values(doc, sheet_id, rows, cols):
    if rows == 0 or cols == 0:
        return
    _batch_update(doc, {
        "requests": [{
            "updateCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": rows,
                          "startColumnIndex": 0, "endColumnIndex": cols},
                "fields": "userEnteredValue,userEnteredFormat"
            }
        }]
    })

def _copy_values_and_formats(doc, src_sheet_id, dst_sheet_id, rows, cols):
    if rows == 0 or cols == 0:
        return
    _batch_update(doc, {
        "requests": [{
            "copyPaste": {
                "source":      {"sheetId": src_sheet_id, "startRowIndex": 0, "endRowIndex": rows,
                                "startColumnIndex": 0, "endColumnIndex": cols},
                "destination": {"sheetId": dst_sheet_id, "startRowIndex": 0, "endRowIndex": rows,
                                "startColumnIndex": 0, "endColumnIndex": cols},
                "pasteType": "PASTE_NORMAL",
                "pasteOrientation": "NORMAL"
            }
        }]
    })

def snapshot_congelado(doc_main):
    global _last_closed_log
    HOJA_ORIGEN = "Semana actual"
    HOJA_DEST   = "dia anterior"
    HOJA_META   = "META"

    now_ny = _now_ny()
    hoy = now_ny.strftime("%Y-%m-%d")

    if not _is_trading_day(now_ny) or not _is_rth_open(now_ny):
        if _last_closed_log != hoy:
            print("⏸️  Mercado cerrado o día no hábil → 'dia anterior' sigue congelado.", flush=True)
            _last_closed_log = hoy
        return False

    ws_src  = _ensure_sheet_generic(doc_main, HOJA_ORIGEN)
    ws_dest = _ensure_sheet_generic(doc_main, HOJA_DEST)
    ws_meta = _ensure_sheet_generic(doc_main, HOJA_META, rows=50, cols=2)

    last_session = _meta_read(ws_meta, "last_snapshot_session", "")
    if last_session == hoy:
        print("ℹ️  Snapshot ya hecho hoy. No se repite.", flush=True)
        return False

    used_rows, used_cols = _get_used_size(ws_src)
    if used_rows == 0 or used_cols == 0:
        print("ℹ️  Origen vacío. Nada para congelar.", flush=True)
        _meta_write(ws_meta, "last_snapshot_session", hoy)
        return False

    if ws_dest.row_count < used_rows or ws_dest.col_count < used_cols:
        ws_dest.resize(max(ws_dest.row_count, used_rows), max(ws_dest.col_count, used_cols))

    _clear_range_format_and_values(doc_main, ws_dest.id, used_rows, used_cols)
    _copy_values_and_formats(doc_main, ws_src.id, ws_dest.id, used_rows, used_cols)

    _meta_write(ws_meta, "last_snapshot_session", hoy)
    try:
        ws_dest.update([[f"Snapshot RTH: {hoy} (congelado hasta próxima sesión)"]], "N1")
    except Exception:
        pass

    print(f"✅ 'dia anterior' congelado desde 'Semana actual' — {used_rows}x{used_cols} @ {hoy} NY.", flush=True)
    return True

# ========= Helpers SNAP__<hoja> para N/O =========
def _ensure_snapshot_sheet(doc, nombre_snap: str):
    try:
        ws = doc.worksheet(nombre_snap)
    except gspread.exceptions.WorksheetNotFound:
        ws = doc.add_worksheet(title=nombre_snap, rows=1000, cols=4)
        ws.update(values=[["Ticker","N_prev","N_curr","ts"]], range_name="A1")
        return ws
    headers = ws.get_values("A1:D1")
    if not headers or len(headers[0]) < 4:
        ws.update(values=[["Ticker","N_prev","N_curr","ts"]], range_name="A1")
    return ws

def _leer_snap_both_map(ws_snap):
    """
    Devuelve {ticker: (N_prev, N_curr)} leyendo columnas B y C.
    """
    rows = ws_snap.get_all_values()
    d = {}
    for r in rows[1:]:
        if not r:
            continue
        tk = (r[0] or "").strip().upper()
        if not tk:
            continue
        try:
            n_prev = float(str(r[1]).replace(",", ".")) if len(r) > 1 and r[1] != "" else None
        except Exception:
            n_prev = None
        try:
            n_curr = float(str(r[2]).replace(",", ".")) if len(r) > 2 and r[2] != "" else None
        except Exception:
            n_curr = None
        d[tk] = (n_prev, n_curr)
    return d
def _parse_ny_naive(ts_str):
    if not ts_str:
        return None
    try:
        ny = pytz.timezone("America/New_York")
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        return ny.localize(dt)
    except Exception:
        return None

# ========= Escritura en Google Sheets (incluye L, M, N, O, P, Q) =========
def actualizar_hoja(doc, sheet_title, posicion_fecha):
    # --- Abrir hoja destino ---
    try:
        ws = doc.worksheet(sheet_title)
    except WorksheetNotFound:
        ws = doc.add_worksheet(title=sheet_title, rows=1200, cols=20)

    # Hora NY
    now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
    ny_tz = pytz.timezone("America/New_York")
    now_ny = now_utc.astimezone(ny_tz)
    fecha_txt = f"{now_ny:%Y-%m-%d}"
    hora_txt = now_ny.strftime("%H:%M:%S")

    # ⚡ Ventana de "flash" M (5 minutos post-cierre de hora)
    ws_meta = _ensure_sheet_generic(doc, "META", rows=50, cols=2)
    flash_key = f"m_flash_until__{sheet_title}"
    flash_until_str = _meta_read(ws_meta, flash_key, "")
    flash_until_dt = _parse_ny_naive(flash_until_str)
    within_flash = (flash_until_dt is not None) and (now_ny <= flash_until_dt)

    print(f"[debug] UTC={now_utc:%Y-%m-%d %H:%M:%S} | NY={now_ny:%Y-%m-%d %H:%M:%S}", flush=True)
    print(f"⏳ Actualizando: {sheet_title} (venc. #{posicion_fecha+1})", flush=True)


    # Estado previo (colores/prevH/prevI)
    nombre_estado = f"ESTADO__{sheet_title}"
    ws_estado = _ensure_estado_sheet(doc, nombre_estado)
    estado_prev = _leer_estado(ws_estado)              # {tk: (colorOI, colorVol, estadoL, prev_h, prev_i)}
    estado_nuevo = {}
    cambios_por_ticker = {}

    # SNAP para N/O
    nombre_snap = f"SNAP__{sheet_title}"
    ws_snap = _ensure_snapshot_sheet(doc, nombre_snap)
    snap_map = _leer_snap_both_map(ws_snap)            # {tk: (O, N)}

    # Recolecta datos OI por ticker
    datos = []
    for tk in TICKERS:
        oi_c, oi_p, m_c, m_p, v_c, v_p, exp = obtener_dinero(tk, posicion_fecha)
        datos.append([tk, "CALL", m_c, v_c, exp, oi_c])
        datos.append([tk, "PUT",  m_p, v_p, exp, oi_p])
        time.sleep(0.15)

    # A1: fecha visible
    exp_dates = []
    for _, _, _, _, exp_vto, _ in datos:
        if exp_vto:
            try:
                exp_dates.append(_dt.strptime(exp_vto, "%Y-%m-%d").date())
            except Exception:
                pass
    ultima_exp_str = max(exp_dates).strftime("%Y-%m-%d") if exp_dates else None

    def _calc_friday_from_today(pos_index: int) -> str:
        base = now_ny.date()
        days_to_fri = (4 - base.weekday()) % 7
        if days_to_fri == 0:
            days_to_fri = 7
        first_friday = base + _td(days=days_to_fri)
        target = first_friday + _td(days=7 * pos_index)
        return target.strftime("%Y-%m-%d")

    title_norm = ws.title.strip().lower()
    if title_norm in ("semana actual", "semana siguiente"):
        a1_value = ultima_exp_str or _calc_friday_from_today(posicion_fecha)
    else:
        a1_value = fecha_txt

    try:
        print(f"[OI] Hoja='{ws.title}' A1 <- {a1_value} (pos={posicion_fecha}, exp_max={ultima_exp_str})", flush=True)
        ws.update_cell(1, 1, a1_value)
    except Exception as e:
        print(f"⚠️ No pude escribir A1 en '{ws.title}': {e}", flush=True)

    # Encabezado completo A..Q
    encabezado = [[
        "Fecha", "Hora", "Ticker",                 # A-C
        "Trade Cnt VERDE", "Trade Cnt ROJO",       # D-E (dinero CALL / PUT en MM)
        "VOLUMEN ENTRA", "VOLUMEN SALE",           # F-G (volumen CALL/PUT)
        "TENDENCIA Trade Cnt.", "VOLUMEN.",        # H-I (% normalizado de dinero/volumen)
        "Fuerza",                                  # J (copia H)
        "Relación",                                # K (bolitas)
        "Filtro institucional",                    # L (CALLS/PUTS/"")
        "🔥",                                       # M (3 fuegos si |Q|>=0.5)
        "D−E (N)",                                 # N (SNAP N_curr)
        "N previo (O)",                            # O (SNAP N_prev)
        "Δ (P=N−O)",                               # P
        "Δ% (Q=P/O)"                               # Q
    ]]
    ws.update(values=encabezado, range_name="A2:Q2")
    try:
        ws.batch_clear(["A3:Q1000"])
    except Exception as e:
        print(f"⚠️ No se pudo limpiar A3:Q1000 en {ws.title}: {e}")

    # Agregado por ticker
    agg = _dd(lambda: {"CALL": [0.0, 0], "PUT": [0.0, 0], "EXP": None})
    for tk, side, m_usd, vol, exp, _oi in datos:
        if not agg[tk]["EXP"] and exp:
            agg[tk]["EXP"] = exp
        agg[tk][side][0] += m_usd
        agg[tk][side][1] += vol

    # Construcción de filas + L,M,N,O,P,Q
    filas = []
    for tk in sorted(agg.keys()):
        m_call, v_call = agg[tk]["CALL"]
        m_put,  v_put  = agg[tk]["PUT"]

        # % dif normalizada dinero (H / J)
        if m_call <= 0 and m_put <= 0:
            diff_m = fuerza = 0.0
        else:
            try:
                diff_m = (m_call - m_put) / max(m_call, m_put) * 100
            except ZeroDivisionError:
                diff_m = 0.0
            fuerza = diff_m

        # % dif normalizada volumen (I)
        if v_call <= 0 and v_put <= 0:
            diff_v = fuerza_vol = 0.0
        else:
            try:
                diff_v = (v_call - v_put) / max(v_call, v_put) * 100
            except ZeroDivisionError:
                diff_v = 0.0
            fuerza_vol = diff_v

        color_oi  = "🟢" if fuerza     > 0 else "🔴" if fuerza     < 0 else "⚪"
        color_vol = "🟢" if fuerza_vol > 0 else "🔴" if fuerza_vol < 0 else "⚪"
        relacion  = color_oi + color_vol

        # L (igual al script original, usando H/I en DECIMAL)
        val_h = (diff_m or 0.0) / 100.0
        val_i = (diff_v or 0.0) / 100.0
        clasif_L = _clasificar_filtro_institucional(val_h, val_i)

        # Estado anterior para banderas visuales (H/I/L)
        prev_oi, prev_vol, prev_l, prev_h, prev_i = estado_prev.get(tk, ("", "", "", None, None))
        cambio_oi  = (prev_oi  != "") and (prev_oi  != color_oi)
        cambio_vol = (prev_vol != "") and (prev_vol != color_vol)
        es_alineado = clasif_L in ("CALLS", "PUTS")
        cambio_L = es_alineado and (clasif_L != prev_l)

        # --- N, O desde SNAP + P, Q + M (3 fuegos por Q) ---
        O_prev, N_curr = snap_map.get(tk, (None, None))   # O, N
        P_delta = 0.0
        Q_rel = 0.0
        if (N_curr is not None) and (O_prev is not None):
            P_delta = N_curr - O_prev
            Q_rel = (P_delta / O_prev) if O_prev not in (0, None) else 0.0

        M_text = "🔥🔥🔥" if within_flash and (abs(Q_rel) >= 0.5) else ""

        filas.append({
            "tk": tk,
            "exp": (agg[tk]["EXP"] or a1_value or fecha_txt),
            "hora": hora_txt,
            "m_call": m_call, "m_put": m_put,
            "v_call": v_call, "v_put": v_put,
            "diff_m": diff_m, "diff_v": diff_v,
            "val_h": val_h, "val_i": val_i,
            "rel": relacion, "L": clasif_L, "M": M_text,
            "N": N_curr, "O": O_prev, "P": P_delta, "Q": Q_rel,
            "cambio_oi": cambio_oi, "cambio_vol": cambio_vol, "cambio_L": cambio_L,
        })

        # Guardar estado para siguiente corrida
        estado_nuevo[tk] = (color_oi, color_vol, clasif_L, val_h, val_i)

    # Ordenar por Fuerza (J)
    def fuerza_to_float(s):
        try:
            return float(str(s).replace("%", "").replace(",", "."))
        except Exception:
            return -9999.0

    filas.sort(key=lambda r: -fuerza_to_float(r["diff_m"]))

    # Preparar matriz de escritura A..Q
    resumen = []
    for r in filas:
        resumen.append([
            r["exp"], r["hora"], r["tk"],                   # A-C
            fmt_millones(r["m_call"]),                      # D
            fmt_millones(r["m_put"]),                       # E
            fmt_entero_miles(r["v_call"]),                  # F
            fmt_entero_miles(r["v_put"]),                   # G
            pct_str(r["diff_m"]),                           # H
            pct_str(r["diff_v"]),                           # I
            pct_str(r["diff_m"]),                           # J (Fuerza)
            r["rel"],                                       # K
            r["L"],                                         # L
            r["M"],                                         # M (texto)
            "" if r["N"] is None else r["N"],               # N (num)
            "" if r["O"] is None else r["O"],               # O (num)
            r["P"],                                         # P (num)
            r["Q"],                                         # Q (num)
        ])
        cambios_por_ticker[r["tk"]] = (r["cambio_oi"], r["cambio_vol"], r["cambio_L"])

    if resumen:
        ws.update(values=resumen, range_name=f"A3:Q{len(resumen)+2}", value_input_option="USER_ENTERED")

    # === Formato: % en H/I/J/Q; color H/I/L; fondo dinámico M según (P,Q) ===
    if resumen:
        sheet_id = ws.id
        start_row = 2   # 0-based para fila 3
        total_rows = len(resumen)
        requests_fmt = []

        # % en H, I, J, Q
        requests_fmt += [
            {  # H
                "repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": start_row,
                                         "endRowIndex": start_row + total_rows,
                                         "startColumnIndex": 7, "endColumnIndex": 8},
                               "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                               "fields": "userEnteredFormat.numberFormat"}
            },
            {  # I
                "repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": start_row,
                                         "endRowIndex": start_row + total_rows,
                                         "startColumnIndex": 8, "endColumnIndex": 9},
                               "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                               "fields": "userEnteredFormat.numberFormat"}
            },
            {  # J
                "repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": start_row,
                                         "endRowIndex": start_row + total_rows,
                                         "startColumnIndex": 9, "endColumnIndex": 10},
                               "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                               "fields": "userEnteredFormat.numberFormat"}
            },
            {  # Q
                "repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": start_row,
                                         "endRowIndex": start_row + total_rows,
                                         "startColumnIndex": 16, "endColumnIndex": 17},
                               "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                               "fields": "userEnteredFormat.numberFormat"}
            },
            {  # limpiar fondos H..M
                "repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": start_row,
                                         "endRowIndex": start_row + total_rows,
                                         "startColumnIndex": 7, "endColumnIndex": 13},
                               "cell": {"userEnteredFormat": {"backgroundColor": {"red":1,"green":1,"blue":1}}},
                               "fields": "userEnteredFormat.backgroundColor"}
            },
            {  # centrar M
                "repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": start_row,
                                         "endRowIndex": start_row + total_rows,
                                         "startColumnIndex": 12, "endColumnIndex": 13},
                               "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                               "fields": "userEnteredFormat.horizontalAlignment"}
            },
        ]

        verde    = {"red": 0.80, "green": 1.00, "blue": 0.80}
        rojo     = {"red": 1.00, "green": 0.80, "blue": 0.80}
        amarillo = {"red": 1.00, "green": 1.00, "blue": 0.60}
        blanco   = {"red": 1.00, "green": 1.00, "blue": 1.00}

        def fuerza_to_float_local(s):
            try:
                return float(str(s).replace("%", "").replace(",", "."))
            except Exception:
                return 0.0

        # === Por-fila: coloreo de L, H e I ===
        for idx, row in enumerate(resumen):
            tk = str(row[2]).strip().upper()
            ch_oi, ch_vol, ch_L = cambios_por_ticker.get(tk, (False, False, False))

            # L
            clasif_L = str(row[11])
            if clasif_L == "CALLS":
                bg_l = verde
            elif clasif_L == "PUTS":
                bg_l = rojo
            else:
                bg_l = blanco
            if ch_L:
                bg_l = amarillo

            requests_fmt.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id,
                              "startRowIndex": start_row + idx, "endRowIndex": start_row + idx + 1,
                              "startColumnIndex": 11, "endColumnIndex": 12},
                    "cell": {"userEnteredFormat": {"backgroundColor": bg_l}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

            # H / I por signo (+ amarillo si hubo cambio)
            val_h = fuerza_to_float_local(row[7])
            val_i = fuerza_to_float_local(row[8])
            bg_h = verde if val_h > 0 else rojo if val_h < 0 else blanco
            bg_i = verde if val_i > 0 else rojo if val_i < 0 else blanco
            if ch_oi:
                bg_h = amarillo
            if ch_vol:
                bg_i = amarillo

            requests_fmt += [
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

        # Base de M = blanco (rango completo)
        requests_fmt.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": start_row,
                          "endRowIndex": start_row + total_rows,
                          "startColumnIndex": 12, "endColumnIndex": 13},
                "cell": {"userEnteredFormat": {"backgroundColor": blanco}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

        # Condicionales de M: verde si "🔥🔥🔥" y P>0 ; rojo si "🔥🔥🔥" y P<0
        requests_fmt += [
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": start_row,
                            "endRowIndex": start_row + total_rows,
                            "startColumnIndex": 12,
                            "endColumnIndex": 13
                        }],
                        "booleanRule": {
                            "condition": {"type": "CUSTOM_FORMULA",
                                          "values": [{"userEnteredValue": "=Y($M3=\"🔥🔥🔥\";$P3>0)"}]},
                            "format": {"backgroundColor": verde}
                        }
                    },
                    "index": 0
                }
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": start_row,
                            "endRowIndex": start_row + total_rows,
                            "startColumnIndex": 12,
                            "endColumnIndex": 13
                        }],
                        "booleanRule": {
                            "condition": {"type": "CUSTOM_FORMULA",
                                          "values": [{"userEnteredValue": "=Y($M3=\"🔥🔥🔥\";$P3<0)"}]},
                            "format": {"backgroundColor": rojo}
                        }
                    },
                    "index": 0
                }
            }
        ]

        if requests_fmt:
            ws.spreadsheet.batch_update({"requests": requests_fmt})

    # === SNAP: actualizar SOLO en cierre de HORA NY ===
    ws_meta = _ensure_sheet_generic(doc, "META", rows=50, cols=2)
    hour_key  = f"last_hour_snap__{sheet_title}"
    curr_hour = now_ny.strftime("%Y-%m-%d %H")
    last_hour = _meta_read(ws_meta, hour_key, "")

    if _is_trading_day(now_ny) and _is_rth_open(now_ny) and _es_cierre_hora(now_ny) and last_hour != curr_hour:
        # N_new = m_call - m_put (1 decimal) por ticker
        n_new_map = {r["tk"]: round(r["m_call"] - r["m_put"], 1) for r in filas}
        ts_now = now_ny.strftime("%Y-%m-%d %H:%M:%S")

        # armar tabla completa ordenada por ticker
        data = [["Ticker","N_prev","N_curr","ts"]]
        for tk in sorted(n_new_map.keys()):
            prev_O, prev_N = snap_map.get(tk, (None, None))
            n_prev = prev_N if prev_N is not None else ""
            n_curr = n_new_map[tk]
            data.append([tk, n_prev, n_curr, ts_now])

        try:
            ws_snap.batch_clear(["A2:D10000"])
        except Exception:
            pass
        ws_snap.update(values=data, range_name=f"A1:D{len(data)}")
        _meta_write(ws_meta, hour_key, curr_hour)
        print(f"🧊 SNAP 1H actualizado ({nombre_snap}) @ {ts_now} NY (cierre de hora).", flush=True)

        # 👉 ventana de 5 minutos para M SOLO al cierre de hora
        flash_until = (now_ny + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
        _meta_write(ws_meta, flash_key, flash_until)
        print(f"⏱️ Señal M activa hasta {flash_until} NY para {sheet_title}", flush=True)

    # Persistir estado
    _escribir_estado(ws_estado, estado_nuevo)

    # Devolver mapeo L por ticker (como antes)
    return {r["tk"]: r["L"] for r in filas} if filas else {}

# ========= ACCESOS — helpers existentes (se mantienen) =========
def S(v) -> str:
    if v is None:
        return ""
    try:
        return str(v).strip()
    except Exception:
        return ""

def _iso(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )

def _col_indexes(ws):
    headers = [S(h).lower() for h in ws.row_values(1)]
    def col(name):
        name = name.strip().lower()
        try:
            return headers.index(name) + 1
        except ValueError:
            raise RuntimeError(f"Encabezado '{name}' no encontrado en {ws.title}.")
    return {
        "email": col("email"),
        "duracion": col("duracion"),
        "rol": col("rol"),
        "creado_utc": col("creado_utc"),
        "expira_utc": col("expira_utc"),
        "estado": col("estado"),
        "perm_id": col("perm_id"),
        "nota": col("nota"),
    }

# ========= ACCESOS — modo DRIVE (sin cambios de comportamiento) =========
def _parse_duration_drive(s: str) -> timedelta:
    s = S(s).lower()
    if not s:
        return timedelta(hours=24)
    total_h = 0.0
    for num, unit in re.findall(r"(\d+(?:\.\d+)?)([dh])", s):
        n = float(num)
        total_h += n * (24 if unit == "d" else 1)
    if total_h == 0:
        try:
            total_h = float(s)
        except Exception:
            total_h = 24.0
    return timedelta(hours=total_h)

def _grant_with_optional_exp(email: str, role: str, exp_dt: datetime, send_mail=True, email_message=None):
    base = {"type": "user", "role": role, "emailAddress": email}
    if email_message:
        base["emailMessage"] = email_message
    try:
        body = {**base, "expirationTime": exp_dt.replace(microsecond=0).isoformat()}
        created = (
            drive.permissions()
            .create(fileId=MAIN_FILE_ID, body=body, fields="id", sendNotificationEmail=send_mail)
            .execute()
        )
        return created["id"], "OK"
    except HttpError as e:
        msg = str(e)
        if "cannotSetExpiration" in msg:
            created = (
                drive.permissions()
                .create(fileId=MAIN_FILE_ID, body=base, fields="id", sendNotificationEmail=send_mail)
                .execute()
            )
            return created["id"], "NO_EXP"
        if "invalidSharingRequest" in msg:
            created = (
                drive.permissions()
                .create(fileId=MAIN_FILE_ID, body=base, fields="id", sendNotificationEmail=True)
                .execute()
            )
            return created["id"], "NOTIFIED"
        raise

def _revoke_by_id(perm_id: str):
    drive.permissions().delete(fileId=MAIN_FILE_ID, permissionId=perm_id).execute()

def _get_sa_email_from_env_info() -> str:
    try:
        return S(_creds_info.get("client_email", "")).lower()
    except Exception:
        try:
            return S(json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON", "")).get("client_email", "")).lower()
        except Exception:
            return ""

def procesar_autorizados_drive(accesos_doc, main_file_url):
    hoja_aut = accesos_doc.worksheet(ACCESS_SHEET_TITLE)
    cols = _col_indexes(hoja_aut)
    rows = hoja_aut.get_all_records(default_blank="")
    now = datetime.now(timezone.utc)
    sa_email = _get_sa_email_from_env_info()
    send_mail = (os.getenv("SEND_SHARE_EMAIL", "true").strip().lower() != "false")

    perms = (
        drive.permissions()
        .list(fileId=MAIN_FILE_ID, fields="permissions(id,emailAddress,role,type)")
        .execute()
        .get("permissions", [])
    )
    by_email = {(
        S(p.get("emailAddress")).lower()
    ): p for p in perms if S(p.get("type")).lower() == "user"}

    activados = revocados = sincronizados = 0

    for idx, r in enumerate(rows, start=2):
        email = S(r.get("email")).lower()
        dur_txt = S(r.get("duracion"))
        rol_in = S(r.get("rol")).lower() or "reader"
        estado = S(r.get("estado")).upper()
        perm_id = S(r.get("perm_id"))
        expira = S(r.get("expira_utc"))
        nota = S(r.get("nota"))
        if not email:
            continue

        role = rol_in if rol_in in ("reader", "commenter", "writer") else "reader"

        if email == sa_email:
            hoja_aut.update_cell(idx, cols["nota"], "IGNORADO (service account)")
            continue

        # Revocado manual
        if estado == "REVOCADO":
            pid = perm_id or (by_email.get(email) or {}).get("id")
            try:
                if pid:
                    try:
                        _revoke_by_id(pid)
                    except HttpError as e:
                        if getattr(e, "resp", None) and getattr(e.resp, "status", None) in (404, 400):
                            pass
                        else:
                            raise
                hoja_aut.update_cell(idx, cols["perm_id"], "")
                hoja_aut.update_cell(idx, cols["nota"], "Revocado (manual o ya no existía)")
                by_email.pop(email, None)
                revocados += 1
            except Exception as e:
                hoja_aut.update_cell(idx, cols["nota"], f"ERROR revoke: {e}")
            continue

        # Alta
        if estado in ("", "PENDIENTE"):
            try:
                dur_td = _parse_duration_drive(dur_txt)
                exp_dt = now + dur_td
                pid, modo = _grant_with_optional_exp(email, role, exp_dt, send_mail=send_mail)
                hoja_aut.update_cell(idx, cols["creado_utc"], _iso(now))
                hoja_aut.update_cell(idx, cols["expira_utc"], _iso(exp_dt))
                hoja_aut.update_cell(idx, cols["estado"], "ACTIVO")
                hoja_aut.update_cell(idx, cols["perm_id"], pid)
                hoja_aut.update_cell(idx, cols["nota"], f"Concedido ({modo})")
                by_email[email] = {"id": pid}
                activados += 1
            except Exception as e:
                hoja_aut.update_cell(idx, cols["nota"], f"ERROR grant: {e}")
            continue

        # Activo → sync + vencimiento
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
                    if exp_dt.tzinfo is None:
                        exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    hoja_aut.update_cell(idx, cols["nota"], f"ERROR_PARSE_EXP: {expira}")
                    continue

                if datetime.now(timezone.utc) >= exp_dt:
                    try:
                        pid = (by_email.get(email) or {}).get("id") or perm_id
                        if pid:
                            _revoke_by_id(pid)
                        hoja_aut.update_cell(idx, cols["estado"], "REVOCADO")
                        hoja_aut.update_cell(idx, cols["perm_id"], "")
                        hoja_aut.update_cell(idx, cols["nota"], "Vencimiento automático")
                        by_email.pop(email, None)
                        revocados += 1
                    except Exception as e:
                        hoja_aut.update_cell(idx, cols["nota"], f"ERROR_REVOKE: {e}")

    print(f"✅ AUTORIZADOS (drive) → activados: {activados} | sincronizados: {sincronizados} | revocados: {revocados}")
    return {"activados": activados, "revocados": revocados}

# ========= ACCESOS — modo GROUPS (opcional; se conserva) =========
def _parse_duration_groups(txt):
    txt = (txt or "").strip().lower()
    if txt.endswith("h"):
        return timedelta(hours=int(txt[:-1] or 24))
    if txt.endswith("d"):
        return timedelta(days=int(txt[:-1] or 1))
    if txt.endswith("w"):
        return timedelta(weeks=int(txt[:-1] or 1))
    return timedelta(hours=24)

def _ensure_groups_only_on_file():
    reader_grp = os.getenv("GROUP_READER_EMAIL", "").strip().lower()
    commenter_grp = os.getenv("GROUP_COMMENTER_EMAIL", "").strip().lower()

    perms = (
        drive.permissions()
        .list(fileId=MAIN_FILE_ID, fields="permissions(id,emailAddress,role,type)")
        .execute()
        .get("permissions", [])
    )

    def _S(v): return S(v).lower()
    sa_email = ""
    try:
        sa_email = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON", "")).get("client_email", "").lower()
    except Exception:
        pass

    have_reader = have_commenter = False
    for p in perms:
        p_type = _S(p.get("type"))
        p_id   = S(p.get("id"))
        p_mail = _S(p.get("emailAddress"))
        p_role = _S(p.get("role"))

        if p_type == "group":
            if p_mail == reader_grp:    have_reader = True
            if p_mail == commenter_grp: have_commenter = True

        if p_type == "user" and p_id:
            if p_role in ("owner", "organizer"):
                print(f"🔒 Mantengo OWNER/ORGANIZER {p_mail}")
                continue
            if sa_email and p_mail == sa_email:
                print(f"🔒 Mantengo Service Account {p_mail}")
                continue
            try:
                drive.permissions().delete(fileId=MAIN_FILE_ID, permissionId=p_id).execute()
                print(f"🧹 Borrado permiso USER {p_mail}")
            except Exception as e:
                print(f"⚠️ No pude borrar USER {p_mail}: {e}")

    def _attach_group(group_email, role):
        if not group_email:
            return
        try:
            drive.permissions().create(
                fileId=MAIN_FILE_ID,
                body={"type": "group", "role": role, "emailAddress": group_email},
                fields="id",
                sendNotificationEmail=False,
            ).execute()
            print(f"✅ Adjuntado grupo {group_email} como {role}")
        except HttpError as e:
            msg = str(e).lower()
            if "alreadyexists" in msg or "duplicate" in msg:
                print(f"ℹ️ Grupo {group_email} ya estaba adjunto")
            else:
                raise

    if reader_grp and not have_reader:
        _attach_group(reader_grp, "reader")
    if commenter_grp and not have_commenter:
        _attach_group(commenter_grp, "commenter")

def remove_member(directory, group_email, user_email):
    if not directory or not group_email or not user_email:
        return "skip"
    try:
        directory.members().delete(groupKey=group_email, memberKey=user_email).execute()
        return "ok"
    except HttpError as e:
        msg = str(e).lower()
        if "not found" in msg or "resource not found" in msg:
            return "not_member"
        raise

def procesar_autorizados_groups(accesos_doc, main_file_url):
    try:
        hoja_aut = accesos_doc.worksheet(ACCESS_SHEET_TITLE)
    except gspread.exceptions.WorksheetNotFound:
        hoja_aut = accesos_doc.add_worksheet(title=ACCESS_SHEET_TITLE, rows=500, cols=8)
        hoja_aut.update(values=[["email", "duracion", "rol", "creado_utc", "expira_utc", "estado", "perm_id", "nota"]], range_name="A1")

    rows = hoja_aut.get_all_values()
    if not rows or len(rows) == 1:
        print("ℹ️ AUTORIZADOS vacío.")
        return {"activados": 0, "revocados": 0}

    _ensure_groups_only_on_file()

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

        def grupo_para_rol(rol):
            return GROUP_COMMENTER_EMAIL if (rol or "reader").lower() == "commenter" else GROUP_READER_EMAIL

        def add_member(group_email, user_email):
            if not directory:
                return "skip"
            try:
                directory.members().insert(groupKey=group_email, body={"email": user_email, "role": "MEMBER"}).execute()
                return "ok"
            except HttpError as e:
                msg = str(e).lower()
                if any(s in msg for s in ("duplicate", "memberexists", "already exists")):
                    return "ya_miembro"
                raise

        activados = revocados = 0
        now_utc = datetime.now(timezone.utc)

        sa_email = ""
        try:
            sa_email = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON", "")).get("client_email", "").lower()
        except Exception:
            sa_email = ""

        for i, raw in enumerate(rows[1:], start=2):
            row = (raw + [""] * 8)[:8]
            email, dur_txt, rol, creado, expira, estado, perm_id, nota = [(c or "").strip() for c in row]
            if not any([email, dur_txt, rol, creado, expira, estado, perm_id, nota]):
                continue

            if sa_email and email.lower() == sa_email:
                if nota != "IGNORADO (service account)":
                    hoja_aut.update(values=[["IGNORADO (service account)"]], range_name=f"H{i}")
                continue

            r = (rol or "reader").lower()
            if r not in ("reader", "commenter"):
                r = "reader"
            est = (estado or "").lower()
            grupo = grupo_para_rol(r)

            if est in ("", "pendiente"):
                result = add_member(grupo, email)
                exp_dt = now_utc + _parse_duration_groups(dur_txt or "24h")
                now_iso = now_utc.isoformat(timespec="seconds")
                hoja_aut.update(values=[[now_iso, exp_dt.isoformat(timespec="seconds"), "ACTIVO", f"group:{grupo}"]], range_name=f"D{i}:G{i}")
                hoja_aut.update(values=[[f"Miembro en {grupo}. Link: {main_file_url}"]], range_name=f"H{i}")
                activados += 1
                print(("✅ ACTIVADO " if result == "ok" else "ℹ️ (Idempotente) ") + f"{email} en {grupo} hasta {exp_dt} UTC")
                time.sleep(1.0)
                continue

            if est == "revocado":
                r1 = remove_member(directory, GROUP_READER_EMAIL, email)
                r2 = remove_member(directory, GROUP_COMMENTER_EMAIL, email)
                hoja_aut.update(values=[[f"Revocado (manual) — {r1}/{r2}"]], range_name=f"H{i}")
                revocados += 1
                continue

            if est == "activo" and expira:
                try:
                    exp_dt = datetime.fromisoformat(expira.replace("Z", ""))
                    if exp_dt.tzinfo is None:
                        exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    hoja_aut.update(values=[[f"ERROR_PARSE_EXP: {expira}"]], range_name=f"H{i}")
                    continue

                if now_utc >= exp_dt:
                    r1 = remove_member(directory, GROUP_READER_EMAIL, email)
                    r2 = remove_member(directory, GROUP_COMMENTER_EMAIL, email)
                    hoja_aut.update(values=[["REVOCADO"]], range_name=f"F{i}")
                    hoja_aut.update(values=[[f"Vencimiento automático — {r1}/{r2}"]], range_name=f"H{i}")
                    revocados += 1
                    print(f"🗑️ REVOCADO {email} (vencido)")

        return {"activados": activados, "revocados": revocados}

    except Exception as e:
        print(f"⚠️ procesar_autorizados_groups: {e}")
        return {"activados": 0, "revocados": 0}

# ========= Wrapper accesos =========
def procesar_autorizados(accesos_doc, main_file_url):
    mode = os.getenv("ACCESS_MODE", "drive").strip().lower()
    if mode == "groups":
        return procesar_autorizados_groups(accesos_doc, main_file_url)
    return procesar_autorizados_drive(accesos_doc, main_file_url)

# ========= Runner de UNA corrida =========
def run_once(skip_oi: bool = False):
    doc_main = client.open_by_key(MAIN_FILE_ID)
    accesos = client.open_by_key(ACCESS_FILE_ID)
    main_url = f"https://docs.google.com/spreadsheets/d/{MAIN_FILE_ID}/edit"

    # Snapshot “día anterior” (idempotente por día)
    try:
        snapshot_congelado(doc_main)
    except Exception as e:
        print(f"⚠️ snapshot_congelado() falló: {e}", flush=True)

    if not skip_oi:
        l_vto1 = actualizar_hoja(doc_main, "Semana actual", posicion_fecha=0)
        l_vto2 = actualizar_hoja(doc_main, "Semana siguiente", posicion_fecha=1)
        try:
            print("SERVICIO_IO::L_SEMANA_ACTUAL=", json.dumps(l_vto1, ensure_ascii=False), flush=True)
            print("SERVICIO_IO::L_SEMANA_SIGUIENTE=", json.dumps(l_vto2, ensure_ascii=False), flush=True)
        except Exception:
            pass
    else:
        l_vto1, l_vto2 = {}, {}

    acc = procesar_autorizados(accesos, main_url)
    return {
        "ok": True,
        "main_title": doc_main.title,
        "access_title": accesos.title,
        "activados": acc.get("activados", 0),
        "revocados": acc.get("revocados", 0),
        "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "mode": os.getenv("ACCESS_MODE", "drive"),
        "skipped_oi": skip_oi,
        "L_semana_actual": l_vto1,
        "L_semana_siguiente": l_vto2,
    }

# ========= Flask async guards =========
def _authorized(req: request) -> bool:
    if not OI_SECRET:
        return True
    return req.headers.get("X-Auth-Token", "") == OI_SECRET

def _run_guarded():
    file_lock = _acquire_lock()
    if not file_lock:
        print("⏳ [/update] Ya hay una ejecución en curso (lock inter-proceso); se omite.", flush=True)
        return
    try:
        print("🚀 [update] Inicio actualización OI", flush=True)
        run_once()
        print("✅ [update] Fin actualización OI", flush=True)
    except Exception as e:
        print(f"❌ [/update] Error: {repr(e)}", flush=True)
    finally:
        fcntl.flock(file_lock, fcntl.LOCK_UN)
        file_lock.close()
        print(f"🟣 [/update] Hilo terminado @ {datetime.utcnow().isoformat()}Z", flush=True)

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
            fcntl.flock(file_lock, fcntl.LOCK_UN)
            file_lock.close()
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
        accesos = client.open_by_key(ACCESS_FILE_ID)
        main_url = f"https://docs.google.com/spreadsheets/d/{MAIN_FILE_ID}/edit"
        acc = procesar_autorizados(accesos, main_url)
        print(f"✅ [/apply_access] ok: {acc}", flush=True)
        return jsonify({"ok": True, **acc}), 200
    finally:
        fcntl.flock(file_lock, fcntl.LOCK_UN)
        file_lock.close()

if __name__ == "__main__":
    # Ejecuta local: http://127.0.0.1:8080/run
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
