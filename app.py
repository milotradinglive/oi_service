# ========= Actualización de una hoja objetivo (anti-429) =========
# ⬆️ (ESTA es la “línea antes”. Reemplaza TODO lo que tengas debajo de esta línea
#    hasta antes de:  # ========= Runner de UNA corrida =========
#    por el bloque completo de abajo)

def actualizar_hoja(doc, sheet_title, posicion_fecha, now_ny_base=None):
    """
    Milo — Actualiza una hoja (Semana actual / Semana siguiente) con:
    - Reset selectivo de CF viejo en columnas % (O,S,W,AA)
    - CF nuevo por entrada de dinero (Δ) en N/R/V/Z
    - Cálculo + escritura de tabla principal
    - Escritura de snapshots 5m/15m/1h/1d usando cache en memoria (sin lecturas por ciclo)
    - Persistencia de estado por ticker (ESTADO__<sheet>)
    """
    try:
        ws = _retry(lambda: doc.worksheet(sheet_title))
    except WorksheetNotFound:
        ws = _retry(lambda: doc.add_worksheet(title=sheet_title, rows=2000, cols=27))

    ws_meta = _ensure_sheet_generic(doc, "META", rows=50, cols=2)

    # 1) Limpiar SOLO CF viejo de % (O,S,W,AA)
    _reset_cf_for_columns(
        ws,
        start_row_idx=2,
        end_row_idx=2000,
        cols_0idx=[14, 18, 22, 26],  # O, S, W, AA (0-index)
        ws_meta=ws_meta,
        sheet_title=sheet_title
    )

    # 2) Aplicar CF nuevo por entrada de dinero (Δ) en N/R/V/Z
    _apply_cf_inflow_thresholds(ws, sheet_title, ws_meta)

    # 3) Tiempo base
    ny = now_ny_base or _now_ny()
    fecha_txt = f"{ny:%Y-%m-%d}"
    hora_txt = ny.strftime("%H:%M:%S")
    print(f"⏳ Actualizando: {sheet_title} (venc. #{posicion_fecha+1}) — NY {fecha_txt} {hora_txt}")

    # ===== Estado previo =====
    nombre_estado = f"ESTADO__{sheet_title}"
    ws_estado = _ensure_estado_sheet(doc, nombre_estado)
    estado_prev = _leer_estado(ws_estado)
    estado_nuevo = {}
    cambios_por_ticker = {}

    # ===== Snapshot sheets =====
    ws_snap5m = _ensure_snapshot_sheet(doc, f"SNAP_5min__{sheet_title}")
    ws_snap15 = _ensure_snapshot_sheet(doc, f"SNAP__{sheet_title}")
    ws_snap1h = _ensure_snapshot_sheet(doc, f"SNAP_H1__{sheet_title}")
    ws_snap_d0800 = _ensure_snapshot_sheet(doc, f"SNAP_d0800__{sheet_title}")  # 1d @ 08:00 NY
    ws_snap_d1550 = _ensure_snapshot_sheet(doc, f"SNAP_d1550__{sheet_title}")  # 1d @ 15:50 NY

    # ===== Caches (sin lecturas por ciclo) =====
    cache_5m = _get_cache_for(ws_snap5m)
    cache_15m = _get_cache_for(ws_snap15)
    cache_h1 = _get_cache_for(ws_snap1h)
    cache_d0800 = _get_cache_for(ws_snap_d0800)
    cache_d1550 = _get_cache_for(ws_snap_d1550)

    actualiza_d0800 = _es_snap_0800()  # 08:00 NY (ventana ~90s)
    actualiza_d1550 = _es_snap_1550()  # 15:50 NY (ventana ~90s)

    need_seed_0800 = _after_time(8, 0) and not _daily_snapshot_done_today(ws_snap_d0800)
    need_seed_1550 = _after_time(15, 50) and not _daily_snapshot_done_today(ws_snap_d1550)

    # ===== Recolecta datos Tradier =====
    datos = []
    for tk in TICKERS:
        oi_c, oi_p, m_c, m_p, v_c, v_p, exp = obtener_dinero(tk, posicion_fecha)
        datos.append([tk, "CALL", m_c, v_c, exp, oi_c])
        datos.append([tk, "PUT",  m_p, v_p, exp, oi_p])
        time.sleep(0.15)

    # ===== Agregado por ticker =====
    agg = _dd(lambda: {"CALL": [0.0, 0], "PUT": [0.0, 0], "EXP": None})
    for tk, side, m_usd, vol, exp, _oi in datos:
        if not agg[tk]["EXP"] and exp:
            agg[tk]["EXP"] = exp
        agg[tk][side][0] += m_usd
        agg[tk][side][1] += vol

    # ===== Métricas por ticker + estado =====
    stats = {}
    for tk in agg.keys():
        m_call, v_call = agg[tk]["CALL"]
        m_put, v_put = agg[tk]["PUT"]

        val_h_num = (m_call - m_put) / max(m_call, m_put) if max(m_call, m_put) > 0 else 0.0
        val_i_num = (v_call - v_put) / max(v_call, v_put) if max(v_call, v_put) > 0 else 0.0
        clasif = _clasificar_filtro_institucional(val_h_num, val_i_num)

        prev_oi, prev_vol, prev_l, _ph, _pi = estado_prev.get(tk, ("", "", "", None, None))
        color_oi = "🟢" if val_h_num > 0 else "🔴" if val_h_num < 0 else "⚪"
        color_vol = "🟢" if val_i_num > 0 else "🔴" if val_i_num < 0 else "⚪"

        cambio_oi = (prev_oi != "") and (color_oi != prev_oi)
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

    # ===== Orden por fuerza =====
    filas_sorted = sorted(stats.keys(), key=lambda t: stats[t]["val_h"], reverse=True)

    # ===== Encabezado (fila 2) =====
    from gspread.utils import rowcol_to_a1
    encabezado = [[
        "Fecha", "Hora", "Ticker",
        "Trade Cnt VERDE", "Trade Cnt ROJO",
        "VOLUMEN ENTRA", "VOLUMEN SALE",
        "TENDENCIA Trade Cnt.", "VOLUMEN.",
        "Fuerza", "Filtro institucional",
        "N (5m SNAP)", "O (5m SNAP)", "5m", "5m %",
        "N (15m SNAP)", "O (15m SNAP)", "15m", "15m %",
        "N (1h SNAP)", "O (1h SNAP)", "1h", "1h %",
        "N (1d 08:00)", "N (1d 15:50)", "día", "día %"
    ]]
    end_a1 = rowcol_to_a1(2, len(encabezado[0]))
    _update_values(ws, f"A2:{end_a1}", encabezado)

    # ===== Fórmulas de SNAP lookup =====
    s5 = f"SNAP_5min__{sheet_title}"
    s15 = f"SNAP__{sheet_title}"
    s1h = f"SNAP_H1__{sheet_title}"
    sd0800 = f"SNAP_d0800__{sheet_title}"
    sd1550 = f"SNAP_d1550__{sheet_title}"

    tabla = []
    for i, tk in enumerate(filas_sorted, start=3):
        m_call = stats[tk]["m_call"]; m_put = stats[tk]["m_put"]
        v_call = stats[tk]["v_call"]; v_put = stats[tk]["v_put"]

        H = f"=SI.ERROR((D{i}-E{i})/MAX(D{i};E{i});0)"
        I = f"=SI.ERROR((F{i}-G{i})/MAX(F{i};G{i});0)"
        J = f"=H{i}"
        K = f"=SI(Y(H{i}>0,5; I{i}>0,4);\"CALLS\";SI(Y(H{i}<0; I{i}<0);\"PUTS\";\"\") )"

        L = f"=SI.ERROR(BUSCARV($C{i};'{s5}'!$A:$C;3;FALSO);)"
        M = f"=SI.ERROR(BUSCARV($C{i};'{s5}'!$A:$B;2;FALSO);)"
        N = f"=SI.ERROR(L{i}-M{i};0)"
        O = f"=SI.ERROR(N{i}/MAX(ABS(M{i});0,000001);0)"

        P = f"=SI.ERROR(BUSCARV($C{i};'{s15}'!$A:$C;3;FALSO);)"
        Q = f"=SI.ERROR(BUSCARV($C{i};'{s15}'!$A:$B;2;FALSO);)"
        R = f"=SI.ERROR(P{i}-Q{i};0)"
        S = f"=SI.ERROR(R{i}/MAX(ABS(Q{i});0,000001);0)"

        T = f"=SI.ERROR(BUSCARV($C{i};'{s1h}'!$A:$C;3;FALSO);)"
        U = f"=LET(_u;SI.ERROR(BUSCARV($C{i};'{s1h}'!$A:$B;2;FALSO););SI(ESBLANCO(_u); T{i}; _u))"
        V = f"=SI.ERROR(T{i}-U{i};0)"
        W = f"=SI.ERROR(V{i}/MAX(ABS(U{i});0,000001);0)"

        X = f"=SI.ERROR(BUSCARV($C{i};'{sd0800}'!$A:$C;3;FALSO);)"  # 08:00
        Y = f"=SI.ERROR(BUSCARV($C{i};'{sd1550}'!$A:$C;3;FALSO);)"  # 15:50
        Z = f"=SI.ERROR(Y{i}-X{i};0)"
        AA = f"=SI( O(ESBLANCO(X{i}); ABS(X{i})=0 ); \"\"; SI.ERROR(Z{i}/ABS(X{i});0) )"

        tabla.append([
            agg[tk]["EXP"] or fecha_txt, hora_txt, tk,
            fmt_millones(m_call), fmt_millones(m_put),
            fmt_entero_miles(v_call), fmt_entero_miles(v_put),
            H, I, J, K,
            L, M, N, O,
            P, Q, R, S,
            T, U, V, W,
            X, Y, Z, AA
        ])

    # ===== Escritura tabla principal solo en cortes =====
    hay_corte = (
        _es_corte_5m(ny)
        or _es_corte_15m(ny)
        or (_es_corte_1hConVentana(ny, 3) and _should_run_h1_once(ws_meta, ny, sheet_title))
        or actualiza_d0800
        or actualiza_d1550
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
        for col in (7, 8, 9):
            req.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": start_row + total_rows,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

        # O, S, W, AA -> 0%
        for col in (14, 18, 22, 26):
            req.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": start_row + total_rows,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0%"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

        # Colores por K y cambios en H/I
        verde = {"red": 0.80, "green": 1.00, "blue": 0.80}
        rojo = {"red": 1.00, "green": 0.80, "blue": 0.80}
        amarillo = {"red": 1.00, "green": 1.00, "blue": 0.60}
        blanco = {"red": 1.00, "green": 1.00, "blue": 1.00}

        for idx, row in enumerate(tabla):
            tk = str(row[2]).strip().upper()
            ch_oi, ch_vol, ch_L = cambios_por_ticker.get(tk, (False, False, False))
            clasif = estado_nuevo[tk][2]

            bg_k = verde if clasif == "CALLS" else rojo if clasif == "PUTS" else blanco
            if ch_L:
                bg_k = amarillo

            # K (col 10 0-index)
            req.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row + idx,
                        "endRowIndex": start_row + idx + 1,
                        "startColumnIndex": 10,
                        "endColumnIndex": 11
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": bg_k}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

            bg_h = amarillo if ch_oi else blanco
            bg_i = amarillo if ch_vol else blanco

            # H (col 7) e I (col 8)
            req.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row + idx,
                        "endRowIndex": start_row + idx + 1,
                        "startColumnIndex": 7,
                        "endColumnIndex": 8
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": bg_h}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
            req.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row + idx,
                        "endRowIndex": start_row + idx + 1,
                        "startColumnIndex": 8,
                        "endColumnIndex": 9
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": bg_i}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

        if req:
            _retry(lambda: ws.spreadsheet.batch_update({"requests": req}))

    # ===== SNAPSHOTS (solo escrituras, usando cache) =====
    n_map = {}
    for tk in filas_sorted:
        m_call = agg[tk]["CALL"][0]
        m_put = agg[tk]["PUT"][0]
        n_map[tk] = round(m_call - m_put, 1)

    ts = ny.strftime("%Y-%m-%d %H:%M:%S")

    # 5m — siempre
    data_5m = [["Ticker", "N_prev", "N_curr", "ts"]]
    for tk in sorted(n_map.keys()):
        n_prev = cache_5m.get(tk, "")
        n_curr = n_map[tk]
        data_5m.append([tk, n_prev, n_curr, ts])
        cache_5m[tk] = n_curr
    _retry(lambda: ws_snap5m.batch_clear(["A2:D10000"]))
    _update_values(ws_snap5m, f"A1:D{len(data_5m)}", data_5m, user_entered=False)

    # 15m — cortes :00/:15/:30/:45
    if _es_corte_15m(ny):
        data_15 = [["Ticker", "N_prev", "N_curr", "ts"]]
        for tk in sorted(n_map.keys()):
            n_prev = cache_15m.get(tk, "")
            n_curr = n_map[tk]
            data_15.append([tk, n_prev, n_curr, ts])
            cache_15m[tk] = n_curr
        _update_values(ws_snap15, f"A1:D{len(data_15)}", data_15, user_entered=False)

    # 1h — con ventana de gracia (solo 1 vez por hora)
    if _es_corte_1hConVentana(ny, ventana_min=3) and _should_run_h1_once(ws_meta, ny, sheet_title):
        data_h1 = [["Ticker", "N_prev", "N_curr", "ts"]]
        for tk in sorted(n_map.keys()):
            n_curr = n_map[tk]
            n_prev = cache_h1.get(tk, n_curr)
            data_h1.append([tk, n_prev, n_curr, ts])
            cache_h1[tk] = n_curr
        _retry(lambda: ws_snap1h.batch_clear(["A2:D10000"]))
        _update_values(ws_snap1h, f"A1:D{len(data_h1)}", data_h1, user_entered=False)

    # 1d @ 08:00 NY (o semilla si ya pasó y está vacío)
    if actualiza_d0800 or need_seed_0800:
        ts_now = ny.strftime("%Y-%m-%d %H:%M:%S")
        data_d0800 = [["Ticker", "N_prev", "N_curr", "ts"]]
        for tk in sorted(n_map.keys()):
            n_curr = n_map[tk]
            n_prev = cache_d0800.get(tk, n_curr)
            data_d0800.append([tk, n_prev, n_curr, ts_now])
            cache_d0800[tk] = n_curr
        _retry(lambda: ws_snap_d0800.batch_clear(["A2:D10000"]))
        _update_values(ws_snap_d0800, f"A1:D{len(data_d0800)}", data_d0800, user_entered=False)

    # 1d @ 15:50 NY (o semilla si ya pasó y está vacío)
    if actualiza_d1550 or need_seed_1550:
        ts_now = ny.strftime("%Y-%m-%d %H:%M:%S")
        data_d1550 = [["Ticker", "N_prev", "N_curr", "ts"]]
        for tk in sorted(n_map.keys()):
            n_curr = n_map[tk]
            n_prev = cache_d1550.get(tk, n_curr)
            data_d1550.append([tk, n_prev, n_curr, ts_now])
            cache_d1550[tk] = n_curr
        _retry(lambda: ws_snap_d1550.batch_clear(["A2:D10000"]))
        _update_values(ws_snap_d1550, f"A1:D{len(data_d1550)}", data_d1550, user_entered=False)

    # Persistir estado
    _escribir_estado(ws_estado, estado_nuevo)

    # Retorno mapeo K
    return {tk: estado_nuevo[tk][2] for tk in filas_sorted}


def _apply_cf_inflow_thresholds(ws, sheet_title, ws_meta):
    """
    Milo — Señales por 'entrada de dinero' (Δ en millones) en columnas:
    N(5m), R(15m), V(1h), Z(día).
    Verde si >= umbral, Rojo si <= -umbral.
    """
    key = f"cf_inflow_v1__{sheet_title}"
    if _meta_read(ws_meta, key, "") == "1":
        return

    sheet_id = ws.id
    verde = {"red": 0.80, "green": 1.00, "blue": 0.80}
    rojo = {"red": 1.00, "green": 0.80, "blue": 0.80}

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
                        "condition": {
                            "type": "NUMBER_GREATER_THAN_EQ",
                            "values": [{"userEnteredValue": str(thr)}]
                        },
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
                        "condition": {
                            "type": "NUMBER_LESS_THAN_EQ",
                            "values": [{"userEnteredValue": str(-thr)}]
                        },
                        "format": {"backgroundColor": rojo}
                    }
                },
                "index": 0
            }
        })

    _retry(lambda: ws.spreadsheet.batch_update({"requests": req}))
    _meta_write(ws_meta, key, "1")
    ny = now_ny_base or _now_ny()
    fecha_txt = f"{ny:%Y-%m-%d}"
    hora_txt = ny.strftime("%H:%M:%S")
    print(f"⏳ Actualizando: {sheet_title} (venc. #{posicion_fecha+1}) — NY {fecha_txt} {hora_txt}")

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

    actualiza_d0800 = _es_snap_0800()    # 08:00 NY (ventana ~90s)
    actualiza_d1550 = _es_snap_1550()    # 15:50 NY (ventana ~90s)

    need_seed_0800 = _after_time(8, 0)   and not _daily_snapshot_done_today(ws_snap_d0800)
    need_seed_1550 = _after_time(15, 50) and not _daily_snapshot_done_today(ws_snap_d1550)

    # ===== Semilla diaria (para que Y no venga en blanco al inicio del día) =====
   
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

    # Métricas por ticker y estado
    stats = {}
    for tk in agg.keys():
        m_call, v_call = agg[tk]["CALL"]
        m_put,  v_put  = agg[tk]["PUT"]
        val_h_num = (m_call - m_put) / max(m_call, m_put) if max(m_call, m_put) > 0 else 0.0
        val_i_num = (v_call - v_put) / max(v_call, v_put) if max(v_call, v_put) > 0 else 0.0
        clasif = _clasificar_filtro_institucional(val_h_num, val_i_num)

        prev_oi, prev_vol, prev_l, _ph, _pi = estado_prev.get(tk, ("", "", "", None, None))
        color_oi  = "🟢" if val_h_num > 0 else "🔴" if val_h_num < 0 else "⚪"
        color_vol = "🟢" if val_i_num > 0 else "🔴" if val_i_num < 0 else "⚪"
        cambio_oi  = (prev_oi  != "") and (color_oi  != prev_oi)
        cambio_vol = (prev_vol != "") and (color_vol != prev_vol)
        es_alineado = clasif in ("CALLS","PUTS")
        cambio_L = es_alineado and (clasif != prev_l)

        cambios_por_ticker[tk] = (cambio_oi, cambio_vol, cambio_L)
        estado_nuevo[tk] = (color_oi, color_vol, clasif, val_h_num, val_i_num)

        stats[tk] = {
            "m_call": m_call, "m_put": m_put, "v_call": v_call, "v_put": v_put,
            "val_h": val_h_num, "val_i": val_i_num, "clasif": clasif,
        }

    # Ordenar por val_h desc (tu "Fuerza")
    filas_sorted = sorted(stats.keys(), key=lambda t: stats[t]["val_h"], reverse=True)

    # Encabezado
    from gspread.utils import rowcol_to_a1
    encabezado = [[
        "Fecha","Hora","Ticker",
        "Trade Cnt VERDE","Trade Cnt ROJO",
        "VOLUMEN ENTRA","VOLUMEN SALE",
        "TENDENCIA Trade Cnt.","VOLUMEN.",
        "Fuerza","Filtro institucional",
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
        J  = f"=H{i}"
        K  = f"=SI(Y(H{i}>0,5; I{i}>0,4);\"CALLS\";SI(Y(H{i}<0; I{i}<0);\"PUTS\";\"\") )"

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
            H, I, J, K,
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
        for col in (7, 8, 9):
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

            bg_k = verde if clasif=="CALLS" else rojo if clasif=="PUTS" else blanco
            if ch_L: bg_k = amarillo
            req.append({"repeatCell": {"range": {"sheetId": sheet_id,
                                                 "startRowIndex": start_row + idx, "endRowIndex": start_row + idx + 1,
                                                 "startColumnIndex": 10, "endColumnIndex": 11},
                                       "cell": {"userEnteredFormat": {"backgroundColor": bg_k}},
                                       "fields": "userEnteredFormat.backgroundColor"}})

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

    # 5m — siempre
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
    return {tk: estado_nuevo[tk][2] for tk in filas_sorted}


# ========= Runner de UNA corrida =========
def run_once(skip_oi: bool = False):
    doc_main = client.open_by_key(MAIN_FILE_ID)
    accesos = client.open_by_key(ACCESS_FILE_ID)
    main_url = f"https://docs.google.com/spreadsheets/d/{MAIN_FILE_ID}/edit"

    l_vto1 = l_vto2 = {}
    if not skip_oi:
        now_ny_base = _now_ny()
        l_vto1 = actualizar_hoja(doc_main, "Semana actual", posicion_fecha=0, now_ny_base=now_ny_base)
        l_vto2 = actualizar_hoja(doc_main, "Semana siguiente", posicion_fecha=1, now_ny_base=now_ny_base)
        try:
            print("SERVICIO_IO::K_SEMANA_ACTUAL=", json.dumps(l_vto1, ensure_ascii=False), flush=True)
            print("SERVICIO_IO::K_SEMANA_SIGUIENTE=", json.dumps(l_vto2, ensure_ascii=False), flush=True)
        except Exception:
            pass

    acc = procesar_autorizados_throttled(accesos, main_url)

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
        print("⏳ [/update] Ya hay una ejecución en curso (lock inter-proceso); se omite.", flush=True)
        return
    try:
        print("🚀 [update] Inicio actualización OI", flush=True)
        run_once()
        print("✅ [update] Fin actualización OI", flush=True)
    except Exception as e:
        print(f"❌ [/update] Error: {repr(e)}", flush=True)
        traceback.print_exc()
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
        accesos = client.open_by_key(ACCESS_FILE_ID)
        main_url = f"https://docs.google.com/spreadsheets/d/{MAIN_FILE_ID}/edit"
        acc = procesar_autorizados_throttled(accesos, main_url)
        print(f"✅ [/apply_access] ok: {acc}", flush=True)
        return jsonify({"ok": True, **acc}), 200
    finally:
        fcntl.flock(file_lock, fcntl.LOCK_UN); file_lock.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
