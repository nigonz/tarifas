import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización v10.9", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES DE NORMALIZACIÓN (SOLUCIÓN AL KEYERROR)
# =============================================================================

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def normalizar_columnas(df):
    """Limpia encabezados: mayúsculas, quita puntos y cambia espacios por _"""
    df.columns = [
        str(c).upper().strip()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("__", "_")
        for c in df.columns
    ]
    return df

def clean_ids(df, cols):
    """Asegura que los IDs sean texto puro sin .0"""
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (INAMOVIBLE)
# =============================================================================

def motor_tarifas_original(df_nov, manuales):
    df = normalizar_columnas(df_nov.copy())
    col_id = [c for c in df.columns if any(x in c for x in ['ID', 'GT'])][0]
    col_p = [c for c in df.columns if any(x in c for x in ['LIMITE', 'TARIFA', 'PRECIO'])][0]
    
    val_1scn = df.loc[df[col_id] == '1SCN', col_p].values
    v1_ant = pd.to_numeric(str(val_1scn[0]).replace(',', '.'), errors='coerce') if len(val_1scn) > 0 else 270.0
    factor = manuales['1SCN'] / v1_ant if v1_ant > 0 else 1
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row[col_id]).strip().upper()
        v_ant = pd.to_numeric(str(row[col_p]).replace(',', '.'), errors='coerce')
        if id_t in manuales: v_nue = manuales[id_t]
        elif any(x in id_t for x in ['SGI', 'UPA']): v_nue = manuales['1SCN']
        else: v_nue = v_ant * factor if pd.notnull(v_ant) else manuales['1SCN']
        res.append({'GT': id_t, 'TARIFA_NOV': round(v_ant, 2), 'TARIFA_FEB': round(v_nue, 2)})
    return pd.DataFrame(res)

# =============================================================================
# BLOQUE 2: MOTOR NOMENCLADORES (LÓGICA BUSCARV + NO BORRADO)
# =============================================================================

def motor_sincronizar_maestros(df_v3, df_ts, df_elr):
    # Guardamos nombres originales para devolverlos igual
    cols_v3_orig, cols_ts_orig = df_v3.columns.tolist(), df_ts.columns.tolist()
    
    v3, ts, elr = df_v3.copy(), df_ts.copy(), df_elr.copy()
    v3, ts, elr = normalizar_columnas(v3), normalizar_columnas(ts), normalizar_columnas(elr)
    
    id_v3, id_ts = v3.columns[0], ts.columns[0]
    id_elr = [c for c in elr.columns if 'ID_LINEA_BO' in c or 'ID_LINEA' in c][0]
    col_gt_elr = [c for c in elr.columns if 'GRUPO_TARIFARIO' in c][0]

    v3, ts, elr = clean_ids(v3, [id_v3]), clean_ids(ts, [id_ts]), clean_ids(elr, [id_elr])
    elr_lin = elr.drop_duplicates(subset=[id_elr])

    # 1. ACTUALIZAR V3 (BuscarV + Altas)
    v3 = v3.merge(elr_lin[[id_elr, col_gt_elr]], left_on=id_v3, right_on=id_elr, how='left')
    v3['GT'] = v3[col_gt_elr].fillna(v3.get('GT', ''))
    
    nuevas_v3 = elr_lin[~elr_lin[id_elr].isin(v3[id_v3])]
    if not nuevas_v3.empty:
        altas_v3 = pd.DataFrame(columns=v3.columns)
        altas_v3[id_v3] = nuevas_v3[id_elr]
        altas_v3['RAZON_SOCIAL'] = nuevas_v3.get('NOMBRE_EMPRESA', 'ALTA')
        altas_v3['GT'] = nuevas_v3[col_gt_elr]
        v3 = pd.concat([v3, altas_v3], ignore_index=True)

    # 2. ACTUALIZAR TS (BuscarV + Altas)
    ts = ts.merge(elr_lin[[id_elr, col_gt_elr]], left_on=id_ts, right_on=id_elr, how='left')
    ts['GT'] = ts[col_gt_elr].fillna(ts.get('GT', ''))
    
    nuevas_ts = elr_lin[~elr_lin[id_elr].isin(ts[id_ts])]
    if not nuevas_ts.empty:
        altas_ts = pd.DataFrame(columns=ts.columns)
        altas_ts[id_ts] = nuevas_ts[id_elr]
        altas_ts['GT'] = nuevas_ts[col_gt_elr]
        ts = pd.concat([ts, altas_ts], ignore_index=True)

    # Limpiar columnas temporales y restaurar nombres
    v3_final = v3[v3.columns[:len(cols_v3_orig)]]
    v3_final.columns = cols_v3_orig
    ts_final = ts[ts.columns[:len(cols_ts_orig)]]
    ts_final.columns = cols_ts_orig
    
    return v3_final, ts_final

# =============================================================================
# BLOQUE 3: MOTOR DMK (TANQUE V10.9 - LÓGICA ORIGINAL + s/IVA)
# =============================================================================

def motor_dmk_tanque(f_dmk, df_v3, df_tarifas, fecha_corte, df_pme):
    try:
        # Carga con Pandas (Blindaje 'LM622')
        if f_dmk.name.endswith('.zip'):
            with zipfile.ZipFile(f_dmk) as z:
                with z.open(z.namelist()[0]) as f: df = pd.read_csv(f, sep=None, engine='python', encoding='iso-8859-1', dtype=str)
        else:
            df = pd.read_csv(f_dmk, sep=None, engine='python', encoding='iso-8859-1', dtype=str)

        df = normalizar_columnas(df)
        df = clean_ids(df, ['ID_LINEA', 'DOMINIO'])
        
        lf = pl.from_pandas(df).lazy()
        lf = lf.with_columns(pl.col("FECHA").str.to_date("%d/%m/%Y"))
        
        # Joins con Nomenclador y Tarifas
        v3_p = clean_ids(df_v3.copy(), [df_v3.columns[0]])
        v3_pl = pl.from_pandas(normalizar_columnas(v3_p)).lazy().rename({normalizar_columnas(v3_p).columns[0]: "ID_LINEA"})
        
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(pl.from_pandas(df_tarifas).lazy(), on="GT", how="left")
        
        # Fecha de cambio de tarifa
        corte = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(pl.when(pl.col("FECHA") <= corte).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("T_PRAC"))
        
        # Cálculos Financieros Originales
        num_cols = ['T_PRAC', 'DEBITADO', 'DESCUENTO_X_INTEGRACION', 'CANTIDAD_USOS']
        for c in num_cols:
            if c in lf.collect_schema().names(): lf = lf.with_columns(pl.col(c).cast(pl.Float64).fill_null(0))

        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Utf8) == "621")
              .then(pl.when(pl.col("GT") == "INP").then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS")).otherwise((pl.col("T_PRAC") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS")))
              .otherwise(0).alias("COMP_ATS")
        ])
        
        # Desglose s/IVA (Notebook)
        lf = lf.with_columns([(pl.col("COMP_ATS")/1.105).alias("ATS_S_IVA"), (pl.col("COMP_ITG")/1.105).alias("ITG_S_IVA")])
        
        # Energías (Merge final)
        res = lf.collect().to_pandas()
        pme = clean_ids(normalizar_columnas(df_pme.copy()), ['DOMINIO'])
        dom_list = pme['DOMINIO'].unique()
        
        df_pm = res[res['DOMINIO'].isin(dom_list)].merge(pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_resto = res[~res['DOMINIO'].isin(dom_list)].copy()
        df_resto['DOMINIO'], df_resto['ENERGIA'] = 'NO', 3
        
        return pd.concat([df_pm, df_resto], ignore_index=True)
    except Exception as e:
        st.error(f"Falla en DMK: {e}"); return None

# =============================================================================
# UI INTERFAZ
# =============================================================================

for key in ['v3', 'ts', 'tarifas', 'res_dmk']:
    if key not in st.session_state: st.session_state[key] = None

st.title("🛡️ Fiscalización v10.9")
t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO TTR"])

with t1:
    f_t = st.file_uploader("Cuadro Noviembre", key="t_up")
    if f_t:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"): st.session_state.tarifas = motor_tarifas_original(pd.read_excel(f_t), m)
    if st.session_state.tarifas is not None: st.dataframe(st.session_state.tarifas)

with t2:
    st.info("Actualización automática de V3 y TS vía ELR (Sin borrar registros).")
    c1, c2, c3 = st.columns(3)
    fv3, fts, felr = c1.file_uploader("Nomenclador V3"), c2.file_uploader("Nomenclador TS"), c3.file_uploader("ELR")
    
    if fv3 and fts and felr and st.button("🔄 Sincronizar Maestros"):
        st.session_state.v3, st.session_state.ts = motor_sincronizar_maestros(pd.read_excel(fv3), pd.read_excel(fts), pd.read_excel(felr))
        st.success("Sincronización finalizada.")

    if st.session_state.v3 is not None:
        st.divider()
        col_v, col_t = st.columns(2)
        col_v.download_button("📥 Bajar V3 Actualizado", preparar_descarga(st.session_state.v3), "V3_Actualizado.xlsx")
        col_t.download_button("📥 Bajar TS Actualizado", preparar_descarga(st.session_state.ts), "TS_Actualizado.xlsx")

with t3:
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        dt = st.date_input("Corte Tarifa:", datetime(2026, 2, 14))
        f_dmk, f_en = st.file_uploader("DMK"), st.file_uploader("Parque Móvil")
        if f_dmk and f_en and st.button("⚡ GENERAR TTR"):
            st.session_state.res_dmk = motor_dmk_tanque(f_dmk, st.session_state.v3, st.session_state.tarifas, str(dt), pd.read_excel(f_en))
        if st.session_state.res_dmk is not None:
            st.download_button("📥 DESCARGAR LIQUIDACIÓN", preparar_descarga(st.session_state.res_dmk), "TTR_Final.xlsx")
            st.dataframe(st.session_state.res_dmk.head())
