import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR  v10.8", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES PROTEGIDAS
# =============================================================================

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def clean_ids(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (INAMOVIBLE)
# =============================================================================

def motor_tarifas_jn(df_nov, manuales):
    df = df_nov.copy()
    df.columns = [str(c).upper().strip() for c in df.columns]
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
# BLOQUE 2: MOTOR NOMENCLADORES (V3 + TS - REGLA NO BORRADO)
# =============================================================================

def motor_nomencladores_dual(df_v3, df_ts, df_elr):
    # Guardamos columnas originales
    cols_v3, cols_ts = df_v3.columns.tolist(), df_ts.columns.tolist()
    
    v3_w, ts_w, elr_w = df_v3.copy(), df_ts.copy(), df_elr.copy()
    v3_w.columns = [str(c).upper().strip() for c in v3_w.columns]
    ts_w.columns = [str(c).upper().strip() for c in ts_w.columns]
    elr_w.columns = [str(c).upper().strip() for c in elr_w.columns]
    
    id_l_v3, id_l_ts = v3_w.columns[0], ts_w.columns[0]
    id_l_elr = [c for c in elr_w.columns if 'ID_LINEA_BO' in c or 'ID' in c][0]
    
    v3_w, ts_w, elr_w = clean_ids(v3_w, [id_l_v3]), clean_ids(ts_w, [id_l_ts]), clean_ids(elr_w, [id_l_elr])

    # Sincronización V3 (Agregamos altas)
    elr_lin = elr_w.drop_duplicates(subset=[id_l_elr])
    nuevas_v3 = elr_lin[~elr_lin[id_l_elr].isin(v3_w[id_l_v3])]
    if not nuevas_v3.empty:
        n_v3 = pd.DataFrame(columns=v3_w.columns)
        n_v3[id_l_v3] = nuevas_v3[id_l_elr]
        n_v3['GT'] = nuevas_v3.get('GRUPO_TARIFARIO_LINEA_DNGFF', '')
        v3_w = pd.concat([v3_w, n_v3], ignore_index=True)

    v3_w = v3_w.merge(elr_lin[[id_l_elr, 'GRUPO_TARIFARIO_LINEA_DNGFF']], left_on=id_l_v3, right_on=id_l_elr, how='left')
    v3_w['GT'] = v3_w['GRUPO_TARIFARIO_LINEA_DNGFF'].fillna(v3_w['GT'])

    # Sincronización TS (Agregamos altas)
    nuevas_ts = elr_lin[~elr_lin[id_l_elr].isin(ts_w[id_l_ts])]
    if not nuevas_ts.empty:
        n_ts = pd.DataFrame(columns=ts_w.columns)
        n_ts[id_l_ts] = nuevas_ts[id_l_elr]
        n_ts['GT'] = nuevas_ts.get('GRUPO_TARIFARIO_LINEA_DNGFF', '')
        ts_w = pd.concat([ts_w, n_ts], ignore_index=True)

    ts_w = ts_w.merge(elr_lin[[id_l_elr, 'GRUPO_TARIFARIO_LINEA_DNGFF']], left_on=id_l_ts, right_on=id_l_elr, how='left')
    ts_w['GT'] = ts_w['GRUPO_TARIFARIO_LINEA_DNGFF'].fillna(ts_w['GT'])

    # Retornar con formatos originales
    v3_f = v3_w[v3_w.columns[:len(cols_v3)]]; v3_f.columns = cols_v3
    ts_f = ts_w[ts_w.columns[:len(cols_ts)]]; ts_f.columns = cols_ts
    return v3_f, ts_f

# =============================================================================
# BLOQUE 3: MOTOR DMK (TANQUE V10 - FIX LM622 + IVA)
# =============================================================================

def motor_dmk_tanque(f_dmk, df_v3, df_tarifas, fecha_corte, df_pme):
    try:
        # Carga blindada (LM622)
        if f_dmk.name.endswith('.zip'):
            with zipfile.ZipFile(f_dmk) as z:
                with z.open(z.namelist()[0]) as f: df = pd.read_csv(f, sep=None, engine='python', encoding='iso-8859-1', dtype=str)
        else:
            df = pd.read_csv(f_dmk, sep=None, engine='python', encoding='iso-8859-1', dtype=str)

        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
        df = clean_ids(df, ['ID_LINEA', 'DOMINIO'])
        
        lf = pl.from_pandas(df).lazy()
        lf = lf.with_columns(pl.col("FECHA").str.to_date("%d/%m/%Y"))
        
        v3_p = pl.from_pandas(clean_ids(df_v3.copy(), [df_v3.columns[0]])).lazy().rename({df_v3.columns[0]: "ID_LINEA"})
        lf = lf.join(v3_p, on="ID_LINEA", how="inner").join(pl.from_pandas(df_tarifas).lazy(), on="GT", how="left")
        
        corte = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(pl.when(pl.col("FECHA") <= corte).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("T_PRAC"))
        
        for c in ['T_PRAC', 'DEBITADO', 'DESCUENTO_X_INTEGRACION', 'CANTIDAD_USOS']:
            if c in lf.collect_schema().names(): lf = lf.with_columns(pl.col(c).cast(pl.Float64).fill_null(0))

        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Utf8) == "621")
              .then(pl.when(pl.col("GT") == "INP").then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS")).otherwise((pl.col("T_PRAC") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS")))
              .otherwise(0).alias("COMP_ATS")
        ])
        
        # S/IVA
        lf = lf.with_columns([(pl.col("COMP_ATS")/1.105).alias("ATS_NETO"), (pl.col("COMP_ITG")/1.105).alias("ITG_NETO")])
        
        res = lf.collect().to_pandas()
        pme = clean_ids(df_pme.copy(), ['DOMINIO'])
        df_pm = res[res['DOMINIO'].isin(pme['DOMINIO'].unique())].merge(pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_res = res[~res['DOMINIO'].isin(pme['DOMINIO'].unique())].copy()
        df_res['DOMINIO'], df_res['ENERGIA'] = 'NO', 3
        
        return pd.concat([df_pm, df_res], ignore_index=True)
    except Exception as e:
        st.error(f"Error DMK: {e}"); return None

# --- UI ---
for k in ['v3', 'ts', 'tarifas', 'res_dmk']:
    if k not in st.session_state: st.session_state[k] = None

st.title("🛡️ Fiscalización Natalia v10.8")
t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO TTR"])

with t1:
    f_t = st.file_uploader("Cuadro Noviembre")
    if f_t:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"): st.session_state.tarifas = motor_tarifas_jn(pd.read_excel(f_t), m)
    if st.session_state.tarifas is not None: st.dataframe(st.session_state.tarifas)

with t2:
    col1, col2, col3 = st.columns(3)
    fv3 = col1.file_uploader("Nomenclador V3 (16 col)")
    fts = col2.file_uploader("Nomenclador TS (9 col)")
    felr = col3.file_uploader("ELR Nuevo")
    
    if fv3 and fts and felr and st.button("🔄 Sincronizar Ambos"):
        st.session_state.v3, st.session_state.ts = motor_nomencladores_dual(pd.read_excel(fv3), pd.read_excel(fts), pd.read_excel(felr))
        st.success("V3 y TS Actualizados.")

    if st.session_state.v3 is not None:
        st.divider()
        c1, c2 = st.columns(2)
        c1.download_button("📥 Bajar V3 Final", preparar_descarga(st.session_state.v3), "V3_Final.xlsx")
        c2.download_button("📥 Bajar TS Final", preparar_descarga(st.session_state.ts), "TS_Final.xlsx")

with t3:
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        dt = st.date_input("Fecha Corte:", datetime(2026, 2, 14))
        fdmk, fpm = st.file_uploader("DMK"), st.file_uploader("Parque Móvil")
        if fdmk and fpm and st.button("⚡ GENERAR TTR"):
            st.session_state.res_dmk = motor_dmk_tanque(fdmk, st.session_state.v3, st.session_state.tarifas, str(dt), pd.read_excel(fpm))
        if st.session_state.res_dmk is not None:
            st.download_button("📥 DESCARGAR TTR", preparar_descarga(st.session_state.res_dmk), "TTR_Final.xlsx")
            st.dataframe(st.session_state.res_dmk.head())
