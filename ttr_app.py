import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR v8.6", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES Y PROTECCIÓN
# =============================================================================

def blindar_nombres(df):
    if df is None: return None
    df.columns = [str(c).upper().strip().replace(" ", "_").split('_X')[0].split('_Y')[0] for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def formatear_ids(df, columnas_id):
    for col in columnas_id:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MOTORES DE PROCESAMIENTO
# =============================================================================

def motor_maestro_v8_6(df_v2, df_elr, df_ts):
    df_v2, df_elr, df_ts = blindar_nombres(df_v2), blindar_nombres(df_elr), blindar_nombres(df_ts)
    
    # Identificación dinámica de llaves
    id_l_elr = [c for c in df_elr.columns if 'ID_LINEA_BO' in c or ('ID' in c and 'LINEA' in c)][0]
    id_r_elr = [c for c in df_elr.columns if 'ID_RAMAL_BO' in c or ('ID' in c and 'RAMAL' in c)][0]
    id_l_ts = [c for c in df_ts.columns if 'IDLINEANS' in c or ('ID' in c and 'LINEA' in c)][0]
    id_r_ts = [c for c in df_ts.columns if 'IDRAMALNS' in c or ('ID' in c and 'RAMAL' in c)][0]
    id_l_base = [c for c in df_v2.columns if 'ID_LINEA' in c or 'IDLINEANS' in c][0]

    df_elr = formatear_ids(df_elr, [id_l_elr, id_r_elr])
    df_ts = formatear_ids(df_ts, [id_l_ts, id_r_ts])
    df_v2 = formatear_ids(df_v2, [id_l_base])

    col_gt = [c for c in df_elr.columns if 'GRUPO_TARIF' in c][0]
    col_lin_nom = [c for c in df_elr.columns if 'LINEA' in c and 'BO' not in c and 'ID' not in c][0]
    
    # Cruce Ramales
    elr_map = df_elr[[id_l_elr, id_r_elr, col_gt, col_lin_nom]].drop_duplicates()
    ts_upd = df_ts.merge(elr_map, left_on=[id_l_ts, id_r_ts], right_on=[id_l_elr, id_r_elr], how='left')
    
    # Cruce Maestro V3
    elr_lin_map = df_elr[[id_l_elr, col_gt, col_lin_nom]].drop_duplicates(subset=[id_l_elr])
    if 'GT' in df_v2.columns: df_v2 = df_v2.rename(columns={'GT': 'GT_PREVIO'})
    
    v3_final = df_v2.merge(elr_lin_map, left_on=id_l_base, right_on=id_l_elr, how='left')
    v3_final = v3_final.rename(columns={col_lin_nom: 'LINEA_SILAS_FINAL', id_l_base: 'ID_LINEA', col_gt: 'GT'})
    
    audit = v3_final[['ID_LINEA', 'RAZON_SOCIAL', 'GT']].copy()
    audit['ESTADO'] = audit['GT'].apply(lambda x: '✅ OK' if pd.notnull(x) else '⚠️ Revisar')
    
    return blindar_nombres(v3_final), blindar_nombres(ts_upd), audit

def motor_ttr_v8_6(df_v3, df_tarifas):
    """Calcula la Tarifa Teórica de Referencia."""
    df_v3, df_tarifas = blindar_nombres(df_v3), blindar_nombres(df_tarifas)
    if 'GT' not in df_tarifas.columns:
        st.error("El archivo de tarifas debe tener la columna 'GT'")
        return None
    
    # El cruce se hace por Grupo Tarifario (GT)
    df_v3['GT'] = df_v3['GT'].astype(str).str.strip().str.upper()
    df_tarifas['GT'] = df_tarifas['GT'].astype(str).str.strip().str.upper()
    
    ttr_final = df_v3.merge(df_tarifas, on='GT', how='left')
    return ttr_final

# =============================================================================
# BLOQUE 2: INTERFAZ STREAMLIT
# =============================================================================

# Persistencia de datos
for key in ['v3', 'ts', 'audit', 'ttr']:
    if key not in st.session_state: st.session_state[key] = None

st.title("Fiscalización TTR v8.6")

t1, t2, t3 = st.tabs(["💰 TARIFAS / TTR", "📋 NOMENCLADORES", "📂 PROCESO DMK"])

with t1:
    st.header("Cálculo de Tarifa Teórica de Referencia")
    if st.session_state.v3 is None:
        st.warning("⚠️ Primero debés sincronizar los Nomencladores en la pestaña siguiente.")
    else:
        f_tar = st.file_uploader("Subir Cuadro Tarifario (Excel/CSV)", key="u_tar")
        if f_tar and st.button("📊 Calcular TTR"):
            df_tar_raw = pd.read_excel(f_tar) if f_tar.name.endswith('xlsx') else pd.read_csv(f_tar)
            st.session_state.ttr = motor_ttr_v8_6(st.session_state.v3, df_tar_raw)
            st.success("TTR calculada exitosamente.")
        
        if st.session_state.ttr is not None:
            st.dataframe(st.session_state.ttr.head(10))
            towrite = io.BytesIO()
            st.session_state.ttr.to_excel(towrite, index=False)
            st.download_button("📥 Descargar TTR", towrite.getvalue(), "TTR_Final.xlsx")

with t2:
    st.header("Sincronización de Nomencladores")
    col1, col2, col3 = st.columns(3)
    fv2 = col1.file_uploader("Nomenclador v2", key="uv2")
    felr = col2.file_uploader("Archivo ELR", key="uelr")
    fts = col3.file_uploader("Archivo TS", key="uts")
    
    if fv2 and felr and fts and st.button("🔄 Sincronizar"):
        v3, ts, audit = motor_maestro_v8_6(pd.read_excel(fv2), pd.read_excel(felr), pd.read_excel(fts))
        st.session_state.v3, st.session_state.ts, st.session_state.audit = v3, ts, audit
        st.rerun()

    if st.session_state.audit is not None:
        st.dataframe(st.session_state.audit)

with t3:
    st.header("Liquidación DMK")
    st.info("Utiliza el Nomenclador V3 generado en la pestaña anterior.")
    # (Aquí iría tu motor_dmk_v8_6 que ya tienes definido)
