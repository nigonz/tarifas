import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización Natalia v11.1", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES PROTEGIDAS
# =============================================================================

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def normalizar_columnas(df):
    """Limpia encabezados para que coincidan con las fórmulas."""
    df.columns = [
        str(c).upper().strip()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("__", "_")
        for c in df.columns
    ]
    return df

def clean_ids(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 2: MOTOR NOMENCLADORES (LÓGICA BUSCARV ID_LINEA_BO)
# =============================================================================

def motor_sincronizar_v11_1(df_v3, df_ts, df_elr):
    # Respaldar columnas originales para la descarga final
    cols_v3_orig = df_v3.columns.tolist()
    cols_ts_orig = df_ts.columns.tolist()
    
    v3 = normalizar_columnas(df_v3.copy())
    ts = normalizar_columnas(df_ts.copy())
    elr = normalizar_columnas(df_elr.copy())
    
    # Identificación de columnas maestras del ELR
    col_id_bo = [c for c in elr.columns if 'ID_LINEA_BO' in c][0]
    col_nombre_elr = [c for c in elr.columns if c == 'LINEA'][0]
    col_ramal_bo = [c for c in elr.columns if 'ID_RAMAL_BO' in c][0]
    col_gt_elr = [c for c in elr.columns if 'GRUPO_TARIFARIO' in c][0]

    v3 = clean_ids(v3, [v3.columns[0], v3.columns[1]]) # ID_LINEA, Linea SILAS
    elr = clean_ids(elr, [col_id_bo, col_nombre_elr, col_ramal_bo])
    ts = clean_ids(ts, [ts.columns[0], ts.columns[3]]) # IdLineaNS, IdRamalNS

    # --- ACCIÓN 1: ACTUALIZAR IDs EN V3 (BUSCARV POR NOMBRE) ---
    # Según tu lógica: Buscar con 'LINEA' (ELR) el nuevo valor de 'ID_LINEA_BO'
    elr_map = elr[[col_nombre_elr, col_id_bo, col_gt_elr]].drop_duplicates(subset=[col_nombre_elr])
    
    # Unimos por nombre de línea (V3 col 1 es 'LINEA_SILAS_DNGFF')
    v3 = v3.merge(elr_map, left_on=v3.columns[1], right_on=col_nombre_elr, how='left')
    
    # El ID_LINEA (col 0) se reemplaza por el ID_LINEA_BO si hay coincidencia
    v3[v3.columns[0]] = v3[col_id_bo].fillna(v3[v3.columns[0]])
    v3['GT'] = v3[col_gt_elr].fillna(v3.get('GT', ''))

    # --- ACCIÓN 2: AGREGAR RAMALES NUEVOS EN TS (#N/A) ---
    # Ramales en ELR que NO existen en TS.IdRamalNS (Columna 3 del TS)
    nuevas_ts = elr[~elr[col_ramal_bo].isin(ts[ts.columns[3]])]
    
    if not nuevas_ts.empty:
        # Tomamos datos únicos de ramales nuevos
        nuevas_ts = nuevas_ts.drop_duplicates(subset=[col_ramal_bo])
        altas_ts = pd.DataFrame(columns=ts.columns)
        altas_ts[ts.columns[0]] = nuevas_ts[col_id_bo]    # IdLineaNS
        altas_ts[ts.columns[2]] = nuevas_ts[col_nombre_elr] # LineaSILAS
        altas_ts[ts.columns[3]] = nuevas_ts[col_ramal_bo]  # IdRamalNS
        altas_ts['GT'] = nuevas_ts[col_gt_elr]
        ts = pd.concat([ts, altas_ts], ignore_index=True)

    # Reconstruir DataFrames finales
    v3_final = v3[v3.columns[:len(cols_v3_orig)]]
    v3_final.columns = cols_v3_orig
    ts_final = ts[ts.columns[:len(cols_ts_orig)]]
    ts_final.columns = cols_ts_orig
    
    return v3_final, ts_final

# =============================================================================
# INTERFAZ (UI) - CORRECCIÓN DE KEYS
# =============================================================================

# Inicializamos el estado para los DATOS, no para los WIDGETS
if 'v3_data' not in st.session_state: st.session_state.v3_data = None
if 'ts_data' not in st.session_state: st.session_state.ts_data = None

st.title("🛡️ Fiscalización  v11.1")

st.subheader("Sincronización de Nomencladores (Lógica de Mapeo)")
col1, col2, col3 = st.columns(3)

# Usamos keys diferentes a los nombres de los datos procesados
fv3 = col1.file_uploader("Nomenclador V3", key="v3_file")
fts = col2.file_uploader("Nomenclador TS", key="ts_file")
felr = col3.file_uploader("Archivo ELR", key="elr_file")

if fv3 and fts and felr and st.button("🔄 Ejecutar Mapeo"):
    v3_res, ts_res = motor_sincronizar_v11_1(pd.read_excel(fv3), pd.read_excel(fts), pd.read_excel(felr))
    st.session_state.v3_data = v3_res
    st.session_state.ts_data = ts_res
    st.success("Sincronización terminada. IDs de V3 actualizados y ramales nuevos incorporados.")

if st.session_state.v3_data is not None:
    st.divider()
    c1, c2 = st.columns(2)
    c1.download_button("📥 Bajar V3 (IDs BO)", preparar_descarga(st.session_state.v3_data), "V3_Actualizado.xlsx")
    c2.download_button("📥 Bajar TS (Nuevos Ramales)", preparar_descarga(st.session_state.ts_data), "TS_Actualizado.xlsx")
    
    st.write("Vista previa V3:")
    st.dataframe(st.session_state.v3_data.head())
