import streamlit as st
import pandas as pd
import io
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización Natalia v11.0", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES DE NORMALIZACIÓN
# =============================================================================

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def normalizar_columnas(df):
    """Limpia encabezados para evitar KeyErrors."""
    df.columns = [
        str(c).upper().strip()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("__", "_")
        for c in df.columns
    ]
    return df

def clean_ids(df, cols):
    """Asegura texto puro en IDs."""
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 2: MOTOR NOMENCLADORES (LÓGICA BUSCARV CAPTURAS)
# =============================================================================

def motor_sincronizar_v11(df_v3, df_ts, df_elr):
    # Respaldar estructuras originales
    cols_v3_orig = df_v3.columns.tolist()
    cols_ts_orig = df_ts.columns.tolist()
    
    v3 = normalizar_columnas(df_v3.copy())
    ts = normalizar_columnas(df_ts.copy())
    elr = normalizar_columnas(df_elr.copy())
    
    # Identificar columnas según ELR compartido
    # En V3 la columna de nombre suele ser 'LINEA_SILAS_DNGFF' o similar
    # En ELR es 'LINEA' y el nuevo ID es 'ID_LINEA_BO'
    col_nombre_v3 = v3.columns[1] # 'LINEA_SILAS_DNGFF'
    col_nombre_elr = [c for c in elr.columns if c == 'LINEA'][0]
    col_id_bo = [c for c in elr.columns if 'ID_LINEA_BO' in c][0]
    col_ramal_bo = [c for c in elr.columns if 'ID_RAMAL_BO' in c][0]
    col_gt_elr = [c for c in elr.columns if 'GRUPO_TARIFARIO' in c][0]

    # Limpiar para el cruce
    v3 = clean_ids(v3, [v3.columns[0], col_nombre_v3])
    elr = clean_ids(elr, [col_id_bo, col_nombre_elr, col_ramal_bo])
    ts = clean_ids(ts, [ts.columns[0], ts.columns[3]]) # IdLineaNS, IdRamalNS

    # --- ACCIÓN 1: ACTUALIZAR IDs EN V3 (TU BUSCARV) ---
    # Buscamos por nombre de línea para traer el ID nuevo
    elr_lin = elr.drop_duplicates(subset=[col_nombre_elr])
    v3 = v3.merge(elr_lin[[col_nombre_elr, col_id_bo, col_gt_elr]], 
                  left_on=col_nombre_v3, right_on=col_nombre_elr, how='left')
    
    # Si encontró el ID nuevo, lo reemplaza. Si no, deja el viejo.
    v3[v3.columns[0]] = v3[col_id_bo].fillna(v3[v3.columns[0]])
    v3['GT'] = v3[col_gt_elr].fillna(v3.get('GT', ''))

    # --- ACCIÓN 2: AGREGAR RAMALES NUEVOS EN TS ---
    # Buscamos ramales que den N/A (que no existan en IdRamalNS)
    nuevas_ts = elr[~elr[col_ramal_bo].isin(ts[ts.columns[3]])] # col 3 es IdRamalNS
    
    if not nuevas_ts.empty:
        altas_ts = pd.DataFrame(columns=ts.columns)
        altas_ts[ts.columns[0]] = nuevas_ts[col_id_bo] # IdLineaNS
        altas_ts[ts.columns[2]] = nuevas_ts[col_nombre_elr] # LineaSILAS
        altas_ts[ts.columns[3]] = nuevas_ts[col_ramal_bo] # IdRamalNS
        altas_ts['GT'] = nuevas_ts[col_gt_elr]
        altas_ts['JURISDICCION'] = nuevas_ts.get('JURISDICCION', '')
        ts = pd.concat([ts, altas_ts], ignore_index=True)

    # Restaurar formatos y columnas originales
    v3_f = v3[v3.columns[:len(cols_v3_orig)]]
    v3_f.columns = cols_v3_orig
    ts_f = ts[ts.columns[:len(cols_ts_orig)]]
    ts_f.columns = cols_ts_orig
    
    return v3_f, ts_f

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

st.title("🛡️ Fiscalización Natalia v11.0")

if 'v3' not in st.session_state: st.session_state.v3 = None
if 'ts' not in st.session_state: st.session_state.ts = None

st.subheader("Sincronización de Nomencladores (Mapeo de IDs)")
col1, col2, col3 = st.columns(3)
fv3 = col1.file_uploader("Nomenclador V3", key="v3")
fts = col2.file_uploader("Nomenclador TS", key="ts")
felr = col3.file_uploader("ELR (Novedades)", key="elr")

if fv3 and fts and felr and st.button("🔄 Ejecutar Mapeo"):
    v3_res, ts_res = motor_sincronizar_v11(pd.read_excel(fv3), pd.read_excel(fts), pd.read_excel(felr))
    st.session_state.v3, st.session_state.ts = v3_res, ts_res
    st.success("Sincronización finalizada: IDs actualizados y ramales nuevos agregados.")

if st.session_state.v3 is not None:
    st.divider()
    c1, c2 = st.columns(2)
    c1.download_button("📥 Bajar V3 (IDs Actualizados)", preparar_descarga(st.session_state.v3), "V3_Actualizado.xlsx")
    c2.download_button("📥 Bajar TS (Ramales Nuevos)", preparar_descarga(st.session_state.ts), "TS_Actualizado.xlsx")
