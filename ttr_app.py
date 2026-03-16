import streamlit as st
import pandas as pd
import io

# --- CONFIGURACIÓN DEL SISTEMA OFICIAL ---
st.set_page_config(page_title="Sistema de Fiscalización TTR", layout="wide")

def preparar_descarga(df):
    """Genera el archivo Excel para descarga."""
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def format_ids(df, column_name):
    """Asegura que los IDs sean texto limpio para que el cruce sea idéntico al BUSCARV."""
    if column_name in df.columns:
        df[column_name] = df[column_name].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    return df

# =============================================================================
# MOTOR DE SINCRONIZACIÓN (LÓGICA BUSCARV DE EXCEL)
# =============================================================================

def motor_sincronizacion_oficial(df_v, df_ts, df_elr):
    # 1. Copias y limpieza de columnas
    v = df_v.copy()
    ts = df_ts.copy()
    elr = df_elr.copy()
    elr.columns = [str(c).strip().upper() for c in elr.columns]

    # 2. Definición de las llaves (Columnas de tus BUSCARV)
    col_v_id = v.columns[0]          # ID_LINEA
    col_elr_id = 'ID LINEA BO'
    col_ts_id = ts.columns[3]        # IdRamalNS
    col_elr_ramal = 'ID RAMAL BO'

    # 3. Formateo de IDs para evitar errores de tipo
    v = format_ids(v, col_v_id)
    elr = format_ids(elr, col_elr_id)
    elr = format_ids(elr, col_elr_ramal)
    ts = format_ids(ts, col_ts_id)

    # --- PROCESO NOMENCLADOR V ---
    # ¿Qué IDs del ELR no están en el Nomenclador V?
    ids_v_actuales = set(v[col_v_id].unique())
    nuevas_v = elr[~elr[col_elr_id].isin(ids_v_actuales)].drop_duplicates(subset=[col_elr_id])

    if not nuevas_v.empty:
        # Creamos las filas nuevas con la estructura del Nomenclador V
        filas_nuevas_v = pd.DataFrame(columns=v.columns)
        filas_nuevas_v[col_v_id] = nuevas_v[col_elr_id]
        filas_nuevas_v[v.columns[1]] = nuevas_v['LINEA']
        filas_nuevas_v['GT'] = nuevas_v.get('GRUPO TARIFARIO LINEA DNGFF', '')
        v = pd.concat([v, filas_nuevas_v], ignore_index=True)

    # --- PROCESO NOMENCLADOR TS ---
    # ¿Qué Ramales del ELR no están en el TS?
    ids_ts_actuales = set(ts[col_ts_id].unique())
    nuevas_ts = elr[~elr[col_elr_ramal].isin(ids_ts_actuales)].drop_duplicates(subset=[col_elr_ramal])

    if not nuevas_ts.empty:
        # Creamos las filas nuevas con la estructura del TS
        filas_nuevas_ts = pd.DataFrame(columns=ts.columns)
        filas_nuevas_ts[ts.columns[0]] = nuevas_ts.get('JURISDICCION', '')
        filas_nuevas_ts[ts.columns[1]] = nuevas_ts[col_elr_id]
        filas_nuevas_ts[ts.columns[2]] = nuevas_ts['LINEA']
        filas_nuevas_ts[ts.columns[3]] = nuevas_ts[col_elr_ramal]
        filas_nuevas_ts['GT'] = nuevas_ts.get('GRUPO TARIFARIO LINEA DNGFF', '')
        ts = pd.concat([ts, filas_nuevas_ts], ignore_index=True)

    return v, ts

# =============================================================================
# INTERFAZ DE USUARIO
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")
st.subheader("Módulo de Sincronización de Nomencladores")

if 'v_final' not in st.session_state: st.session_state.v_final = None
if 'ts_final' not in st.session_state: st.session_state.ts_final = None

# Cargadores de archivos
c1, c2, c3 = st.columns(3)
file_v = c1.file_uploader("Nomenclador V", key="f_v")
file_ts = c2.file_uploader("Nomenclador TS", key="f_ts")
file_elr = c3.file_uploader("Archivo ELR", key="f_elr")

if file_v and file_ts and file_elr and st.button("🚀 Sincronizar por ID"):
    try:
        # Ejecutamos la lógica de Jack
        res_v, res_ts = motor_sincronizacion_oficial(
            pd.read_excel(file_v), 
            pd.read_excel(file_ts), 
            pd.read_excel(file_elr)
        )
        st.session_state.v_final = res_v
        st.session_state.ts_final = res_ts
        st.success("Sincronización finalizada: IDs inexistentes agregados a los maestros.")
    except Exception as e:
        st.error(f"Se produjo un error en el proceso: {e}")

# Descargas
if st.session_state.v_final is not None:
    st.divider()
    desc_v, desc_ts = st.columns(2)
    desc_v.download_button("📥 Descargar Nomenclador V", preparar_descarga(st.session_state.v_final), "Nomenclador_V_Sync.xlsx")
    desc_ts.download_button("📥 Descargar Nomenclador TS", preparar_descarga(st.session_state.ts_final), "Nomenclador_TS_Sync.xlsx")
