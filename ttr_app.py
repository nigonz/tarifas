import streamlit as st
import pandas as pd
import io

# --- CONFIGURACIÓN DEL SISTEMA ---
st.set_page_config(page_title="Sistema de Fiscalización TTR", layout="wide")

def preparar_descarga(df):
    """Genera el archivo Excel para descarga."""
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def format_ids(df, column_name):
    """Asegura que los IDs se traten como texto para un macheo exacto sin decimales."""
    if column_name in df.columns:
        df[column_name] = df[column_name].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    return df

# =============================================================================
# MOTOR DE SINCRONIZACIÓN (LÓGICA ESTRICTA DE IDs)
# =============================================================================

def motor_sincronizacion_ids_puro(df_v, df_ts, df_elr):
    # 1. Copias de trabajo
    v = df_v.copy()
    ts = df_ts.copy()
    elr = df_elr.copy()

    # 2. Definición de columnas según especificación estricta
    # ELR: ID LINEA BO, ID RAMAL BO
    # Nomenclador V: ID_LINEA
    # TS: IdRamalNS
    col_elr_linea = 'ID LINEA BO'
    col_elr_ramal = 'ID RAMAL BO'
    col_v_id = 'ID_LINEA'
    col_ts_id = 'IdRamalNS'

    # 3. Formateo de las columnas de macheo
    v = format_ids(v, col_v_id)
    ts = format_ids(ts, col_ts_id)
    elr = format_ids(elr, col_elr_linea)
    elr = format_ids(elr, col_elr_ramal)

    # --- PROCESO NOMENCLADOR V (Actualización por ID LINEA BO) ---
    ids_v_actuales = set(v[col_v_id].unique())
    # Identificar IDs en ELR que no están en V
    nuevas_lineas = elr[~elr[col_elr_linea].isin(ids_v_actuales)].drop_duplicates(subset=[col_elr_linea])

    if not nuevas_lineas.empty:
        filas_v = pd.DataFrame(columns=v.columns)
        filas_v[col_v_id] = nuevas_lineas[col_elr_linea]
        # Se completan datos básicos del ELR para evitar filas vacías
        filas_v['RAZON_SOCIAL'] = nuevas_lineas.get('NOMBRE EMPRESA', '')
        filas_v['GT'] = nuevas_lineas.get('GRUPO TARIFARIO LINEA DNGFF', '')
        v = pd.concat([v, filas_v], ignore_index=True)

    # --- PROCESO NOMENCLADOR TS (Actualización por ID RAMAL BO) ---
    ids_ts_actuales = set(ts[col_ts_id].unique())
    # Identificar Ramales en ELR que no están en TS
    nuevos_ramales = elr[~elr[col_elr_ramal].isin(ids_ts_actuales)].drop_duplicates(subset=[col_elr_ramal])

    if not nuevos_ramales.empty:
        filas_ts = pd.DataFrame(columns=ts.columns)
        filas_ts[col_ts_id] = nuevos_ramales[col_elr_ramal]
        # Se vincula al ID de línea correspondiente del ELR
        filas_ts['IdLineaNS'] = nuevos_ramales[col_elr_linea]
        filas_ts['GT'] = nuevos_ramales.get('GRUPO TARIFARIO LINEA DNGFF', '')
        ts = pd.concat([ts, filas_ts], ignore_index=True)

    return v, ts

# =============================================================================
# INTERFAZ DE USUARIO
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")
st.subheader("Sincronización por Identificadores Numéricos")

if 'v_final' not in st.session_state: st.session_state.v_final = None
if 'ts_final' not in st.session_state: st.session_state.ts_final = None

c1, c2, c3 = st.columns(3)
f_v = c1.file_uploader("Subir Nomenclador V", key="v_up")
f_ts = c2.file_uploader("Subir Nomenclador TS", key="ts_up")
f_elr = c3.file_uploader("Subir ELR", key="elr_up")

if f_v and f_ts and f_elr and st.button("🚀 Iniciar Sincronización"):
    try:
        # Ejecución de la lógica ID a ID
        res_v, res_ts = motor_sincronizacion_ids_puro(
            pd.read_excel(f_v), 
            pd.read_excel(f_ts), 
            pd.read_excel(f_elr)
        )
        st.session_state.v_final = res_v
        st.session_state.ts_final = res_ts
        st.success("Sincronización finalizada. Los nuevos IDs han sido incorporados.")
    except Exception as e:
        st.error(f"Error en el proceso: {e}")

if st.session_state.v_final is not None:
    st.divider()
    d1, d2 = st.columns(2)
    d1.download_button("📥 Descargar Nomenclador V", preparar_descarga(st.session_state.v_final), "Nomenclador_V_Sync.xlsx")
    d2.download_button("📥 Descargar Nomenclador TS", preparar_descarga(st.session_state.ts_final), "Nomenclador_TS_Sync.xlsx")
