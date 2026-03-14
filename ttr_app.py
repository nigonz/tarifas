import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR Natalia v8.7", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES
# =============================================================================

def blindar_nombres(df):
    if df is None: return None
    df.columns = [str(c).upper().strip().replace(" ", "_").split('_X')[0].split('_Y')[0] for c in df.columns]
    return df

def formatear_ids(df, columnas_id):
    for col in columnas_id:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MÓDULO TARIFAS (PROYECCIÓN Y MES PARTIDO)
# =============================================================================

def motor_proyeccion_tarifas(df_nov, manuales):
    """Calcula las tarifas de febrero basándose en las de noviembre."""
    df = blindar_nombres(df_nov.copy())
    # Identificar columna de ID (GT)
    col_id = [c for c in df.columns if 'ID' in c or 'GT' in c][0]
    
    # Factor de escala basado en 1SCN
    v1_ant = pd.to_numeric(df.loc[df[col_id] == '1SCN', 'LIMITE_INFERIOR'].astype(str).str.replace(',', '.'), errors='coerce').values[0]
    factor = manuales['1SCN'] / v1_ant
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row[col_id]).strip().upper()
        v_ant = pd.to_numeric(str(row['LIMITE_INFERIOR']).replace(',', '.'), errors='coerce')
        
        # Lógica de cálculo (idéntica a tu v8.4)
        if id_t in manuales: v_nue = manuales[id_t]
        elif 'SEN' in id_t and 'SESN' not in id_t: v_nue = manuales.get(id_t.replace('SEN', 'SCN'), manuales['1SCN']) * 1.25
        elif 'SEAN' in id_t and 'SEASN' not in id_t: v_nue = manuales.get(id_t.replace('SEAN', 'SCN'), manuales['1SCN']) * 1.75
        elif 'SCSN' in id_t: v_nue = manuales.get(id_t.replace('SCSN', 'SCN'), manuales['1SCN']) * 1.59
        elif 'SESN' in id_t: v_nue = (manuales.get(id_t.replace('SESN', 'SCN'), manuales['1SCN']) * 1.59) * 1.25
        elif 'SEASN' in id_t: v_nue = (manuales.get(id_t.replace('SEASN', 'SCN'), manuales['1SCN']) * 1.59) * 1.75
        else: v_nue = v_ant * factor

        res.append({'GT': id_t, 'TARIFA_NOV': round(v_ant, 2), 'TARIFA_FEB': round(v_nue, 2)})
    return pd.DataFrame(res)

# =============================================================================
# BLOQUE 2: MOTOR MAESTRO (FIX 16 COLUMNAS)
# =============================================================================

def motor_maestro_v8_7(df_v2, df_elr):
    cols_org = df_v2.columns.tolist()
    ids_org = df_v2[df_v2.columns[0]].astype(str).str.strip().str.upper().tolist()
    
    df_v2_i = blindar_nombres(df_v2.copy())
    df_elr_i = blindar_nombres(df_elr.copy())
    
    id_l_base = df_v2_i.columns[0]
    id_l_elr = [c for c in df_elr_i.columns if 'ID_LINEA_BO' in c or 'ID' in c][0]
    col_gt_elr = [c for c in df_elr_i.columns if 'GRUPO_TARIF' in c][0]
    
    # Cruce limpio
    elr_map = df_elr_i[[id_l_elr, col_gt_elr]].drop_duplicates(subset=[id_l_elr])
    v3 = df_v2_i.merge(elr_map, left_on=id_l_base, right_on=id_l_elr, how='left')
    
    # Pisar GT y restaurar molde
    v3['GT'] = v3[col_gt_elr].fillna(v3['GT'])
    v3 = v3[v3[id_l_base].isin(ids_org)]
    v3 = v3[df_v2_i.columns]
    v3.columns = cols_org
    return v3

# =============================================================================
# BLOQUE 3: MOTOR DMK (MES PARTIDO + LM622 FIX)
# =============================================================================

def motor_dmk_v8_7(f_dmk, df_v3, df_tarifas_proy, fecha_corte):
    # Lectura Polars ciega (todo texto para evitar LM622 error)
    data = f_dmk.getvalue()
    lf = pl.read_csv(io.BytesIO(data), separator=";", infer_schema_length=0).lazy()
    
    # Normalizar columnas y IDs
    lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.collect_schema().names()})
    lf = lf.with_columns([
        pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip_chars().str.to_uppercase(),
        pl.col("FECHA").str.to_date("%d/%m/%Y")
    ])
    
    # Preparar Tarifas para Polars
    df_tar_pl = pl.from_pandas(df_tarifas_proy).lazy()
    
    # Unir DMK con Nomenclador V3 (para tener el GT de cada línea)
    df_v3_pl = pl.from_pandas(formatear_ids(df_v3.copy(), [df_v3.columns[0]])).lazy()
    df_v3_pl = df_v3_pl.rename({df_v3.columns[0]: "ID_LINEA"})
    
    lf = lf.join(df_v3_pl, on="ID_LINEA", how="inner")
    
    # Unir con Tarifas
    lf = lf.join(df_tar_pl, on="GT", how="left")
    
    # LÓGICA DE MES PARTIDO
    corte = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
    lf = lf.with_columns(
        pl.when(pl.col("FECHA") <= corte)
        .then(pl.col("TARIFA_NOV"))
        .otherwise(pl.col("TARIFA_FEB"))
        .alias("TARIFA_APLICADA")
    )
    
    return lf.collect().to_pandas()

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

st.title("Fiscalización TTR Natalia v8.7")

# Estados persistentes
for k in ['v3', 'tarifas']:
    if k not in st.session_state: st.session_state[k] = None

t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO DMK"])

with t1:
    st.header("Cálculo de Tarifas (Noviembre -> Febrero)")
    f_nov = st.file_uploader("Subir Cuadro Tarifario Noviembre", key="tn")
    if f_nov:
        c = st.columns(5)
        m = {
            '1SCN': c[0].number_input("1SCN (Feb)", 494.33),
            '2SCN': c[1].number_input("2SCN (Feb)", 551.24),
            '3SCN': c[2].number_input("3SCN (Feb)", 593.70),
            '4SCN': c[3].number_input("4SCN (Feb)", 636.21),
            '5SCN': c[4].number_input("5SCN (Feb)", 678.42)
        }
        if st.button("📊 Proyectar Tarifas"):
            df_n = pd.read_excel(f_nov) if f_nov.name.endswith('xlsx') else pd.read_csv(f_nov)
            st.session_state.tarifas = motor_proyeccion_tarifas(df_n, m)
            st.success("Tarifas Febrero calculadas.")
    
    if st.session_state.tarifas is not None:
        st.dataframe(st.session_state.tarifas)

with t2:
    st.header("Sincronización Maestra (16 Columnas)")
    c1, c2 = st.columns(2)
    f_v2 = c1.file_uploader("Nomenclador Molde (V3 anterior)")
    f_elr = c2.file_uploader("ELR Febrero")
    if f_v2 and f_elr and st.button("🔄 Actualizar"):
        st.session_state.v3 = motor_maestro_v8_7(pd.read_excel(f_v2), pd.read_excel(f_elr))
        st.success("Nomenclador actualizado (Espejo de 16 columnas).")

with t3:
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        st.header("Liquidación Final (Mes Partido)")
        f_corte = st.date_input("Último día de tarifa vieja:", datetime(2026, 2, 14))
        f_dmk = st.file_uploader("Subir DMK (CSV)")
        if f_dmk and st.button("⚡ Procesar Liquidación"):
            res = motor_dmk_v8_7(f_dmk, st.session_state.v3, st.session_state.tarifas, str(f_corte))
            st.dataframe(res.head())
            # Descarga...
    else:
        st.info("💡 Completá los pasos 1 y 2 para habilitar la liquidación.")
