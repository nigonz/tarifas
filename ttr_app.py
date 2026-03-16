import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR Natalia v9.0", layout="wide")

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
# BLOQUE 1: MOTORES (TARIFAS Y MAESTRO)
# =============================================================================

def motor_proyeccion_tarifas(df_nov, manuales):
    df = blindar_nombres(df_nov.copy())
    col_id = [c for c in df.columns if 'ID' in c or 'GT' in c][0]
    v1_ant = pd.to_numeric(df.loc[df[col_id] == '1SCN', 'LIMITE_INFERIOR'].astype(str).str.replace(',', '.'), errors='coerce').values[0]
    factor = manuales['1SCN'] / v1_ant
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row[col_id]).strip().upper()
        v_ant = pd.to_numeric(str(row['LIMITE_INFERIOR']).replace(',', '.'), errors='coerce')
        if id_t in manuales: v_nue = manuales[id_t]
        elif 'SEN' in id_t and 'SESN' not in id_t: v_nue = manuales.get(id_t.replace('SEN', 'SCN'), manuales['1SCN']) * 1.25
        elif 'SEAN' in id_t and 'SEASN' not in id_t: v_nue = manuales.get(id_t.replace('SEAN', 'SCN'), manuales['1SCN']) * 1.75
        elif 'SCSN' in id_t: v_nue = manuales.get(id_t.replace('SCSN', 'SCN'), manuales['1SCN']) * 1.59
        elif 'SESN' in id_t: v_nue = (manuales.get(id_t.replace('SESN', 'SCN'), manuales['1SCN']) * 1.59) * 1.25
        elif 'SEASN' in id_t: v_nue = (manuales.get(id_t.replace('SEASN', 'SCN'), manuales['1SCN']) * 1.59) * 1.75
        else: v_nue = v_ant * factor
        res.append({'GT': id_t, 'TARIFA_NOV': round(v_ant, 2), 'TARIFA_FEB': round(v_nue, 2)})
    return pd.DataFrame(res)

def motor_maestro_v9_0(df_v2, df_elr):
    cols_org = df_v2.columns.tolist()
    ids_org = df_v2[df_v2.columns[0]].astype(str).str.strip().str.upper().tolist()
    df_v2_i, df_elr_i = blindar_nombres(df_v2.copy()), blindar_nombres(df_elr.copy())
    id_l_base = df_v2_i.columns[0]
    id_l_elr = [c for c in df_elr_i.columns if 'ID_LINEA_BO' in c or 'ID' in c][0]
    col_gt_elr = [c for c in df_elr_i.columns if 'GRUPO_TARIF' in c][0]
    elr_map = df_elr_i[[id_l_elr, col_gt_elr]].drop_duplicates(subset=[id_l_elr])
    v3 = df_v2_i.merge(elr_map, left_on=id_l_base, right_on=id_l_elr, how='left')
    v3['GT'] = v3[col_gt_elr].fillna(v3['GT'])
    v3 = v3[v3[id_l_base].isin(ids_org)]
    v3 = v3[df_v2_i.columns]
    v3.columns = cols_org
    return v3

# =============================================================================
# BLOQUE 2: MOTOR DMK (LIQUIDACIÓN COMPLETA)
# =============================================================================

def motor_dmk_v9_0(f_dmk, df_v3, df_tarifas, fecha_corte, df_en):
    try:
        # Lectura segura con Polars (Fix LM622)
        if f_dmk.name.endswith('.zip'):
            with zipfile.ZipFile(f_dmk) as z:
                csv_f = [n for n in z.namelist() if n.endswith('.csv')][0]
                with z.open(csv_f) as f: data = f.read()
        else: data = f_dmk.getvalue()
        
        lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=";", infer_schema_length=0).lazy()
        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.collect_schema().names()})
        lf = lf.with_columns([
            pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip_chars().str.to_uppercase(),
            pl.col("FECHA").str.to_date("%d/%m/%Y")
        ])
        
        # Joins
        v3_pl = pl.from_pandas(formatear_ids(df_v3.copy(), [df_v3.columns[0]])).lazy()
        v3_pl = v3_pl.rename({df_v3.columns[0]: "ID_LINEA"})
        tar_pl = pl.from_pandas(df_tarifas).lazy()
        en_pl = pl.from_pandas(blindar_nombres(df_en)).lazy()
        
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(tar_pl, on="GT", how="left").join(en_pl, on="DOMINIO", how="left")
        
        # Lógica Mes Partido
        corte = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(pl.when(pl.col("FECHA") <= corte).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("TARIFA_PRACTICADA"))
        
        # Cálculos
        cols_calc = ["CANTIDAD_USOS", "DESCUENTO_X_INTEGRACION", "DEBITADO"]
        lf = lf.with_columns([pl.col(c).cast(pl.Float64).fill_null(0) for c in cols_calc])
        
        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Utf8) == "621")
              .then(pl.when(pl.col("GT") == "INP").then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS")).otherwise((pl.col("TARIFA_PRACTICADA") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS")))
              .otherwise(0).alias("COMP_ATS")
        ])
        
        return lf.group_by(['GT', 'ID_LINEA', 'DOMINIO', 'ENERGIA']).agg([pl.col('CANTIDAD_USOS').sum(), pl.col('COMP_ITG').sum(), pl.col('COMP_ATS').sum()]).collect().to_pandas()
    except Exception as e:
        st.error(f"Error en liquidación: {e}")
        return None

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

for k in ['v3', 'tarifas', 'res_dmk']:
    if k not in st.session_state: st.session_state[k] = None

st.title("🛡️ Fiscalización TTR Natalia v9.0")
t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO DMK"])

with t1:
    st.header("1. Proyección Tarifaria")
    f_nov = st.file_uploader("Cuadro Noviembre", key="tn")
    if f_nov:
        c = st.columns(5); m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"): st.session_state.tarifas = motor_proyeccion_tarifas(pd.read_excel(f_nov), m)
    if st.session_state.tarifas is not None: st.dataframe(st.session_state.tarifas)

with t2:
    st.header("2. Sincronización V3")
    f_v2 = st.file_uploader("Nomenclador Molde"); f_elr = st.file_uploader("ELR Febrero")
    if f_v2 and f_elr and st.button("🔄 Actualizar"):
        st.session_state.v3 = motor_maestro_v9_0(pd.read_excel(f_v2), pd.read_excel(f_elr))
        st.success("¡V3 listo!")

with t3:
    st.header("3. Liquidación con Persistencia")
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        corte_dt = st.date_input("Fecha de cambio:", datetime(2026, 2, 14))
        f_dmk = st.file_uploader("DMK"); f_en = st.file_uploader("Energías")
        
        if f_dmk and f_en and st.button("⚡ GENERAR"):
            st.session_state.res_dmk = motor_dmk_v9_0(f_dmk, st.session_state.v3, st.session_state.tarifas, str(corte_dt), pd.read_excel(f_en))
        
        if st.session_state.res_dmk is not None:
            st.success("Cálculo finalizado.")
            st.dataframe(st.session_state.res_dmk.head())
            buf = io.BytesIO(); st.session_state.res_dmk.to_excel(buf, index=False)
            st.download_button("📥 Descargar Liquidación Final", buf.getvalue(), "Liquidacion_TTR_Final.xlsx", key="btn_dl")
    else:
        st.warning("⚠️ Completá Tarifas y Nomenclador primero.")
