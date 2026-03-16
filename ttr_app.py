import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN DE NIVEL PRODUCCIÓN ---
st.set_page_config(page_title="Fiscalización TTR Natalia v9.6", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES DE NORMALIZACIÓN
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
# BLOQUE 1: MOTOR DE TARIFAS (PROYECCIÓN)
# =============================================================================

def motor_proyeccion_tarifas(df_nov, manuales):
    df = blindar_nombres(df_nov.copy())
    col_id = [c for c in df.columns if 'ID' in c or 'GT' in c][0]
    # Calculamos factor basado en 1SCN
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

# =============================================================================
# BLOQUE 2: MOTOR MAESTRO (ACTUALIZACIÓN V3 + TS + ALTAS NUEVAS)
# =============================================================================

def motor_maestro_v9_6(df_v3_maestro, df_ts_maestro, df_elr_nuevo):
    # Guardamos moldes originales
    cols_v3_org = df_v3_maestro.columns.tolist()
    cols_ts_org = df_ts_maestro.columns.tolist()
    
    v3_i = blindar_nombres(df_v3_maestro.copy())
    ts_i = blindar_nombres(df_ts_maestro.copy())
    elr_i = blindar_nombres(df_elr_nuevo.copy())
    
    id_l_elr = [c for c in elr_i.columns if 'ID_LINEA_BO' in c or 'ID' in c][0]
    id_r_elr = [c for c in elr_i.columns if 'ID_RAMAL_BO' in c or 'RAMAL' in c][0]
    id_l_v3 = v3_i.columns[0]
    id_l_ts = [c for c in ts_i.columns if 'IDLINEANS' in c or 'ID_LINEA' in c][0]
    id_r_ts = [c for c in ts_i.columns if 'IDRAMALNS' in c or 'ID_RAMAL' in c][0]

    elr_i = formatear_ids(elr_i, [id_l_elr, id_r_elr])
    v3_i = formatear_ids(v3_i, [id_l_v3])
    ts_i = formatear_ids(ts_i, [id_l_ts, id_r_ts])

    # 1. ACTUALIZAR NOMENCLADOR V3 (Líneas)
    elr_lin = elr_i.drop_duplicates(subset=[id_l_elr])
    ids_en_v3 = set(v3_i[id_l_v3])
    nuevas_lineas = elr_lin[~elr_lin[id_l_elr].isin(ids_en_v3)].copy()
    
    if not nuevas_lineas.empty:
        nuevas_v3 = pd.DataFrame(columns=v3_i.columns)
        nuevas_v3[id_l_v3] = nuevas_lineas[id_l_elr]
        nuevas_v3['RAZON_SOCIAL'] = nuevas_lineas['NOMBRE_EMPRESA']
        nuevas_v3['CUIT'] = nuevas_lineas['CUIT']
        nuevas_v3['GT'] = nuevas_lineas['GRUPO_TARIFARIO_LINEA_DNGFF']
        nuevas_v3['OBSERVACION'] = "ALTA DETECTADA EN ELR"
        v3_i = pd.concat([v3_i, nuevas_v3], ignore_index=True)

    v3_i = v3_i.merge(elr_lin[[id_l_elr, 'GRUPO_TARIFARIO_LINEA_DNGFF']], left_on=id_l_v3, right_on=id_l_elr, how='left')
    v3_i['GT'] = v3_i['GRUPO_TARIFARIO_LINEA_DNGFF'].fillna(v3_i['GT'])
    v3_final = v3_i[v3_i.columns[:len(cols_v3_org)]] 
    v3_final.columns = cols_v3_org

    # 2. ACTUALIZAR TS (Ramales)
    ts_i['LLAVE'] = ts_i[id_l_ts] + "_" + ts_i[id_r_ts]
    elr_i['LLAVE'] = elr_i[id_l_elr] + "_" + elr_i[id_r_elr]
    ids_en_ts = set(ts_i['LLAVE'])
    nuevos_ram = elr_i[~elr_i['LLAVE'].isin(ids_en_ts)].copy()
    
    if not nuevos_ram.empty:
        nuevos_ts = pd.DataFrame(columns=ts_i.columns)
        nuevos_ts[id_l_ts] = nuevos_ram[id_l_elr]
        nuevos_ts[id_r_ts] = nuevos_ram[id_r_elr]
        nuevos_ts['GT'] = nuevos_ram['GRUPO_TARIFARIO_LINEA_DNGFF']
        nuevos_ts['OBSERVACION'] = "NUEVO RAMAL"
        ts_i = pd.concat([ts_i, nuevos_ts], ignore_index=True)

    ts_i = ts_i.merge(elr_i[['LLAVE', 'GRUPO_TARIFARIO_LINEA_DNGFF']], on='LLAVE', how='left', suffixes=('', '_ELR'))
    ts_i['GT'] = ts_i['GRUPO_TARIFARIO_LINEA_DNGFF'].fillna(ts_i['GT'])
    ts_final = ts_i[ts_i.columns[:len(cols_ts_org)]]
    ts_final.columns = cols_ts_org

    return v3_final, ts_final

# =============================================================================
# BLOQUE 3: MOTOR DMK (LIQUIDACIÓN DINÁMICA)
# =============================================================================

def motor_dmk_v9_6(f_dmk, df_v3, df_tarifas, fecha_corte, df_en):
    try:
        data = f_dmk.getvalue() if not f_dmk.name.endswith('.zip') else zipfile.ZipFile(f_dmk).read(zipfile.ZipFile(f_dmk).namelist()[0])
        lf = pl.read_csv(io.BytesIO(data), separator=";", infer_schema_length=0, encoding='iso-8859-1').lazy()
        
        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.collect_schema().names()})
        lf = lf.with_columns([
            pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip_chars().str.to_uppercase(),
            pl.col("FECHA").str.to_date("%d/%m/%Y")
        ])
        
        # Joins
        v3_pl = pl.from_pandas(formatear_ids(df_v3.copy(), [df_v3.columns[0]])).lazy().rename({df_v3.columns[0]: "ID_LINEA"})
        tar_pl = pl.from_pandas(df_tarifas).lazy()
        en_pl = pl.from_pandas(blindar_nombres(df_en)).lazy()
        
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(tar_pl, on="GT", how="left").join(en_pl, on="DOMINIO", how="left")
        
        # Mes Partido
        corte = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(pl.when(pl.col("FECHA") <= corte).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("TARIFA_PRACTICADA"))
        
        # Compensaciones
        cols = ["CANTIDAD_USOS", "DESCUENTO_X_INTEGRACION", "DEBITADO"]
        lf = lf.with_columns([pl.col(c).cast(pl.Float64).fill_null(0) for c in cols])
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
# INTERFAZ DE USUARIO (UI)
# =============================================================================

# Inicialización de memoria para que nada desaparezca
for k in ['v3', 'ts', 'tarifas', 'res_dmk']:
    if k not in st.session_state: st.session_state[k] = None

st.title("🛡️ Fiscalización TTR Natalia v9.6")
t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO DMK"])

with t1:
    st.header("Cálculo de Tarifas Proyectadas")
    f_nov = st.file_uploader("Subir Cuadro Tarifario Noviembre", key="f_t")
    if f_nov:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Generar Tarifas Febrero"):
            st.session_state.tarifas = motor_proyeccion_tarifas(pd.read_excel(f_nov), m)
    if st.session_state.tarifas is not None:
        st.dataframe(st.session_state.tarifas)

with t2:
    st.header("Actualización V3 y TS (Con Altas del ELR)")
    c1, c2, c3 = st.columns(3)
    fv3 = c1.file_uploader("Nomenclador V3 (16 col)")
    fts = c2.file_uploader("Nomenclador TS (9 col)")
    felr = c3.file_uploader("ELR Nuevo")
    
    if fv3 and fts and felr and st.button("🔄 Sincronizar Archivos Maestros"):
        v3_res, ts_res = motor_maestro_v9_6(pd.read_excel(fv3), pd.read_excel(fts), pd.read_excel(felr))
        st.session_state.v3, st.session_state.ts = v3_res, ts_res
        st.success(f"Sincronización OK. Líneas: {len(v3_res)} | Ramales: {len(ts_res)}")

    if st.session_state.v3 is not None:
        st.divider()
        col_v3, col_ts = st.columns(2)
        # Descarga V3
        buf_v3 = io.BytesIO(); st.session_state.v3.to_excel(buf_v3, index=False)
        col_v3.download_button("📥 Bajar V3 Actualizado", buf_v3.getvalue(), "Nomenclador_V3_Feb26.xlsx")
        # Descarga TS
        buf_ts = io.BytesIO(); st.session_state.ts.to_excel(buf_ts, index=False)
        col_ts.download_button("📥 Bajar TS Actualizado", buf_ts.getvalue(), "TS_Feb26.xlsx")
        st.dataframe(st.session_state.v3.head())

with t3:
    st.header("Liquidación Final DMK")
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        corte = st.date_input("Fecha cambio de tarifa (inclusive):", datetime(2026, 2, 14))
        f_dmk = st.file_uploader("Subir DMK (CSV/ZIP)")
        f_en = st.file_uploader("Subir Energías (Excel)")
        if f_dmk and f_en and st.button("⚡ Ejecutar Liquidación"):
            st.session_state.res_dmk = motor_dmk_v9_6(f_dmk, st.session_state.v3, st.session_state.tarifas, str(corte), pd.read_excel(f_en))
        
        if st.session_state.res_dmk is not None:
            buf_dmk = io.BytesIO(); st.session_state.res_dmk.to_excel(buf_dmk, index=False)
            st.download_button("📥 DESCARGAR LIQUIDACIÓN FINAL", buf_dmk.getvalue(), "Liquidacion_Final_Feb.xlsx", key="dl_dmk")
            st.dataframe(st.session_state.res_dmk.head())
    else:
        st.warning("⚠️ Debes completar las pestañas 1 y 2 primero.")
