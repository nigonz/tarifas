import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR Natalia v9.1", layout="wide")

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

def motor_maestro_v9_2(df_v2, df_elr, df_ts):
    """Actualiza Nomenclador V3 (16 col) y TS (Ramales) usando el ELR."""
    # 1. Preparar Moldes
    cols_v3_org = df_v2.columns.tolist()
    ids_v3_org = df_v2[df_v2.columns[0]].astype(str).str.strip().str.upper().tolist()
    
    # 2. Normalización Interna
    v2_i = blindar_nombres(df_v2.copy())
    elr_i = blindar_nombres(df_elr.copy())
    ts_i = blindar_nombres(df_ts.copy())
    
    # Identificar Llaves Dinámicamente
    id_l_elr = [c for c in elr_i.columns if 'ID_LINEA_BO' in c or 'ID' in c][0]
    id_r_elr = [c for c in elr_i.columns if 'ID_RAMAL_BO' in c or 'RAMAL' in c][0]
    
    id_l_ts = [c for c in ts_i.columns if 'IDLINEANS' in c or 'ID_LINEA' in c][0]
    id_r_ts = [c for c in ts_i.columns if 'IDRAMALNS' in c or 'ID_RAMAL' in c][0]
    
    id_l_v2 = v2_i.columns[0]
    col_gt_elr = [c for c in elr_i.columns if 'GRUPO_TARIF' in c][0]
    col_nom_elr = [c for c in elr_i.columns if 'LINEA' in c and 'ID' not in c and 'BO' not in c][0]

    # Formatear IDs para el cruce
    elr_i = formatear_ids(elr_i, [id_l_elr, id_r_elr])
    ts_i = formatear_ids(ts_i, [id_l_ts, id_r_ts])
    v2_i = formatear_ids(v2_i, [id_l_v2])

    # --- PARTE A: ACTUALIZACIÓN TS (RAMAL POR RAMAL) ---
    
    elr_ramal_map = elr_i[[id_l_elr, id_r_elr, col_gt_elr, col_nom_elr]].drop_duplicates()
    ts_upd = ts_i.merge(elr_ramal_map, left_on=[id_l_ts, id_r_ts], right_on=[id_l_elr, id_r_elr], how='left')
    
    # --- PARTE B: ACTUALIZACIÓN V3 (MOLDE 16 COLUMNAS) ---
    elr_linea_map = elr_i[[id_l_elr, col_gt_elr, col_nom_elr]].drop_duplicates(subset=[id_l_elr])
    v3_upd = v2_i.merge(elr_linea_map, left_on=id_l_v2, right_on=id_l_elr, how='left')
    
    # Pisar GT y Limpiar Estructura
    v3_upd['GT'] = v3_upd[col_gt_elr].fillna(v3_upd['GT'])
    v3_upd = v3_upd[v3_upd[id_l_v2].isin(ids_v3_org)] # Solo las 443 líneas
    v3_upd = v3_upd[v2_i.columns] # Solo las columnas del molde
    v3_upd.columns = cols_v3_org # Restaurar nombres originales
    
    return v3_upd, ts_upd

# =============================================================================
# BLOQUE 2: MOTOR DMK (LIQUIDACIÓN CON FIX DE TARIFAS)
# =============================================================================

def motor_dmk_v9_1(f_dmk, df_v3, df_tarifas, fecha_corte, df_en):
    try:
        # 1. Lectura segura (Polars)
        data = f_dmk.getvalue() if not f_dmk.name.endswith('.zip') else zipfile.ZipFile(f_dmk).read(zipfile.ZipFile(f_dmk).namelist()[0])
        lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=";", infer_schema_length=0).lazy()
        
        # 2. Normalización de DMK
        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.collect_schema().names()})
        lf = lf.with_columns([
            pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip_chars().str.to_uppercase(),
            pl.col("FECHA").str.to_date("%d/%m/%Y")
        ])
        
        # 3. Joins con Normalización de GT (Evita que la tarifa sea 0)
        v3_pl = pl.from_pandas(formatear_ids(df_v3.copy(), [df_v3.columns[0]])).lazy()
        v3_pl = v3_pl.rename({df_v3.columns[0]: "ID_LINEA"})
        v3_pl = v3_pl.with_columns(pl.col("GT").str.strip_chars().str.to_uppercase()) # Normalizar GT en V3
        
        tar_pl = pl.from_pandas(df_tarifas).lazy()
        tar_pl = tar_pl.with_columns(pl.col("GT").str.strip_chars().str.to_uppercase()) # Normalizar GT en Tarifas
        
        en_pl = pl.from_pandas(blindar_nombres(df_en)).lazy()
        en_pl = en_pl.with_columns(pl.col("DOMINIO").str.strip_chars().str.to_uppercase())
        
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(tar_pl, on="GT", how="left").join(en_pl, on="DOMINIO", how="left")
        
        # 4. Mes Partido
        corte = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(pl.when(pl.col("FECHA") <= corte).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("TARIFA_PRACTICADA"))
        
        # 5. Cálculos (Convertir a Float para evitar errores)
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

st.title("🛡️ Fiscalización TTR Natalia v9.1")
t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO DMK"])

with t1:
    st.header("1. Proyección Tarifaria")
    f_nov = st.file_uploader("Cuadro Noviembre", key="tn")
    if f_nov:
        c = st.columns(5); m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"): st.session_state.tarifas = motor_proyeccion_tarifas(pd.read_excel(f_nov), m)
    if st.session_state.tarifas is not None: st.dataframe(st.session_state.tarifas)

for k in ['v3_final', 'ts_final', 'tarifas_proy', 'res_dmk']:
    if k not in st.session_state: st.session_state[k] = None

st.title("🛡️ Sistema de Fiscalización v9.2")
t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO DMK"])

with t2:
    st.header("Sincronización de Nomenclador y Ramales (TS)")
    c1, c2, c3 = st.columns(3)
    f_v2 = c1.file_uploader("Nomenclador Molde (V3)", key="uv3")
    f_elr = c2.file_uploader("ELR Febrero", key="uelr")
    f_ts = c3.file_uploader("Archivo TS (Ramales)", key="uts")
    
    if f_v2 and f_elr and f_ts and st.button("🔄 EJECUTAR ACTUALIZACIÓN MAESTRA"):
        v3_res, ts_res = motor_maestro_v9_2(pd.read_excel(f_v2), pd.read_excel(f_elr), pd.read_excel(f_ts))
        st.session_state.v3_final = v3_res
        st.session_state.ts_final = ts_res
        st.success("✅ Actualización completada.")

    # MOSTRAR BOTONES DE DESCARGA (Persistentes)
    if st.session_state.v3_final is not None:
        st.divider()
        st.subheader("📥 Descargar Archivos Actualizados")
        col_dl1, col_dl2 = st.columns(2)
        
        # Botón Nomenclador V3
        buf_v3 = io.BytesIO()
        st.session_state.v3_final.to_excel(buf_v3, index=False)
        col_dl1.download_button("📂 Bajar Nomenclador V3 (16 col)", buf_v3.getvalue(), "Nomenclador_V3_Feb.xlsx", key="dl_v3")
        
        # Botón TS
        buf_ts = io.BytesIO()
        st.session_state.ts_final.to_excel(buf_ts, index=False)
        col_dl2.download_button("📂 Bajar TS Actualizado", buf_ts.getvalue(), "TS_Actualizado_Feb.xlsx", key="dl_ts")
        
        st.dataframe(st.session_state.v3_final.head(10))

with t3:
    st.header("3. Liquidación con Persistencia")
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        corte_dt = st.date_input("Fecha de cambio:", datetime(2026, 2, 14))
        f_dmk = st.file_uploader("DMK"); f_en = st.file_uploader("Energías")
        
        if f_dmk and f_en and st.button("⚡ GENERAR"):
            st.session_state.res_dmk = motor_dmk_v9_1(f_dmk, st.session_state.v3, st.session_state.tarifas, str(corte_dt), pd.read_excel(f_en))
        
        if st.session_state.res_dmk is not None:
            # BOTÓN DE DESCARGA ARRIBA PARA QUE NO SE PIERDA
            col_res1, col_res2 = st.columns([2, 1])
            col_res1.success("✅ Liquidación calculada con éxito.")
            
            # Generar el Excel en memoria
            buf = io.BytesIO()
            st.session_state.res_dmk.to_excel(buf, index=False)
            
            col_res2.download_button(
                label="📥 DESCARGAR RESULTADO FINAL",
                data=buf.getvalue(),
                file_name=f"Liquidacion_TTR_Final.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="btn_descarga_fijo"
            )
            
            st.dataframe(st.session_state.res_dmk, use_container_width=True)
    else:
        st.warning("⚠️ Completá Tarifas y Nomenclador primero.")
