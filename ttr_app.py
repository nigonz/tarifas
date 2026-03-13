import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile

# --- CONFIGURACIÓN DE PRODUCCIÓN ---
st.set_page_config(page_title="Fiscalización TTR Natalia v7.5", layout="wide")

# =============================================================================
# UTILIDADES DE NORMALIZACIÓN (EL ESCUDO)
# =============================================================================

def blindar_df(df):
    """Limpia nombres de columnas y quita sufijos de merge automáticamente."""
    if df is None: return None
    df.columns = [str(c).upper().split('_X')[0].split('_Y')[0].strip().replace(" ", "_") for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df

# =============================================================================
# MOTOR 2: ACTUALIZACIÓN MAESTRA (LÓGICA ELR DICIEMBRE)
# =============================================================================

def motor_maestro_v75(df_v2, df_elr, df_ts):
    # 1. Blindaje y Normalización
    df_v2 = blindar_df(df_v2)
    df_elr = blindar_df(df_elr)
    df_ts = blindar_df(df_ts)

    # 2. Identificación de IDs (Línea y Ramal)
    # Buscamos IDs en el ELR (Referencia Diciembre)
    id_l_elr = [c for c in df_elr.columns if 'ID' in c and 'LINEA' in c][0]
    id_r_elr = [c for c in df_elr.columns if 'ID' in c and 'RAMAL' in c][0]
    
    # Buscamos IDs en el TS (Nomenclador Ramal)
    id_l_ts = [c for c in df_ts.columns if 'ID' in c and 'LINEA' in c or 'IDLINEANS' in c][0]
    id_r_ts = [c for c in df_ts.columns if 'ID' in c and 'RAMAL' in c or 'IDRAMALNS' in c][0]
    
    # Buscamos ID en el Maestro Base
    id_l_base = [c for c in df_v2.columns if 'ID' in c and 'LINEA' in c][0]

    # Limpieza de valores (Texto sin .0)
    for d, c_l, c_r in [(df_elr, id_l_elr, id_r_elr), (df_ts, id_l_ts, id_r_ts)]:
        d[c_l] = d[c_l].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        d[c_r] = d[c_r].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df_v2[id_l_base] = df_v2[id_l_base].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # 3. ACTUALIZACIÓN DEL TS (RAMAL POR RAMAL)
    # Traemos GT y LINEA del ELR
    col_gt = [c for c in df_elr.columns if 'GRUPO' in c and 'TARIF' in c][0]
    col_lin = [c for c in df_elr.columns if 'LINEA' in c and 'BO' not in c and 'ID' not in c][0]
    
    elr_map = df_elr[[id_l_elr, id_r_elr, col_gt, col_lin]].drop_duplicates()
    
    # Cruzamos usando la DOBLE LLAVE (Línea + Ramal) para que sea exacto como en tu Excel
    ts_upd = df_ts.merge(elr_map, left_on=[id_l_ts, id_r_ts], right_on=[id_l_elr, id_r_elr], how='left')
    ts_upd = blindar_df(ts_upd)

    # 4. ACTUALIZACIÓN DEL MAESTRO V3 (Nivel Línea)
    elr_lin_map = df_elr[[id_l_elr, col_gt, col_lin]].drop_duplicates(subset=[id_l_elr])
    v3_final = df_v2.merge(elr_lin_map, left_on=id_l_base, right_on=id_l_elr, how='left')
    v3_final = blindar_df(v3_final)
    
    # Forzamos nombres para el motor de Polars
    v3_final = v3_final.rename(columns={col_lin: 'LINEA_SILAS_FINAL', id_l_base: 'ID_LINEA'})

    return v3_final, ts_upd

# =============================================================================
# MOTOR 3: DMK (POLARS + ZIP)
# =============================================================================

def motor_dmk_v75(f_sube, df_v3, df_en):
    try:
        if f_sube.name.endswith('.zip'):
            with zipfile.ZipFile(f_sube) as z:
                csv_f = [n for n in z.namelist() if n.endswith('.csv')][0]
                with z.open(csv_f) as f: data = f.read()
        else: data = f_sube.getvalue()
        
        lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=";", infer_schema_length=10000).lazy()
        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.collect_schema().names()})
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))
        
        v3_pl = pl.from_pandas(df_v3).lazy()
        en_pl = pl.from_pandas(blindar_df(df_en)).lazy()

        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(en_pl, on="DOMINIO", how="left")
        lf = lf.with_columns([
            pl.when(pl.col("ENERGIA").is_null()).then(pl.lit("NO")).otherwise(pl.col("DOMINIO")).alias("DOMINIO"),
            pl.col("ENERGIA").fill_null(3)
        ])
        
        # Cálculos de Compensación
        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO") == 621)
              .then(pl.when(pl.col("GT") == "INP").then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS")).otherwise((pl.col("TARIFA_BASE_ITG") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))).otherwise(0).alias("COMP_ATS")
        ])
        
        final = lf.group_by(['GT', 'LINEA_SILAS_FINAL', 'ID_LINEA', 'DOMINIO', 'ENERGIA']).agg([pl.col('CANTIDAD_USOS').sum(), pl.col('COMP_ITG').sum(), pl.col('COMP_ATS').sum()]).collect().to_pandas()
        return final
    except Exception as e:
        st.error(f"Error en DMK: {e}")
        return None

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

if 'df_v3' not in st.session_state: st.session_state.df_v3 = None
if 'df_ts_upd' not in st.session_state: st.session_state.df_ts_upd = None
if 'per' not in st.session_state: st.session_state.per = "Febrero"

st.title(f"Sistema TTR Natalia v7.5 - Producción")

t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO DMK"])

with t2:
    st.header("Sincronización Línea + Ramal (Lógica Diciembre)")
    c1, c2, c3 = st.columns(3)
    f_v2 = c1.file_uploader("Nomenclador v2 Base")
    f_elr = c2.file_uploader("ELR de Referencia (Feb/Dic)")
    f_ts = c3.file_uploader("Nomenclador TS (Ramales)")
    
    if f_v2 and f_elr and f_ts and st.button("🔄 Ejecutar Sincronización Doble"):
        v3, ts_up = motor_maestro_v75(pd.read_excel(f_v2), pd.read_excel(f_elr), pd.read_excel(f_ts))
        st.session_state.df_v3 = v3
        st.session_state.df_ts_upd = ts_up
        st.success("✅ Sincronización exitosa por Línea y Ramal.")

    if st.session_state.df_v3 is not None:
        col_d1, col_d2 = st.columns(2)
        
        # Descarga Maestro V3
        buf_v3 = io.BytesIO()
        st.session_state.df_v3.to_excel(buf_v3, index=False)
        col_d1.download_button("📥 Bajar Nomenclador V3", buf_v3.getvalue(), f"Maestro_V3_{st.session_state.per}.xlsx", key="v3")
        
        # Descarga TS Actualizado
        buf_ts = io.BytesIO()
        st.session_state.df_ts_upd.to_excel(buf_ts, index=False)
        col_d2.download_button("📥 Bajar TS Actualizado", buf_ts.getvalue(), f"TS_Actualizado_{st.session_state.per}.xlsx", key="ts")
        
        st.dataframe(st.session_state.df_v3.head())

# (El código de Tab 1 y Tab 3 se mantiene con la lógica de persistencia de la v7.4)
