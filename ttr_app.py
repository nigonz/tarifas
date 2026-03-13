import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile

# --- CONFIGURACIÓN DE NIVEL PRODUCCIÓN ---
st.set_page_config(page_title="Fiscalización TTR v8.6", layout="wide")

# =============================================================================
# BLOQUE 0: EL ESCUDO DE NORMALIZACIÓN
# =============================================================================

def blindar_nombres(df):
    """Estandariza encabezados y elimina basura de los cruces de datos."""
    if df is None: return None
    df.columns = [str(c).upper().strip().replace(" ", "_").split('_X')[0].split('_Y')[0] for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def formatear_ids(df, columnas_id):
    """Fuerza IDs a texto limpio y evita errores de atributos en Pandas."""
    for col in columnas_id:
        if col in df.columns:
            # Se asegura el encadenamiento correcto de .str para evitar AttributeError
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: ACTUALIZACIÓN Y AUDITORÍA DE NOMENCLADORES
# =============================================================================

def motor_maestro_v8_6(df_v2, df_elr, df_ts):
    # 1. Normalización total de archivos
    df_v2, df_elr, df_ts = blindar_nombres(df_v2), blindar_nombres(df_elr), blindar_nombres(df_ts)

    # 2. Identificación de llaves (Línea y Ramal)
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
    
    # Cruce 1: Actualización del archivo de Ramales (TS)
    elr_map = df_elr[[id_l_elr, id_r_elr, col_gt, col_lin_nom]].drop_duplicates()
    ts_upd = df_ts.merge(elr_map, left_on=[id_l_ts, id_r_ts], right_on=[id_l_elr, id_r_elr], how='left')
    ts_upd = blindar_nombres(ts_upd)

    # Cruce 2: Actualización Maestro V3 + Auditoría
    elr_lin_map = df_elr[[id_l_elr, col_gt, col_lin_nom]].drop_duplicates(subset=[id_l_elr])
    if 'GT' in df_v2.columns:
        df_v2 = df_v2.rename(columns={'GT': 'GT_PREVIO'})
    
    v3_final = df_v2.merge(elr_lin_map, left_on=id_l_base, right_on=id_l_elr, how='left')
    v3_final = blindar_nombres(v3_final)
    
    # Generar Tabla Detalle para Auditoría
    audit = v3_final[[id_l_base, 'RAZON_SOCIAL', 'GT_PREVIO', col_gt]].copy() if 'GT_PREVIO' in v3_final.columns else v3_final[[id_l_base, 'RAZON_SOCIAL', col_gt]].copy()
    audit['ESTADO'] = audit[col_gt].apply(lambda x: '✅ Actualizado' if pd.notnull(x) else '⚠️ Sin cambios')
    
    # Renombrado final para compatibilidad con DMK
    v3_final = v3_final.rename(columns={col_lin_nom: 'LINEA_SILAS_FINAL', id_l_base: 'ID_LINEA', col_gt: 'GT'})
    
    return v3_final, ts_upd, audit

# =============================================================================
# BLOQUE 2: MOTOR DMK (SOLUCIÓN LM622 + STRIP_CHARS)
# =============================================================================

def motor_dmk_v8_6(f_sube, df_v3, df_en):
    try:
        if f_sube.name.endswith('.zip'):
            with zipfile.ZipFile(f_sube) as z:
                csv_f = [n for n in z.namelist() if n.endswith('.csv')][0]
                with z.open(csv_f) as f: data = f.read()
        else: data = f_sube.getvalue()
        
        # SOLUCIÓN: infer_schema_length=0 obliga a Polars a leer todo como texto
        # Esto permite que 'LM622' no cause conflictos con IDs numéricos
        try:
            lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=";", 
                             infer_schema_length=0).lazy()
            if len(lf.collect_schema().names()) < 5: raise Exception()
        except:
            lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=",", 
                             infer_schema_length=0).lazy()

        # Normalización DMK - Se usa strip_chars() para versiones nuevas de Polars
        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.collect_schema().names()})
        lf = lf.with_columns(pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip_chars().str.to_uppercase())
        
        v3_pl = pl.from_pandas(formatear_ids(df_v3.copy(), ["ID_LINEA"])).lazy()
        en_pl = pl.from_pandas(blindar_nombres(df_en)).lazy()
        en_pl = en_pl.with_columns(pl.col("DOMINIO").cast(pl.Utf8).str.strip_chars().str.to_uppercase())

        # Integración de datos
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(en_pl, on="DOMINIO", how="left")
        
        lf = lf.with_columns([
            pl.when(pl.col("ENERGIA").is_null()).then(pl.lit("NO")).otherwise(pl.col("DOMINIO")).alias("DOMINIO"),
            pl.col("ENERGIA").fill_null("3")
        ])
        
        # Cálculos (Se fuerza Float64 para operaciones matemáticas seguras)
        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION").cast(pl.Float64) * pl.col("CANTIDAD_USOS").cast(pl.Float64)).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Utf8) == "621")
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO").cast(pl.Float64) / 0.45) * 0.55 * pl.col("CANTIDAD_USOS").cast(pl.Float64))
                    .otherwise((pl.col("TARIFA_BASE_ITG").cast(pl.Float64) - pl.col("DEBITADO").cast(pl.Float64) - pl.col("DESCUENTO_X_INTEGRACION").cast(pl.Float64)) * pl.col("CANTIDAD_USOS").cast(pl.Float64))
              ).otherwise(0).alias("COMP_ATS")
        ])
        
        final = lf.group_by(['GT', 'LINEA_SILAS_FINAL', 'ID_LINEA', 'DOMINIO', 'ENERGIA']).agg([
            pl.col('CANTIDAD_USOS').cast(pl.Int64).sum(), 
            pl.col('COMP_ITG').sum(), 
            pl.col('COMP_ATS').sum()
        ]).collect().to_pandas()
        
        return final
    except Exception as e:
        st.error(f"Error crítico en DMK: {e}")
        return None

# =============================================================================
# INTERFAZ DE USUARIO (UI)
# =============================================================================

for k in ['v3', 'ts', 'audit', 'per']:
    if k not in st.session_state: st.session_state[k] = None if k != 'per' else "Febrero"

st.title(f"Módulo de Fiscalización TTR v8.6")

t1, t2, t3 = st.tabs(["💰 TARIFAS", "📋 NOMENCLADORES", "📂 PROCESO DMK"])

with t2:
    st.header("Sincronización Maestra y Auditoría")
    c1, c2, c3 = st.columns(3)
    fv2 = c1.file_uploader("Nomenclador v2 Base", key="nv2")
    felr = c2.file_uploader("ELR de Referencia", key="elr")
    fts = c3.file_uploader("Nomenclador TS", key="nts")
    
    if fv2 and felr and fts and st.button("🔄 Ejecutar Sincronización"):
        v3, ts_up, audit = motor_maestro_v8_6(pd.read_excel(fv2), pd.read_excel(felr), pd.read_excel(fts))
        st.session_state.v3, st.session_state.ts, st.session_state.audit = v3, ts_up, audit

    if st.session_state.audit is not None:
        st.subheader("📋 Detalle de Auditoría")
        st.dataframe(st.session_state.audit, use_container_width=True)
        col_a, col_b = st.columns(2)
        
        # Descargas persistentes
        b_v3 = io.BytesIO()
        st.session_state.v3.to_excel(b_v3, index=False)
        col_a.download_button("📥 Descargar Maestro V3", b_v3.getvalue(), "V3_Actualizado.xlsx", key="dl_v3")
        
        b_ts = io.BytesIO()
        st.session_state.ts.to_excel(b_ts, index=False)
        col_b.download_button("📥 Descargar TS Actualizado", b_ts.getvalue(), "TS_Actualizado.xlsx", key="dl_ts")

with t3:
    if st.session_state.v3 is not None:
        st.header("Liquidación Final DMK")
        fdmk = st.file_uploader("Archivo DMK (ZIP o CSV)", key="dmk_f")
        fen = st.file_uploader("Archivo de Energías", key="en_f")
        if fdmk and fen and st.button("⚡ Procesar Liquidación"):
            res = motor_dmk_v8_6(fdmk, st.session_state.v3, pd.read_excel(fen))
            if res is not None:
                st.dataframe(res.head())
                b_res = io.BytesIO()
                res.to_excel(b_res, index=False)
                st.download_button("📥 Bajar Resultado Liquidación", b_res.getvalue(), "Liquidacion_TTR.xlsx")
    else:
        st.warning("⚠️ Debes sincronizar los nomencladores en la pestaña anterior.")
