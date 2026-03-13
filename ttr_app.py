import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile

# --- CONFIGURACIÓN DE NIVEL PRODUCCIÓN ---
st.set_page_config(page_title="Fiscalización TTR v8.4", layout="wide")

# =============================================================================
# BLOQUE 0: EL ESCUDO (NORMALIZACIÓN ABSOLUTA)
# =============================================================================

def blindar_nombres(df):
    """Limpia nombres de columnas, elimina sufijos de merge y quita duplicados."""
    if df is None: return None
    df.columns = [str(c).upper().strip().replace(" ", "_").split('_X')[0].split('_Y')[0] for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def formatear_ids(df, columnas_id):
    """Fuerza los IDs a texto limpio para evitar errores de match."""
    for col in columnas_id:
        if col in df.columns:
            # CORRECCIÓN PANDAS: .str.strip().str.upper() para evitar AttributeError
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (PROYECCIÓN)
# =============================================================================

def motor_tarifas_v8(df_base, manuales):
    df = blindar_nombres(df_base.copy())
    col_id = [c for c in df.columns if 'ID' in c][0]
    for col in ['LIMITE_INFERIOR', 'LIMITE_SUPERIOR']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
    
    v1_ant = df.loc[df[col_id] == '1SCN', 'LIMITE_INFERIOR'].values[0]
    factor = manuales['1SCN'] / v1_ant
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row[col_id]).strip().upper()
        v_min_ant, v_max_ant = row['LIMITE_INFERIOR'], row['LIMITE_SUPERIOR']
        
        if id_t in manuales: v_min = v_max = manuales[id_t]
        elif 'SEN' in id_t and 'SESN' not in id_t: v_min = v_max = manuales.get(id_t.replace('SEN', 'SCN'), manuales['1SCN']) * 1.25
        elif 'SEAN' in id_t and 'SEASN' not in id_t: v_min = v_max = manuales.get(id_t.replace('SEAN', 'SCN'), manuales['1SCN']) * 1.75
        elif 'SCSN' in id_t: v_min = v_max = manuales.get(id_t.replace('SCSN', 'SCN'), manuales['1SCN']) * 1.59
        elif 'SESN' in id_t: v_min = v_max = (manuales.get(id_t.replace('SESN', 'SCN'), manuales['1SCN']) * 1.59) * 1.25
        elif 'SEASN' in id_t: v_min = v_max = (manuales.get(id_t.replace('SEASN', 'SCN'), manuales['1SCN']) * 1.59) * 1.75
        else: v_min, v_max = v_min_ant * factor, v_max_ant * factor

        res.append({
            'ID': id_t, 'ANTERIOR': round(v_min_ant, 2), 'NUEVO': round(v_min, 2),
            'VAR_%': round(((v_min/v_min_ant)-1)*100, 2) if v_min_ant > 0 else 0,
            'LIMITE_SUPERIOR': round(v_max, 2)
        })
    return pd.DataFrame(res)

# =============================================================================
# BLOQUE 2: MOTOR DE NOMENCLADORES (AUDITORÍA + DOBLE LLAVE)
# =============================================================================

def motor_maestro_v8_4(df_v2, df_elr, df_ts):
    df_v2, df_elr, df_ts = blindar_nombres(df_v2), blindar_nombres(df_elr), blindar_nombres(df_ts)

    id_l_elr = [c for c in df_elr.columns if 'ID_LINEA_BO' in c or ('ID' in c and 'LINEA' in c)][0]
    id_r_elr = [c for c in df_elr.columns if 'ID_RAMAL_BO' in c or ('ID' in c and 'RAMAL' in c)][0]
    id_l_ts = [c for c in df_ts.columns if 'IDLINEANS' in c or ('ID' in c and 'LINEA' in c)][0]
    id_r_ts = [c for c in df_ts.columns if 'IDRAMALNS' in c or ('ID' in c and 'RAMAL' in c)][0]
    id_l_base = [c for c in df_v2.columns if 'ID_LINEA' in c][0]

    df_elr = formatear_ids(df_elr, [id_l_elr, id_r_elr])
    df_ts = formatear_ids(df_ts, [id_l_ts, id_r_ts])
    df_v2 = formatear_ids(df_v2, [id_l_base])

    col_gt = [c for c in df_elr.columns if 'GRUPO_TARIF' in c][0]
    col_lin_nom = [c for c in df_elr.columns if 'LINEA' in c and 'BO' not in c and 'ID' not in c][0]
    
    # Actualización TS (Ramal por Ramal)
    elr_map = df_elr[[id_l_elr, id_r_elr, col_gt, col_lin_nom]].drop_duplicates()
    ts_actualizado = df_ts.merge(elr_map, left_on=[id_l_ts, id_r_ts], right_on=[id_l_elr, id_r_elr], how='left')
    ts_actualizado = blindar_nombres(ts_actualizado)

    # Actualización Maestro V3 + Auditoría
    elr_lin_map = df_elr[[id_l_elr, col_gt, col_lin_nom]].drop_duplicates(subset=[id_l_elr])
    if 'GT' in df_v2.columns:
        df_v2 = df_v2.rename(columns={'GT': 'GT_ANTERIOR'})
    
    v3_final = df_v2.merge(elr_lin_map, left_on=id_l_base, right_on=id_l_elr, how='left')
    v3_final = blindar_nombres(v3_final)
    
    # Auditoría Detail
    audit = v3_final[[id_l_base, 'RAZON_SOCIAL', 'GT_ANTERIOR', col_gt]].copy() if 'GT_ANTERIOR' in v3_final.columns else v3_final[[id_l_base, 'RAZON_SOCIAL', col_gt]].copy()
    audit['ESTADO'] = audit[col_gt].apply(lambda x: '✅ Actualizado' if pd.notnull(x) else '⚠️ Sin datos ELR')
    
    v3_final = v3_final.rename(columns={col_lin_nom: 'LINEA_SILAS_FINAL', id_l_base: 'ID_LINEA', col_gt: 'GT'})
    return v3_final, ts_actualizado, audit

# =============================================================================
# BLOQUE 3: MOTOR DMK (CORRECCIÓN POLARS STRIP_CHARS)
# =============================================================================

def motor_dmk_v8_4(f_sube, df_v3, df_en):
    try:
        if f_sube.name.endswith('.zip'):
            with zipfile.ZipFile(f_sube) as z:
                csv_f = [n for n in z.namelist() if n.endswith('.csv')][0]
                with z.open(csv_f) as f: data = f.read()
        else: data = f_sube.getvalue()
        
        # Schema overrides para forzar texto en IDs
        try:
            lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=";", 
                             infer_schema_length=10000, 
                             schema_overrides={"ID_LINEA": pl.Utf8, "RAMAL": pl.Utf8}).lazy()
            if len(lf.collect_schema().names()) < 5: raise Exception()
        except:
            lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=",", 
                             infer_schema_length=10000,
                             schema_overrides={"ID_LINEA": pl.Utf8, "RAMAL": pl.Utf8}).lazy()

        # Normalización DMK - CORRECCIÓN: Usamos strip_chars() en lugar de strip()
        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.collect_schema().names()})
        lf = lf.with_columns(pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip_chars().str.to_uppercase())
        
        df_v3_proc = formatear_ids(df_v3.copy(), ["ID_LINEA"])
        v3_pl = pl.from_pandas(df_v3_proc).lazy()
        
        en_pl = pl.from_pandas(blindar_nombres(df_en)).lazy()
        en_pl = en_pl.with_columns(pl.col("DOMINIO").cast(pl.Utf8).str.strip_chars().str.to_uppercase())

        # Join y Lógica de Negocio
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(en_pl, on="DOMINIO", how="left")
        lf = lf.with_columns([
            pl.when(pl.col("ENERGIA").is_null()).then(pl.lit("NO")).otherwise(pl.col("DOMINIO")).alias("DOMINIO"),
            pl.col("ENERGIA").fill_null(3)
        ])
        
        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO") == 621)
              .then(pl.when(pl.col("GT") == "INP").then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS")).otherwise((pl.col("TARIFA_BASE_ITG") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))).otherwise(0).alias("COMP_ATS")
        ])
        
        final = lf.group_by(['GT', 'LINEA_SILAS_FINAL', 'ID_LINEA', 'DOMINIO', 'ENERGIA']).agg([pl.col('CANTIDAD_USOS').sum(), pl.col('COMP_ITG').sum(), pl.col('COMP_ATS').sum()]).collect().to_pandas()
        return final
    except Exception as e:
        st.error(f"Error crítico en DMK: {e}")
        return None

# =============================================================================
# INTERFAZ (UI) - PERSISTENCIA Y AUDITORÍA
# =============================================================================

for key in ['df_v3', 'df_ts_upd', 'df_tarifas', 'df_audit', 'periodo']:
    if key not in st.session_state: st.session_state[key] = None if key != 'periodo' else "Febrero"

st.title(f"Fiscalización TTR Natalia v8.4 - {st.session_state.periodo} 2026")

t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO DMK"])

with t1:
    f_ref = st.file_uploader("Subir Tarifas Noviembre", key="f_t")
    if f_ref:
        st.session_state.periodo = st.text_input("Mes:", st.session_state.periodo)
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("🔄 Calcular Tarifas"):
            db = pd.read_excel(f_ref) if f_ref.name.endswith('.xlsx') else pd.read_csv(f_ref)
            st.session_state.df_tarifas = motor_tarifas_v8(db, m)
    if st.session_state.df_tarifas is not None:
        st.dataframe(st.session_state.df_tarifas.head())
        buf1 = io.BytesIO()
        st.session_state.df_tarifas.to_excel(buf1, index=False)
        st.download_button(f"📥 Bajar Tarifas_{st.session_state.periodo}.xlsx", buf1.getvalue(), f"Tarifas_{st.session_state.periodo}.xlsx", key="dl_t")

with t2:
    st.header("Sincronización Maestra (Auditoría ELR)")
    c1, c2, c3 = st.columns(3)
    fv2 = c1.file_uploader("Nomenclador v2 Base")
    felr = c2.file_uploader("ELR Actualizado")
    fts = c3.file_uploader("Nomenclador TS")
    
    if fv2 and felr and fts and st.button("🔄 Sincronizar Archivos"):
        v3, ts_up, audit = motor_maestro_v8_4(pd.read_excel(fv2), pd.read_excel(felr), pd.read_excel(fts))
        st.session_state.df_v3, st.session_state.df_ts_upd, st.session_state.df_audit = v3, ts_up, audit
        st.success("Sincronización Completa.")

    if st.session_state.df_audit is not None:
        st.subheader("📋 Auditoría: Cambios detectados por el ELR")
        st.dataframe(st.session_state.df_audit, use_container_width=True)
        col1, col2 = st.columns(2)
        b_v3 = io.BytesIO()
        st.session_state.df_v3.to_excel(b_v3, index=False)
        col1.download_button("📥 Bajar Maestro V3", b_v3.getvalue(), f"V3_{st.session_state.periodo}.xlsx", key="dl_v3")
        b_ts = io.BytesIO()
        st.session_state.df_ts_upd.to_excel(b_ts, index=False)
        col2.download_button("📥 Bajar TS Actualizado", b_ts.getvalue(), f"TS_{st.session_state.periodo}.xlsx", key="dl_ts")

with t3:
    if st.session_state.df_v3 is not None:
        st.header(f"Liquidación DMK {st.session_state.periodo}")
        f_dmk = st.file_uploader("DMK (ZIP o CSV)")
        f_en = st.file_uploader("Energías")
        if f_dmk and f_en and st.button("⚡ Procesar Liquidación"):
            res = motor_dmk_v8_4(f_dmk, st.session_state.df_v3, pd.read_excel(f_en))
            if res is not None:
                st.dataframe(res.head())
                b_dmk = io.BytesIO()
                res.to_excel(b_dmk, index=False)
                st.download_button(f"📥 Bajar DMK Final", b_dmk.getvalue(), f"Liquidacion_DMK_{st.session_state.periodo}.xlsx", key="dl_dmk")
    else:
