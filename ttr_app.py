import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile

# --- CONFIGURACIÓN DE PRODUCCIÓN ---
st.set_page_config(page_title="Sistema de Fiscalización TTR v7.0", layout="wide")

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (PROYECCIÓN Y AUDITORÍA)
# =============================================================================

def motor_tarifas_produccion(df_base, manuales):
    df = df_base.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    
    col_id = [c for c in df.columns if 'ID' in c][0]
    for col in ['LIMITE INFERIOR', 'LIMITE SUPERIOR']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    v1_ant = df.loc[df[col_id] == '1SCN', 'LIMITE INFERIOR'].values[0]
    factor = manuales['1SCN'] / v1_ant
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row[col_id]).strip()
        v_min_ant, v_max_ant = row['LIMITE INFERIOR'], row['LIMITE SUPERIOR']
        
        if id_t in manuales:
            v_min = v_max = manuales[id_t]
            regla = "MANUAL"
        elif 'SEN' in id_t and 'SESN' not in id_t:
            v_min = v_max = manuales.get(id_t.replace('SEN', 'SCN'), manuales['1SCN']) * 1.25
            regla = "x1.25"
        elif 'SEAN' in id_t and 'SEASN' not in id_t:
            v_min = v_max = manuales.get(id_t.replace('SEAN', 'SCN'), manuales['1SCN']) * 1.75
            regla = "x1.75"
        elif 'SCSN' in id_t:
            v_min = v_max = manuales.get(id_t.replace('SCSN', 'SCN'), manuales['1SCN']) * 1.59
            regla = "x1.59"
        elif 'SESN' in id_t:
            v_min = v_max = (manuales.get(id_t.replace('SESN', 'SCN'), manuales['1SCN']) * 1.59) * 1.25
            regla = "SESN"
        elif 'SEASN' in id_t:
            v_min = v_max = (manuales.get(id_t.replace('SEASN', 'SCN'), manuales['1SCN']) * 1.59) * 1.75
            regla = "SEASN"
        else:
            v_min, v_max = v_min_ant * factor, v_max_ant * factor
            regla = "AJUSTE %"

        res.append({
            'Id': id_t, 'Anterior': round(v_min_ant, 2), 'Nuevo': round(v_min, 2),
            'Var %': round(((v_min/v_min_ant)-1)*100, 2) if v_min_ant > 0 else 0,
            'Regla': regla, 'Limite Superior': round(v_max, 2)
        })
    return pd.DataFrame(res)

# =============================================================================
# BLOQUE 2: GENERADOR NOMENCLADOR V3 (TRIPLE CRUCE)
# =============================================================================

def motor_v3_produccion(df_v2, df_elr, df_ts):
    # Normalización agresiva
    for d in [df_v2, df_elr, df_ts]:
        d.columns = [str(c).strip().upper() for c in d.columns]
        for c in d.columns:
            if 'ID' in c and 'LINEA' in c or 'IDLINEANS' in c:
                d[c] = d[c].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # Buscamos columnas por palabras clave
    col_id_base = [c for c in df_v2.columns if 'ID_LINEA' in c or 'IDLINEANS' in c][0]
    col_id_elr = [c for c in df_elr.columns if 'ID LINEA BO' in c or 'ID_LINEA' in c][0]
    col_id_ts = [c for c in df_ts.columns if 'IDLINEANS' in c or 'ID_LINEA' in c][0]

    # Cruce 1: Base + ELR
    col_gt_elr = [c for c in df_elr.columns if 'GRUPO' in c and 'TARIF' in c][0]
    col_lin_elr = [c for c in df_elr.columns if 'LINEA' in c and 'BO' not in c and 'DNGFF' not in c][0]
    
    elr_map = df_elr[[col_id_elr, col_gt_elr, col_lin_elr]].drop_duplicates(subset=[col_id_elr])
    res = df_v2.merge(elr_map, left_on=col_id_base, right_on=col_id_elr, how='left')

    # Cruce 2: + Nomenclador TS
    col_ts_final = [c for c in df_ts.columns if 'TIPO' in c and 'SERV' in c][0]
    ts_map = df_ts[[col_id_ts, col_ts_final]].drop_duplicates(subset=[col_id_ts])
    
    final = res.merge(ts_map, left_on=col_id_base, right_on=col_id_ts, how='left')
    return final.loc[:, ~final.columns.duplicated()]

# =============================================================================
# BLOQUE 3: DMK PESADO (POLARS + ZIP)
# =============================================================================

def motor_dmk_produccion(f_sube, df_v3, df_en):
    try:
        if f_sube.name.endswith('.zip'):
            with zipfile.ZipFile(f_sube) as z:
                csv_file = [n for n in z.namelist() if n.endswith('.csv')][0]
                with z.open(csv_file) as f: data = f.read()
        else: data = f_sube.getvalue()

        # Polars con detección de separador
        try:
            lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=";", infer_schema_length=10000).lazy()
            if len(lf.collect_schema().names()) < 5: raise Exception()
        except:
            lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=",", infer_schema_length=10000).lazy()

        lf = lf.rename({c: c.strip().upper() for c in lf.columns})
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))
        
        v3_pl = pl.from_pandas(df_v3).lazy().rename({c: c.strip().upper() for c in df_v3.columns})
        en_pl = pl.from_pandas(df_en).lazy().rename({c: c.strip().upper() for c in df_en.columns})

        # Cruces y Lógica de Carriles
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(en_pl, on="DOMINIO", how="left")
        lf = lf.with_columns([
            pl.when(pl.col("ENERGIA").is_null()).then(pl.lit("NO")).otherwise(pl.col("DOMINIO")).alias("DOMINIO"),
            pl.col("ENERGIA").fill_null(3)
        ])

        # Cálculos de Compensación (Fórmula INP incluida)
        lf = lf.with_columns([
            (pl.col("DESCUENTO X INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO") == 621)
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA BASE ITG") - pl.col("DEBITADO") - pl.col("DESCUENTO X INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS")
        ])

        final = lf.group_by(['GT', 'LINEA SILAS DNGFF', 'ID_LINEA', 'DOMINIO', 'ENERGIA']).agg([pl.col('CANTIDAD_USOS').sum(), pl.col('COMP_ITG').sum(), pl.col('COMP_ATS').sum()]).collect().to_pandas()
        final['COMP_ATS s/IVA'] = final['COMP_ATS'] / 1.105
        final['COMP_ITG s/IVA'] = final['COMP_ITG'] / 1.105
        return final
    except Exception as e:
        st.error(f"Error en DMK: {e}")
        return None

# =============================================================================
# INTERFAZ UNIFICADA
# =============================================================================

if 'df_tarifas' not in st.session_state: st.session_state.df_tarifas = None
if 'df_v3' not in st.session_state: st.session_state.df_v3 = None
if 'periodo' not in st.session_state: st.session_state.periodo = "Febrero"

st.title(f"Módulo de Fiscalización - {st.session_state.periodo} 2026")

t1, t2, t3 = st.tabs(["💰 TARIFAS", "📋 NOMENCLADOR V3", "📂 PROCESO DMK"])

with t1:
    f_ref = st.file_uploader("Subir Tarifas Noviembre")
    if f_ref:
        st.session_state.periodo = st.text_input("Mes:", st.session_state.periodo)
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("🔄 Calcular Tarifas"):
            df_b = pd.read_excel(f_ref) if f_ref.name.endswith('.xlsx') else pd.read_csv(f_ref)
            st.session_state.df_tarifas = motor_tarifas_produccion(df_b, m)
            st.dataframe(st.session_state.df_tarifas.head())

with t2:
    f_v2 = st.file_uploader("Nomenclador v2")
    f_elr = st.file_uploader("ELR Febrero")
    f_ts = st.file_uploader("Nomenclador TS")
    if f_v2 and f_elr and f_ts and st.button("🔄 Sincronizar V3"):
        st.session_state.df_v3 = motor_v3_produccion(pd.read_excel(f_v2), pd.read_excel(f_elr), pd.read_excel(f_ts))
        st.success("Nomenclador Maestro V3 Listo.")

with t3:
    if st.session_state.df_v3 is not None:
        f_sube = st.file_uploader("DMK (ZIP/CSV)")
        f_en = st.file_uploader("Energías")
        if f_sube and f_en and st.button("⚡ Procesar DMK"):
            res = motor_dmk_produccion(f_sube, st.session_state.df_v3, pd.read_excel(f_en))
            if res is not None:
                st.dataframe(res.head())
                buf = io.BytesIO()
                res.to_excel(buf, index=False)
                st.download_button(f"📥 Bajar DMK_{st.session_state.periodo}.xlsx", buf.getvalue(), f"DMK_{st.session_state.periodo}.xlsx")
