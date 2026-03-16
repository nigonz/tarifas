import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Sistema de Fiscalización TTR", layout="wide")

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (PROYECCIÓN)
# =============================================================================

def motor_tarifas_proyeccion(df_nov, manuales):
    df = df_nov.copy()
    df.columns = [str(c).upper().strip() for c in df.columns]
    col_id = [c for c in df.columns if any(x in c for x in ['ID', 'GT'])][0]
    col_p = [c for c in df.columns if any(x in c for x in ['LIMITE', 'TARIFA', 'PRECIO'])][0]
    
    val_1scn = df.loc[df[col_id] == '1SCN', col_p].values
    v1_ant = pd.to_numeric(str(val_1scn[0]).replace(',', '.'), errors='coerce') if len(val_1scn) > 0 else 270.0
    factor = manuales['1SCN'] / v1_ant if v1_ant > 0 else 1
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row[col_id]).strip().upper()
        v_ant = pd.to_numeric(str(row[col_p]).replace(',', '.'), errors='coerce')
        if id_t in manuales: v_nue = manuales[id_t]
        elif any(x in id_t for x in ['SGI', 'UPA']): v_nue = manuales['1SCN']
        else: v_nue = v_ant * factor if pd.notnull(v_ant) else manuales['1SCN']
        res.append({'GT': id_t, 'TARIFA_NOV': round(v_ant, 2), 'TARIFA_FEB': round(v_nue, 2)})
    return pd.DataFrame(res)

# =============================================================================
# BLOQUE 2: MOTOR TTR (V14.1 - SIN COLUMNA FECHA)
# =============================================================================

def procesar_dmk_v14_1(f_zip, df_v, df_tarifas, f_ener):
    try:
        # 1. CARGA DE DMK (Todo como Texto)
        if f_zip.name.endswith('.zip'):
            with zipfile.ZipFile(f_zip) as z:
                with z.open(z.namelist()[0]) as f:
                    lf = pl.read_csv(f.read(), separator=';', encoding='iso-8859-1', infer_schema_length=0).lazy()
        else:
            lf = pl.read_csv(f_zip, separator=';', encoding='iso-8859-1', infer_schema_length=0).lazy()

        # 2. SELECCIÓN DE NOMENCLADOR (ID_LINEA)
        v_pl = pl.from_pandas(df_v[['ID_LINEA', 'GT', 'PROVINCIA', 'MUNICIPIO']]).lazy().select([
            pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", "").alias("ID_LINEA_KEY"),
            pl.col("GT").cast(pl.Utf8), 
            pl.col("PROVINCIA").cast(pl.Utf8), 
            pl.col("MUNICIPIO").cast(pl.Utf8)
        ])
        
        tar_pl = pl.from_pandas(df_tarifas).lazy().with_columns(pl.col("GT").cast(pl.Utf8).alias("GT_TAR"))
        
        # 3. CRUCE DE DATOS
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", "").str.strip_chars())
        lf = lf.join(v_pl, left_on="ID_LINEA", right_on="ID_LINEA_KEY", how="inner")
        lf = lf.join(tar_pl, left_on="GT", right_on="GT_TAR", how="left")

        # 4. REGLAS DE NEGOCIO (BE y CABA)
        lf = lf.with_columns([
            pl.when(pl.col("CONTRATO").is_in(["830", "831", "832", "833"])).then(pl.lit("SI")).otherwise(pl.lit("NO")).alias("BE"),
            pl.when(pl.col("GT") == "DF").then(pl.lit("CABA")).otherwise(pl.col("PROVINCIA")).alias("PROV_FINAL")
        ])

        # 5. CÁLCULOS (Usamos directamente TARIFA_FEB)
        cols_num = ["TARIFA_FEB", "DEBITADO", "DESCUENTO_X_INTEGRACION", "CANTIDAD_USOS"]
        for c in cols_num:
            lf = lf.with_columns(pl.col(c).cast(pl.Float64, strict=False).fill_null(0))

        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP. ITG"),
            pl.when(pl.col("CONTRATO") == "621")
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA_FEB") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP. ATS")
        ])

        lf = lf.with_columns([
            (pl.col("COMP. ATS") / 1.105).alias("COMP. ATS s/IVA"),
            (pl.col("COMP. ITG") / 1.105).alias("COMP. ITG s/IVA")
        ])

        # 6. ENERGÍAS Y AGRUPACIÓN
        df_res = lf.collect().to_pandas()
        pme_df = pd.read_excel(f_ener)
        pme_df['DOMINIO'] = pme_df['DOMINIO'].astype(str).str.strip().str.upper()
        
        df_pm = df_res[df_res['DOMINIO'].isin(pme_df['DOMINIO'])].merge(pme_df[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_resto = df_res[~df_res['DOMINIO'].isin(pme_df['DOMINIO'])].copy()
        df_resto['DOMINIO'], df_resto['ENERGIA'] = 'NO', 3
        
        df_final = pd.concat([df_pm, df_resto], ignore_index=True)

        agrupadores = ['PROV_FINAL', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'BE', 'TARIFA_FEB', 'DEBITADO', 'DESCUENTO_X_INTEGRACION']
        
        return df_final.groupby(agrupadores, as_index=False).agg({
            'CANTIDAD_USOS': 'sum', 'COMP. ITG': 'sum', 'COMP. ATS': 'sum', 'COMP. ATS s/IVA': 'sum', 'COMP. ITG s/IVA': 'sum'
        })

    except Exception as e:
        st.error(f"Error en liquidación: {e}")
        return None

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")

if 'tar_v141' not in st.session_state: st.session_state.tar_v141 = None
if 'ttr_v141' not in st.session_state: st.session_state.ttr_v141 = None

t_tabs = st.tabs(["💰 1. CONFIGURAR TARIFAS", "⚡ 2. PROCESAR DMK"])

with t_tabs[0]:
    f_n = st.file_uploader("Cuadro Noviembre", key="up_n_141")
    if f_n:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar Tarifas"):
            st.session_state.tar_v141 = motor_tarifas_proyeccion(pd.read_excel(f_n), m)
    if st.session_state.tar_v141 is not None: st.dataframe(st.session_state.tar_v141)

with t_tabs[1]:
    if st.session_state.tar_v141 is not None:
        ca, cb = st.columns(2)
        fv = ca.file_uploader("Nomenclador V", key="up_v_141")
        fe = cb.file_uploader("Energías", key="up_e_141")
        fzip = st.file_uploader("DMK (ZIP/CSV)", key="up_z_141")
        
        if fv and fe and fzip and st.button("🚀 GENERAR LIQUIDACIÓN FINAL"):
            with st.spinner("Procesando (Sin dependencia de columna FECHA)..."):
                st.session_state.ttr_v141 = procesar_dmk_v14_1(fzip, pd.read_excel(fv), st.session_state.tar_v141, fe)
            
            if st.session_state.ttr_v141 is not None:
                st.success("TTR Finalizado con éxito.")
                st.download_button("📥 DESCARGAR EXCEL", preparar_descarga(st.session_state.ttr_v141), "Liquidacion_Final_TTR.xlsx")
                st.dataframe(st.session_state.ttr_v141.head(10))
    else:
        st.warning("Configurá las tarifas primero.")
