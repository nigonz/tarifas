import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR JN", layout="wide")

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (SIN CAMBIOS)
# =============================================================================

def motor_tarifas_proyeccion(df_nov, manuales):
    df = df_nov.copy()
    df.columns = [str(c).upper().strip() for c in df.columns]
    col_id = [c for c in df.columns if "GT" in c or "ID" in c][0]
    col_p = [c for c in df.columns if "TARIFA" in c or "PRECIO" in c][0]
    
    val_1scn = df.loc[df[col_id] == '1SCN', col_p].values
    v1_ant = pd.to_numeric(str(val_1scn[0]).replace(',', '.'), errors='coerce') if len(val_1scn) > 0 else 270.0
    factor = manuales['1SCN'] / v1_ant if v1_ant > 0 else 1
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row[col_id]).strip().upper()
        v_ant = pd.to_numeric(str(row[col_p]).replace(',', '.'), errors='coerce')
        v_nue = manuales.get(id_t, v_ant * factor if pd.notnull(v_ant) else manuales['1SCN'])
        if any(x in id_t for x in ['SGI', 'UPA']) and id_t not in manuales: v_nue = manuales['1SCN']
        res.append({'GT': id_t, 'TARIFA_FEB': round(v_nue, 2)})
    return pd.DataFrame(res)

# =============================================================================
# BLOQUE 2: MOTOR DMK (V14.7 - SIN COLUMNAS SILAS/DNGFF)
# =============================================================================

def procesar_dmk_v14_7(f_zip, df_v, df_tarifas, f_ener):
    try:
        # 1. CARGA DE DMK (Solo columnas estrictamente necesarias)
        cols_dmk = ["ID_EMPRESA", "ID_LINEA", "DOMINIO", "DEBITADO", "CONTRATO", "DESCUENTO X INTEGRACION", "CANTIDAD_USOS"]
        
        if f_zip.name.endswith('.zip'):
            with zipfile.ZipFile(f_zip) as z:
                with z.open(z.namelist()[0]) as f:
                    lf = pl.read_csv(f.read(), separator=';', encoding='iso-8859-1', 
                                     columns=cols_dmk, infer_schema_length=0).lazy()
        else:
            lf = pl.read_csv(f_zip, separator=';', encoding='iso-8859-1', 
                             columns=cols_dmk, infer_schema_length=0).lazy()

        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.columns})

        # 2. PREPARAR NOMENCLADOR (LIMPIEZA TOTAL DE COLUMNAS SILAS/DNGFF)
        # Solo conservamos lo operativo para las fórmulas y el agrupado
        v_pl = pl.from_pandas(df_v[['ID_LINEA', 'GT', 'PROVINCIA', 'MUNICIPIO']]).lazy().select([
            pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", "").alias("ID_LINEA_KEY"),
            pl.col("GT").cast(pl.Utf8), 
            pl.col("PROVINCIA").cast(pl.Utf8), 
            pl.col("MUNICIPIO").cast(pl.Utf8)
        ])
        
        tar_pl = pl.from_pandas(df_tarifas).lazy().with_columns(pl.col("GT").cast(pl.Utf8).alias("GT_TAR"))
        
        # 3. CRUCE
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", "").str.strip_chars())
        lf = lf.join(v_pl, left_on="ID_LINEA", right_on="ID_LINEA_KEY", how="inner")
        lf = lf.join(tar_pl, left_on="GT", right_on="GT_TAR", how="left")

        # 4. REGLAS DE NEGOCIO
        lf = lf.with_columns([
            pl.when(pl.col("CONTRATO").is_in(["830", "831", "832", "833"])).then(pl.lit("SI")).otherwise(pl.lit("NO")).alias("BE"),
            pl.when(pl.col("GT") == "DF").then(pl.lit("CABA")).otherwise(pl.col("PROVINCIA")).alias("PROV_FINAL")
        ])

        # 5. CÁLCULOS
        cols_num = ["TARIFA_FEB", "DEBITADO", "DESCUENTO_X_INTEGRACION", "CANTIDAD_USOS"]
        for c in cols_num:
            lf = lf.with_columns(pl.col(c).cast(pl.Float64, strict=False).fill_null(0))

        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO") == "621")
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA_FEB") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS")
        ])

        # 6. ENERGÍAS Y AGRUPACIÓN
        df_res = lf.collect().to_pandas()
        pme_df = pd.read_excel(f_ener)
        pme_df.columns = [str(c).strip().upper() for c in pme_df.columns]
        pme_df['DOMINIO'] = pme_df['DOMINIO'].astype(str).str.strip().str.upper()
        
        df_pm = df_res[df_res['DOMINIO'].isin(pme_df['DOMINIO'])].merge(pme_df[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_resto = df_res[~df_res['DOMINIO'].isin(pme_df['DOMINIO'])].copy()
        df_resto['DOMINIO'], df_resto['ENERGIA'] = 'NO', 3
        
        df_final = pd.concat([df_pm, df_resto], ignore_index=True)

        # Netos
        df_final['COMP. ATS s/IVA'] = df_final['COMP_ATS'] / 1.105
        df_final['COMP. ITG s/IVA'] = df_final['COMP_ITG'] / 1.105

        # Agrupadores finales (LIMPIOS de Silas/DNGFF)
        agrupadores = ['PROV_FINAL', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'ID_LINEA', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'BE', 'TARIFA_FEB', 'DEBITADO', 'DESCUENTO_X_INTEGRACION']
        
        return df_final.groupby(agrupadores, as_index=False).agg({
            'CANTIDAD_USOS': 'sum', 'COMP_ITG': 'sum', 'COMP_ATS': 'sum', 'COMP. ATS s/IVA': 'sum', 'COMP. ITG s/IVA': 'sum'
        })

    except Exception as e:
        st.error(f"Error técnico: {e}")
        return None

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")

if 'memo_tar' not in st.session_state: st.session_state.memo_tar = None
if 'memo_ttr' not in st.session_state: st.session_state.memo_ttr = None

tabs = st.tabs(["💰 1. TARIFAS", "⚡ 2. PROCESAR TTR"])

with tabs[0]:
    f_nov = st.file_uploader("Cuadro Noviembre", key="f_nov_147")
    if f_nov:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"):
            st.session_state.memo_tar = motor_tarifas_proyeccion(pd.read_excel(f_nov), m)
    if st.session_state.memo_tar is not None: st.dataframe(st.session_state.memo_tar)

with tabs[1]:
    if st.session_state.memo_tar is not None:
        ca, cb = st.columns(2)
        fv = ca.file_uploader("Nomenclador V", key="f_v_147")
        fe = cb.file_uploader("Energías", key="f_e_147")
        fzip = st.file_uploader("DMK (ZIP/CSV)", key="f_z_147")
        
        if fv and fe and fzip and st.button("🚀 INICIAR PROCESO"):
            with st.spinner("Procesando con motor limpio..."):
                st.session_state.memo_ttr = procesar_dmk_v14_7(fzip, pd.read_excel(fv), st.session_state.memo_tar, fe)
            
            if st.session_state.memo_ttr is not None:
                st.success("TTR Finalizado.")
                st.download_button("📥 DESCARGAR RESULTADO", preparar_descarga(st.session_state.memo_ttr), "TTR_Final_Limpio.xlsx")
                st.dataframe(st.session_state.memo_ttr.head(20))
    else:
        st.warning("Cargá las tarifas primero.")
