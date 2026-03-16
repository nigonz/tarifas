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

def normalizar_columnas(df):
    df.columns = [str(c).strip().upper().replace(" ", "_").replace(".", "") for c in df.columns]
    return df

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (CORREGIDO PARA "LIMITE SUPERIOR")
# =============================================================================

def motor_tarifas_proyeccion(df_nov, manuales):
    df = normalizar_columnas(df_nov.copy())
    
    # Buscamos columnas con lógica flexible
    col_id = [c for c in df.columns if "ID" in c or "GT" in c][0]
    # Agregamos "LIMITE" a la búsqueda para que encuentre tu Excel
    col_p = [c for c in df.columns if any(x in c for x in ["TARIFA", "PRECIO", "LIMITE"])][0]
    
    val_1scn = df.loc[df[col_id] == '1SCN', col_p].values
    # Limpieza de comas y puntos para los precios del Excel
    def clean_num(x):
        return pd.to_numeric(str(x).replace(',', '.'), errors='coerce')

    v1_ant = clean_num(val_1scn[0]) if len(val_1scn) > 0 else 270.0
    factor = manuales['1SCN'] / v1_ant if v1_ant > 0 else 1
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row[col_id]).strip().upper()
        v_ant = clean_num(row[col_p])
        v_nue = manuales.get(id_t, v_ant * factor if pd.notnull(v_ant) else manuales['1SCN'])
        if any(x in id_t for x in ['SGI', 'UPA']) and id_t not in manuales: v_nue = manuales['1SCN']
        res.append({'GT': id_t, 'TARIFA_FEB': round(v_nue, 2)})
    return pd.DataFrame(res)

# =============================================================================
# BLOQUE 2: MOTOR TTR (V14.8 - COLUMNAS LIMPIAS)
# =============================================================================

def procesar_dmk_v14_8(f_zip, df_v, df_tarifas, f_ener):
    try:
        # 1. CARGA DE DMK (Solo lo necesario)
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

        # 2. PREPARAR NOMENCLADOR (Sin Silas ni DNGFF)
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

        # 4. REGLAS
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

        # 6. ENERGÍAS
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

        agrupadores = ['PROV_FINAL', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'ID_LINEA', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'BE', 'TARIFA_FEB', 'DEBITADO', 'DESCUENTO_X_INTEGRACION']
        
        return df_final.groupby(agrupadores, as_index=False).agg({
            'CANTIDAD_USOS': 'sum', 'COMP_ITG': 'sum', 'COMP_ATS': 'sum', 'COMP. ATS s/IVA': 'sum', 'COMP. ITG s/IVA': 'sum'
        })

    except Exception as e:
        st.error(f"Error técnico: {e}")
        return None

# =============================================================================
# UI (INTERFAZ)
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")

if 'm_v148' not in st.session_state: st.session_state.m_v148 = None
if 't_v148' not in st.session_state: st.session_state.t_v148 = None

t = st.tabs(["💰 1. TARIFAS", "⚡ 2. PROCESAR TTR"])

with t[0]:
    f = st.file_uploader("Subir Cuadro Noviembre", key="up_f_148")
    if f:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"):
            st.session_state.m_v148 = motor_tarifas_proyeccion(pd.read_excel(f), m)
    if st.session_state.m_v148 is not None: st.dataframe(st.session_state.m_v148)

with t[1]:
    if st.session_state.m_v148 is not None:
        ca, cb = st.columns(2)
        fv = ca.file_uploader("Nomenclador V", key="up_v_148")
        fe = cb.file_uploader("Energías", key="up_e_148")
        fz = st.file_uploader("DMK (ZIP/CSV)", key="up_z_148")
        
        if fv and fe and fz and st.button("🚀 INICIAR LIQUIDACIÓN"):
            with st.spinner("Procesando..."):
                st.session_state.t_v148 = procesar_dmk_v14_8(fz, pd.read_excel(fv), st.session_state.m_v148, fe)
            
            if st.session_state.t_v148 is not None:
                st.success("TTR Finalizado.")
                st.download_button("📥 DESCARGAR EXCEL", preparar_descarga(st.session_state.t_v148), "TTR_Final_Procesado.xlsx")
                st.dataframe(st.session_state.t_v148.head(20))
    else:
        st.warning("Configurá las tarifas primero en la pestaña 1.")
