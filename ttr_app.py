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
# BLOQUE 1: CÁLCULO DEL FACTOR DE AUMENTO
# =============================================================================

if 'factor_ajuste' not in st.session_state: st.session_state.factor_ajuste = 1.0

def calcular_factor_febrero(df_nov, manual_1scn):
    df = df_nov.copy()
    df.columns = [str(c).upper().strip() for c in df.columns]
    col_id = [c for c in df.columns if "ID" in c or "GT" in c][0]
    col_p = [c for c in df.columns if "LIMITE" in c or "TARIFA" in c][0]
    
    # Obtenemos el valor viejo de 1SCN
    val_viejo = df.loc[df[col_id] == '1SCN', col_p].values
    v1_ant = pd.to_numeric(str(val_viejo[0]).replace(',', '.'), errors='coerce') if len(val_viejo) > 0 else 270.0
    
    # Factor = Nuevo / Viejo
    return manual_1scn / v1_ant if v1_ant > 0 else 1.0

# =============================================================================
# BLOQUE 2: MOTOR TTR (V15.0 - LÓGICA NOTEBOOK PURA)
# =============================================================================

def procesar_dmk_v15(f_zip, df_v, df_ener, factor):
    try:
        # 1. CARGA (Solo lo que está en el DMK)
        cols_dmk = ["ID_EMPRESA", "ID_LINEA", "DOMINIO", "DEBITADO", "CONTRATO", "DESCUENTO X INTEGRACION", "CANTIDAD_USOS", "TARIFA BASE ITG"]
        
        if f_zip.name.endswith('.zip'):
            with zipfile.ZipFile(f_zip) as z:
                with z.open(z.namelist()[0]) as f:
                    lf = pl.read_csv(f.read(), separator=';', encoding='iso-8859-1', columns=cols_dmk, infer_schema_length=0).lazy()
        else:
            lf = pl.read_csv(f_zip, separator=';', encoding='iso-8859-1', columns=cols_dmk, infer_schema_length=0).lazy()

        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.columns})

        # 2. NOMENCLADOR (Solo para Provincia, Municipio y BE)
        v_pl = pl.from_pandas(df_v[['ID_LINEA', 'GT', 'PROVINCIA', 'MUNICIPIO']]).lazy().select([
            pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", "").alias("ID_LINEA_KEY"),
            pl.col("GT").cast(pl.Utf8), 
            pl.col("PROVINCIA").cast(pl.Utf8), 
            pl.col("MUNICIPIO").cast(pl.Utf8)
        ])
        
        # 3. CRUCE
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", "").str.strip_chars())
        lf = lf.join(v_pl, left_on="ID_LINEA", right_on="ID_LINEA_KEY", how="inner")

        # 4. ACTUALIZACIÓN DE TARIFA A FEBRERO
        # Convertimos columnas a número (Soft cast)
        cols_fin = ["TARIFA_BASE_ITG", "DEBITADO", "DESCUENTO_X_INTEGRACION", "CANTIDAD_USOS"]
        for c in cols_fin:
            lf = lf.with_columns(pl.col(c).cast(pl.Float64, strict=False).fill_null(0))

        # APLICAMOS EL FACTOR DE FEBRERO
        lf = lf.with_columns((pl.col("TARIFA_BASE_ITG") * factor).alias("TARIFA_FEB"))

        # 5. FÓRMULAS NOTEBOOK (ATS e ITG)
        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO") == "621")
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA_FEB") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS"),
            pl.when(pl.col("CONTRATO").is_in(["830", "831", "832", "833"])).then(pl.lit("SI")).otherwise(pl.lit("NO")).alias("BE"),
            pl.when(pl.col("GT") == "DF").then(pl.lit("CABA")).otherwise(pl.col("PROVINCIA")).alias("PROV_FINAL")
        ])

        # 6. ENERGÍAS Y AGRUPACIÓN
        df_res = lf.collect().to_pandas()
        pme_df = pd.read_excel(df_ener)
        pme_df.columns = [str(c).strip().upper() for c in pme_df.columns]
        pme_df['DOMINIO'] = pme_df['DOMINIO'].astype(str).str.strip().str.upper()
        
        df_pm = df_res[df_res['DOMINIO'].isin(pme_df['DOMINIO'])].merge(pme_df[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_resto = df_res[~df_res['DOMINIO'].isin(pme_df['DOMINIO'])].copy()
        df_resto['DOMINIO'], df_resto['ENERGIA'] = 'NO', 3
        df_final = pd.concat([df_pm, df_resto], ignore_index=True)

        # Netos s/IVA
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

tabs = st.tabs(["💰 1. CONFIGURAR TARIFAS", "⚡ 2. PROCESAR DMK"])

with tabs[0]:
    f_n = st.file_uploader("Cuadro Noviembre", key="u_n_15")
    if f_n:
        val_manual = st.number_input("Nuevo Valor 1SCN (Febrero):", 494.33)
        if st.button("📊 Calcular Factor de Ajuste"):
            st.session_state.factor_ajuste = calcular_factor_febrero(pd.read_excel(f_n), val_manual)
            st.success(f"Factor calculado: {st.session_state.factor_ajuste:.4f}")

with tabs[1]:
    ca, cb = st.columns(2)
    fv = ca.file_uploader("Nomenclador V", key="u_v_15")
    fe = cb.file_uploader("Energías", key="u_e_15")
    fz = st.file_uploader("DMK (ZIP/CSV)", key="u_z_15")
    
    if fv and fe and fz and st.button("🚀 GENERAR LIQUIDACIÓN TTR"):
        with st.spinner("Procesando con lógica de Notebook..."):
            res = procesar_dmk_v15(fz, pd.read_excel(fv), fe, st.session_state.factor_ajuste)
            if res is not None:
                st.success("TTR Finalizado.")
                st.download_button("📥 DESCARGAR EXCEL", preparar_descarga(res), "Liquidacion_Final_TTR.xlsx")
                st.dataframe(res.head(20))
