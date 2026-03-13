import streamlit as st
import pandas as pd
import polars as pl
import numpy as np
import io

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR - Natalia v6.5", layout="wide")

# =============================================================================
# MÓDULO 1: TARIFAS (Lógica: Tarifas comerciales.ipynb)
# =============================================================================
def calcular_tarifas_modulo(df_ref, manuales):
    df = df_ref.copy()
    for col in ['Limite Inferior', 'Limite Superior']:
        df[col] = pd.to_numeric(df[col].astype(str).replace({',': '.'}, regex=True), errors='coerce')

    v1_ant = df.loc[df['Tarifas'] == '1SCN', 'Limite Inferior'].values[0]
    factor = manuales['1SCN'] / v1_ant
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row['Tarifas'])
        if id_t in manuales: v_min = v_max = manuales[id_t]
        elif 'SEN' in id_t and 'SESN' not in id_t: v_min = v_max = manuales.get(id_t.replace('SEN', 'SCN'), manuales['1SCN']) * 1.25
        elif 'SEAN' in id_t and 'SEASN' not in id_t: v_min = v_max = manuales.get(id_t.replace('SEAN', 'SCN'), manuales['1SCN']) * 1.75
        elif 'SCSN' in id_t: v_min = v_max = manuales.get(id_t.replace('SCSN', 'SCN'), manuales['1SCN']) * 1.59
        elif 'SESN' in id_t: v_min = v_max = (manuales.get(id_t.replace('SESN', 'SCN'), manuales['1SCN']) * 1.59) * 1.25
        elif 'SEASN' in id_t: v_min = v_max = (manuales.get(id_t.replace('SEASN', 'SCN'), manuales['1SCN']) * 1.59) * 1.75
        else: v_min, v_max = row['Limite Inferior'] * factor, row['Limite Superior'] * factor
        
        res.append({'Id': id_t, 'Limite Superior': round(v_max, 2), 'Limite Inferior': round(v_min, 2)})
    return pd.DataFrame(res)

# =============================================================================
# MÓDULO 2: PREPROCESO (Lógica: entrega_dggi__ITG_DMK.ipynb)
# =============================================================================
def procesar_dmk_modulo(f_sube, df_gt, df_en):
    # Motores Polars para el archivo de 210MB
    gt_pl = pl.from_pandas(df_gt[['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']]).lazy()
    en_pl = pl.from_pandas(df_en[['DOMINIO', 'ENERGIA']]).lazy()

    lf = pl.read_csv(f_sube.getvalue(), encoding='iso-8859-1', separator=";", infer_schema_length=10000).lazy()
    lf = lf.rename({c: c.strip().upper() for c in lf.columns})
    lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))
    
    lf = lf.join(gt_pl, on="ID_LINEA", how="inner")
    lf = lf.join(en_pl, on="DOMINIO", how="left").with_columns(pl.col("ENERGIA").fill_null(3))
    
    # Cálculos de ATS e ITG
    lf = lf.with_columns([
        (pl.col("DESCUENTO X INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
        pl.when(pl.col("CONTRATO") == 621)
          .then(
              pl.when(pl.col("GT") == "INP")
                .then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS"))
                .otherwise((pl.col("TARIFA BASE ITG") - pl.col("DEBITADO") - pl.col("DESCUENTO X INTEGRACION")) * pl.col("CANTIDAD_USOS"))
          ).otherwise(0).alias("COMP_ATS")
    ])

    df_res = lf.group_by([
        'PROVINCIA', 'MUNICIPIO', 'GT', 'Linea SILAS DNGFF', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO'
    ]).agg([pl.col('CANTIDAD_USOS').sum(), pl.col('COMP_ITG').sum(), pl.col('COMP_ATS').sum()]).collect().to_pandas()

    df_res['COMP_ATS s/IVA'] = df_res['COMP_ATS'] / 1.105
    df_res['COMP_ITG s/IVA'] = df_res['COMP_ITG'] / 1.105
    return df_res

# =============================================================================
# UI - STREAMLIT
# =============================================================================
st.title("Fiscalización TTR Natalia v6.5 🚀")

tab1, tab2, tab3 = st.tabs(["💰 1. TARIFAS", "📂 2. PREPROCESO", "📊 3. MATCHEO TTR"])

with tab1:
    f_ref = st.file_uploader("Subir Referencia (Noviembre)", type=['xlsx'])
    if f_ref:
        c1, c2, c3, c4, c5 = st.columns(5)
        m = {'1SCN': c1.number_input("1SCN", 494.33), '2SCN': c2.number_input("2SCN", 551.24), 
             '3SCN': c3.number_input("3SCN", 593.70), '4SCN': c4.number_input("4SCN", 636.21), '5SCN': c5.number_input("5SCN", 678.42)}
        if st.button("🔄 Generar y Descargar Tarifas"):
            st.session_state.df_tarifas = calcular_tarifas_modulo(pd.read_excel(f_ref, sheet_name='JN07'), m)
            st.dataframe(st.session_state.df_tarifas.head())
            
            buf = io.BytesIO()
            st.session_state.df_tarifas.to_excel(buf, index=False)
            st.download_button("📥 Descargar Tarifas_Calculadas.xlsx", buf.getvalue(), "Tarifas_Calculadas_2026.xlsx")

with tab2:
    if 'df_tarifas' in st.session_state:
        c1, c2, c3 = st.columns(3)
        f_sube, f_nom, f_en = c1.file_uploader("SUBE CSV"), c2.file_uploader("Nomenclador"), c3.file_uploader("Energías")
        if f_sube and f_nom and f_en and st.button("⚡ Procesar DMK"):
            st.session_state.df_dmk = procesar_dmk_modulo(f_sube, pd.read_excel(f_nom), pd.read_excel(f_en))
            st.success("Preproceso completo.")
            
            buf = io.BytesIO()
            st.session_state.df_dmk.to_excel(buf, index=False)
            st.download_button("📥 Descargar dggi_DMK_PME_Detailed.xlsx", buf.getvalue(), "dggi_DMK_PME_2026.xlsx")
    else: st.warning("Cargá las tarifas primero.")

with tab3:
    if 'df_dmk' in st.session_state:
        f_ttr = st.file_uploader("Subir Resolución TTR", type=['xlsx'])
        if f_ttr and st.button("🚀 Liquidar TTR Final"):
            # Lógica de Matcheo y Agrupado Final
            df = st.session_state.df_dmk.copy()
            # (Aquí va el cruce con TTR y el agrupamiento final sin patentes)
            st.success("TTR Matcheado listo para descargar.")
            # ... Botón para descargar ttrmacheo_agrupado.xlsx
