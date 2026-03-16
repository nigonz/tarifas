import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Sistema de Fiscalización TTR", layout="wide")

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (NOVEDADES)
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
# BLOQUE 2: MOTOR TTR (V13.9 - SIN COLUMNAS INNECESARIAS)
# =============================================================================

def procesar_dmk_v13_9(f_zip, df_v, df_tarifas, f_ener, f_corte):
    try:
        # 1. CARGA DE DMK (Todo como Texto Puro)
        if f_zip.name.endswith('.zip'):
            with zipfile.ZipFile(f_zip) as z:
                with z.open(z.namelist()[0]) as f:
                    lf = pl.read_csv(f.read(), separator=';', encoding='iso-8859-1', infer_schema_length=0).lazy()
        else:
            lf = pl.read_csv(f_zip, separator=';', encoding='iso-8859-1', infer_schema_length=0).lazy()

        # 2. SELECCIÓN QUIRÚRGICA DEL NOMENCLADOR (Eliminamos Linea SILAS DNGFF)
        v_pl = pl.from_pandas(df_v[['ID_LINEA', 'GT', 'PROVINCIA', 'MUNICIPIO']]).lazy().select([
            pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", "").alias("ID_LINEA_KEY"),
            pl.col("GT").cast(pl.Utf8), 
            pl.col("PROVINCIA").cast(pl.Utf8), 
            pl.col("MUNICIPIO").cast(pl.Utf8)
        ])
        
        tar_pl = pl.from_pandas(df_tarifas).lazy().with_columns(pl.col("GT").cast(pl.Utf8).alias("GT_TAR"))
        
        pme_pl = pl.from_pandas(pd.read_excel(f_ener)).lazy().select([
            pl.col("DOMINIO").cast(pl.Utf8).str.strip_chars().str.upper(),
            pl.col("ENERGIA").cast(pl.Utf8).alias("ENERGIA_PM")
        ])

        # 3. CRUCE DE DATOS
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", "").str.strip_chars())
        lf = lf.join(v_pl, left_on="ID_LINEA", right_on="ID_LINEA_KEY", how="inner")
        lf = lf.join(tar_pl, left_on="GT", right_on="GT_TAR", how="left")

        # 4. REGLAS DE NEGOCIO (BE, CABA, TARIFA)
        corte_dt = datetime.strptime(f_corte, "%Y-%m-%d").date()
        
        lf = lf.with_columns([
            pl.col("FECHA").str.to_date("%d/%m/%Y").alias("FECHA_DT"),
            pl.when(pl.col("CONTRATO").is_in(["830", "831", "832", "833"])).then(pl.lit("SI")).otherwise(pl.lit("NO")).alias("BE"),
            pl.when(pl.col("GT") == "DF").then(pl.lit("CABA")).otherwise(pl.col("PROVINCIA")).alias("PROV_FINAL")
        ])

        lf = lf.with_columns(
            pl.when(pl.col("FECHA_DT") <= corte_dt).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("TARIFA_BASE_ITG_V")
        )

        # 5. CÁLCULOS (Conversión numérica solo al operar)
        cols_calc = ["TARIFA_BASE_ITG_V", "DEBITADO", "DESCUENTO_X_INTEGRACION", "CANTIDAD_USOS"]
        for c in cols_calc:
            lf = lf.with_columns(pl.col(c).cast(pl.Float64, strict=False).fill_null(0))

        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO") == "621")
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA_BASE_ITG_V") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS")
        ])

        lf = lf.with_columns([
            (pl.col("COMP_ATS") / 1.105).alias("COMP_ATS_s_IVA"),
            (pl.col("COMP_ITG") / 1.105).alias("COMP_ITG_s_IVA")
        ])

        # 6. ENERGÍAS Y AGRUPACIÓN
        df_res = lf.collect().to_pandas()
        pme_data = pme_pl.collect().to_pandas()
        
        df_pm = df_res[df_res['DOMINIO'].isin(pme_data['DOMINIO'])].merge(pme_data, on='DOMINIO', how='left')
        df_resto = df_res[~df_res['DOMINIO'].isin(pme_data['DOMINIO'])].copy()
        
        df_resto['DOMINIO'], df_resto['ENERGIA_PM'] = 'NO', "3"
        df_final = pd.concat([df_pm, df_resto], ignore_index=True)

        # Reporte final sin la columna conflictiva
        agrupadores = ['PROV_FINAL', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA_PM', 'CONTRATO', 'BE', 'TARIFA_BASE_ITG_V', 'DEBITADO', 'DESCUENTO_X_INTEGRACION']
        
        return df_final.groupby(agrupadores, as_index=False).agg({
            'CANTIDAD_USOS': 'sum', 'COMP_ITG': 'sum', 'COMP_ATS': 'sum', 'COMP_ATS_s_IVA': 'sum', 'COMP_ITG_s_IVA': 'sum'
        })

    except Exception as e:
        st.error(f"Error técnico: {e}")
        return None

# =============================================================================
# UI (INTERFAZ)
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")

if 'memo_tarifas' not in st.session_state: st.session_state.memo_tarifas = None
if 'memo_ttr' not in st.session_state: st.session_state.memo_ttr = None

tabs = st.tabs(["💰 1. TARIFAS", "⚡ 2. PROCESAR DMK"])

with tabs[0]:
    f_tar = st.file_uploader("Subir Cuadro Noviembre", key="up_tar_139")
    if f_tar:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"):
            st.session_state.memo_tarifas = motor_tarifas_proyeccion(pd.read_excel(f_tar), m)
    if st.session_state.memo_tarifas is not None: st.dataframe(st.session_state.memo_tarifas)

with tabs[1]:
    if st.session_state.memo_tarifas is not None:
        ca, cb = st.columns(2)
        fv = ca.file_uploader("Nomenclador V", key="up_v_139")
        fe = cb.file_uploader("Energías", key="up_e_139")
        fzip = st.file_uploader("DMK (ZIP/CSV)", key="up_z_139")
        dt_corte = st.date_input("Fecha Cambio de Tarifa:", datetime(2026, 2, 14))
        
        if fv and fe and fzip and st.button("🚀 INICIAR PROCESO TTR"):
            with st.spinner("Procesando (Excluyendo columnas con basura)..."):
                st.session_state.memo_ttr = procesar_dmk_v13_9(fzip, pd.read_excel(fv), st.session_state.memo_tarifas, fe, str(dt_corte))
            
            if st.session_state.memo_ttr is not None:
                st.success("TTR Finalizado.")
                st.download_button("📥 DESCARGAR RESULTADO", preparar_descarga(st.session_state.memo_ttr), "TTR_Final.xlsx")
                st.dataframe(st.session_state.memo_ttr.head(10))
    else:
        st.warning("Configurá las tarifas primero.")
