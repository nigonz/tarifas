import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN DEL SISTEMA OFICIAL ---
st.set_page_config(page_title="Sistema de Fiscalización TTR", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES (SIN CAMBIOS)
# =============================================================================

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (TAL CUAL LO TENÍAS)
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
# BLOQUE 2: MOTOR TTR (REPLICANDO TU NOTEBOOK CON ALTA VELOCIDAD)
# =============================================================================

def procesar_dmk_final_blindado(f_dmk, df_v, df_tarifas, f_ener, f_corte):
    try:
        # 1. LECTURA EFICIENTE (Evita caídas de página)
        if f_dmk.name.endswith('.zip'):
            with zipfile.ZipFile(f_dmk) as z:
                with z.open(z.namelist()[0]) as f:
                    # Leemos como Polars directamente (mucho más rápido)
                    lf = pl.read_csv(f.read(), separator=';', encoding='iso-8859-1', infer_schema_length=0).lazy()
        else:
            lf = pl.read_csv(f_dmk, separator=';', encoding='iso-8859-1', infer_schema_length=0).lazy()

        # 2. LIMPIEZA DE MAESTROS
        v_pl = pl.from_pandas(df_v).lazy().select([
            pl.col(df_v.columns[0]).cast(pl.Utf8).str.replace(r"\.0$", "").alias("ID_LINEA"),
            pl.col("GT"), pl.col("Linea SILAS DNGFF"), pl.col("PROVINCIA"), pl.col("MUNICIPIO")
        ])
        
        tar_pl = pl.from_pandas(df_tarifas).lazy().rename({"GT": "GT_TAR"})
        pme_pl = pl.from_pandas(pd.read_excel(f_ener)).lazy().select([
            pl.col("DOMINIO").str.strip().str.upper(),
            pl.col("ENERGIA").cast(pl.Int64)
        ])

        # 3. PROCESO DE CRUCE (Tu lógica de Notebook)
        # Inner join con Nomenclador V
        lf = lf.with_columns(pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip())
        lf = lf.join(v_pl, on="ID_LINEA", how="inner")
        
        # Join con Tarifas
        lf = lf.join(tar_pl, left_on="GT", right_on="GT_TAR", how="left")

        # 4. REGLAS DE NEGOCIO Y CÁLCULOS
        corte_dt = datetime.strptime(f_corte, "%Y-%m-%d").date()
        
        lf = lf.with_columns([
            # Clasificación de Tarifa por Fecha
            pl.col("FECHA").str.to_date("%d/%m/%Y").alias("FECHA_DT"),
            # Flag BE (830-833)
            pl.when(pl.col("CONTRATO").cast(pl.Int64).is_in([830, 831, 832, 833])).then(pl.lit("SI")).otherwise(pl.lit("NO")).alias("BE"),
            # DF a CABA
            pl.when(pl.col("GT") == "DF").then(pl.lit("CABA")).otherwise(pl.col("PROVINCIA")).alias("PROV_FINAL")
        ])

        lf = lf.with_columns(
            pl.when(pl.col("FECHA_DT") <= corte_dt).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("TARIFA_BASE_ITG")
        )

        # 5. FÓRMULAS DE COMPENSACIÓN (Exactas de tu script)
        for col in ['TARIFA_BASE_ITG', 'DEBITADO', 'DESCUENTO_X_INTEGRACION', 'CANTIDAD_USOS']:
            lf = lf.with_columns(pl.col(col).cast(pl.Float64).fill_null(0))

        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Int64) == 621)
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA_BASE_ITG") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS")
        ])

        # Netos s/IVA
        lf = lf.with_columns([
            (pl.col("COMP_ATS") / 1.105).alias("COMP_ATS_s_IVA"),
            (pl.col("COMP_ITG") / 1.105).alias("COMP_ITG_s_IVA")
        ])

        # 6. AGRUPACIÓN Y ENERGÍAS
        df_res = lf.collect().to_pandas()
        
        # Cruce con Energías
        pme_df = pme_pl.collect().to_pandas()
        df_pm = df_res[df_res['DOMINIO'].isin(pme_df['DOMINIO'])].merge(pme_df, on='DOMINIO', how='left')
        df_resto = df_res[~df_res['DOMINIO'].isin(pme_df['DOMINIO'])].copy()
        
        df_resto['DOMINIO'], df_resto['ENERGIA'] = 'NO', 3
        df_final = pd.concat([df_pm, df_resto], ignore_index=True)

        # Groupby final para que el Excel no pese 1GB
        agrupadores = ['PROV_FINAL', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'Linea SILAS DNGFF', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'BE', 'TARIFA_BASE_ITG', 'DEBITADO', 'DESCUENTO_X_INTEGRACION']
        
        return df_final.groupby(agrupadores, as_index=False).agg({
            'CANTIDAD_USOS': 'sum',
            'COMP_ITG': 'sum',
            'COMP_ATS': 'sum',
            'COMP_ATS_s_IVA': 'sum',
            'COMP_ITG_s_IVA': 'sum'
        })

    except Exception as e:
        st.error(f"Error en el motor: {e}")
        return None

# =============================================================================
# UI (INTERFAZ) - TOTALMENTE LIMPIA DE KEYS CONFLICTIVAS
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")

if 'memo_tarifas' not in st.session_state: st.session_state.memo_tarifas = None
if 'memo_ttr' not in st.session_state: st.session_state.memo_ttr = None

t1, t2 = st.tabs(["💰 1. TARIFAS", "⚡ 2. LIQUIDACIÓN DMK"])

with t1:
    f_base = st.file_uploader("Cuadro Noviembre", key="up_tar_nov")
    if f_base:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"):
            st.session_state.memo_tarifas = motor_tarifas_proyeccion(pd.read_excel(f_base), m)
    if st.session_state.memo_tarifas is not None: st.dataframe(st.session_state.memo_tarifas)

with t2:
    if st.session_state.memo_tarifas is not None:
        ca, cb = st.columns(2)
        fv = ca.file_uploader("Nomenclador V (Actualizado)", key="up_v_final")
        fe = cb.file_uploader("Energías", key="up_ener_final")
        fzip = st.file_uploader("DMK (ZIP/CSV)", key="up_dmk_zip")
        corte = st.date_input("Fecha Corte Febrero:", datetime(2026, 2, 14), key="up_date")
        
        if fv and fe and fzip and st.button("🚀 GENERAR TTR"):
            with st.spinner("Procesando con motor Polars (Anti-caída)..."):
                st.session_state.memo_ttr = procesar_dmk_final_blindado(fzip, pd.read_excel(fv), st.session_state.memo_tarifas, fe, str(corte))
            
            if st.session_state.memo_ttr is not None:
                st.success("Cálculo terminado.")
                st.download_button("📥 DESCARGAR TTR (.xlsx)", preparar_descarga(st.session_state.memo_ttr), "TTR_Final.xlsx")
                st.dataframe(st.session_state.memo_ttr.head(20))
    else:
        st.warning("Primero cargá las tarifas en la pestaña anterior.")
