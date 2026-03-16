import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN OFICIAL ---
st.set_page_config(page_title="Sistema de Fiscalización TTR", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES DE EXPORTACIÓN
# =============================================================================

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def clean_ids(df, col):
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (PROYECCIÓN JN)
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
# BLOQUE 2: MOTOR DMK (REPLICANDO TU NOTEBOOK)
# =============================================================================

def procesar_liquidacion_dmk_notebook(f_dmk, df_v, df_tarifas, f_energias, fecha_corte):
    try:
        # 1. CARGA BLINDADA (LM622)
        if f_dmk.name.endswith('.zip'):
            with zipfile.ZipFile(f_dmk) as z:
                with z.open(z.namelist()[0]) as f: 
                    df_raw = pd.read_csv(f, sep=None, engine='python', encoding='iso-8859-1', dtype=str)
        else:
            df_raw = pd.read_csv(f_dmk, sep=None, engine='python', encoding='iso-8859-1', dtype=str)

        df_raw.columns = [c.strip().upper().replace(" ", "_") for c in df_raw.columns]
        
        # 2. PREPARAR MAESTROS
        v_clean = clean_ids(df_v.copy(), df_v.columns[0])
        pme = clean_ids(pd.read_excel(f_energias), 'DOMINIO')
        
        # 3. PROCESAMIENTO CON POLARS (Lógica de tu Notebook)
        lf = pl.from_pandas(df_raw).lazy()
        
        # Formateo de IDs y Fechaviaje
        lf = lf.with_columns([
            pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip(),
            pl.col("DOMINIO").str.strip().str.upper(),
            pl.col("FECHA").str.to_date("%d/%m/%Y")
        ])

        # Joins: Nomenclador y Tarifas
        v_pl = pl.from_pandas(v_clean).lazy().rename({v_clean.columns[0]: "ID_LINEA"})
        tar_pl = pl.from_pandas(df_tarifas).lazy()
        
        lf = lf.join(v_pl, on="ID_LINEA", how="inner").join(tar_pl, on="GT", how="left")

        # 4. REGLAS DE NEGOCIO (Tu Script)
        corte_dt = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        
        lf = lf.with_columns([
            # Definir Tarifa según fecha
            pl.when(pl.col("FECHA") <= corte_dt).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("TARIFA_BASE_ITG"),
            # Flag BE (Contratos 830-833)
            pl.when(pl.col("CONTRATO").cast(pl.Int64).is_in([830, 831, 832, 833])).then(pl.lit("SI")).otherwise(pl.lit("NO")).alias("BE"),
            # Forzar CABA para GT DF
            pl.when(pl.col("GT") == "DF").then(pl.lit("CABA")).otherwise(pl.col("PROVINCIA")).alias("PROVINCIA")
        ])

        # 5. CÁLCULOS FINANCIEROS (Fórmulas Notebook)
        for c in ['TARIFA_BASE_ITG', 'DEBITADO', 'DESCUENTO_X_INTEGRACION', 'CANTIDAD_USOS']:
            lf = lf.with_columns(pl.col(c).cast(pl.Float64).fill_null(0))

        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Int64) == 621)
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA_BASE_ITG") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS")
        ])

        # Desglose s/IVA
        lf = lf.with_columns([
            (pl.col("COMP_ATS") / 1.105).alias("COMP_ATS_S_IVA"),
            (pl.col("COMP_ITG") / 1.105).alias("COMP_ITG_S_IVA")
        ])

        # 6. AGRUPACIÓN Y ENERGÍAS (Finalización)
        res_pd = lf.collect().to_pandas()
        
        # Clasificación de Energías
        dom_pme = pme['DOMINIO'].unique()
        df_pm = res_pd[res_pd['DOMINIO'].isin(dom_pme)].merge(pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_resto = res_pd[~res_pd['DOMINIO'].isin(dom_pme)].copy()
        
        df_resto['DOMINIO'] = 'NO'
        df_resto['ENERGIA'] = 3
        
        df_final = pd.concat([df_pm, df_resto], ignore_index=True)

        # Agrupación final para el Excel (Para que no sea inmanejable)
        agrupadores = ['PROVINCIA', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'Linea SILAS DNGFF', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'BE', 'TARIFA_BASE_ITG', 'DEBITADO', 'DESCUENTO_X_INTEGRACION']
        
        return df_final.groupby(agrupadores, as_index=False).agg({
            'CANTIDAD_USOS': 'sum',
            'COMP_ITG': 'sum',
            'COMP_ATS': 'sum',
            'COMP_ATS_S_IVA': 'sum',
            'COMP_ITG_S_IVA': 'sum'
        })

    except Exception as e:
        st.error(f"Error en Liquidación: {e}")
        return None

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")

if 'tarifas' not in st.session_state: st.session_state.tarifas = None
if 'ttr_final' not in st.session_state: st.session_state.ttr_final = None

tab1, tab2 = st.tabs(["💰 1. TARIFAS", "⚡ 2. LIQUIDACIÓN DMK"])

with tab1:
    f_base = st.file_uploader("Cuadro Noviembre")
    if f_base:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"):
            st.session_state.tarifas = motor_tarifas_proyeccion(pd.read_excel(f_base), m)
    if st.session_state.tarifas is not None: st.dataframe(st.session_state.tarifas)

with tab2:
    if st.session_state.tarifas is not None:
        c1, c2 = st.columns(2)
        fv = c1.file_uploader("Nomenclador V")
        fen = c2.file_uploader("Energías")
        fzip = st.file_uploader("DMK (ZIP/CSV)")
        dt = st.date_input("Corte Febrero:", datetime(2026, 2, 14))
        
        if fv and fen and fzip and st.button("🚀 GENERAR TTR"):
            with st.spinner("Procesando datos del Notebook..."):
                st.session_state.ttr_final = procesar_liquidacion_dmk_notebook(fzip, pd.read_excel(fv), st.session_state.tarifas, fen, str(dt))
            
            if st.session_state.ttr_final is not None:
                st.success("TTR Generado.")
                st.download_button("📥 DESCARGAR EXCEL FINAL", preparar_descarga(st.session_state.ttr_final), "Liquidacion_TTR_Final.xlsx")
                st.dataframe(st.session_state.ttr_final.head(20))
    else:
        st.warning("Configurá las tarifas primero.")
