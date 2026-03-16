import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN DEL SISTEMA OFICIAL ---
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

def clean_ids_pandas(df, col):
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (LÓGICA ORIGINAL)
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
# BLOQUE 2: MOTOR DMK / TTR (REPLICANDO TU NOTEBOOK - OPTIMIZADO)
# =============================================================================

def procesar_dmk_ttr_oficial(f_zip, df_v, df_tarifas, f_energias, fecha_corte):
    try:
        # 1. CARGA BLINDADA (Lectura como texto para evitar errores tipo LM622)
        if f_zip.name.endswith('.zip'):
            with zipfile.ZipFile(f_zip) as z:
                with z.open(z.namelist()[0]) as f: 
                    df_raw = pd.read_csv(f, sep=None, engine='python', encoding='iso-8859-1', dtype=str)
        else:
            df_raw = pd.read_csv(f_zip, sep=None, engine='python', encoding='iso-8859-1', dtype=str)

        df_raw.columns = [c.strip().upper().replace(" ", "_") for c in df_raw.columns]
        
        # 2. PREPARAR MAESTROS
        v_clean = clean_ids_pandas(df_v.copy(), df_v.columns[0])
        pme = clean_ids_pandas(pd.read_excel(f_energias), 'DOMINIO')
        
        # 3. PROCESAMIENTO PESADO CON POLARS (Evita que la App se caiga)
        lf = pl.from_pandas(df_raw).lazy()
        
        lf = lf.with_columns([
            pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip(),
            pl.col("DOMINIO").str.strip().str.upper(),
            pl.col("FECHA").str.to_date("%d/%m/%Y")
        ])

        # Cruce con Nomenclador y Tarifas
        v_pl = pl.from_pandas(v_clean).lazy().rename({v_clean.columns[0]: "ID_LINEA"})
        tar_pl = pl.from_pandas(df_tarifas).lazy()
        
        lf = lf.join(v_pl, on="ID_LINEA", how="inner").join(tar_pl, on="GT", how="left")

        # 4. LÓGICA DE TU NOTEBOOK (BE, CABA, TARIFA)
        corte_dt = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        
        lf = lf.with_columns([
            pl.when(pl.col("FECHA") <= corte_dt).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("TARIFA_BASE_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Int64).is_in([830, 831, 832, 833])).then(pl.lit("SI")).otherwise(pl.lit("NO")).alias("BE"),
            pl.when(pl.col("GT") == "DF").then(pl.lit("CABA")).otherwise(pl.col("PROVINCIA")).alias("PROVINCIA")
        ])

        # 5. CÁLCULOS FINANCIEROS (Fórmulas TTR)
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

        # Desglose Neto (s/IVA)
        lf = lf.with_columns([
            (pl.col("COMP_ATS") / 1.105).alias("COMP_ATS_S_IVA"),
            (pl.col("COMP_ITG") / 1.105).alias("COMP_ITG_S_IVA")
        ])

        # 6. ENERGÍAS Y AGRUPACIÓN
        res_pd = lf.collect().to_pandas()
        
        # Merge de Energías (Dominio "NO" / Energía 3 para los faltantes)
        dom_validos = pme['DOMINIO'].unique()
        df_pm = res_pd[res_pd['DOMINIO'].isin(dom_validos)].merge(pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_resto = res_pd[~res_pd['DOMINIO'].isin(dom_validos)].copy()
        
        df_resto['DOMINIO'], df_resto['ENERGIA'] = 'NO', 3
        df_final = pd.concat([df_pm, df_resto], ignore_index=True)

        # Agrupación final idéntica a tu Notebook
        agrupadores = ['PROVINCIA', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'Linea SILAS DNGFF', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'BE', 'TARIFA_BASE_ITG', 'DEBITADO', 'DESCUENTO_X_INTEGRACION']
        
        return df_final.groupby(agrupadores, as_index=False).agg({
            'CANTIDAD_USOS': 'sum',
            'COMP_ITG': 'sum',
            'COMP_ATS': 'sum',
            'COMP_ATS_S_IVA': 'sum',
            'COMP_ITG_S_IVA': 'sum'
        })

    except Exception as e:
        st.error(f"Error en procesamiento: {e}")
        return None

# =============================================================================
# INTERFAZ (UI) - CORRECCIÓN DE KEYS
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")

# Usamos nombres claros en session_state que no choquen con los widgets
if 'res_tarifas' not in st.session_state: st.session_state.res_tarifas = None
if 'res_ttr' not in st.session_state: st.session_state.res_ttr = None

t1, t2 = st.tabs(["💰 1. CONFIGURAR TARIFAS", "⚡ 2. PROCESAR TTR"])

with t1:
    f_tar = st.file_uploader("Cuadro Noviembre", key="file_tarifa")
    if f_tar:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"):
            st.session_state.res_tarifas = motor_tarifas_proyeccion(pd.read_excel(f_tar), m)
    if st.session_state.res_tarifas is not None: st.dataframe(st.session_state.res_tarifas)

with t2:
    if st.session_state.res_tarifas is not None:
        c1, c2 = st.columns(2)
        fv = c1.file_uploader("Nomenclador V (Actualizado)", key="file_v")
        fen = c2.file_uploader("Energías", key="file_en")
        fzip = st.file_uploader("DMK (Archivo ZIP o CSV)", key="file_dmk")
        corte = st.date_input("Fecha Cambio de Tarifa:", datetime(2026, 2, 14), key="date_corte")
        
        if fv and fen and fzip and st.button("🚀 GENERAR TTR"):
            with st.spinner("Procesando millones de datos..."):
                st.session_state.res_ttr = procesar_dmk_ttr_oficial(fzip, pd.read_excel(fv), st.session_state.res_tarifas, fen, str(corte))
            
            if st.session_state.res_ttr is not None:
                st.success("TTR Final Generado.")
                st.download_button("📥 DESCARGAR LIQUIDACIÓN EXCEL", preparar_descarga(st.session_state.res_ttr), "TTR_Final.xlsx")
                st.dataframe(st.session_state.res_ttr.head(20))
    else:
        st.warning("Debe proyectar las tarifas primero en la pestaña 1.")
