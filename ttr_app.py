import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN OFICIAL ---
st.set_page_config(page_title="Sistema de Fiscalización TTR", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES DE DESCARGA Y LIMPIEZA
# =============================================================================

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def clean_ids(df, col):
    """Asegura IDs limpios en DataFrames de Pandas antes de cruzar."""
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (SIN TOCAR)
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
# BLOQUE 2: MOTOR DMK / TTR (OPTIMIZADO PARA EVITAR CAÍDAS)
# =============================================================================

def procesar_liquidacion_ttr(f_dmk, df_v, df_tarifas, f_energias, fecha_corte):
    try:
        # 1. Carga de DMK con blindaje de tipos (LM622)
        if f_dmk.name.endswith('.zip'):
            with zipfile.ZipFile(f_dmk) as z:
                with z.open(z.namelist()[0]) as f: 
                    df_dmk_raw = pd.read_csv(f, sep=None, engine='python', encoding='iso-8859-1', dtype=str)
        else:
            df_dmk_raw = pd.read_csv(f_dmk, sep=None, engine='python', encoding='iso-8859-1', dtype=str)

        # Normalizar nombres de columnas DMK
        df_dmk_raw.columns = [c.strip().upper().replace(" ", "_") for c in df_dmk_raw.columns]
        
        # 2. Preparar Maestros (Nomenclador V y Energías)
        v_clean = clean_ids(df_v.copy(), df_v.columns[0])
        pme = clean_ids(pd.read_excel(f_energias), 'DOMINIO')
        
        # 3. Procesamiento Pesado con Polars (Más eficiente en memoria)
        lf = pl.from_pandas(df_dmk_raw).lazy()
        
        # Formatear IDs y Fechas
        lf = lf.with_columns([
            pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip(),
            pl.col("DOMINIO").str.strip().str.upper(),
            pl.col("FECHA").str.to_date("%d/%m/%Y")
        ])

        # Joins: Nomenclador V y Tarifas
        v_pl = pl.from_pandas(v_clean).lazy().rename({v_clean.columns[0]: "ID_LINEA"})
        tar_pl = pl.from_pandas(df_tarifas).lazy()
        
        lf = lf.join(v_pl, on="ID_LINEA", how="inner").join(tar_pl, on="GT", how="left")

        # 4. Lógica de Precios por Fecha
        corte_dt = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(
            pl.when(pl.col("FECHA") <= corte_dt).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("TARIFA_APLICADA")
        )

        # 5. Fórmulas de Liquidación
        # Convertir a número las columnas críticas
        num_cols = ['TARIFA_APLICADA', 'DEBITADO', 'DESCUENTO_X_INTEGRACION', 'CANTIDAD_USOS']
        for c in num_cols:
            lf = lf.with_columns(pl.col(c).cast(pl.Float64).fill_null(0))

        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("BRUTO_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Utf8) == "621")
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA_APLICADA") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("BRUTO_ATS")
        ])

        # Desglose s/IVA (1.105)
        lf = lf.with_columns([
            (pl.col("BRUTO_ATS") / 1.105).alias("NETO_ATS"),
            (pl.col("BRUTO_ITG") / 1.105).alias("NETO_ITG")
        ])

        # 6. Cruce con Energías y Limpieza Final
        res_pd = lf.collect().to_pandas()
        
        # Merge de Energías
        dom_validos = pme['DOMINIO'].unique()
        df_con_e = res_pd[res_pd['DOMINIO'].isin(dom_validos)].merge(pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_sin_e = res_pd[~res_pd['DOMINIO'].isin(dom_validos)].copy()
        
        # Regla: Si no existe en Parque Móvil -> Energía 3, Dominio NO
        df_sin_e['DOMINIO'] = 'NO'
        df_sin_e['ENERGIA'] = 3
        
        return pd.concat([df_con_e, df_sin_e], ignore_index=True)

    except Exception as e:
        st.error(f"Error en el Motor DMK: {e}")
        return None

# =============================================================================
# INTERFAZ DE USUARIO (STREAMS)
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")

if 'tarifas' not in st.session_state: st.session_state.tarifas = None
if 'ttr_final' not in st.session_state: st.session_state.ttr_final = None

tab1, tab2 = st.tabs(["💰 1. CONFIGURAR TARIFAS", "⚡ 2. PROCESAR TTR"])

with tab1:
    st.subheader("Proyección Cuadro Tarifario Febrero")
    f_base = st.file_uploader("Subir Cuadro Base Noviembre", key="base")
    if f_base:
        c = st.columns(5)
        m = {
            '1SCN': c[0].number_input("1SCN", 494.33),
            '2SCN': c[1].number_input("2SCN", 551.24),
            '3SCN': c[2].number_input("3SCN", 593.70),
            '4SCN': c[3].number_input("4SCN", 636.21),
            '5SCN': c[4].number_input("5SCN", 678.42)
        }
        if st.button("📊 Calcular Proyección"):
            st.session_state.tarifas = motor_tarifas_proyeccion(pd.read_excel(f_base), m)
            st.success("Tarifas calculadas con éxito.")
    
    if st.session_state.tarifas is not None:
        st.dataframe(st.session_state.tarifas)

with tab2:
    st.subheader("Generación de Liquidación TTR Final")
    if st.session_state.tarifas is not None:
        c1, c2 = st.columns(2)
        f_v = c1.file_uploader("Nomenclador V (Excel)", key="v_final")
        f_en = c2.file_uploader("Parque Móvil (Energías)", key="en_final")
        
        f_zip = st.file_uploader("Subir DMK (Archivo ZIP o CSV)", key="dmk_final")
        corte = st.date_input("Fecha Cambio de Tarifa:", datetime(2026, 2, 14))
        
        if f_v and f_en and f_zip and st.button("🚀 INICIAR PROCESO TTR"):
            with st.spinner("Procesando millones de registros... Esto puede tardar unos segundos."):
                st.session_state.ttr_final = procesar_liquidacion_ttr(
                    f_zip, pd.read_excel(f_v), st.session_state.tarifas, f_en, str(corte)
                )
            
            if st.session_state.ttr_final is not None:
                st.success("¡Liquidación procesada con éxito!")
                st.download_button(
                    "📥 DESCARGAR TTR FINAL (.xlsx)", 
                    preparar_descarga(st.session_state.ttr_final), 
                    "TTR_Liquidacion_Final.xlsx"
                )
                st.dataframe(st.session_state.ttr_final.head(20))
    else:
        st.warning("⚠️ Primero debés configurar las tarifas en la pestaña anterior.")
