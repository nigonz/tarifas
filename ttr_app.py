import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Fiscalización TTR v10.1", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES (LIMPIEZA Y EXCEL)
# =============================================================================

def preparar_descarga(df):
    """Genera un archivo Excel en memoria para descarga."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Fiscalizacion')
    return output.getvalue()

def blindar_nombres(df):
    if df is None: return None
    df.columns = [str(c).upper().strip().replace(" ", "_") for c in df.columns]
    return df

def formatear_ids_string(df, columnas):
    for col in columnas:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MOTOR TARIFAS
# =============================================================================

def motor_tarifas(df_nov, manuales):
    df = blindar_nombres(df_nov.copy())
    col_id = [c for c in df.columns if 'ID' in c or 'GT' in c][0]
    col_p = [c for c in df.columns if 'LIMITE_INFERIOR' in c or 'TARIFA' in c or 'PRECIO' in c][0]
    
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
        res.append({'GT': id_t, 'TARIFA_NOV': round(v_ant, 2) if pd.notnull(v_ant) else 0.0, 'TARIFA_FEB': round(v_nue, 2)})
    return pd.DataFrame(res)

# =============================================================================
# BLOQUE 2: MOTOR NOMENCLADORES (V3 + TS + ALTAS)
# =============================================================================

def motor_nomencladores(df_v3, df_ts, df_elr):
    cols_v3 = df_v3.columns.tolist()
    cols_ts = df_ts.columns.tolist()
    
    v3_i, ts_i, elr_i = blindar_nombres(df_v3), blindar_nombres(df_ts), blindar_nombres(df_elr)
    id_l_elr = [c for c in elr_i.columns if 'ID_LINEA_BO' in c or 'ID' in c][0]
    id_l_v3 = v3_i.columns[0]
    
    v3_i = formatear_ids_string(v3_i, [id_l_v3])
    elr_i = formatear_ids_string(elr_i, [id_l_elr])

    # Sincronización V3 con Altas (Filas Amarillas)
    elr_lin = elr_i.drop_duplicates(subset=[id_l_elr])
    nuevas = elr_lin[~elr_lin[id_l_elr].isin(v3_i[id_l_v3])]
    
    if not nuevas.empty:
        n_v3 = pd.DataFrame(columns=v3_i.columns)
        n_v3[id_l_v3] = nuevas[id_l_elr]
        n_v3['RAZON_SOCIAL'] = nuevas.get('NOMBRE_EMPRESA', 'NUEVA ALTA')
        n_v3['GT'] = nuevas.get('GRUPO_TARIFARIO_LINEA_DNGFF', '')
        v3_i = pd.concat([v3_i, n_v3], ignore_index=True)

    v3_i = v3_i.merge(elr_lin[[id_l_elr, 'GRUPO_TARIFARIO_LINEA_DNGFF']], left_on=id_l_v3, right_on=id_l_elr, how='left')
    v3_i['GT'] = v3_i['GRUPO_TARIFARIO_LINEA_DNGFF'].fillna(v3_i['GT'])
    
    v3_f = v3_i[v3_i.columns[:len(cols_v3)]]
    v3_f.columns = cols_v3
    return v3_f, ts_i[ts_i.columns[:len(cols_ts)]]

# =============================================================================
# BLOQUE 3: MOTOR DMK (SÁNDWICH PANDAS-POLARS - FIX LM622)
# =============================================================================

def motor_dmk_tanque(f_dmk, df_v3, df_tarifas, fecha_corte, df_en):
    try:
        # Carga ultra-tolerante con Pandas para evitar error de tipo LM622
        if f_dmk.name.endswith('.zip'):
            with zipfile.ZipFile(f_dmk) as z:
                with z.open(z.namelist()[0]) as f: 
                    df_raw = pd.read_csv(f, sep=None, engine='python', encoding='iso-8859-1', dtype=str)
        else:
            df_raw = pd.read_csv(f_dmk, sep=None, engine='python', encoding='iso-8859-1', dtype=str)

        df_raw.columns = [c.strip().upper().replace(" ", "_") for c in df_raw.columns]
        df_raw['ID_LINEA'] = df_raw['ID_LINEA'].str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
        
        # Procesamiento rápido con Polars
        lf = pl.from_pandas(df_raw).lazy()
        lf = lf.with_columns(pl.col("FECHA").str.to_date("%d/%m/%Y"))
        
        v3_pl = pl.from_pandas(formatear_ids_string(df_v3.copy(), [df_v3.columns[0]])).lazy().rename({df_v3.columns[0]: "ID_LINEA"})
        tar_pl = pl.from_pandas(df_tarifas).lazy()
        en_pl = pl.from_pandas(blindar_nombres(df_en)).lazy()
        
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(tar_pl, on="GT", how="left").join(en_pl, on="DOMINIO", how="left")
        
        corte = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(pl.when(pl.col("FECHA") <= corte).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("T_PRAC"))
        
        for c in ["CANTIDAD_USOS", "DESCUENTO_X_INTEGRACION", "DEBITADO"]:
            lf = lf.with_columns(pl.col(c).cast(pl.Float64).fill_null(0))
        
        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Utf8) == "621")
              .then(pl.when(pl.col("GT") == "INP").then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS")).otherwise((pl.col("T_PRAC") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS")))
              .otherwise(0).alias("COMP_ATS")
        ])
        return lf.group_by(['GT', 'ID_LINEA', 'DOMINIO', 'ENERGIA']).agg([pl.col('CANTIDAD_USOS').sum(), pl.col('COMP_ITG').sum(), pl.col('COMP_ATS').sum()]).collect().to_pandas()
    except Exception as e:
        st.error(f"Error en DMK: {e}")
        return None

# =============================================================================
# INTERFAZ DE USUARIO (UI)
# =============================================================================

# Inicializar sesión
for key in ['tarifas', 'v3', 'ts', 'res_dmk']:
    if key not in st.session_state: st.session_state[key] = None

st.title("🛡️ Sistema de Fiscalización v10.1")
t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO DMK"])

with t1:
    st.header("1. Proyección de Tarifas")
    f_t = st.file_uploader("Subir Cuadro Noviembre", key="f_t")
    if f_t:
        c = st.columns(6)
        manuales = {
            '1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24),
            '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21),
            '5SCN': c[4].number_input("5SCN", 678.42), 'INP': c[5].number_input("INP", 200.0)
        }
        if st.button("📊 Calcular Tarifas"):
            st.session_state.tarifas = motor_tarifas(pd.read_excel(f_t), manuales)
            st.success("Tarifas calculadas.")
    if st.session_state.tarifas is not None:
        st.dataframe(st.session_state.tarifas)

with t2:
    st.header("2. Sincronización de Nomencladores")
    col1, col2, col3 = st.columns(3)
    f_v3 = col1.file_uploader("Nomenclador V3", key="f_v3")
    f_ts = col2.file_uploader("Nomenclador TS", key="f_ts")
    f_elr = col3.file_uploader("ELR Nuevo", key="f_elr")
    
    if f_v3 and f_ts and f_elr and st.button("🔄 Ejecutar Sincronización"):
        v3_res, ts_res = motor_nomencladores(pd.read_excel(f_v3), pd.read_excel(f_ts), pd.read_excel(f_elr))
        st.session_state.v3, st.session_state.ts = v3_res, ts_res
        st.success("Archivos sincronizados.")

    if st.session_state.v3 is not None:
        st.divider()
        st.download_button("📥 Bajar Nomenclador V3", preparar_descarga(st.session_state.v3), "V3_Actualizado.xlsx")
        st.dataframe(st.session_state.v3.head())

with t3:
    st.header("3. Proceso de Liquidación DMK")
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        corte = st.date_input("Fecha cambio de tarifa:", datetime(2026, 2, 14))
        f_dmk = st.file_uploader("Subir DMK (CSV/ZIP)", key="f_dmk")
        f_en = st.file_uploader("Subir Energías", key="f_en")
        
        if f_dmk and f_en and st.button("⚡ GENERAR TTR"):
            st.session_state.res_dmk = motor_dmk_tanque(f_dmk, st.session_state.v3, st.session_state.tarifas, str(corte), pd.read_excel(f_en))
        
        if st.session_state.res_dmk is not None:
            st.success("✅ Liquidación finalizada.")
            st.download_button("📥 DESCARGAR RESULTADO FINAL", preparar_descarga(st.session_state.res_dmk), "Liquidacion_Final.xlsx")
            st.dataframe(st.session_state.res_dmk)
    else:
        st.warning("⚠️ Debés completar las pestañas 1 y 2 primero.")
