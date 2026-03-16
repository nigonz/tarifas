import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR Natalia v9.9.1", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES (DESCARGAS Y LIMPIEZA)
# =============================================================================

def preparar_descarga_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='TTR_Resultados')
    return output.getvalue()

def blindar_nombres(df):
    if df is None: return None
    df.columns = [str(c).upper().strip().replace(" ", "_").split('_X')[0].split('_Y')[0] for c in df.columns]
    return df

def formatear_ids(df, columnas_id):
    for col in columnas_id:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (NOV -> FEB)
# =============================================================================

def motor_proyeccion_tarifas(df_nov, manuales):
    df = blindar_nombres(df_nov.copy())
    col_id = [c for c in df.columns if 'ID' in c or 'GT' in c][0]
    col_p = [c for c in df.columns if 'LIMITE_INFERIOR' in c or 'TARIFA' in c or 'PRECIO' in c][0]
    
    v1_ant_row = df.loc[df[col_id] == '1SCN', col_p]
    v1_ant = pd.to_numeric(str(v1_ant_row.values[0]).replace(',', '.'), errors='coerce') if not v1_ant_row.empty else 270.00
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
# BLOQUE 2: MOTOR MAESTRO (V3 + TS + ALTAS)
# =============================================================================

def motor_maestro(df_v3_m, df_ts_m, df_elr):
    cols_v3, cols_ts = df_v3_m.columns.tolist(), df_ts_m.columns.tolist()
    v3_i, ts_i, elr_i = blindar_nombres(df_v3_m.copy()), blindar_nombres(df_ts_m.copy()), blindar_nombres(df_elr.copy())
    
    id_l_elr = [c for c in elr_i.columns if 'ID_LINEA_BO' in c or 'ID' in c][0]
    id_l_v3 = v3_i.columns[0]
    
    elr_lin = elr_i.drop_duplicates(subset=[id_l_elr])
    nuevas = elr_lin[~elr_lin[id_l_elr].astype(str).isin(v3_i[id_l_v3].astype(str))]
    
    if not nuevas.empty:
        n_v3 = pd.DataFrame(columns=v3_i.columns)
        n_v3[id_l_v3] = nuevas[id_l_elr]
        n_v3['GT'] = nuevas['GRUPO_TARIFARIO_LINEA_DNGFF']
        v3_i = pd.concat([v3_i, n_v3], ignore_index=True)

    v3_i = v3_i.merge(elr_lin[[id_l_elr, 'GRUPO_TARIFARIO_LINEA_DNGFF']], left_on=id_l_v3, right_on=id_l_elr, how='left')
    v3_i['GT'] = v3_i['GRUPO_TARIFARIO_LINEA_DNGFF'].fillna(v3_i['GT'])
    v3_f = v3_i[v3_i.columns[:len(cols_v3)]]; v3_f.columns = cols_v3
    return v3_f, ts_i[ts_i.columns[:len(cols_ts)]]

# =============================================================================
# BLOQUE 3: EL NUEVO MOTOR DMK (CON SCHEMA OVERRIDES)
# =============================================================================

def motor_dmk_blindado(f_dmk, df_v3, df_tarifas, fecha_corte, df_en):
    try:
        if f_dmk.name.endswith('.zip'):
            with zipfile.ZipFile(f_dmk) as z:
                csv_f = [n for n in z.namelist() if n.endswith('.csv')][0]
                with z.open(csv_f) as f: data = f.read()
        else: data = f_dmk.getvalue()

        # ESTRATEGIA DEFINITIVA: 
        # 1. Leemos solo los encabezados primero para saber qué columnas hay.
        headers = pd.read_csv(io.BytesIO(data), sep=';', encoding='iso-8859-1', nrows=0).columns.tolist()
        
        # 2. Creamos un diccionario forzando TODAS las columnas a String (Utf8).
        # Esto impide que Polars intente convertir 'LM622' a número.
        esquema_forzado = {col: pl.Utf8 for col in headers}

        # 3. Cargamos el archivo usando schema_overrides.
        lf = pl.read_csv(io.BytesIO(data), 
                         separator=";", 
                         encoding='iso-8859-1',
                         schema_overrides=esquema_forzado).lazy()
        
        # Limpieza de nombres de columnas
        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.collect_schema().names()})
        
        # Normalización de IDs y Fechas (ahora que todo es String es seguro)
        lf = lf.with_columns([
            pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip_chars().str.to_uppercase(),
            pl.col("FECHA").str.to_date("%d/%m/%Y")
        ])
        
        # Preparar dataframes de apoyo
        v3_pl = pl.from_pandas(formatear_ids(df_v3.copy(), [df_v3.columns[0]])).lazy().rename({df_v3.columns[0]: "ID_LINEA"})
        v3_pl = v3_pl.with_columns(pl.col("ID_LINEA").cast(pl.Utf8)) # Aseguramos mismo tipo

        tar_pl = pl.from_pandas(df_tarifas).lazy()
        en_pl = pl.from_pandas(blindar_nombres(df_en)).lazy()
        
        # Joins
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner")
        lf = lf.join(tar_pl, on="GT", how="left")
        lf = lf.join(en_pl, on="DOMINIO", how="left")
        
        # Lógica Mes Partido
        corte = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(pl.when(pl.col("FECHA") <= corte).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("T_PRAC"))
        
        # Casteo de columnas numéricas solo para el cálculo final
        num_cols = ["CANTIDAD_USOS", "DESCUENTO_X_INTEGRACION", "DEBITADO"]
        lf = lf.with_columns([pl.col(c).cast(pl.Float64).fill_null(0) for c in num_cols])
        
        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Utf8) == "621")
              .then(pl.when(pl.col("GT") == "INP").then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS")).otherwise((pl.col("T_PRAC") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS")))
              .otherwise(0).alias("COMP_ATS")
        ])
        
        return lf.group_by(['GT', 'ID_LINEA', 'DOMINIO', 'ENERGIA']).agg([pl.col('CANTIDAD_USOS').sum(), pl.col('COMP_ITG').sum(), pl.col('COMP_ATS').sum()]).collect().to_pandas()
    except Exception as e:
        st.error(f"Error crítico en DMK: {e}")
        return None

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

for k in ['v3', 'tarifas', 'res_dmk']:
    if k not in st.session_state: st.session_state[k] = None

st.title("🛡️ Fiscalización TTR v9.9.1")
t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO DMK"])

with t1:
    f_n = st.file_uploader("Cuadro Noviembre", key="tn")
    if f_n:
        c = st.columns(6)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42), 'INP': c[5].number_input("INP", 200.0)}
        if st.button("📊 Generar Tarifas"): st.session_state.tarifas = motor_proyeccion_tarifas(pd.read_excel(f_n), m)

with t2:
    fv3, fts, felr = st.file_uploader("V3"), st.file_uploader("TS"), st.file_uploader("ELR")
    if fv3 and fts and felr and st.button("🔄 Sincronizar"):
        st.session_state.v3, _ = motor_maestro(pd.read_excel(fv3), pd.read_excel(fts), pd.read_excel(felr))
        st.success("V3 Actualizado.")

with t3:
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        dt = st.date_input("Corte de Tarifa:", datetime(2026, 2, 14))
        fdmk, fen = st.file_uploader("DMK"), st.file_uploader("Energías")
        if fdmk and fen and st.button("⚡ PROCESAR"):
            st.session_state.res_dmk = motor_dmk_blindado(fdmk, st.session_state.v3, st.session_state.tarifas, str(dt), pd.read_excel(fen))
        
        if st.session_state.res_dmk is not None:
            st.download_button("📥 DESCARGAR TTR", preparar_descarga_excel(st.session_state.res_dmk), "TTR_Final.xlsx")
            st.dataframe(st.session_state.res_dmk.head())
