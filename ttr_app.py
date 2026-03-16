import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR Natalia v9.8", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES DE DESCARGA Y LIMPIEZA
# =============================================================================

def preparar_descarga_excel(df):
    """Convierte un DataFrame en un objeto descargable para Streamlit."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
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
# BLOQUE 1: MOTOR DE TARIFAS (FIX NaN E INP)
# =============================================================================

def motor_proyeccion_tarifas_v9_8(df_nov, manuales):
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
        elif 'SEN' in id_t and 'SESN' not in id_t: v_nue = manuales.get(id_t.replace('SEN', 'SCN'), manuales['1SCN']) * 1.25
        elif 'SEAN' in id_t and 'SEASN' not in id_t: v_nue = manuales.get(id_t.replace('SEAN', 'SCN'), manuales['1SCN']) * 1.75
        elif 'SCSN' in id_t: v_nue = manuales.get(id_t.replace('SCSN', 'SCN'), manuales['1SCN']) * 1.59
        elif 'SESN' in id_t: v_nue = (manuales.get(id_t.replace('SESN', 'SCN'), manuales['1SCN']) * 1.59) * 1.25
        elif 'SEASN' in id_t: v_nue = (manuales.get(id_t.replace('SEASN', 'SCN'), manuales['1SCN']) * 1.59) * 1.75
        elif any(x in id_t for x in ['SGI', 'UPA']): v_nue = manuales['1SCN']
        else: v_nue = v_ant * factor if pd.notnull(v_ant) else manuales['1SCN']

        res.append({'GT': id_t, 'TARIFA_NOV': round(v_ant, 2) if pd.notnull(v_ant) else 0.0, 'TARIFA_FEB': round(v_nue, 2)})
    return pd.DataFrame(res)

# =============================================================================
# BLOQUE 2: MOTOR MAESTRO (SINCRONIZACIÓN V3 + TS)
# =============================================================================

def motor_maestro_v9_8(df_v3_m, df_ts_m, df_elr):
    cols_v3, cols_ts = df_v3_m.columns.tolist(), df_ts_m.columns.tolist()
    v3_i, ts_i, elr_i = blindar_nombres(df_v3_m.copy()), blindar_nombres(df_ts_m.copy()), blindar_nombres(df_elr.copy())
    
    id_l_elr = [c for c in elr_i.columns if 'ID_LINEA_BO' in c or 'ID' in c][0]
    id_r_elr = [c for c in elr_i.columns if 'ID_RAMAL_BO' in c or 'RAMAL' in c][0]
    id_l_v3 = v3_i.columns[0]
    id_l_ts = [c for c in ts_i.columns if 'IDLINEANS' in c or 'ID_LINEA' in c][0]
    id_r_ts = [c for c in ts_i.columns if 'IDRAMALNS' in c or 'ID_RAMAL' in c][0]

    elr_i = formatear_ids(elr_i, [id_l_elr, id_r_elr])
    v3_i = formatear_ids(v3_i, [id_l_v3]); ts_i = formatear_ids(ts_i, [id_l_ts, id_r_ts])

    # Sincronización V3 con Altas
    elr_lin = elr_i.drop_duplicates(subset=[id_l_elr])
    nuevas = elr_lin[~elr_lin[id_l_elr].isin(set(v3_i[id_l_v3]))]
    if not nuevas.empty:
        n_v3 = pd.DataFrame(columns=v3_i.columns)
        n_v3[id_l_v3] = nuevas[id_l_elr]; n_v3['RAZON_SOCIAL'] = nuevas['NOMBRE_EMPRESA']
        n_v3['GT'] = nuevas['GRUPO_TARIFARIO_LINEA_DNGFF']; n_v3['OBSERVACION'] = "ALTA ELR"
        v3_i = pd.concat([v3_i, n_v3], ignore_index=True)

    v3_i = v3_i.merge(elr_lin[[id_l_elr, 'GRUPO_TARIFARIO_LINEA_DNGFF']], left_on=id_l_v3, right_on=id_l_elr, how='left')
    v3_i['GT'] = v3_i['GRUPO_TARIFARIO_LINEA_DNGFF'].fillna(v3_i['GT'])
    v3_f = v3_i[v3_i.columns[:len(cols_v3)]]; v3_f.columns = cols_v3

    # Sincronización TS
    ts_i['KEY'] = ts_i[id_l_ts] + "_" + ts_i[id_r_ts]
    elr_i['KEY'] = elr_i[id_l_elr] + "_" + elr_i[id_r_elr]
    n_ram = elr_i[~elr_i['KEY'].isin(set(ts_i['KEY']))]
    if not n_ram.empty:
        n_ts = pd.DataFrame(columns=ts_i.columns)
        n_ts[id_l_ts] = n_ram[id_l_elr]; n_ts[id_r_ts] = n_ram[id_r_elr]
        n_ts['GT'] = n_ram['GRUPO_TARIFARIO_LINEA_DNGFF']; n_ts['OBSERVACION'] = "NUEVO RAMAL"
        ts_i = pd.concat([ts_i, n_ts], ignore_index=True)

    ts_i = ts_i.merge(elr_i[['KEY', 'GRUPO_TARIFARIO_LINEA_DNGFF']], on='KEY', how='left')
    ts_i['GT'] = ts_i['GRUPO_TARIFARIO_LINEA_DNGFF'].fillna(ts_i['GT'])
    ts_f = ts_i[ts_i.columns[:len(cols_ts)]]; ts_f.columns = cols_ts

    return v3_f, ts_f

# =============================================================================
# BLOQUE 3: MOTOR DMK (SOLUCIÓN DEFINITIVA LM622 + MES PARTIDO)
# =============================================================================

def motor_dmk_v9_8(f_dmk, df_v3, df_tarifas, fecha_corte, df_en):
    try:
        data = f_dmk.getvalue() if not f_dmk.name.endswith('.zip') else zipfile.ZipFile(f_dmk).read(zipfile.ZipFile(f_dmk).namelist()[0])
        
        # FIX LM622: infer_schema_length=0 fuerza a leer ID_LINEA como String desde la fila 1
        lf = pl.read_csv(io.BytesIO(data), separator=";", infer_schema_length=0, encoding='iso-8859-1').lazy()
        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.collect_schema().names()})
        
        lf = lf.with_columns([
            pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip_chars().str.to_uppercase(),
            pl.col("FECHA").str.to_date("%d/%m/%Y")
        ])
        
        v3_pl = pl.from_pandas(formatear_ids(df_v3.copy(), [df_v3.columns[0]])).lazy().rename({df_v3.columns[0]: "ID_LINEA"})
        tar_pl = pl.from_pandas(df_tarifas).lazy(); en_pl = pl.from_pandas(blindar_nombres(df_en)).lazy()
        
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(tar_pl, on="GT", how="left").join(en_pl, on="DOMINIO", how="left")
        
        corte = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(pl.when(pl.col("FECHA") <= corte).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("T_PRAC"))
        
        cols = ["CANTIDAD_USOS", "DESCUENTO_X_INTEGRACION", "DEBITADO"]
        lf = lf.with_columns([pl.col(c).cast(pl.Float64).fill_null(0) for c in cols])
        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Utf8) == "621")
              .then(pl.when(pl.col("GT") == "INP").then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS")).otherwise((pl.col("T_PRAC") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS")))
              .otherwise(0).alias("COMP_ATS")
        ])
        return lf.group_by(['GT', 'ID_LINEA', 'DOMINIO', 'ENERGIA']).agg([pl.col('CANTIDAD_USOS').sum(), pl.col('COMP_ITG').sum(), pl.col('COMP_ATS').sum()]).collect().to_pandas()
    except Exception as e:
        st.error(f"Error en liquidación: {e}"); return None

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

for k in ['v3', 'ts', 'tarifas', 'res_dmk']:
    if k not in st.session_state: st.session_state[k] = None

st.title("🛡️ Fiscalización TTR Natalia v9.8")
tabs = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO DMK"])

with tabs[0]:
    st.header("1. Proyección Tarifaria")
    f_n = st.file_uploader("Cuadro Noviembre", key="tnov")
    if f_n:
        c = st.columns(6)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42), 'INP': c[5].number_input("INP", 200.0)}
        if st.button("📊 Generar Tarifas"): st.session_state.tarifas = motor_proyeccion_tarifas_v9_8(pd.read_excel(f_n), m)
    if st.session_state.tarifas is not None: st.dataframe(st.session_state.tarifas)

with tabs[1]:
    st.header("2. Sincronización V3 y TS")
    c1, c2, c3 = st.columns(3)
    fv3, fts, felr = c1.file_uploader("V3 (16 col)"), c2.file_uploader("TS (9 col)"), c3.file_uploader("ELR Nuevo")
    if fv3 and fts and felr and st.button("🔄 Sincronizar"):
        st.session_state.v3, st.session_state.ts = motor_maestro_v9_8(pd.read_excel(fv3), pd.read_excel(fts), pd.read_excel(felr))
        st.success("Sincronización terminada con éxito.")
    
    if st.session_state.v3 is not None:
        st.divider()
        col1, col2 = st.columns(2)
        # DESCARGAS CORREGIDAS
        col1.download_button("📥 Bajar Nomenclador V3", preparar_descarga_excel(st.session_state.v3), "V3_Actualizado.xlsx")
        col2.download_button("📥 Bajar Nomenclador TS", preparar_descarga_excel(st.session_state.ts), "TS_Actualizado.xlsx")
        st.dataframe(st.session_state.v3.head())

with tabs[2]:
    st.header("3. Liquidación DMK")
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        dt = st.date_input("Fecha cambio de tarifa:", datetime(2026, 2, 14))
        fdmk, fen = st.file_uploader("DMK"), st.file_uploader("Energías")
        if fdmk and fen and st.button("⚡ PROCESAR"):
            st.session_state.res_dmk = motor_dmk_v9_8(fdmk, st.session_state.v3, st.session_state.tarifas, str(dt), pd.read_excel(fen))
        if st.session_state.res_dmk is not None:
            st.download_button("📥 DESCARGAR LIQUIDACIÓN", preparar_descarga_excel(st.session_state.res_dmk), "Liquidacion_Final_TTR.xlsx")
            st.dataframe(st.session_state.res_dmk.head())
