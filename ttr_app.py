import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

st.set_page_config(page_title="Fiscalización TTR v10.3", layout="wide")

# --- BLOQUE 0: UTILIDADES ---
def preparar_descarga(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def clean_ids(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# --- BLOQUE 1: MOTOR DE TARIFAS ---
def motor_tarifas(df_nov, manuales):
    # (Mantenemos tu lógica de proyección de la pestaña 1)
    df = df_nov.copy()
    df.columns = [str(c).upper().strip() for c in df.columns]
    col_id = [c for c in df.columns if 'ID' in c or 'GT' in c][0]
    col_p = [c for c in df.columns if 'LIMITE_INFERIOR' in c or 'TARIFA' in c or 'PRECIO' in c][0]
    
    v1_ant = pd.to_numeric(str(df.loc[df[col_id] == '1SCN', col_p].values[0]).replace(',', '.'), errors='coerce')
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

# --- BLOQUE 2: MOTOR DMK (TU LÓGICA + FIX LM622) ---
def motor_dmk_original(f_dmk, df_v3, df_tarifas, fecha_corte, df_pme):
    try:
        # 1. LEER DMK (Dtype=str para evitar error LM622)
        if f_dmk.name.endswith('.zip'):
            with zipfile.ZipFile(f_dmk) as z:
                with z.open(z.namelist()[0]) as f: 
                    df = pd.read_csv(f, sep=None, engine='python', encoding='iso-8859-1', dtype=str)
        else:
            df = pd.read_csv(f_dmk, sep=None, engine='python', encoding='iso-8859-1', dtype=str)

        # 2. FILTRAR COLUMNAS SEGÚN TU CÓDIGO
        # Usamos tus nombres exactos: 'TARIFA BASE ITG', 'DEBITADO', etc.
        cols_dmk = ['ID_EMPRESA', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'FECHA', 'CONTRATO', 
                    'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS', 'MONTO', 'DEBITADO']
        
        # Verificar si las columnas existen (evita el crash)
        df.columns = [c.strip().upper() for c in df.columns]
        actual_cols = [c for c in cols_dmk if c in df.columns]
        df = df[actual_cols]

        # 3. CONVERTIR A POLARS PARA VELOCIDAD Y CRUCES
        df = clean_ids(df, ['ID_LINEA', 'DOMINIO'])
        lf = pl.from_pandas(df).lazy()
        
        # 4. JOIN CON NOMENCLADOR (Tus columnas: GT, PROVINCIA, MUNICIPIO)
        v3_c = df_v3.copy()
        v3_c = clean_ids(v3_c, [v3_c.columns[0]])
        v3_pl = pl.from_pandas(v3_c).lazy().rename({v3_c.columns[0]: "ID_LINEA"})
        
        lf = lf.join(v3_pl, on="ID_LINEA", how="left")

        # 5. LÓGICA DE MES PARTIDO (TTR)
        tar_pl = pl.from_pandas(df_tarifas).lazy()
        lf = lf.join(tar_pl, on="GT", how="left")
        
        corte_dt = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(pl.col("FECHA").str.to_date("%d/%m/%Y"))
        
        # Decidir qué tarifa usar según la fecha
        lf = lf.with_columns(
            pl.when(pl.col("FECHA") <= corte_dt).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("TARIFA_PRACTICADA")
        )

        # 6. CÁLCULOS ORIGINALES (ATS / ITG / IVA)
        num_cols = ['TARIFA_PRACTICADA', 'DEBITADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS']
        for c in num_cols:
            if c in lf.collect_schema().names():
                lf = lf.with_columns(pl.col(c).cast(pl.Float64).fill_null(0))

        lf = lf.with_columns([
            (pl.col("DESCUENTO X INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Utf8) == "621")
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA_PRACTICADA") - pl.col("DEBITADO") - pl.col("DESCUENTO X INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS")
        ])

        # Agregar s/IVA (Tus fórmulas / 1.105)
        lf = lf.with_columns([
            (pl.col("COMP_ATS") / 1.105).alias("COMP_ATS_S_IVA"),
            (pl.col("COMP_ITG") / 1.105).alias("COMP_ITG_S_IVA")
        ])

        # 7. LÓGICA DE ENERGÍAS (Merge con PME)
        res_final = lf.collect().to_pandas()
        pme = clean_ids(df_pme.copy(), ['DOMINIO'])
        
        # Dividir dominios como hacés vos
        dominios_pme = pme['DOMINIO'].unique()
        df_pm = res_final[res_final['DOMINIO'].isin(dominios_pme)].copy()
        df_resto = res_final[~res_final['DOMINIO'].isin(dominios_pme)].copy()
        
        # Pegar Energías
        df_pm = df_pm.merge(pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_resto['DOMINIO'] = 'NO'
        df_resto['ENERGIA'] = 3
        
        # Unir y GroupBy final
        final = pd.concat([df_pm, df_resto], ignore_index=True)
        
        # Agrupamos como en tu código
        group_cols = ['PROVINCIA', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO']
        final_grouped = final.groupby(group_cols, as_index=False).agg({
            'CANTIDAD_USOS': 'sum',
            'COMP_ITG': 'sum',
            'COMP_ATS': 'sum',
            'COMP_ITG_S_IVA': 'sum',
            'COMP_ATS_S_IVA': 'sum'
        })
        
        return final_grouped

    except Exception as e:
        st.error(f"Error procesando DMK: {e}")
        return None

# --- UI INTERFAZ ---
for k in ['v3', 'tarifas', 'res_dmk']:
    if k not in st.session_state: st.session_state[k] = None

st.title("🛡️ Fiscalización Natalia v10.3")
t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. PROCESO TTR"])

with t1:
    f_t = st.file_uploader("Cuadro Noviembre", key="t_up")
    if f_t:
        c = st.columns(6)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42), 'INP': c[5].number_input("INP", 200.0)}
        if st.button("📊 Calcular Tarifas"):
            st.session_state.tarifas = motor_tarifas(pd.read_excel(f_t), m)
    if st.session_state.tarifas is not None: st.dataframe(st.session_state.tarifas)

with t2:
    fv3 = st.file_uploader("Subir Nomenclador V3", key="v3_up")
    if fv3 and st.button("🔄 Cargar Nomenclador"):
        st.session_state.v3 = pd.read_excel(fv3)
        st.success("Nomenclador cargado.")

with t3:
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        dt = st.date_input("Fecha cambio de tarifa:", datetime(2026, 2, 14))
        fdmk = st.file_uploader("Subir DMK (CSV/ZIP)", key="dmk_up")
        fen = st.file_uploader("Subir Parque Movil (Energías)", key="en_up")
        
        if fdmk and fen and st.button("⚡ GENERAR LIQUIDACIÓN TTR"):
            st.session_state.res_dmk = motor_dmk_original(fdmk, st.session_state.v3, st.session_state.tarifas, str(dt), pd.read_excel(fen))
        
        if st.session_state.res_dmk is not None:
            st.download_button("📥 DESCARGAR EXCEL FINAL", preparar_descarga(st.session_state.res_dmk), "TTR_Liquidacion_Final.xlsx")
            st.dataframe(st.session_state.res_dmk.head())
    else:
        st.warning("⚠️ Primero completá las pestañas 1 y 2.")
