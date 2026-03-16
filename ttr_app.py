import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Fiscalización TTR v10.7", layout="wide")

# =============================================================================
# BLOQUE 0: UTILIDADES PROTEGIDAS
# =============================================================================

def preparar_descarga(df):
    """Genera Excel en memoria sin riesgo de NoneType."""
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def clean_ids(df, cols):
    """Asegura que los IDs sean texto limpio para evitar errores de tipo."""
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (Lógica JN Original)
# =============================================================================

def motor_tarifas_jn(df_nov, manuales):
    df = df_nov.copy()
    df.columns = [str(c).upper().strip() for c in df.columns]
    
    # Identificación de columnas críticas
    col_id = [c for c in df.columns if any(x in c for x in ['ID', 'GT'])][0]
    col_p = [c for c in df.columns if any(x in c for x in ['LIMITE', 'TARIFA', 'PRECIO'])][0]
    
    # Factor de incremento basado en 1SCN
    val_1scn = df.loc[df[col_id] == '1SCN', col_p].values
    v1_ant = pd.to_numeric(str(val_1scn[0]).replace(',', '.'), errors='coerce') if len(val_1scn) > 0 else 270.0
    factor = manuales['1SCN'] / v1_ant if v1_ant > 0 else 1
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row[col_id]).strip().upper()
        v_ant = pd.to_numeric(str(row[col_p]).replace(',', '.'), errors='coerce')
        
        # Aplicación de multiplicadores JN
        if id_t in manuales: v_nue = manuales[id_t]
        elif any(x in id_t for x in ['SGI', 'UPA']): v_nue = manuales['1SCN']
        else: v_nue = v_ant * factor if pd.notnull(v_ant) else manuales['1SCN']
        
        res.append({'GT': id_t, 'TARIFA_NOV': round(v_ant, 2), 'TARIFA_FEB': round(v_nue, 2)})
    return pd.DataFrame(res)

# =============================================================================
# BLOQUE 2: MOTOR NOMENCLADORES (Regla de No-Borrado)
# =============================================================================

def motor_nomencladores_v3(df_v3, df_elr):
    v3_cols = df_v3.columns.tolist()
    v3_work = df_v3.copy()
    elr_work = df_elr.copy()
    
    # Normalizar encabezados e IDs
    v3_work.columns = [str(c).upper().strip() for c in v3_work.columns]
    elr_work.columns = [str(c).upper().strip() for c in elr_work.columns]
    
    id_l_v3 = v3_work.columns[0]
    id_l_elr = [c for c in elr_work.columns if 'ID_LINEA_BO' in c or 'ID' in c][0]
    
    v3_work = clean_ids(v3_work, [id_l_v3])
    elr_work = clean_ids(elr_work, [id_l_elr])

    # Sincronización: Agregar nuevas, no borrar viejas
    elr_lin = elr_work.drop_duplicates(subset=[id_l_elr])
    nuevas = elr_lin[~elr_lin[id_l_elr].isin(v3_work[id_l_v3])]
    
    if not nuevas.empty:
        df_nuevas = pd.DataFrame(columns=v3_work.columns)
        df_nuevas[id_l_v3] = nuevas[id_l_elr]
        df_nuevas['RAZON_SOCIAL'] = nuevas.get('NOMBRE_EMPRESA', 'ALTA ELR')
        df_nuevas['GT'] = nuevas.get('GRUPO_TARIFARIO_LINEA_DNGFF', '')
        v3_work = pd.concat([v3_work, df_nuevas], ignore_index=True)

    # Actualizar GT sin alterar registros
    v3_work = v3_work.merge(elr_lin[[id_l_elr, 'GRUPO_TARIFARIO_LINEA_DNGFF']], left_on=id_l_v3, right_on=id_l_elr, how='left')
    v3_work['GT'] = v3_work['GRUPO_TARIFARIO_LINEA_DNGFF'].fillna(v3_work['GT'])
    
    v3_final = v3_work[v3_work.columns[:len(v3_cols)]]
    v3_final.columns = v3_cols
    return v3_final

# =============================================================================
# BLOQUE 3: MOTOR DMK (Lógica Notebook + Fix LM622)
# =============================================================================

def motor_dmk_original_tanque(f_dmk, df_v3, df_tarifas, fecha_corte, df_pme):
    try:
        # 1. Carga con Pandas (Blindaje LM622)
        if f_dmk.name.endswith('.zip'):
            with zipfile.ZipFile(f_dmk) as z:
                with z.open(z.namelist()[0]) as f: 
                    df = pd.read_csv(f, sep=None, engine='python', encoding='iso-8859-1', dtype=str)
        else:
            df = pd.read_csv(f_dmk, sep=None, engine='python', encoding='iso-8859-1', dtype=str)

        # Normalizar nombres según tu notebook original
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
        df = clean_ids(df, ['ID_LINEA', 'DOMINIO'])
        
        # 2. Paso a Polars para cálculos de Mes Partido
        lf = pl.from_pandas(df).lazy()
        lf = lf.with_columns(pl.col("FECHA").str.to_date("%d/%m/%Y"))
        
        # Cruce con Nomenclador V3
        v3_base = clean_ids(df_v3.copy(), [df_v3.columns[0]])
        v3_pl = pl.from_pandas(v3_base).lazy().rename({v3_base.columns[0]: "ID_LINEA"})
        tar_pl = pl.from_pandas(df_tarifas).lazy()
        
        lf = lf.join(v3_pl, on="ID_LINEA", how="inner").join(tar_pl, on="GT", how="left")
        
        # 3. Lógica TTR: Definir Tarifa según Fecha
        corte_dt = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
        lf = lf.with_columns(
            pl.when(pl.col("FECHA") <= corte_dt).then(pl.col("TARIFA_NOV")).otherwise(pl.col("TARIFA_FEB")).alias("TARIFA_ACTUAL")
        )

        # 4. Fórmulas Financieras Originales (Notebook DMK)
        num_cols = ['TARIFA_ACTUAL', 'DEBITADO', 'DESCUENTO_X_INTEGRACION', 'CANTIDAD_USOS']
        for c in num_cols:
            if c in lf.collect_schema().names():
                lf = lf.with_columns(pl.col(c).cast(pl.Float64).fill_null(0))

        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO").cast(pl.Utf8) == "621")
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA_ACTUAL") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS")
        ])

        # Netos s/IVA (Dividido 1.105)
        lf = lf.with_columns([
            (pl.col("COMP_ATS") / 1.105).alias("ATS_NETO"),
            (pl.col("COMP_ITG") / 1.105).alias("ITG_NETO")
        ])

        # 5. Lógica de Energías (PME)
        res_pd = lf.collect().to_pandas()
        pme = clean_ids(df_pme.copy(), ['DOMINIO'])
        dom_existentes = pme['DOMINIO'].unique()
        
        df_con_energia = res_pd[res_pd['DOMINIO'].isin(dom_existentes)].merge(pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_sin_energia = res_pd[~res_pd['DOMINIO'].isin(dom_existentes)].copy()
        df_sin_energia['DOMINIO'], df_sin_energia['ENERGIA'] = 'NO', 3
        
        return pd.concat([df_con_energia, df_sin_energia], ignore_index=True)

    except Exception as e:
        st.error(f"Falla en DMK: {e}")
        return None

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

for key in ['tarifas', 'v3', 'res_dmk']:
    if key not in st.session_state: st.session_state[key] = None

st.title("🛡️ Sistema de Fiscalización v10.7")
t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📋 2. NOMENCLADORES", "📂 3. LIQUIDACIÓN DMK"])

with t1:
    st.subheader("Proyección Tarifaria JN")
    f_t = st.file_uploader("Subir Cuadro Base (Excel)")
    if f_t:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar Febrero"):
            st.session_state.tarifas = motor_tarifas_jn(pd.read_excel(f_t), m)
    if st.session_state.tarifas is not None:
        st.dataframe(st.session_state.tarifas)

with t2:
    st.subheader("Actualización de Nomencladores")
    f_v3 = st.file_uploader("Nomenclador V3 (16 col)")
    f_elr = st.file_uploader("ELR Actualizado")
    if f_v3 and f_elr and st.button("🔄 Sincronizar"):
        st.session_state.v3 = motor_nomencladores_v3(pd.read_excel(f_v3), pd.read_excel(f_elr))
        st.success("Sincronización terminada.")
    if st.session_state.v3 is not None:
        st.download_button("📥 Bajar V3 Final", preparar_descarga(st.session_state.v3), "V3_Actualizado.xlsx")
        st.dataframe(st.session_state.v3.head())

with t3:
    st.subheader("Proceso TTR (Liquidación DMK)")
    if st.session_state.v3 is not None and st.session_state.tarifas is not None:
        corte = st.date_input("Fecha Cambio de Tarifa:", datetime(2026, 2, 14))
        f_dmk = st.file_uploader("Subir DMK (CSV/ZIP)")
        f_pme = st.file_uploader("Subir Parque Móvil (Energías)")
        if f_dmk and f_pme and st.button("⚡ GENERAR TTR"):
            st.session_state.res_dmk = motor_dmk_original_tanque(f_dmk, st.session_state.v3, st.session_state.tarifas, str(corte), pd.read_excel(f_pme))
        
        if st.session_state.res_dmk is not None:
            st.download_button("📥 DESCARGAR RESULTADO FINAL", preparar_descarga(st.session_state.res_dmk), "TTR_Liquidacion_Final.xlsx")
            st.dataframe(st.session_state.res_dmk.head())
    else:
        st.warning("⚠️ Debes completar las pestañas anteriores primero.")
