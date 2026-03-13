import streamlit as st
import pandas as pd
import polars as pl
import numpy as np
import io
import zipfile

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Fiscalización TTR Natalia v6.5", layout="wide")

# =============================================================================
# MOTORES DE LÓGICA (PROCESAMIENTO)
# =============================================================================
import streamlit as st
import pandas as pd
import io

# =============================================================================
# MOTOR DE NOMENCLADOR MAESTRO V3 (El "Súper Buscar V")
# =============================================================================

def motor_v3_maestro(df_v2, df_elr, df_ts):
    """
    Sincroniza los 3 archivos usando un escudo de columnas para evitar errores.
    """
    # 1. Normalización de Columnas (Mayúsculas y sin espacios)
    for df in [df_v2, df_elr, df_ts]:
        df.columns = [str(c).strip().upper() for c in df.columns]

    # 2. Identificación Inteligente de IDs (El Escudo)
    # Buscamos la columna que contenga 'LINEA' e 'ID' en cada archivo
    def encontrar_id(df):
        for c in df.columns:
            if ('ID' in c and 'LINEA' in c) or ('IDLINEANS' in c):
                return c
        return df.columns[0] # Fallback al primero si no encuentra

    id_v2 = encontrar_id(df_v2)
    id_elr = encontrar_id(df_elr)
    id_ts = encontrar_id(df_ts)

    # Limpieza de IDs (Texto y sin .0)
    for df, col in zip([df_v2, df_elr, df_ts], [id_v2, id_elr, id_ts]):
        df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # 3. CRUCE 1: Base v2 + ELR (Actualiza GT y Línea SILAS)
    # Buscamos columnas del ELR por palabras clave
    col_gt_elr = [c for c in df_elr.columns if 'GRUPO' in c and 'TARIF' in c][0]
    col_lin_elr = [c for c in df_elr.columns if 'LINEA' in c and 'BO' not in c and 'DNGFF' not in c][0]
    
    elr_reducido = df_elr[[id_elr, col_gt_elr, col_lin_elr]].drop_duplicates(subset=[id_elr])
    
    # Unimos (Merge)
    res = df_v2.merge(elr_reducido, left_on=id_v2, right_on=id_elr, how='left')

    # 4. CRUCE 2: Resultado + Nomenclador TS (Trae el TS Final)
    # Buscamos columna de TS en el archivo de ramales
    col_ts_file = [c for c in df_ts.columns if 'TIPO' in c and 'SERV' in c][0]
    
    ts_reducido = df_ts[[id_ts, col_ts_file]].drop_duplicates(subset=[id_ts])
    
    # Unimos
    final = res.merge(ts_reducido, left_on=id_v2, right_on=id_ts, how='left')

    # 5. Limpieza de columnas duplicadas tras el merge
    final = final.loc[:, ~final.columns.duplicated()]
    
    return final

# =============================================================================
# INTERFAZ (UI) - PESTAÑA 2: NOMENCLADOR
# =============================================================================

# (Asumimos que esto vive dentro de tu st.tabs)

with tab_v3: # Esta es la Pestaña 2
    st.header("Generador de Nomenclador Maestro V3 📋")
    st.markdown("Subí los 3 archivos para sincronizar la base de Febrero.")

    c1, c2, c3 = st.columns(3)
    f_v2 = c1.file_uploader("Nomenclador Base v2", type=['xlsx', 'csv'])
    f_elr = c2.file_uploader("ELR Febrero", type=['xlsx', 'csv'])
    f_ts = c3.file_uploader("Nomenclador TS (Ramales)", type=['xlsx', 'csv'])

    if f_v2 and f_elr and f_ts:
        if st.button("🔄 Sincronizar Archivos"):
            # Lectura automática
            d_v2 = pd.read_excel(f_v2) if f_v2.name.endswith('.xlsx') else pd.read_csv(f_v2)
            d_elr = pd.read_excel(f_elr) if f_elr.name.endswith('.xlsx') else pd.read_csv(f_elr)
            d_ts = pd.read_excel(f_ts) if f_ts.name.endswith('.xlsx') else pd.read_csv(f_ts)
            
            with st.spinner("Cruzando datos..."):
                st.session_state.df_v3 = motor_v3_maestro(d_v2, d_elr, d_ts)
                st.success("✅ Nomenclador V3 generado. Ya podés usarlo en la Tab de DMK.")
                st.dataframe(st.session_state.df_v3.head())

                # Botón de Descarga
                buf = io.BytesIO()
                st.session_state.df_v3.to_excel(buf, index=False)
                st.download_button("📥 Descargar NOMENCLADOR_V3_FEBRERO.xlsx", buf.getvalue(), "NOMENCLADOR_V3_FEBRERO.xlsx")

def motor_tarifas_auditoria(df_base, manuales):
    """Proyecta tarifas y genera tabla de auditoría"""
    df = df_base.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for col in ['Limite Inferior', 'Limite Superior']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    try:
        v1_ant = df.loc[df['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = manuales['1SCN'] / v1_ant
    except: return None

    res = []
    for _, row in df.iterrows():
        id_t = str(row['Id']).strip()
        v_min_ant, v_max_ant = row['Limite Inferior'], row['Limite Superior']
        
        if id_t in manuales:
            v_min = v_max = manuales[id_t]
            regla = "Manual (SCN)"
        elif 'SEN' in id_t and 'SESN' not in id_t:
            v_min = v_max = manuales.get(id_t.replace('SEN', 'SCN'), manuales['1SCN']) * 1.25
            regla = "Factor 1.25 (SEN)"
        elif 'SEAN' in id_t and 'SEASN' not in id_t:
            v_min = v_max = manuales.get(id_t.replace('SEAN', 'SCN'), manuales['1SCN']) * 1.75
            regla = "Factor 1.75 (SEAN)"
        elif 'SCSN' in id_t:
            v_min = v_max = manuales.get(id_t.replace('SCSN', 'SCN'), manuales['1SCN']) * 1.59
            regla = "Factor 1.59 (SCSN)"
        elif 'SESN' in id_t:
            v_min = v_max = (manuales.get(id_t.replace('SESN', 'SCN'), manuales['1SCN']) * 1.59) * 1.25
            regla = "Compuesta (SESN)"
        elif 'SEASN' in id_t:
            v_min = v_max = (manuales.get(id_t.replace('SEASN', 'SCN'), manuales['1SCN']) * 1.59) * 1.75
            regla = "Compuesta (SEASN)"
        else:
            v_min, v_max = v_min_ant * factor, v_max_ant * factor
            regla = "Ajuste General (%)"

        res.append({
            'Id': id_t, 'Noviembre': round(v_min_ant, 2), 'Nuevo': round(v_min, 2),
            'Variación %': round(((v_min/v_min_ant)-1)*100, 2) if v_min_ant > 0 else 0,
            'Regla': regla, 'Limite Superior': round(v_max, 2)
        })
    return pd.DataFrame(res)

def motor_dmk_polars(f_sube, df_gt, df_en):
    """Maneja ZIP de 210MB, detecta separador y calcula ATS/ITG"""
    try:
        # 1. Leer el contenido (ZIP o CSV)
        if f_sube.name.endswith('.zip'):
            with zipfile.ZipFile(f_sube) as z:
                csv_file = [n for n in z.namelist() if n.endswith('.csv')][0]
                with z.open(csv_file) as f:
                    data = f.read()
        else:
            data = f_sube.getvalue()

        # 2. Motor Polars con detección de separador
        try:
            lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=";", infer_schema_length=10000).lazy()
            # Test rápido: ¿leyó bien las columnas?
            test = lf.select(pl.all().head(1)).collect()
            if len(test.columns) < 5: raise Exception("Re-try with comma")
        except:
            lf = pl.read_csv(io.BytesIO(data), encoding='iso-8859-1', separator=",", infer_schema_length=10000).lazy()

        # 3. Limpieza Agresiva
        lf = lf.rename({c: c.strip().upper() for c in lf.columns})
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))
        
        # 4. Cruces
        gt_pl = pl.from_pandas(df_gt).lazy().rename({c: c.strip().upper() for c in df_gt.columns})
        en_pl = pl.from_pandas(df_en).lazy().rename({c: c.strip().upper() for c in df_en.columns})
        gt_pl = gt_pl.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))

        lf = lf.join(gt_pl, on="ID_LINEA", how="inner").join(en_pl, on="DOMINIO", how="left")
        
        # Lógica Carriles PME + Resto
        lf = lf.with_columns([
            pl.when(pl.col("ENERGIA").is_null()).then(pl.lit("NO")).otherwise(pl.col("DOMINIO")).alias("DOMINIO"),
            pl.col("ENERGIA").fill_null(3)
        ])

        # Compensaciones
        lf = lf.with_columns([
            (pl.col("DESCUENTO X INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO") == 621)
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA BASE ITG") - pl.col("DEBITADO") - pl.col("DESCUENTO X INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS")
        ])

        res = lf.group_by(['PROVINCIA', 'MUNICIPIO', 'GT', 'LINEA SILAS DNGFF', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION']).agg([pl.col('CANTIDAD_USOS').sum(), pl.col('COMP_ITG').sum(), pl.col('COMP_ATS').sum()]).collect().to_pandas()
        
        res['COMP_ATS s/IVA'] = res['COMP_ATS'] / 1.105
        res['COMP_ITG s/IVA'] = res['COMP_ITG'] / 1.105
        return res
    except Exception as e:
        st.error(f"Error crítico en DMK: {e}")
        return None

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

if 'df_tarifas' not in st.session_state: st.session_state.df_tarifas = None
if 'df_dmk' not in st.session_state: st.session_state.df_dmk = None
if 'per_global' not in st.session_state: st.session_state.per_global = "Febrero"

st.title(f"Fiscalización TTR Natalia - Periodo: {st.session_state.per_global} 2026")

t1, t2 = st.tabs(["💰 1. TARIFAS", "📂 2. PREPROCESO DMK"])

with t1:
    f_ref = st.file_uploader("Subir Referencia Noviembre", type=['xlsx', 'csv'])
    if f_ref:
        st.session_state.per_global = st.text_input("Mes a liquidar:", value=st.session_state.per_global)
        hoja = st.text_input("Hoja Excel:", value="JN11")
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        
        if st.button("🔄 Calcular Tarifas"):
            df_b = pd.read_excel(f_ref, sheet_name=hoja) if f_ref.name.endswith('.xlsx') else pd.read_csv(f_ref)
            st.session_state.df_tarifas = motor_tarifas_auditoria(df_b, m)

    if st.session_state.df_tarifas is not None:
        st.subheader("📋 Auditoría Visual")
        st.dataframe(st.session_state.df_tarifas[['Id', 'Noviembre', 'Nuevo', 'Variación %', 'Regla']], use_container_width=True)
        buf = io.BytesIO()
        st.session_state.df_tarifas[['Id', 'Nuevo', 'Limite Superior']].rename(columns={'Nuevo': 'Limite Inferior'}).to_excel(buf, index=False)
        st.download_button(f"📥 Bajar Tarifas_{st.session_state.per_global}.xlsx", buf.getvalue(), f"Tarifas_{st.session_state.per_global}.xlsx")

with t2:
    if st.session_state.df_tarifas is not None:
        st.header(f"Motor DMK Pesado ({st.session_state.per_global})")
        c1, c2, c3 = st.columns(3)
        f_sube = c1.file_uploader("ZIP / CSV del DMK", type=['zip', 'csv'])
        f_nom = c2.file_uploader("Nomenclador")
        f_en = c3.file_uploader("Energías")
        
        if f_sube and f_nom and f_en and st.button("⚡ Procesar 210MB"):
            with st.spinner("Polars trabajando..."):
                st.session_state.df_dmk = motor_dmk_polars(f_sube, pd.read_excel(f_nom), pd.read_excel(f_en))
        
        if st.session_state.df_dmk is not None:
            st.success("✅ Procesado!")
            st.dataframe(st.session_state.df_dmk.head())
            buf2 = io.BytesIO()
            st.session_state.df_dmk.to_excel(buf2, index=False)
            st.download_button(f"📥 Bajar dggi_DMK_{st.session_state.per_global}.xlsx", buf2.getvalue(), f"dggi_DMK_{st.session_state.per_global}.xlsx")
    else:
        st.warning("⚠️ Primero calculá las tarifas en la Tab 1.")
        with tab_v3: # Esta es la Pestaña 2
    st.header("Generador de Nomenclador Maestro V3 📋")
    st.markdown("Subí los 3 archivos para sincronizar la base de Febrero.")

    c1, c2, c3 = st.columns(3)
    f_v2 = c1.file_uploader("Nomenclador Base v2", type=['xlsx', 'csv'])
    f_elr = c2.file_uploader("ELR Febrero", type=['xlsx', 'csv'])
    f_ts = c3.file_uploader("Nomenclador TS (Ramales)", type=['xlsx', 'csv'])

    if f_v2 and f_elr and f_ts:
        if st.button("🔄 Sincronizar Archivos"):
            # Lectura automática
            d_v2 = pd.read_excel(f_v2) if f_v2.name.endswith('.xlsx') else pd.read_csv(f_v2)
            d_elr = pd.read_excel(f_elr) if f_elr.name.endswith('.xlsx') else pd.read_csv(f_elr)
            d_ts = pd.read_excel(f_ts) if f_ts.name.endswith('.xlsx') else pd.read_csv(f_ts)
            
            with st.spinner("Cruzando datos..."):
                st.session_state.df_v3 = motor_v3_maestro(d_v2, d_elr, d_ts)
                st.success("✅ Nomenclador V3 generado. Ya podés usarlo en la Tab de DMK.")
                st.dataframe(st.session_state.df_v3.head())

                # Botón de Descarga
                buf = io.BytesIO()
                st.session_state.df_v3.to_excel(buf, index=False)
                st.download_button("📥 Descargar NOMENCLADOR_V3_FEBRERO.xlsx", buf.getvalue(), "NOMENCLADOR_V3_FEBRERO.xlsx")
