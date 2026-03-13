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
