import streamlit as st
import pandas as pd
import polars as pl
import numpy as np
import io

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Fiscalización TTR Natalia v6.5", layout="wide")

# =============================================================================
# MOTORES DE LÓGICA (LAS FUNCIONES)
# =============================================================================

def motor_tarifas_auditoria(df_base, manuales):
    """Proyecta tarifas y genera tabla de auditoría (Lógica Colab)"""
    df = df_base.copy()
    df.columns = [str(c).strip() for c in df.columns]
    
    for col in ['Limite Inferior', 'Limite Superior']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    try:
        v1_ant = df.loc[df['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = manuales['1SCN'] / v1_ant
    except:
        return None

    res = []
    for _, row in df.iterrows():
        id_t = str(row['Id']).strip()
        v_min_ant, v_max_ant = row['Limite Inferior'], row['Limite Superior']
        
        # Aplicación de Reglas de Acuerdo de Cod
        if id_t in manuales:
            v_min = v_max = manuales[id_t]
            regla = "Ingreso Manual (SCN)"
        elif 'SEN' in id_t and 'SESN' not in id_t:
            base = manuales.get(id_t.replace('SEN', 'SCN'), manuales['1SCN'])
            v_min = v_max = base * 1.25
            regla = "Factor 1.25 (SEN)"
        elif 'SEAN' in id_t and 'SEASN' not in id_t:
            base = manuales.get(id_t.replace('SEAN', 'SCN'), manuales['1SCN'])
            v_min = v_max = base * 1.75
            regla = "Factor 1.75 (SEAN)"
        elif 'SCSN' in id_t:
            base = manuales.get(id_t.replace('SCSN', 'SCN'), manuales['1SCN'])
            v_min = v_max = base * 1.59
            regla = "Factor 1.59 (SCSN)"
        elif 'SESN' in id_t:
            base_scsn = manuales.get(id_t.replace('SESN', 'SCN'), manuales['1SCN']) * 1.59
            v_min = v_max = base_scsn * 1.25
            regla = "Compuesta (SCSN * 1.25)"
        elif 'SEASN' in id_t:
            base_scsn = manuales.get(id_t.replace('SEASN', 'SCN'), manuales['1SCN']) * 1.59
            v_min = v_max = base_scsn * 1.75
            regla = "Compuesta (SCSN * 1.75)"
        else:
            v_min, v_max = v_min_ant * factor, v_max_ant * factor
            regla = "Ajuste General (%)"

        variacion = ((v_min / v_min_ant) - 1) * 100 if v_min_ant > 0 else 0
        res.append({
            'Id': id_t,
            'Base Anterior': round(v_min_ant, 2),
            'Nuevo Proyectado': round(v_min, 2),
            'Variación %': round(variacion, 2),
            'Regla Aplicada': regla,
            'Limite Superior': round(v_max, 2)
        })
    return pd.DataFrame(res)

def motor_dmk_polars(f_sube, df_gt, df_en):
    """
    Procesamiento de 210MB con soporte para ZIP y CSV.
    Detecta si es un archivo comprimido y extrae el contenido para Polars.
    """
    try:
        # 1. Manejo del archivo (ZIP o CSV directo)
        if f_sube.name.endswith('.zip'):
            with zipfile.ZipFile(f_sube) as z:
                # Buscamos el primer archivo .csv dentro del ZIP
                csv_filename = [n for n in z.namelist() if n.endswith('.csv')][0]
                with z.open(csv_filename) as f:
                    contenido_csv = f.read()
        else:
            contenido_csv = f_sube.getvalue()

        # 2. Preparar Nomencladores
        gt_pl = pl.from_pandas(df_gt[['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']]).lazy()
        en_pl = pl.from_pandas(df_en[['DOMINIO', 'ENERGIA']]).lazy()

        # 3. Leer con Polars (Lazy)
        lf = pl.read_csv(contenido_csv, encoding='iso-8859-1', separator=";", infer_schema_length=10000).lazy()
        
        # Limpieza de nombres y casteo
        lf = lf.rename({c: c.strip().upper() for c in lf.columns})
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))
        gt_pl = gt_pl.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))

        # 4. Cruce de Datos y Lógica de Carriles
        lf = lf.join(gt_pl, on="ID_LINEA", how="inner").join(en_pl, on="DOMINIO", how="left")
        
        lf = lf.with_columns([
            pl.when(pl.col("ENERGIA").is_null()).then(pl.lit("NO")).otherwise(pl.col("DOMINIO")).alias("DOMINIO"),
            pl.col("ENERGIA").fill_null(3)
        ])

        # 5. Cálculos de Compensaciones (ATS e ITG)
        lf = lf.with_columns([
            (pl.col("DESCUENTO X INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO") == 621)
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA BASE ITG") - pl.col("DEBITADO") - pl.col("DESCUENTO X INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS")
        ])

        res = lf.group_by(['PROVINCIA', 'MUNICIPIO', 'GT', 'Linea SILAS DNGFF', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION']).agg([pl.col('CANTIDAD_USOS').sum(), pl.col('COMP_ITG').sum(), pl.col('COMP_ATS').sum()]).collect().to_pandas()
        
        res['COMP_ATS s/IVA'] = res['COMP_ATS'] / 1.105
        res['COMP_ITG s/IVA'] = res['COMP_ITG'] / 1.105
        return res

    except Exception as e:
        st.error(f"Error al procesar el archivo comprimido: {e}")
        return None
# =============================================================================
# INTERFAZ DE USUARIO (TABS)
# =============================================================================

st.title("Sistema Fiscalización TTR Natalia v6.5 🚀")

# MEMORIA DE LA APP
if 'df_tarifas' not in st.session_state: st.session_state.df_tarifas = None
if 'df_dmk' not in st.session_state: st.session_state.df_dmk = None
if 'periodo_actual' not in st.session_state: st.session_state.periodo_actual = "Febrero"

tab1, tab2 = st.tabs(["💰 1. TARIFAS", "📂 2. PREPROCESO DMK"])

with tab1:
    f_ref = st.file_uploader("Subir Referencia (Noviembre)", type=['xlsx', 'csv'])
    if f_ref:
        st.session_state.periodo_actual = st.text_input("Mes de liquidación actual:", value=st.session_state.periodo_actual)
        hoja_ref = st.text_input("Nombre de la hoja en el Excel:", value="JN11")
        
        st.subheader(f"Configuración SCN {st.session_state.periodo_actual}")
        c = st.columns(5)
        m = {
            '1SCN': c[0].number_input("1SCN", value=494.33),
            '2SCN': c[1].number_input("2SCN", value=551.24),
            '3SCN': c[2].number_input("3SCN", value=593.70),
            '4SCN': c[3].number_input("4SCN", value=636.21),
            '5SCN': c[4].number_input("5SCN", value=678.42)
        }
        
        if st.button("🔄 Generar Auditoría y Descarga"):
            with st.spinner("Proyectando tarifas..."):
                df_base = pd.read_excel(f_ref, sheet_name=hoja_ref) if f_ref.name.endswith('.xlsx') else pd.read_csv(f_ref)
                st.session_state.df_tarifas = motor_tarifas_auditoria(df_base, m)

    # ESTO HACE QUE LA TABLA NO DESAPAREZCA
    if st.session_state.df_tarifas is not None:
        st.divider()
        st.subheader(f"📋 Auditoría de Tarifas: {st.session_state.periodo_actual} 2026")
        st.dataframe(st.session_state.df_tarifas[['Id', 'Base Anterior', 'Nuevo Proyectado', 'Variación %', 'Regla Aplicada']], use_container_width=True)
        
        # Botón de Descarga 1
        buf = io.BytesIO()
        df_out = st.session_state.df_tarifas[['Id', 'Nuevo Proyectado', 'Limite Superior']].rename(columns={'Nuevo Proyectado': 'Limite Inferior'})
        df_out.to_excel(buf, index=False)
        st.download_button(
            label=f"📥 Descargar Tarifas_Calculadas_{st.session_state.periodo_actual}_2026.xlsx",
            data=buf.getvalue(),
            file_name=f"Tarifas_Calculadas_{st.session_state.periodo_actual}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

with tab2:
    if st.session_state.df_tarifas is not None:
        st.header(f"Procesamiento DMK: {st.session_state.periodo_actual} 2026")
        col1, col2, col3 = st.columns(3)
        # Ahora aceptamos tanto .csv como .zip
        f_sube = col1.file_uploader("Archivo DMK (CSV o ZIP)", type=['csv', 'zip'])
        f_nom = col2.file_uploader("Nomenclador (Excel)")
        f_en = col3.file_uploader("Energías (Excel)")
        
        if f_sube and f_nom and f_en and st.button("⚡ Iniciar Motor Polars"):
            with st.spinner("Calculando compensaciones..."):
                st.session_state.df_dmk = motor_dmk_polars(f_sube, pd.read_excel(f_nom), pd.read_excel(f_en))
                st.success("✅ DMK procesado correctamente.")

        if st.session_state.df_dmk is not None:
            st.dataframe(st.session_state.df_dmk.head())
            # Botón de Descarga 2
            buf2 = io.BytesIO()
            st.session_state.df_dmk.to_excel(buf2, index=False)
            st.download_button(
                label=f"📥 Descargar dggi_DMK_PME_{st.session_state.periodo_actual}_2026.xlsx",
                data=buf2.getvalue(),
                file_name=f"dggi_DMK_{st.session_state.periodo_actual}_2026.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    else:
        st.warning("⚠️ Primero calculá las tarifas en la pestaña 1.")
