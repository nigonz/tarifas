import streamlit as st
import pandas as pd
import io

# =============================================================================
# MOTOR DE CÁLCULO Y AUDITORÍA
# =============================================================================

def motor_tarifas_con_auditoria(df_prev, dic_manuales):
    """
    Proyecta tarifas y genera una tabla comparativa para validación inmediata.
    """
    df_prev.columns = [str(c).strip() for c in df_prev.columns]
    
    for col in ['Limite Inferior', 'Limite Superior']:
        if col in df_prev.columns:
            df_prev[col] = pd.to_numeric(df_prev[col].astype(str).str.replace(',', '.'), errors='coerce')

    try:
        v1_anterior = df_prev.loc[df_prev['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor_ajuste = dic_manuales['1SCN'] / v1_anterior
    except (IndexError, KeyError):
        st.error("No se encontró la tarifa '1SCN' en la columna 'Id'.")
        return None

    resultados = []
    
    for _, row in df_prev.iterrows():
        id_t = str(row['Id']).strip()
        v_min_ant = row['Limite Inferior']
        v_max_ant = row['Limite Superior']
        regla = ""

        # APLICACIÓN DE REGLAS SEGÚN ACUERDO
        if id_t in dic_manuales:
            v_min = v_max = dic_manuales[id_t]
            regla = "Ingreso Manual (SCN)"
        elif 'SEN' in id_t and 'SESN' not in id_t:
            base = dic_manuales.get(id_t.replace('SEN', 'SCN'), dic_manuales['1SCN'])
            v_min = v_max = base * 1.25
            regla = "Factor 1.25 (SEN)"
        elif 'SEAN' in id_t and 'SEASN' not in id_t:
            base = dic_manuales.get(id_t.replace('SEAN', 'SCN'), dic_manuales['1SCN'])
            v_min = v_max = base * 1.75
            regla = "Factor 1.75 (SEAN)"
        elif 'SCSN' in id_t:
            base = dic_manuales.get(id_t.replace('SCSN', 'SCN'), dic_manuales['1SCN'])
            v_min = v_max = base * 1.59
            regla = "Factor 1.59 (SCSN)"
        elif 'SESN' in id_t:
            base_scsn = dic_manuales.get(id_t.replace('SESN', 'SCN'), dic_manuales['1SCN']) * 1.59
            v_min = v_max = base_scsn * 1.25
            regla = "Compuesta (SCSN * 1.25)"
        elif 'SEASN' in id_t:
            base_scsn = dic_manuales.get(id_t.replace('SEASN', 'SCN'), dic_manuales['1SCN']) * 1.59
            v_min = v_max = base_scsn * 1.75
            regla = "Compuesta (SCSN * 1.75)"
        else:
            v_min, v_max = v_min_ant * factor_ajuste, v_max_ant * factor_ajuste
            regla = f"Ajuste General (%)"

        variacion = ((v_min / v_min_ant) - 1) * 100 if v_min_ant > 0 else 0

        resultados.append({
            'Id': id_t,
            'Noviembre (Base)': round(v_min_ant, 2),
            'Enero 2026 (Nuevo)': round(v_min, 2),
            'Variación %': round(variacion, 2),
            'Regla Aplicada': regla,
            'Limite Superior': round(v_max, 2)
        })

    return pd.DataFrame(resultados)

def motor_dmk_pesado(f_sube, df_gt, df_en):
    """
    Procesa el DMK de 210MB con la lógica de Energías y Compensaciones.
    Aplica el 'Doble Carril': Si el dominio no está en Energías, es 'NO' y Diesel.
    """
    try:
        # 1. Preparar Nomencladores
        gt_pl = pl.from_pandas(df_gt[['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']]).lazy()
        en_pl = pl.from_pandas(df_en[['DOMINIO', 'ENERGIA']]).lazy()

        # 2. Leer SUBE (Lazy)
        lf = pl.read_csv(f_sube.getvalue(), encoding='iso-8859-1', separator=";", infer_schema_length=10000).lazy()
        
        # Limpieza de nombres y casteo
        lf = lf.rename({c: c.strip().upper() for c in lf.columns})
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))
        gt_pl = gt_pl.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))

        # 3. CRUCE DE DATOS (Doble Carril Inteligente)
        lf = lf.join(gt_pl, on="ID_LINEA", how="inner")
        
        # Unimos con Energías (Left Join)
        lf = lf.join(en_pl, on="DOMINIO", how="left")

        # Lógica del 'Resto': Si no hay energía, Dominio = "NO" y Energía = 3 (Diesel)
        lf = lf.with_columns([
            pl.when(pl.col("ENERGIA").is_null())
              .then(pl.lit("NO"))
              .otherwise(pl.col("DOMINIO"))
              .alias("DOMINIO"),
            pl.col("ENERGIA").fill_null(3)
        ])

        # 4. CÁLCULO DE COMPENSACIONES (Acuerdo de Cod)
        lf = lf.with_columns([
            (pl.col("DESCUENTO X INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO") == 621)
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45) * 0.55 * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA BASE ITG") - pl.col("DEBITADO") - pl.col("DESCUENTO X INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS")
        ])

        # 5. AGRUPAMIENTO FINAL DETALLADO (Mantiene Dominio y Energía)
        res_detallado = lf.group_by([
            'PROVINCIA', 'MUNICIPIO', 'GT', 'Linea SILAS DNGFF', 'ID_LINEA', 'RAMAL', 
            'DOMINIO', 'ENERGIA', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION'
        ]).agg([
            pl.col('CANTIDAD_USOS').sum(),
            pl.col('COMP_ITG').sum(),
            pl.col('COMP_ATS').sum()
        ]).collect().to_pandas()

        # 6. IVA Y FINALIZACIÓN
        res_detallado['COMP_ATS s/IVA'] = res_detallado['COMP_ATS'] / 1.105
        res_detallado['COMP_ITG s/IVA'] = res_detallado['COMP_ITG'] / 1.105
        
        return res_detallado

    except Exception as e:
        st.error(f"Error en Módulo DMK: {e}")
        return None

# =============================================================================
# INTERFAZ DE USUARIO
# =============================================================================

st.title("Fiscalización TTR Natalia v6.0 🚀")

if 'df_tarifas_2026' not in st.session_state: st.session_state.df_tarifas_2026 = None
if 'df_dmk_detallado' not in st.session_state: st.session_state.df_dmk_detallado = None

tab1, tab2, tab3 = st.tabs(["💰 1. TARIFAS", "📂 2. PREPROCESO DMK", "🚀 3. TTR FINAL"])

# --- TAB 1: TARIFAS (Con corrección de periodo) ---
with tab1:
    f_ref = st.file_uploader("Subir Referencia Noviembre", type=['xlsx', 'csv'])
    if f_ref:
        periodo_actual = st.text_input("Periodo de liquidación:", value="Febrero")
        # (Aquí iría el código de cálculo de tarifas que ya validamos...)
        st.info(f"Calculando tarifas para el periodo: {periodo_actual} 2026")

# --- TAB 2: PREPROCESO DMK (Blindado) ---
with tab2:
    if st.session_state.df_tarifas_2026 is not None:
        st.header("Módulo de Procesamiento DMK (210MB)")
        periodo = st.text_input("Confirmar Periodo para el archivo:", value="Febrero")
        
        c1, c2, c3 = st.columns(3)
        f_sube = c1.file_uploader("Subir DGGI_DMK (CSV)", type=['csv'])
        f_nom = c2.file_uploader("Subir Nomenclador (Excel)", type=['xlsx'])
        f_en = c3.file_uploader("Subir Energías (Excel)", type=['xlsx'])

        if f_sube and f_nom and f_en:
            if st.button("⚡ Iniciar Motor Polars"):
                with st.spinner("Procesando ATS e ITG..."):
                    df_gt = pd.read_excel(f_nom)
                    df_en = pd.read_excel(f_en)
                    
                    resultado = motor_dmk_pesado(f_sube, df_gt, df_en)
                    
                    if resultado is not None:
                        st.session_state.df_dmk_detallado = resultado
                        st.success(f"Archivo de {periodo} procesado con éxito.")
                        st.dataframe(resultado.head())
                        
                        # BOTÓN DE DESCARGA DETALLADO
                        buf = io.BytesIO()
                        with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
                            resultado.to_excel(writer, index=False, sheet_name=f'DMK_{periodo}')
                        
                        st.download_button(
                            label=f"📥 Descargar dggi_DMK_PME_{periodo}_2026.xlsx",
                            data=buf.getvalue(),
                            file_name=f"dggi_DMK_PME_{periodo}_2026.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
    else:
        st.warning("⚠️ Primero completá el Módulo de Tarifas en la Tab 1.")
