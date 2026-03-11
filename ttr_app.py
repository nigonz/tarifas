import pandas as pd
import numpy as np
import streamlit as st
import io
from datetime import datetime

# =============================================================================
# 1. MOTOR DE CÁLCULOS (LOGICA DE NEGOCIO)
# =============================================================================

def proyectar_tarifas_febrero(df_nov, nuevas_scn):
    """Calcula la escala tarifaria proyectada."""
    try:
        v1_nov = df_nov.loc[df_nov['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = nuevas_scn['1SCN'] / v1_nov
    except:
        return df_nov, 1.0

    df = df_nov.copy()
    for i, row in df.iterrows():
        id_t = str(row['Id'])
        if id_t in nuevas_scn: v_final = nuevas_scn[id_t]
        elif 'SEN' in id_t and 'SESN' not in id_t: v_final = nuevas_scn.get(id_t.replace('SEN', 'SCN'), nuevas_scn['1SCN']) * 1.25
        elif 'SCSN' in id_t: v_final = nuevas_scn.get(id_t.replace('SCSN', 'SCN'), nuevas_scn['1SCN']) * 1.59
        elif 'SEAN' in id_t and 'SEASN' not in id_t: v_final = nuevas_scn.get(id_t.replace('SEAN', 'SCN'), nuevas_scn['1SCN']) * 1.75
        elif 'SESN' in id_t: v_final = (nuevas_scn.get(id_t.replace('SESN', 'SCN'), nuevas_scn['1SCN']) * 1.59) * 1.25
        elif 'SEASN' in id_t: v_final = (nuevas_scn.get(id_t.replace('SEASN', 'SCN'), nuevas_scn['1SCN']) * 1.59) * 1.75
        else: v_final = row['Limite Inferior'] * factor
        df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(v_final, 2)
    return df, factor

def preproceso_dmk_energias(f_csv, nom_gt, df_pme):
    """Lógica PME: Cruza SUBE con Nomenclador y separa por Energías."""
    # 1. Carga robusta (detecta si es coma o punto y coma automáticamente)
    df = pd.read_csv(f_csv, encoding='ISO-8859-1', sep=None, engine='python')
    
    # Normalización de columnas para evitar errores de espacios o mayúsculas
    df.columns = df.columns.str.strip()
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.strip()
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.strip()
    
    # 2. Filtrado por Nomenclador (Solo líneas válidas)
    df_filtered = df[df['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()
    
    if df_filtered.empty:
        st.error("⚠️ Error: No hubo coincidencias entre las líneas del CSV y el Nomenclador. Revisa la columna ID_LINEA.")
        return pd.DataFrame()

    # 3. Merge con datos del Nomenclador (GT, Provincia, etc.)
    _df2_ = pd.merge(df_filtered, nom_gt[['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']], how='left', on='ID_LINEA')
    _df2_.loc[_df2_['GT'] == 'DF', 'PROVINCIA'] = 'CABA'

    # 4. Clasificación por Dominios (PME vs Resto)
    dominios_pme = df_pme['DOMINIO'].unique()
    
    # Casos PME (GNC/Elect) - Se mantienen dominios individuales
    df_pm = _df2_[_df2_['DOMINIO'].isin(dominios_pme)].copy()
    df_pm = df_pm.merge(df_pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
    
    # Casos Resto (Incluye dominios negativos y gasoil) - Se agrupan bajo "NO"
    df_resto = _df2_[~_df2_['DOMINIO'].isin(dominios_pme)].copy()
    
    # 5. Agrupaciones para reducir tamaño (Aplastado)
    group_cols = ['PROVINCIA', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'Linea SILAS DNGFF', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'VIAJE INTEGRADO', 'DESCUENTO X INTEGRACION']
    
    # Agrupar PM (mantiene su dominio real)
    res_pm = df_pm.groupby(group_cols + ['ENERGIA'], as_index=False, dropna=False).agg({'CANTIDAD_USOS':'sum', 'MONTO':'sum'})
    
    # Agrupar Resto (aplasta todo a DOMINIO "NO" y ENERGIA 3)
    group_cols_resto = [c for c in group_cols if c != 'DOMINIO']
    res_resto = df_resto.groupby(group_cols_resto, as_index=False, dropna=False).agg({'CANTIDAD_USOS':'sum', 'MONTO':'sum'})
    res_resto['DOMINIO'] = 'NO'
    res_resto['ENERGIA'] = 3
    
    # 6. Unión y Cálculos Finales
    final = pd.concat([res_pm, res_resto], ignore_index=True)
    
    for col in ['TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS']:
        final[col] = pd.to_numeric(final[col], errors='coerce').fillna(0)
    
    final['COMP. ITG'] = final['DESCUENTO X INTEGRACION'] * final['CANTIDAD_USOS']
    final['COMP. ATS'] = final.apply(lambda x: (
        ((x['DEBITADO'] / 0.45 * 0.55) * x['CANTIDAD_USOS'] if x['GT'] == 'INP' 
         else (x['TARIFA BASE ITG'] - x['DEBITADO'] - x['DESCUENTO X INTEGRACION']) * x['CANTIDAD_USOS'])
    ) if x['CONTRATO'] == 621 else 0, axis=1)
    
    final['COMP. ATS s/IVA'] = final['COMP. ATS'] / 1.105
    final['COMP. ITG s/IVA'] = final['COMP. ITG'] / 1.105
    
    return final

# =============================================================================
# 2. INTERFAZ STREAMLIT
# =============================================================================

st.set_page_config(page_title="Fiscalización TTR v2.0", layout="wide")
st.title("Sistema de Liquidación de Compensaciones")

if 'df_tarifas' not in st.session_state: st.session_state['df_tarifas'] = None
if 'df_pme' not in st.session_state: st.session_state['df_pme'] = None

tab1, tab2, tab3 = st.tabs(["💰 TARIFAS", "📂 PRE-PROCESO PME", "🚀 DETERMINACIÓN TTR"])

with tab1:
    st.header("1. Generador de Escala Tarifaria")
    f_nov = st.file_uploader("Subir Base Noviembre (Excel)", type=['xlsx'])
    if f_nov:
        c = st.columns(5)
        n1 = c[0].number_input("1SCN", value=650.0); n2 = c[1].number_input("2SCN", value=724.09)
        n3 = c[2].number_input("3SCN", value=798.17); n4 = c[3].number_input("4SCN", value=872.24)
        n5 = c[4].number_input("5SCN", value=946.33)
        if st.button("🔄 Calcular Febrero"):
            df_n = pd.read_excel(f_nov, sheet_name='JN11')
            res, fac = proyectar_tarifas_febrero(df_n, {'1SCN':n1, '2SCN':n2, '3SCN':n3, '4SCN':n4, '5SCN':n5})
            st.session_state['df_tarifas'] = res
            st.success(f"Diccionario calculado. Factor: {fac:.4f}")
    
    if st.session_state['df_tarifas'] is not None:
        buf = io.BytesIO()
        st.session_state['df_tarifas'].to_excel(buf, index=False)
        st.download_button("📥 Descargar Tarifas", buf.getvalue(), "Diccionario_Feb.xlsx")

with tab2:
    st.header("2. Pre-proceso DMK Energías")
    col1, col2, col3 = st.columns(3)
    f_sube = col1.file_uploader("Archivo DGGI (CSV)", type=['csv'])
    f_nom = col2.file_uploader("Nomenclador GT", type=['xlsx'])
    f_ener = col3.file_uploader("Parque Móvil Energías", type=['xlsx'])
    
    if f_sube and f_nom and f_ener:
        if st.button("🚀 Procesar Base PME"):
            with st.spinner("Procesando datos..."):
                nom_gt = pd.read_excel(f_nom)
                df_en_data = pd.read_excel(f_ener)
                res = preproceso_dmk_energias(f_sube, nom_gt, df_en_data)
                if not res.empty:
                    st.session_state['df_pme'] = res
                    st.success(f"¡Listo! Se generaron {len(res):,} registros.")
                    st.dataframe(res.head())

    if st.session_state['df_pme'] is not None:
        buf2 = io.BytesIO()
        st.session_state['df_pme'].to_excel(buf2, index=False)
        st.download_button("📥 Descargar dggi_DMK_PME_Final", buf2.getvalue(), "dggi_DMK_PME_Final.xlsx")

with tab3:
    st.header("3. Determinación TTR")
    st.info("Pestaña en desarrollo: Aquí se cruzará la base PME con el Diccionario de Tarifas.")
