import pandas as pd
import numpy as np
import streamlit as st
import io
from datetime import datetime

# =============================================================================
# 1. FUNCIONES CORE: MOTOR DE CÁLCULOS
# =============================================================================

def proyectar_tarifas_febrero(df_nov, nuevas_scn):
    """Lógica: Aplica factor de aumento basado en 1SCN y reglas de nodos."""
    try:
        v1_nov = df_nov.loc[df_nov['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = nuevas_scn['1SCN'] / v1_nov
    except IndexError:
        st.error("No se encontró el nodo '1SCN' en la base de Noviembre.")
        return df_nov, 1.0

    df = df_nov.copy()
    for i, row in df.iterrows():
        id_t = str(row['Id'])
        if id_t in nuevas_scn: 
            v_final = nuevas_scn[id_t]
        elif 'SEN' in id_t and 'SESN' not in id_t: 
            v_final = nuevas_scn.get(id_t.replace('SEN', 'SCN'), nuevas_scn['1SCN']) * 1.25
        elif 'SCSN' in id_t: 
            v_final = nuevas_scn.get(id_t.replace('SCSN', 'SCN'), nuevas_scn['1SCN']) * 1.59
        elif 'SEAN' in id_t and 'SEASN' not in id_t: 
            v_final = nuevas_scn.get(id_t.replace('SEAN', 'SCN'), nuevas_scn['1SCN']) * 1.75
        elif 'SESN' in id_t: 
            v_final = (nuevas_scn.get(id_t.replace('SESN', 'SCN'), nuevas_scn['1SCN']) * 1.59) * 1.25
        elif 'SEASN' in id_t: 
            v_final = (nuevas_scn.get(id_t.replace('SEASN', 'SCN'), nuevas_scn['1SCN']) * 1.59) * 1.75
        else: 
            v_final = row['Limite Inferior'] * factor
        
        df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(v_final, 2)
    return df, factor

def preproceso_pme_completo(f_csv, nom_gt, df_pme):
    """Lógica Original: Filtra por nomenclador, separa energías y calcula ATS/ITG."""
    
    # 1. Carga con auto-detección de separador (, o ;)
    df = pd.read_csv(f_csv, encoding='ISO-8859-1', sep=None, engine='python')
    
    # Limpieza de columnas para asegurar match
    df.columns = df.columns.str.strip()
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.strip()
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.strip()
    
    # 2. Filtrado por Nomenclador
    df_filtered = df[df['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()
    
    # Columnas necesarias del crudo
    cols_sube = ['ID_EMPRESA', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'MK', 'TARIFA BASE ITG', 
                 'DEBITADO', 'CONTRATO', 'VIAJE INTEGRADO', 'DESCUENTO X INTEGRACION', 
                 'CANTIDAD_USOS', 'MONTO']
    df_filtered = df_filtered[cols_sube]
    
    # 3. Merge con Nomenclador
    columns_nom = ['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']
    _df2_ = pd.merge(df_filtered, nom_gt[columns_nom], how='left', on='ID_LINEA')
    
    # 4. Separación por Energías (PME)
    dominios_pme = df_pme['DOMINIO'].unique()
    
    # Casos con dominios específicos (PME)
    df_pm = _df2_[_df2_['DOMINIO'].isin(dominios_pme)].copy()
    df_pm = df_pm.merge(df_pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
    
    # Casos resto (Se aplastan a DOMINIO "NO")
    df_resto = _df2_[~_df2_['DOMINIO'].isin(dominios_pme)].copy()
    df_resto['DOMINIO'] = 'NO'
    df_resto['ENERGIA'] = 3 # Valor según tu script original
    
    # 5. Agrupaciones (Aplastado de dominios negativos y resto)
    group_cols = ['PROVINCIA', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'Linea SILAS DNGFF', 
                  'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 
                  'TARIFA BASE ITG', 'DEBITADO', 'VIAJE INTEGRADO', 'DESCUENTO X INTEGRACION']
    
    df_pm_grouped = df_pm.groupby(group_cols, as_index=False).agg({'CANTIDAD_USOS': 'sum', 'MONTO': 'sum'})
    df_resto_grouped = df_resto.groupby(group_cols, as_index=False).agg({'CANTIDAD_USOS': 'sum', 'MONTO': 'sum'})
    
    # Unir ambas bases
    final = pd.concat([df_pm_grouped, df_resto_grouped], ignore_index=True)
    
    # 6. Cálculos de Liquidación
    for col in ['TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS', 'CONTRATO']:
        final[col] = pd.to_numeric(final[col], errors='coerce')
    
    final['COMP. ITG'] = final['DESCUENTO X INTEGRACION'] * final['CANTIDAD_USOS']
    
    # Fórmula ATS según GT y Contrato
    final['COMP. ATS'] = final.apply(
        lambda x: (
            ((x['DEBITADO'] / 0.45) * 0.55 * x['CANTIDAD_USOS']) if x['GT'] == 'INP' 
            else (x['TARIFA BASE ITG'] - x['DEBITADO'] - x['DESCUENTO X INTEGRACION']) * x['CANTIDAD_USOS']
        ) if x['CONTRATO'] == 621 else 0, 
        axis=1
    )
    
    final['COMP. ATS s/IVA'] = final['COMP. ATS'] / 1.105
    final['COMP. ITG s/IVA'] = final['COMP. ITG'] / 1.105
    
    return final

# =============================================================================
# 2. INTERFAZ STREAMLIT
# =============================================================================

st.set_page_config(page_title="Fiscalización TTR v2.0", layout="wide")
st.title("Sistema de Liquidación de Compensaciones (TTR)")

# Inicializar estados
if 'df_tarifas' not in st.session_state: st.session_state['df_tarifas'] = None
if 'df_pme' not in st.session_state: st.session_state['df_pme'] = None

tab1, tab2, tab3 = st.tabs(["💰 TARIFAS", "📂 PRE-PROCESO PME", "🚀 DETERMINACIÓN TTR"])

# --- TAB 1: TARIFAS ---
with tab1:
    st.header("1. Generador de Escala Tarifaria")
    f_nov = st.file_uploader("Base Noviembre (Excel)", type=['xlsx'])
    
    if f_nov:
        st.subheader("Nuevos Valores SCN (Febrero)")
        c = st.columns(5)
        n1 = c[0].number_input("1SCN", value=650.0)
        n2 = c[1].number_input("2SCN", value=724.09)
        n3 = c[2].number_input("3SCN", value=798.17)
        n4 = c[3].number_input("4SCN", value=872.24)
        n5 = c[4].number_input("5SCN", value=946.33)
        
        if st.button("🔄 Calcular Diccionario Febrero"):
            df_n = pd.read_excel(f_nov, sheet_name='JN11')
            scn_dict = {'1SCN':n1, '2SCN':n2, '3SCN':n3, '4SCN':n4, '5SCN':n5}
            res, fac = proyectar_tarifas_febrero(df_n, scn_dict)
            st.session_state['df_tarifas'] = res
            st.success(f"Diccionario generado. Aumento proyectado: {((fac-1)*100):.2f}%")

    if st.session_state['df_tarifas'] is not None:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='xlsxwriter') as wr:
            st.session_state['df_tarifas'].to_excel(wr, index=False)
        st.download_button("📥 Descargar Diccionario Febrero", data=buf.getvalue(), file_name="Diccionario_Feb_Control.xlsx")

# --- TAB 2: PRE-PROCESO ---
with tab2:
    st.header("2. Pre-proceso DGGI con Energías (DMK)")
    c1, c2, c3 = st.columns(3)
    f_csv = c1.file_uploader("Archivo DGGI (CSV/ZIP)", type=['csv', 'zip'])
    f_gt = c2.file_uploader("Nomenclador GT (Excel)", type=['xlsx'])
    f_en = c3.file_uploader("Parque Móvil Energías (Excel)", type=['xlsx'])
    
    if f_csv and f_gt and f_en:
        if st.button("🚀 Iniciar Motor de Pre-proceso"):
            with st.spinner("Procesando... esto puede demorar por el tamaño del CSV"):
                nom_gt = pd.read_excel(f_gt)
                df_en_data = pd.read_excel(f_en)
                
                res_pme = preproceso_pme_completo(f_csv, nom_gt, df_en_data)
                st.session_state['df_pme'] = res_pme
                st.success(f"Proceso completado: {len(res_pme):,} filas generadas.")

    if st.session_state['df_pme'] is not None:
        st.dataframe(st.session_state['df_pme'].head(10))
        buf2 = io.BytesIO()
        with pd.ExcelWriter(buf2, engine='xlsxwriter') as wr:
            st.session_state['df_pme'].to_excel(wr, index=False)
        st.download_button("📥 Descargar base_dggi_DMK_PME", data=buf2.getvalue(), file_name="dggi_DMK_PME_Final.xlsx")

# --- TAB 3: TTR ---
with tab3:
    st.header("3. Consolidación Final TTR")
    if st.session_state['df_pme'] is None or st.session_state['df_tarifas'] is None:
        st.warning("Debes completar las pestañas 1 y 2 antes de proceder.")
    else:
        st.info("Aquí se realizará el macheo final de la base PME con el Diccionario de Tarifas.")
        if st.button("🚀 Calcular Liquidación Final"):
            # Aquí irá la lógica de macheo final
            st.write("Calculando...")
            st.balloons()
