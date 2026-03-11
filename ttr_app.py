import pandas as pd
import numpy as np
import streamlit as st
import xlsxwriter
import io
from datetime import datetime

# =============================================================================
# 1. FUNCIONES CORE: MOTOR DE CÁLCULOS
# =============================================================================

def proyectar_tarifas_febrero(df_nov, nuevas_scn):
    """Lógica de Colab: Aplica Acordada 1 y factor de aumento (31.36%)"""
    v1_nov = df_nov.loc[df_nov['Id'] == '1SCN', 'Limite Inferior'].values[0]
    factor = nuevas_scn['1SCN'] / v1_nov
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

def preproceso_pme_jn(f_csv, nom_gt, df_pme):
    """Lógica PME: Separa JN con dominios/energías y DF/PBA operativo"""
    df = pd.read_csv(f_csv, encoding='ISO-8859-1', delimiter=';', low_memory=False)
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.strip()
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.strip()
    
    df_ = df[df['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()
    _df2_ = pd.merge(df_, nom_gt[['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']], how='left', on='ID_LINEA')
    
    # Separamos Jurisdicción Nacional (JN) para abrir por dominios
    df_jn = _df2_[_df2_['GT'].isin(['SGI', 'SGII', 'SGIKM'])].copy()
    dominios_pme = df_pme['DOMINIO'].unique()
    
    # 1. JN Especiales (GNC/Elect)
    df_jn_pme = df_jn[df_jn['DOMINIO'].isin(dominios_pme)].merge(df_pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
    
    # 2. JN Resto (Gasoil) + DF/PBA
    df_resto = _df2_[~_df2_['DOMINIO'].isin(dominios_pme)].copy()
    df_resto['DOMINIO'] = 'NO'; df_resto['ENERGIA'] = 1 # Gasoil
    
    # Agrupaciones
    group_pme = ['ID_EMPRESA','ID_LINEA','RAMAL','DOMINIO','ENERGIA','CONTRATO','TARIFA BASE ITG','DEBITADO','DESCUENTO X INTEGRACION','GT','Linea SILAS DNGFF','PROVINCIA','MUNICIPIO']
    group_op = [c for c in group_pme if c not in ['DOMINIO', 'ENERGIA']]
    
    res_pme = df_jn_pme.groupby(group_pme, as_index=False).agg({'CANTIDAD_USOS':'sum', 'MONTO':'sum'})
    res_op = df_resto.groupby(group_op, as_index=False).agg({'CANTIDAD_USOS':'sum', 'MONTO':'sum'})
    res_op['DOMINIO'] = 'NO'; res_op['ENERGIA'] = 1
    
    final = pd.concat([res_pme, res_op], ignore_index=True)
    # Cálculos ATS/ITG (Fórmulas originales del usuario)
    for col in ['TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS']:
        final[col] = pd.to_numeric(final[col], errors='coerce')
    final['COMP. ITG'] = final['DESCUENTO X INTEGRACION'] * final['CANTIDAD_USOS']
    final['COMP. ATS'] = final.apply(lambda x: ((x['DEBITADO'] / 0.45 * 0.55) * x['CANTIDAD_USOS'] if x['GT'] == 'INP' else (x['TARIFA BASE ITG'] - x['DEBITADO'] - x['DESCUENTO X INTEGRACION']) * x['CANTIDAD_USOS']) if x['CONTRATO'] == 621 else 0, axis=1)
    final['COMP. ATS s/IVA'] = final['COMP. ATS'] / 1.105
    final['COMP. ITG s/IVA'] = final['COMP. ITG'] / 1.105
    return final

# =============================================================================
# 2. INTERFAZ Y ESTADOS
# =============================================================================

st.set_page_config(page_title="Fiscalización TTR v2.0", layout="wide")
st.title("Compensaciones")

# Inicializar estados para que los botones de descarga no desaparezcan
if 'df_tarifas' not in st.session_state: st.session_state['df_tarifas'] = None
if 'df_pme' not in st.session_state: st.session_state['df_pme'] = None
if 'ttr_final' not in st.session_state: st.session_state['ttr_final'] = None

tab1, tab2, tab3 = st.tabs(["💰 TARIFAS", "📂 PRE-PROCESO PME", "🚀 DETERMINACIÓN TTR"])

# --- TAB 1: TARIFAS ---
with tab1:
    st.header("1. Generador de Escala Tarifaria")
    f_nov = st.file_uploader("Base Noviembre (Excel con JN11)", type=['xlsx'])
    c = st.columns(5)
    n1 = c[0].number_input("1SCN", value=650.0); n2 = c[1].number_input("2SCN", value=724.09)
    # (Agregá n3, n4, n5 igual)
    if f_nov and st.button("🔄 Calcular Febrero"):
        df_n = pd.read_excel(f_nov, sheet_name='JN11')
        res, fac = proyectar_tarifas_febrero(df_n, {'1SCN':n1, '2SCN':n2})
        st.session_state['df_tarifas'] = res
        st.success(f"Cálculo listo (Aumento: {((fac-1)*100):.2f}%)")
    
    if st.session_state['df_tarifas'] is not None:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='xlsxwriter') as wr: st.session_state['df_tarifas'].to_excel(wr, index=False)
        st.download_button("📥 Descargar Diccionario Febrero", data=buf.getvalue(), file_name="Diccionario_Feb_Control.xlsx")

# --- TAB 2: PRE-PROCESO ---
with tab2:
    st.header("2. Pre-proceso DGGI con Energías JN")
    c1, c2, c3 = st.columns(3)
    f_csv = c1.file_uploader("Crudo SUBE (ZIP)", type=['zip','csv'])
    f_gt = c2.file_uploader("Nomenclador GT", type=['xlsx'])
    f_en = c3.file_uploader("Base Energías Renovables", type=['xlsx'])
    
    if f_csv and f_gt and f_en and st.button("🚀 Iniciar Motor PME"):
        nom_gt = pd.read_excel(f_gt)
        df_en_data = pd.read_excel(f_en)
        res_pme = preproceso_dggi_completo(f_csv, nom_gt, df_en_data)
        st.session_state['df_pme'] = res_pme
        st.success(f"Base PME generada: {len(res_pme):,} filas.")

    if st.session_state['df_pme'] is not None:
        buf2 = io.BytesIO()
        with pd.ExcelWriter(buf2, engine='xlsxwriter') as wr: st.session_state['df_pme'].to_excel(wr, index=False)
        st.download_button("📥 Descargar base_dggi_DMK_PME", data=buf2.getvalue(), file_name="dggi_PME_Control.xlsx")

# --- TAB 3: DETERMINACIÓN TTR ---
with tab3:
    st.header("3. Cálculo de TTR (Jurisdicciones)")
    st.info("Configurá las resoluciones y el switch de fecha para JN.")
    
    col_r, col_f = st.columns(2)
    res_otros = col_r.text_input("Resolución DF/PBA:", value="6")
    partido = col_f.checkbox("¿Mes con aumento a mitad de periodo? (Ej. Febrero)")
    
    if partido:
        f_switch = st.date_input("Fecha de cambio de tarifa:", value=datetime(2026, 2, 19))
    
    if st.button("🚀 Procesar TTR y Consolidar"):
        if st.session_state['df_pme'] is None:
            st.error("Primero procesá la Tab 2.")
        else:
            with st.spinner("Macheando tarifas y aplastando dominios..."):
                # Aquí corre la lógica tool_procesar_jn/df/pba que unificamos
                # Si partido=True, divide la base PME en dos bolsas antes de machear
                # Al final agrupa por CONCAT_MACHEO3
                st.session_state['ttr_final'] = st.session_state['df_pme'] # Simulamos el resultado
                st.balloons()
    
    if st.session_state['ttr_final'] is not None:
        st.subheader("📥 Reportes Finales")
        d1, d2 = st.columns(2)
        d1.download_button("📊 Reporte Ejecutivo (Aplastado)", data=b"", file_name="TTR_Ejecutivo_Liquidacion.xlsx")
        d2.download_button("📈 Base para Power BI", data=b"", file_name="PowerBI_Usos.xlsx")
