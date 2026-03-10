mport pandas as pd
import numpy as np
import streamlit as st
import xlsxwriter
import io
from datetime import datetime

# =============================================================================
# 1. SOLAPA 1: CÁLCULO DE TARIFAS (ACORDADA 1)
# =============================================================================

def generar_diccionario_tarifas(f_nov, nuevas_scn):
    """Genera la nueva escala tarifaria aplicando los multiplicadores de la Acordada 1"""
    df = pd.read_excel(f_nov, sheet_name='JN11')
    v1_nov = df.loc[df['Id'] == '1SCN', 'Limite Inferior'].values[0]
    factor = nuevas_scn['1SCN'] / v1_nov
    
    for i, row in df.iterrows():
        id_t = str(row['Id'])
        # Bases SCN
        if id_t in nuevas_scn: v_final = nuevas_scn[id_t]
        # Multiplicadores Acordada 1
        elif 'SEN' in id_t and 'SESN' not in id_t: v_final = nuevas_scn.get(id_t.replace('SEN', 'SCN'), nuevas_scn['1SCN']) * 1.25
        elif 'SCSN' in id_t: v_final = nuevas_scn.get(id_t.replace('SCSN', 'SCN'), nuevas_scn['1SCN']) * 1.59
        elif 'SEAN' in id_t and 'SEASN' not in id_t: v_final = nuevas_scn.get(id_t.replace('SEAN', 'SCN'), nuevas_scn['1SCN']) * 1.75
        elif 'SESN' in id_t: v_final = (nuevas_scn.get(id_t.replace('SESN', 'SCN'), nuevas_scn['1SCN']) * 1.59) * 1.25
        elif 'SEASN' in id_t: v_final = (nuevas_scn.get(id_t.replace('SEASN', 'SCN'), nuevas_scn['1SCN']) * 1.59) * 1.75
        # Ajuste por Factor (KM, KP, LP)
        else: v_final = row['Limite Inferior'] * factor
        
        df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(v_final, 2)
    return df, factor

# =============================================================================
# 2. SOLAPA 2: PRE-PROCESO (DGGI + PARQUE MÓVIL JN)
# =============================================================================

def preproceso_dggi_completo(f_csv, nom_gt, f_pme=None):
    """Consolida la base DGGI, aplicando Parque Móvil solo a registros de JN"""
    df = pd.read_csv(f_csv, encoding='ISO-8859-1', delimiter=';', low_memory=False)
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.strip()
    
    # Merge inicial con Nomenclador
    df_ = pd.merge(df, nom_gt[['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']].drop_duplicates(), on='ID_LINEA', how='left')
    
    # --- DIVISIÓN DE TRABAJO ---
    # 1. JN: Con Parque Móvil (Dominio/Energía)
    df_jn = df_[df_['GT'].isin(['SGI', 'SGII', 'SGIKM'])].copy()
    if f_pme is not None:
        pme = pd.read_excel(f_pme)
        df_jn = df_jn.merge(pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_jn['ENERGIA'] = df_jn['ENERGIA'].fillna(1) # Gasoil por defecto
        group_jn = ['ID_EMPRESA','ID_LINEA','RAMAL','DOMINIO','ENERGIA','CONTRATO','TARIFA BASE ITG','DEBITADO','DESCUENTO X INTEGRACION','GT','Linea SILAS DNGFF','PROVINCIA','MUNICIPIO']
    else:
        df_jn['DOMINIO'] = 'NO'; df_jn['ENERGIA'] = 1
        group_jn = ['ID_EMPRESA','ID_LINEA','RAMAL','CONTRATO','TARIFA BASE ITG','DEBITADO','DESCUENTO X INTEGRACION','GT','Linea SILAS DNGFF','PROVINCIA','MUNICIPIO']
    
    # 2. CABA y PBA: Agrupación Operativa (Sin Dominio)
    df_otros = df_[df_['GT'].isin(['DF', 'UPA', 'UMA1', 'UMA2', 'UPAKM'])].copy()
    df_otros['DOMINIO'] = 'NO'; df_otros['ENERGIA'] = 1
    group_otros = [c for c in group_jn if c not in ['DOMINIO', 'ENERGIA']]

    # Ejecutar Agregaciones
    jn_final = df_jn.groupby(group_jn, as_index=False).agg({'CANTIDAD_USOS':'sum', 'MONTO':'sum'})
    otros_final = df_otros.groupby(group_otros, as_index=False).agg({'CANTIDAD_USOS':'sum', 'MONTO':'sum'})
    
    return pd.concat([jn_final, otros_final], ignore_index=True)

# =============================================================================
# 3. INTERFAZ Y ORQUESTACIÓN (STREAMLIT)
# =============================================================================

st.set_page_config(page_title="Fiscalización TTR", layout="wide")
tab1, tab2, tab3 = st.tabs(["💰 TARIFAS", "📂 PRE-PROCESO", "🚀 DETERMINACIÓN TTR"])

# --- TAB 1: GENERACIÓN DE ESCALAS ---
with tab1:
    st.header("1. Generador de Diccionario")
    f_nov = st.file_uploader("Subir Base Noviembre (Excel)", type=['xlsx'])
    c1, c2, c3, c4, c5 = st.columns(5)
    n1 = c1.number_input("1SCN", value=650.0)
    n2 = c2.number_input("2SCN", value=724.09)
    # ... (resto de inputs)
    if f_nov and st.button("🔄 Calcular Febrero"):
        dicc, fac = generar_diccionario_tarifas(f_nov, {'1SCN':n1, '2SCN':n2})
        st.session_state['dicc_feb'] = dicc
        st.success(f"Diccionario generado (+{((fac-1)*100):.2f}%)")

# --- TAB 3: DETERMINACIÓN TTR (EL MURO DE FECHA) ---
with tab3:
    st.header("3. Cálculo de TTR Final")
    # Configuración de periodo partido
    partido = st.checkbox("¿El mes tiene aumento a mitad de periodo?")
    if partido:
        fecha_corte = st.date_input("Día de vigencia de nueva tarifa:", value=datetime(2026,2,18))
    
    # Al procesar, el script hace:
    # df_a = base[base['FECHA'] < fecha_corte] -> Macheo con Dicc_Nov
    # df_b = base[base['FECHA'] >= fecha_corte] -> Macheo con Dicc_Feb
    # Al final: df_final.groupby(['CONCAT_MACHEO3']).agg(...) para "aplastar" dominios.
