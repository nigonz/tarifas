import pandas as pd
import numpy as np
import streamlit as st
import xlsxwriter
import io

# =============================================================================
# 1. FUNCIONES MAESTRAS (EL MOTOR)
# =============================================================================

def procesar_base_dggi(df_csv, nom_gt):
    # 1. Filtrar por líneas válidas en el nomenclador
    df_ = df_csv[df_csv['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()
    
    # 2. Seleccionar columnas necesarias
    cols_interes = ['ID_EMPRESA', 'ID_LINEA','RAMAL','TARIFA BASE ITG', 'DEBITADO', 'CONTRATO', 
                    'VIAJE INTEGRADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS', 'MONTO']
    df_ = df_[cols_interes]

    # 3. Agrupar y sumarizar
    _df_ = df_.groupby(['ID_EMPRESA', 'ID_LINEA', 'RAMAL', 'CONTRATO','TARIFA BASE ITG', 'DEBITADO',
                        'VIAJE INTEGRADO', 'DESCUENTO X INTEGRACION'], as_index=False).agg({
        'CANTIDAD_USOS': 'sum',
        'MONTO': 'sum'
    })

    # 4. Merge con Nomenclador para traer datos geográficos y GT
    cols_nom = ['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']
    _df2_ = pd.merge(_df_, nom_gt[cols_nom], how='left', on='ID_LINEA')

    # 5. Cálculos de componentes
    _df2_['BE'] = np.where(_df2_['CONTRATO'].isin([830, 831, 832, 833]), 'SI', 'NO')
    
    for col in ['TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS', 'CONTRATO']:
        _df2_[col] = pd.to_numeric(_df2_[col], errors='coerce')

    _df2_['COMP. ITG'] = _df2_['DESCUENTO X INTEGRACION'] * _df2_['CANTIDAD_USOS']
    
    _df2_['COMP. ATS'] = _df2_.apply(
        lambda x: ((x['DEBITADO'] / 0.45 * 0.55) * x['CANTIDAD_USOS'] if x['GT'] == 'INP' 
        else (x['TARIFA BASE ITG'] - x['DEBITADO'] - x['DESCUENTO X INTEGRACION']) * x['CANTIDAD_USOS']) 
        if x['CONTRATO'] == 621 else 0, axis=1
    )
    
    _df2_['COMP. ATS s/IVA'] = _df2_['COMP. ATS'] / 1.105
    _df2_['COMP. ITG s/IVA'] = _df2_['COMP. ITG'] / 1.105
    
    _df2_.loc[_df2_['GT'] == 'DF', 'PROVINCIA'] = 'CABA'
    return _df2_

def consolidar_excels(df_caba, df_jn, df_pba):
    df_caba['Jurisdicción'] = 'CABA'
    df_jn['Jurisdicción'] = 'JN'
    df_pba['Jurisdicción'] = 'PBA'
    return pd.concat([df_caba, df_jn, df_pba], ignore_index=True)

# --- AQUÍ IRÍAN LAS FUNCIONES tool_procesar_df, tool_procesar_jn y tool_procesar_pba ---
# (Las omito en este bloque para que no sea infinito, pero dejalas pegadas como estaban)

# =============================================================================
# 2. SEGURIDAD
# =============================================================================

if "acceso_concedido" not in st.session_state:
    st.session_state["acceso_concedido"] = False

if not st.session_state["acceso_concedido"]:
    st.title("🔒 Acceso Restringido")
    try:
        clave_maestra = st.secrets["CLAVE_SECRETA"]
    except:
        clave_maestra = "2470" 

    clave_ingresada = st.text_input("Contraseña:", type="password")
    if st.button("Entrar"):
        if clave_ingresada == clave_maestra:
            st.session_state["acceso_concedido"] = True
            st.rerun()
        else:
            st.error("❌ Contraseña incorrecta")
    st.stop()

# =============================================================================
# 3. INTERFAZ (TABS)
# =============================================================================

st.title("Procedimiento de Macheo TTR")

tab1, tab2 = st.tabs(["🚀 DETERMINACIÓN TTR", "📂 PRE-PROCESO DGGI"])

with tab2:
    st.header("Generador de Base DGGI")
    st.write("Subí el CSV de la DGGI y el Nomenclador para generar el 'Archivo Base'.")
    
    c1, c2 = st.columns(2)
    with c1:
        f_csv = st.file_uploader("Subir CSV DGGI", type=['csv'])
    with c2:
        f_nom = st.file_uploader("Subir Nomenclador ", type=['xlsx'])

    if f_csv and f_nom:
        if st.button("🚀 Generar Base DGGI"):
            try:
                # Cambiamos la forma de leer para que acepte .zip o .csv directo
                if f_dggi.name.endswith('.zip'):
                    df_csv = pd.read_csv(f_dggi, encoding='ISO-8859-1', delimiter=';', compression='zip')
                else:
                    df_csv = pd.read_csv(f_dggi, encoding='ISO-8859-1', delimiter=';')
                    nom_gt = pd.read_excel(f_nom)
                    res = procesar_base_dggi(df_csv, nom_gt)
                st.success("¡Base generada!")
                st.dataframe(res.head())
                
                output_dggi = io.BytesIO()
                with pd.ExcelWriter(output_dggi, engine='xlsxwriter') as writer:
                    res.to_excel(writer, index=False, sheet_name='Base')
                output_dggi.seek(0)
                
                st.download_button("📥 Descargar Base DGGI", output_dggi, "base_dggi_procesada.xlsx")
            except Exception as e:
                st.error(f"Error: {e}")

with tab1:
    st.header("Determinación de TTR")
    # ACÁ VAN TUS SELECTORES (Jurisdicción, Mes, Año)
    # Y LOS 5 FILE_UPLOADER que usabas antes
    # Y EL BOTÓN DE PROCESAR CON LA LÓGICA DE CONSOLIDAR AL FINAL
    st.info("Recordá subir aquí el archivo que descargaste en la otra pestaña.")
