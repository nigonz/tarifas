import pandas as pd
import numpy as np
import streamlit as st
import xlsxwriter
import io

# =============================================================================
# 1. MOTOR DE PROCESAMIENTO (FUNCIONES)
# =============================================================================

def procesar_base_dggi(f_csv, nom_gt):
    """Procesa el archivo pesado de DGGI por partes (chunks)"""
    lista_pedazos = []
    # Detecta si es ZIP o CSV
    compression = 'zip' if f_csv.name.endswith('.zip') else None
    
    # Lee de a 50.000 filas para no saturar la memoria
    for chunk in pd.read_csv(f_csv, encoding='ISO-8859-1', delimiter=';', 
                             compression=compression, chunksize=50000):
        
        # Filtra por las líneas que están en el nomenclador
        chunk_filtrado = chunk[chunk['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()
        
        if not chunk_filtrado.empty:
            lista_pedazos.append(chunk_filtrado)
            
    if not lista_pedazos:
        return pd.DataFrame()
        
    df_final = pd.concat(lista_pedazos, ignore_index=True)
    
    # Cálculos de Componentes (ATS e ITG)
    for col in ['TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS', 'CONTRATO']:
        df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

    df_final['COMP. ITG'] = df_final['DESCUENTO X INTEGRACION'] * df_final['CANTIDAD_USOS']
    df_final['COMP. ATS'] = df_final.apply(
        lambda x: ((x['DEBITADO'] / 0.45 * 0.55) * x['CANTIDAD_USOS'] if x.get('GT') == 'INP' 
        else (x['TARIFA BASE ITG'] - x['DEBITADO'] - x['DESCUENTO X INTEGRACION']) * x['CANTIDAD_USOS']) 
        if x['CONTRATO'] == 621 else 0, axis=1
    )
    
    df_final['COMP. ATS s/IVA'] = df_final['COMP. ATS'] / 1.105
    df_final['COMP. ITG s/IVA'] = df_final['COMP. ITG'] / 1.105
    
    return df_final

def consolidar_excels(df_caba, df_jn, df_pba):
    """Une los tres resultados en uno solo"""
    df_caba['Jurisdicción'] = 'CABA'
    df_jn['Jurisdicción'] = 'JN'
    df_pba['Jurisdicción'] = 'PBA'
    return pd.concat([df_caba, df_jn, df_pba], ignore_index=True)

# Aquí van tus funciones tool_procesar_df, tool_procesar_jn y tool_procesar_pba
# Asegurate de que estén pegadas al borde izquierdo (sin espacios antes de 'def')
# [PEGAR TUS 3 FUNCIONES AQUÍ]

# =============================================================================
# 2. SEGURIDAD (EL PATOVICA)
# =============================================================================

if "acceso_concedido" not in st.session_state:
    st.session_state["acceso_concedido"] = False

if not st
