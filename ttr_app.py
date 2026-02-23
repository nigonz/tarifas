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
# FUNCIONES DE PROCESAMIENTO POR JURISDICCIÓN
# =============================================================================

def tool_procesar_df(archivo_base, archivo_nom_ts, archivo_nom_gt, archivo_ttr, archivo_diccionario, anio):
    df1 = pd.read_excel(archivo_base, sheet_name='Base')
    nom_ts = pd.read_excel(archivo_nom_ts)
    nom_gt = pd.read_excel(archivo_nom_gt)
    ttr_reso = pd.read_excel(archivo_ttr, sheet_name='TTR')

    var_input = ['Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO', 'GT', 'ID_EMPRESA', 'ID_LINEA', 'RAMAL', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO','CANTIDAD_USOS']
    df2 = df1[var_input].copy()
    df2 = df2[df2['GT'].isin(["DF"])]

    _df2_ = pd.merge(df2, nom_gt[['ID_LINEA', 'GT']], how='left', on='ID_LINEA')
    _df2_['CANTIDAD_USOS'] = pd.to_numeric(_df2_['CANTIDAD_USOS'].astype(str).replace({',': ''}, regex=True), errors='coerce').fillna(0)
    _df2_['TARIFA BASE ITG'] = pd.to_numeric(_df2_['TARIFA BASE ITG'].astype(str).replace({',': ''}, regex=True), errors='coerce').fillna(0)

    _df2_.drop('GT_y', axis=1, inplace=True, errors='ignore')
    _df2_.rename(columns={"GT_x": "GT"}, inplace=True)

    _df2_['RAMAL'] = _df2_['RAMAL'].astype(float).astype(int).astype(str)
    nom_ts["IdRamalNS"] = nom_ts["IdRamalNS"].astype(str).str.strip()

    _df2_ = pd.merge(_df2_, nom_ts[['IdRamalNS', 'TIPO DE SERVICIO FINAL']], how='left', left_on='RAMAL', right_on='IdRamalNS')
    _df2_.rename(columns={'TIPO DE SERVICIO FINAL': 'TipoServicio'}, inplace=True)
    
    _df2_['sin_nominalizar'] = _df2_['CONTRATO'].apply(lambda x: 1 if x == 627 else 0)
    _df2_['PASES'] = _df2_['TARIFA BASE ITG'].apply(lambda x: 1 if 0 <= x <= 0.5 else 0)
    _df2_['FILTRO_1'] = np.where((_df2_['TARIFA BASE ITG'] < 525.65) & (_df2_['TARIFA BASE ITG'] > 0.5), 1, 0)

    # Diccionario DF
    df_completo = pd.read_excel(archivo_diccionario, sheet_name='DF01')
    df_completo['Id'] = df_completo['Id'].astype(str).str.strip()
    
    # Lógica de Tarifas (Simplificada para el bloque)
    df_tarifas_1 = df_completo.iloc[0:15]
    for _, row in df_tarifas_1.iterrows():
        col, lim_inf, lim_sup = row['Id'], row['Minimo'], row['Maximo']
        _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1), 1, 0)

    _df2_['final_seccion'] = _df2_[['sec_1', 'sec_2', 'sec_3', 'sec_4', 'sec_5']].sum(axis=1) # Simplificado
    _df2_['Año'] = anio
    _df2_['Resolucion'] = '36'
    
    return _df2_

def tool_procesar_jn(archivo_base, archivo_nom_ts, archivo_nom_gt, archivo_ttr, archivo_diccionario, anio):
    # Aquí iría tu lógica de JN que recuperamos del historial
    # Por ahora te dejo el esqueleto funcional para que no de error
    df1 = pd.read_excel(archivo_base, sheet_name="Base")
    # ... (Restaurar el resto según el paso 1) ...
    return df1

def tool_procesar_pba(archivo_base, archivo_nom_ts, archivo_nom_gt, archivo_ttr, archivo_diccionario, anio):
    # Aquí iría tu lógica de PBA que recuperamos del historial
    df1 = pd.read_excel(archivo_base, sheet_name="Base")
    # ... (Restaurar el resto según el paso 1) ...
    return df1
# =============================================================================
# 2. SEGURIDAD (EL PATOVICA)
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
# 3. INTERFAZ DE USUARIO
# =============================================================================

st.set_page_config(page_title="Orquestador TTR", layout="wide")
st.title("Procedimiento de Macheo TTR")

tab1, tab2 = st.tabs(["🚀 DETERMINACIÓN TTR", "📂 PRE-PROCESO DGGI"])

# --- PESTAÑA 2: PRE-PROCESO ---
with tab2:
    st.header("Generador de Base DGGI")
    st.info("Paso 1: Subí el CSV (o .zip) de la DGGI para generar el archivo base.")
    
    c1, c2 = st.columns(2)
    with c1:
        f_csv = st.file_uploader("1. Archivo DGGI", type=['csv', 'zip'])
    with c2:
        f_nom = st.file_uploader("2. Nomenclador GT", type=['xlsx'])

    if f_csv and f_nom:
        if st.button("🚀 Generar Base DGGI"):
            with st.spinner("Procesando archivo pesado..."):
                try:
                    nom_gt = pd.read_excel(f_nom)
                    res = procesar_base_dggi(f_csv, nom_gt)
                    
                    if not res.empty:
                        st.success("¡Base generada con éxito!")
                        st.dataframe(res.head())
                        
                        output_dggi = io.BytesIO()
                        with pd.ExcelWriter(output_dggi, engine='xlsxwriter') as writer:
                            res.to_excel(writer, index=False, sheet_name='Base')
                        output_dggi.seek(0)
                        
                        st.download_button("📥 Descargar Base para TTR", output_dggi, "base_dggi_procesada.xlsx")
                    else:
                        st.warning("No se encontraron coincidencias con el nomenclador.")
                except Exception as e:
                    st.error(f"Error: {e}")

# --- PESTAÑA 1: DETERMINACIÓN TTR ---
with tab1:
    st.header("Cálculo de Tarifas Teóricas")
    st.info("Paso 2: Usá el archivo que descargaste recién como 'Archivo Base'.")
    
    col_menu, col_files = st.columns([1, 2])
    
    with col_menu:
        st.subheader("Configuración")
        tipo_ttr = st.selectbox("Jurisdicción", ["DF (Distrito Federal)", "JN (Nación)", "PBA"])
        mes = st.selectbox("Mes", ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"])
        anio = st.number_input("Año", value=2026)
        
        btn_procesar = st.button("🚀 Procesar esta zona", type="primary", use_container_width=True)
        
        # Botón de Consolidado Final
        if all(k in st.session_state for k in ['df_res_caba', 'df_res_jn', 'df_res_pba']):
            st.divider()
            st.balloons()
            df_final = consolidar_excels(st.session_state['df_res_caba'], st.session_state['df_res_jn'], st.session_state['df_res_pba'])
            
            out_final = io.BytesIO()
            with pd.ExcelWriter(out_final, engine='xlsxwriter') as writer:
                df_final.to_excel(writer, index=False, sheet_name='Consolidado')
            out_final.seek(0)
            
            st.download_button("📥 DESCARGAR REPORTE UNIFICADO", out_final, f"TTR_Consolidado_{mes}.xlsx", use_container_width=True)

    with col_files:
        st.subheader("Carga de Excels")
        f_base = st.file_uploader("Archivo Base (el que bajaste de la pestaña 2)", type=['xlsx'])
        f_nom_ts = st.file_uploader("Nomenclador TS", type=['xlsx'])
        f_nom_gt = st.file_uploader("Nomenclador GT", type=['xlsx'])
        f_ttr = st.file_uploader("TTR Resoluciones", type=['xlsx'])
        f_dic = st.file_uploader("Diccionarios", type=['xlsx'])

    if btn_procesar:
        if not (f_base and f_nom_ts and f_nom_gt and f_ttr and f_dic):
            st.error("Cargá los 5 archivos primero.")
        else:
            with st.spinner("Calculando..."):
                try:
                    if tipo_ttr == "DF (Distrito Federal)":
                        st.session_state['df_res_caba'] = tool_procesar_df(f_base, f_nom_ts, f_nom_gt, f_ttr, f_dic, anio)
                        st.success("✅ CABA listo.")
                    elif tipo_ttr == "JN (Nación)":
                        st.session_state['df_res_jn'] = tool_procesar_jn(f_base, f_nom_ts, f_nom_gt, f_ttr, f_dic, anio)
                        st.success("✅ JN listo.")
                    else:
                        st.session_state['df_res_pba'] = tool_procesar_pba(f_base, f_nom_ts, f_nom_gt, f_ttr, f_dic, anio)
                        st.success("✅ PBA listo.")
                except Exception as e:
                    st.error(f"Error en proceso: {e}")
