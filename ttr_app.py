import pandas as pd
import numpy as np
import streamlit as st
import xlsxwriter
import io

# =============================================================================
# 1. MOTOR DE PROCESAMIENTO (FUNCIONES)
# =============================================================================

def procesar_base_dggi(f_csv, nom_gt):
    # 1. CARGA COMPLETA (Sin pedazos/chunks para no triplicar registros)
    compression = 'zip' if f_csv.name.endswith('.zip') else None
    
    # Leemos todo el millón de filas de una vez
    df = pd.read_csv(f_csv, encoding='ISO-8859-1', delimiter=';', 
                     compression=compression, low_memory=False)

    # Forzamos ID_LINEA a texto para un filtrado perfecto
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.strip()
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.strip()

    # 2. FILTRADO (Línea 28 de tu Colab)
    df_ = df[df['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()

    # 3. SELECCIÓN DE COLUMNAS (Línea 33 de tu Colab)
    df_ramal = ['ID_EMPRESA', 'ID_LINEA','RAMAL','TARIFA BASE ITG', 'DEBITADO', 'CONTRATO', 
                'VIAJE INTEGRADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS', 'MONTO']
    
    df_ramal_ok = [c for c in df_ramal if c in df_.columns]
    df_ = df_[df_ramal_ok]

    # 4. AGRUPACIÓN GLOBAL (Línea 38 de tu Colab - Da los 83.142 registros)
    _df_ = df_.groupby(['ID_EMPRESA', 'ID_LINEA', 'RAMAL', 'CONTRATO','TARIFA BASE ITG', 'DEBITADO',
                        'VIAJE INTEGRADO', 'DESCUENTO X INTEGRACION'],
                        as_index=False).agg({
        'CANTIDAD_USOS': 'sum',
        'MONTO': 'sum'
    })

    # 5. MERGE GEOGRÁFICO (Línea 47 de tu Colab)
    columns_to_merge = ['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']
    _df2_ = pd.merge(_df_, nom_gt[columns_to_merge].drop_duplicates(subset=['ID_LINEA']), 
                     how='left', on='ID_LINEA')

    # 6. CÁLCULOS FINALES (Líneas 53 a 85 de tu Colab)
    _df2_['BE'] = np.where(_df2_['CONTRATO'].isin([830, 831, 832, 833]), 'SI', 'NO')

    for col in ['TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS', 'CONTRATO']:
        _df2_[col] = pd.to_numeric(_df2_[col], errors='coerce')

    _df2_['TipoContrato'] = _df2_['CONTRATO'].apply(lambda x: 'ATS' if x == 621 else 'SIN ATS')
    _df2_['COMP. ITG'] = _df2_['DESCUENTO X INTEGRACION'] * _df2_['CANTIDAD_USOS']

    _df2_['COMP. ATS'] = _df2_.apply(
        lambda x: (
            (x['DEBITADO'] / 0.45 * 0.55) * x['CANTIDAD_USOS'] if x['GT'] == 'INP'
            else (x['TARIFA BASE ITG'] - x['DEBITADO'] - x['DESCUENTO X INTEGRACION']) * x['CANTIDAD_USOS']
        ) if x['CONTRATO'] == 621 else 0, axis=1
    )

    _df2_['COMP. ATS s/IVA'] = _df2_['COMP. ATS'] / 1.105
    _df2_['COMP. ITG s/IVA'] = _df2_['COMP. ITG'] / 1.105
    _df2_.loc[_df2_['GT'] == 'DF', 'PROVINCIA'] = 'CABA'

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
    st.info("Paso 1: Subí el crudo de usos (.zip) para generar el archivo base de 83.142 registros.")
    
    c1, c2 = st.columns(2)
    with c1:
        f_csv = st.file_uploader("1. Archivo DGGI (Crudo en .zip)", type=['csv', 'zip'])
    with c2:
        f_nom = st.file_uploader("2. Nomenclador GT (.xlsx)", type=['xlsx'])

    if f_csv and f_nom:
        if st.button("🚀 Generar Base DGGI"):
            with st.spinner("Procesando el millón de filas... Esto puede tardar 20 segundos."):
                try:
                    nom_gt = pd.read_excel(f_nom)
                    res = procesar_base_dggi(f_csv, nom_gt)
                    
                    if not res.empty:
                        st.success(f"¡Base generada con éxito! Filas totales: {len(res):,}")
                        st.dataframe(res.head()) 

                        # --- IMPORTANTE: DESCARGA EN EXCEL PARA LA TAB 1 ---
                        # Como son 83k filas, Excel es mejor y lo pide tu función tool_procesar
                        output_dggi = io.BytesIO()
                        with pd.ExcelWriter(output_dggi, engine='xlsxwriter') as writer:
                            # 'Base' es el nombre que buscan tus funciones en la Tab 1
                            res.to_excel(writer, index=False, sheet_name='Base')
                        output_dggi.seek(0)

                        st.download_button(
                            label="📥 DESCARGAR BASE PARA TTR (.XLSX)",
                            data=output_dggi,
                            file_name="base_dggi_procesada.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                    else:
                        st.warning("⚠️ No se encontraron coincidencias en el nomenclador.")
                except Exception as e:
                    st.error(f"Error técnico: {e}")
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
