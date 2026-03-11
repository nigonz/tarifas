import pandas as pd
import numpy as np
import streamlit as st
import io

# =============================================================================
# 1. MOTOR DE CÁLCULOS
# =============================================================================

def proyectar_tarifas(df_nov, nuevas_scn):
    """Lógica Simplificada: Inf y Sup idénticos. Resuelve dependencias de nodos 1-5."""
    try:
        # 1. Usamos un único factor basado en el aumento del Limite Inferior de 1SCN
        v1_nov = df_nov.loc[df_nov['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = nuevas_scn['1SCN'] / v1_nov
    except:
        return df_nov, 1.0

    df = df_nov.copy()
    
    # 2. Aseguramos que los 5 SCN base (del 1 al 5) estén calculados
    # Esto garantiza que SEN, SCSN, etc. no se 'cuelguen' todos del 1SCN
    scn_full = {}
    for i in range(1, 6):
        id_scn = f"{i}SCN"
        if id_scn in nuevas_scn and nuevas_scn[id_scn] > 0:
            scn_full[id_scn] = nuevas_scn[id_scn]
        else:
            base_nov = df.loc[df['Id'] == id_scn, 'Limite Inferior'].values[0]
            scn_full[id_scn] = base_nov * factor

    # 3. Procesamos toda la grilla igualando ambas columnas al mismo valor
    for i, row in df.iterrows():
        id_t = str(row['Id'])
        v_final = 0
        
        # --- CASO A: NODOS ESPECIALES (Mantenemos multiplicadores del Excel) ---
        if any(x in id_t for x in ['SEN', 'SCSN', 'SEAN', 'SESN', 'SEASN']):
            # Buscamos el número de nodo (3SEN -> 3, 4SCSN -> 4, etc.)
            num_nodo = id_t[0] if id_t[0].isdigit() else "1"
            base_base = scn_full.get(f"{num_nodo}SCN", scn_full["1SCN"])
            
            if 'SESN' in id_t:    v_final = (base_base * 1.59) * 1.25
            elif 'SEASN' in id_t: v_final = (base_base * 1.59) * 1.75
            elif 'SCSN' in id_t:  v_final = base_base * 1.59
            elif 'SEN' in id_t:   v_final = base_base * 1.25
            elif 'SEAN' in id_t:  v_final = base_base * 1.75
            
        # --- CASO B: NODOS SCN (Ya calculados en el paso 2) ---
        elif id_t in scn_full:
            v_final = scn_full[id_t]
            
        # --- CASO C: RESTO DE NODOS (Incluye KM, KP y otros) ---
        else:
            # Usamos el Limite Inferior como base universal de proyección
            v_final = row['Limite Inferior'] * factor
            
        # RESULTADO FINAL: Forzamos a que ambas columnas sean iguales y redondeadas
        df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(v_final, 2)
        
    return df, factor
def preproceso_dmk_energias(f_csv, nom_gt, df_pme):
    """Procesamiento de SUBE + Energías Renovables."""
    df = pd.read_csv(f_csv, encoding='ISO-8859-1', sep=None, engine='python')
    df.columns = df.columns.str.strip()
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.strip()
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.strip()
    
    df_ = df[df['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()
    if df_.empty: return pd.DataFrame()

    cols_nom = ['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']
    _df2_ = pd.merge(df_, nom_gt[cols_nom], how='left', on='ID_LINEA')
    _df2_.loc[_df2_['GT'] == 'DF', 'PROVINCIA'] = 'CABA'

    df_pme['DOMINIO'] = df_pme['DOMINIO'].astype(str).str.strip().str.upper()
    _df2_['DOMINIO'] = _df2_['DOMINIO'].astype(str).str.strip().str.upper()
    dominios_especiales = df_pme['DOMINIO'].unique()

    df_con_energia = _df2_[_df2_['DOMINIO'].isin(dominios_especiales)].copy()
    df_con_energia = df_con_energia.merge(df_pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')

    df_resto = _df2_[~_df2_['DOMINIO'].isin(dominios_especiales)].copy()
    df_resto['DOMINIO'] = 'NO'; df_resto['ENERGIA'] = 3

    final = pd.concat([df_con_energia, df_resto], ignore_index=True)
    grupo = ['PROVINCIA', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'Linea SILAS DNGFF', 'ID_LINEA', 
             'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 
             'VIAJE INTEGRADO', 'DESCUENTO X INTEGRACION']
    
    final_res = final.groupby(grupo, as_index=False).agg({'CANTIDAD_USOS': 'sum', 'MONTO': 'sum'})

    for col in ['TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS']:
        final_res[col] = pd.to_numeric(final_res[col], errors='coerce').fillna(0)

    final_res['COMP. ITG'] = final_res['DESCUENTO X INTEGRACION'] * final_res['CANTIDAD_USOS']
    final_res['COMP. ATS'] = final_res.apply(lambda x: (
        ((x['DEBITADO'] / 0.45 * 0.55) * x['CANTIDAD_USOS'] if x['GT'] == 'INP' 
         else (x['TARIFA BASE ITG'] - x['DEBITADO'] - x['DESCUENTO X INTEGRACION']) * x['CANTIDAD_USOS'])
    ) if x['CONTRATO'] == 621 else 0, axis=1)

    final_res['COMP. ATS s/IVA'] = final_res['COMP. ATS'] / 1.105
    final_res['COMP. ITG s/IVA'] = final_res['COMP. ITG'] / 1.105
    return final_res

# =============================================================================
# 2. INTERFAZ STREAMLIT
# =============================================================================

st.set_page_config(page_title="Fiscalización TTR v2.0", layout="wide")

# --- SELECTOR DE MES (Sidebar) ---
st.sidebar.header("📅 Configuración del Periodo")
meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
mes_seleccionado = st.sidebar.selectbox("Seleccione el mes a calcular:", meses, index=1) # Default Febrero
anio_seleccionado = st.sidebar.selectbox("Año:", [2025, 2026], index=1)

st.title(f"Liquidación TTR - {mes_seleccionado} {anio_seleccionado}")

if 'df_tarifas' not in st.session_state: st.session_state['df_tarifas'] = None
if 'df_pme' not in st.session_state: st.session_state['df_pme'] = None

tab1, tab2, tab3 = st.tabs(["💰 TARIFAS", "📂 PRE-PROCESO PME", "🚀 DETERMINACIÓN TTR"])

# --- TAB 1: TARIFAS ---
with tab1:
    st.header(f"1. Generador de Tarifas: {mes_seleccionado}")
    f_nov = st.file_uploader("Subir Base Noviembre Base (Excel)", type=['xlsx'], key="tar_up")
    
    if f_nov:
        c = st.columns(5)
        n1 = c[0].number_input("1SCN", value=650.0)
        n2 = c[1].number_input("2SCN", value=724.09)
        # Puedes agregar n3, n4, n5 si lo necesitas
        
        if st.button("🔄 Calcular Diccionario", key="btn_calc"):
            df_n = pd.read_excel(f_nov, sheet_name='JN11')
            res, fac = proyectar_tarifas(df_n, {'1SCN':n1, '2SCN':n2})
            st.session_state['df_tarifas'] = res
            st.success(f"Diccionario {mes_seleccionado} generado con éxito.")

    if st.session_state['df_tarifas'] is not None:
        # CORRECCIÓN DEL BOTÓN DE DESCARGA
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            st.session_state['df_tarifas'].to_excel(writer, index=False, sheet_name='Tarifas')
        
        st.download_button(
            label=f"📥 Descargar Diccionario_{mes_seleccionado}.xlsx",
            data=output.getvalue(),
            file_name=f"Diccionario_{mes_seleccionado}_{anio_seleccionado}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# --- TAB 2: PRE-PROCESO ---
with tab2:
    st.header(f"2. Pre-proceso DMK Energías: {mes_seleccionado}")
    col1, col2, col3 = st.columns(3)
    f_csv = col1.file_uploader("Archivo DGGI (CSV)", type=['csv'])
    f_nom = col2.file_uploader("Nomenclador GT", type=['xlsx'])
    f_en = col3.file_uploader("Parque Móvil Energías", type=['xlsx'])
    
    if f_csv and f_nom and f_en:
        if st.button("🚀 Iniciar Motor PME"):
            with st.spinner("Procesando datos..."):
                res = preproceso_dmk_energias(f_csv, pd.read_excel(f_nom), pd.read_excel(f_en))
                st.session_state['df_pme'] = res
                st.success(f"Base PME de {mes_seleccionado} lista.")

    if st.session_state['df_pme'] is not None:
        output_pme = io.BytesIO()
        with pd.ExcelWriter(output_pme, engine='xlsxwriter') as writer:
            st.session_state['df_pme'].to_excel(writer, index=False, sheet_name='PME')
        
        st.download_button(
            label=f"📥 Descargar dggi_DMK_PME_{mes_seleccionado}.xlsx",
            data=output_pme.getvalue(),
            file_name=f"dggi_DMK_PME_{mes_seleccionado}_{anio_seleccionado}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
