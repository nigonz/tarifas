import pandas as pd
import numpy as np
import streamlit as st
import io

# =============================================================================
# 1. MOTOR DE LÓGICA (FUNCIONES)
# =============================================================================

def proyectar_tarifas(df_nov, nuevas_scn):
    """Calcula la escala tarifaria completa basada en 1SCN."""
    try:
        # Detectamos el factor de aumento
        v1_nov = df_nov.loc[df_nov['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = nuevas_scn['1SCN'] / v1_nov
    except:
        return df_nov, 1.0

    df = df_nov.copy()
    
    # Aseguramos los 5 SCN base (del 1 al 5) para los multiplicadores
    scn_full = {}
    for i in range(1, 6):
        id_scn = f"{i}SCN"
        if id_scn in nuevas_scn and nuevas_scn[id_scn] > 0:
            scn_full[id_scn] = nuevas_scn[id_scn]
        else:
            base_nov = df.loc[df['Id'] == id_scn, 'Limite Inferior'].values[0]
            scn_full[id_scn] = base_nov * factor

    # Procesamos toda la grilla igualando límites para que sean idénticos
    for i, row in df.iterrows():
        id_t = str(row['Id'])
        v_final = 0
        if any(x in id_t for x in ['SEN', 'SCSN', 'SEAN', 'SESN', 'SEASN']):
            num_nodo = id_t[0] if id_t[0].isdigit() else "1"
            base_base = scn_full.get(f"{num_nodo}SCN", scn_full["1SCN"])
            if 'SESN' in id_t:    v_final = (base_base * 1.59) * 1.25
            elif 'SEASN' in id_t: v_final = (base_base * 1.59) * 1.75
            elif 'SCSN' in id_t:  v_final = base_base * 1.59
            elif 'SEN' in id_t:   v_final = base_base * 1.25
            elif 'SEAN' in id_t:  v_final = base_base * 1.75
        elif id_t in scn_full:
            v_final = scn_full[id_t]
        else:
            v_final = row['Limite Inferior'] * factor
            
        df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(v_final, 2)
    return df, factor

def preproceso_dmk_energias(f_csv, nom_gt, df_pme):
    """Pre-procesamiento robusto de SUBE + Energías Renovables."""
    # Lectura con detección automática de separador
    df = pd.read_csv(f_csv, encoding='ISO-8859-1', sep=None, engine='python')
    df.columns = df.columns.str.strip()
    
    # Normalización de ID_LINEA para evitar el error de los decimales (.0)
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.strip().str.replace('.0', '', regex=False)
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.strip().str.replace('.0', '', regex=False)
    
    df_f = df[df['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()
    if df_f.empty: return pd.DataFrame()

    _df2 = pd.merge(df_f, nom_gt[['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']], on='ID_LINEA', how='left')
    _df2.loc[_df2['GT'] == 'DF', 'PROVINCIA'] = 'CABA'

    # Clasificación de Energías
    df_pme['DOMINIO'] = df_pme['DOMINIO'].astype(str).str.strip().str.upper()
    _df2['DOMINIO'] = _df2['DOMINIO'].astype(str).str.strip().str.upper()
    
    dom_esp = df_pme['DOMINIO'].unique()
    df_con_en = _df2[_df2['DOMINIO'].isin(dom_esp)].copy()
    df_con_en = df_con_en.merge(df_pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
    
    df_resto = _df2[~_df2['DOMINIO'].isin(dom_esp)].copy()
    df_resto['DOMINIO'] = 'NO'; df_resto['ENERGIA'] = 3

    final = pd.concat([df_con_en, df_resto], ignore_index=True)
    grupo = ['PROVINCIA', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'Linea SILAS DNGFF', 'ID_LINEA', 
             'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 
             'VIAJE INTEGRADO', 'DESCUENTO X INTEGRACION']
    
    res = final.groupby(grupo, as_index=False).agg({'CANTIDAD_USOS': 'sum', 'MONTO': 'sum'})
    for col in ['TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION', 'CANTIDAD_USOS']:
        res[col] = pd.to_numeric(res[col], errors='coerce').fillna(0)

    # ATS e ITG
    res['COMP. ITG'] = res['DESCUENTO X INTEGRACION'] * res['CANTIDAD_USOS']
    res['COMP. ATS'] = res.apply(lambda x: (((x['DEBITADO']/0.45)*0.55)*x['CANTIDAD_USOS'] if x['GT']=='INP' else (x['TARIFA BASE ITG']-x['DEBITADO']-x['DESCUENTO X INTEGRACION'])*x['CANTIDAD_USOS']) if x['CONTRATO']==621 else 0, axis=1)
    res['COMP. ATS s/IVA'] = res['COMP. ATS'] / 1.105
    res['COMP. ITG s/IVA'] = res['COMP. ITG'] / 1.105
    return res

def determinar_ttr_motor(df_pme, df_tarifas, anio, reso):
    """Ingeniería Inversa y creación segura de CONCAT_MATCHEO3."""
    df = df_pme.copy()
    
    # Forzamos conversión a string para evitar TypeErrors en el concat
    anio_str = str(int(anio))
    reso_str = str(reso).strip()
    
    # 1. Macheo de Tarifas
    df['TARIFA BASE ITG'] = df['TARIFA BASE ITG'].round(2)
    df_tarifas['Limite Inferior'] = df_tarifas['Limite Inferior'].round(2)
    lookup = df_tarifas.drop_duplicates(subset=['Limite Inferior']).set_index('Limite Inferior')['Id'].to_dict()
    df['NODO_ID'] = df['TARIFA BASE ITG'].map(lookup).fillna("S/D")

    # 2. Secciones y Lógica SGII
    df['SEC_NUM'] = df['NODO_ID'].str.extract('(\d+)').fillna('0')
    df['SEC_FINAL'] = np.where((df['GT'] == "SGII") & (df['SEC_NUM'].isin(['1', '2', '3'])), '4', df['SEC_NUM'])

    # 3. Construcción del CONCAT_MATCHEO3 (Blindado)
    tipo_t = np.where(df['CONTRATO'] == 627, "SN", "N")
    id_linea_str = df['ID_LINEA'].astype(str).str.replace('.0', '', regex=False)
    
    df['CONCAT_MATCHEO3'] = (
        anio_str + 
        reso_str + 
        df['SEC_FINAL'].astype(str) + 
        df['GT'].astype(str) + 
        id_linea_str + 
        "S" + 
        pd.Series(tipo_t, index=df.index).astype(str)
    )
    
    # Factores de Energía
    cond_en = [df['ENERGIA'] == 1, df['ENERGIA'] == 2]
    fact_en = [1.3, 1.5]
    df['FACTOR_CORRECCION'] = np.select(cond_en, fact_en, default=1.0)
    
    return df

# =============================================================================
# 2. INTERFAZ STREAMLIT
# =============================================================================

st.set_page_config(page_title="Fiscalización TTR v2.0", layout="wide")

# Sidebar
st.sidebar.header("📅 Configuración Temporal")
meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
mes_sel = st.sidebar.selectbox("Mes:", meses, index=1)
anio_sel = st.sidebar.selectbox("Año:", [2025, 2026], index=1)

st.title(f"Sistema TTR - {mes_sel} {anio_sel}")

# Estados de sesión
if 'df_tarifas' not in st.session_state: st.session_state['df_tarifas'] = None
if 'df_pme' not in st.session_state: st.session_state['df_pme'] = None
if 'ttr_final' not in st.session_state: st.session_state['ttr_final'] = None

tab1, tab2, tab3 = st.tabs(["💰 1. TARIFAS", "📂 2. PRE-PROCESO PME", "🚀 3. DETERMINACIÓN TTR"])

# --- TAB 1 ---
with tab1:
    st.header("Generador de Escala Tarifaria")
    f_nov = st.file_uploader("Archivo Noviembre (Excel)", type=['xlsx'])
    if f_nov:
        c = st.columns(2)
        n1 = c[0].number_input("Valor 1SCN:", value=650.0)
        n2 = c[1].number_input("Valor 2SCN:", value=724.09)
        if st.button("🔄 Calcular Diccionario"):
            df_n = pd.read_excel(f_nov, sheet_name='JN11')
            st.session_state['df_tarifas'], _ = proyectar_tarifas(df_n, {'1SCN': n1, '2SCN': n2})
            st.success("Diccionario listo.")
    if st.session_state['df_tarifas'] is not None:
        buf1 = io.BytesIO()
        with pd.ExcelWriter(buf1, engine='xlsxwriter') as wr: st.session_state['df_tarifas'].to_excel(wr, index=False)
        st.download_button("📥 Bajar Diccionario", buf1.getvalue(), f"Diccionario_{mes_sel}.xlsx")

# --- TAB 2 ---
with tab2:
    st.header("Consolidación de Datos SUBE")
    c1, c2, c3 = st.columns(3)
    f_csv = c1.file_uploader("Archivo DGGI (CSV)", type=['csv'])
    f_gt = c2.file_uploader("Nomenclador GT", type=['xlsx'])
    f_en = c3.file_uploader("Parque Móvil Energías", type=['xlsx'])
    if f_csv and f_gt and f_en:
        if st.button("🚀 Iniciar Consolidación"):
            with st.spinner("Procesando..."):
                st.session_state['df_pme'] = preproceso_dmk_energias(f_csv, pd.read_excel(f_gt), pd.read_excel(f_en))
                st.success("Pre-proceso finalizado.")
    if st.session_state['df_pme'] is not None:
        buf2 = io.BytesIO()
        with pd.ExcelWriter(buf2, engine='xlsxwriter') as wr: st.session_state['df_pme'].to_excel(wr, index=False)
        st.download_button("📥 Bajar Base PME", buf2.getvalue(), f"Base_PME_{mes_sel}.xlsx")

# --- TAB 3 ---
with tab3:
    st.header("Determinación Final TTR")
    if st.session_state['df_pme'] is None or st.session_state['df_tarifas'] is None:
        st.warning("⚠️ Completa primero las pestañas 1 y 2.")
    else:
        cc = st.columns(2)
        v_anio = cc[0].number_input("Año CONCAT:", value=anio_sel)
        v_reso = cc[1].text_input("Reso CONCAT:", value="86")
        f_teorico = st.file_uploader("Archivo Resoluciones (Excel) - Opcional", type=['xlsx'])
        
        if st.button("🚀 Ejecutar Liquidación"):
            with st.spinner("Macheando y calculando..."):
                res = determinar_ttr_motor(st.session_state['df_pme'], st.session_state['df_tarifas'], v_anio, v_reso)
                if f_teorico:
                    ttr_ref = pd.read_excel(f_teorico, sheet_name='TTR')
                    res = pd.merge(res, ttr_ref[['CONCAT', 'TTR E.C.']], left_on='CONCAT_MATCHEO3', right_on='CONCAT', how='left')
                    res['PAGO_FINAL'] = (res['TTR E.C.'] * res['CANTIDAD_USOS']) * res['FACTOR_CORRECCION']
                st.session_state['ttr_final'] = res
                st.success("TTR Determinado.")
                st.dataframe(res.head(10))

    if st.session_state['ttr_final'] is not None:
        buf3 = io.BytesIO()
        with pd.ExcelWriter(buf3, engine='xlsxwriter') as wr: st.session_state['ttr_final'].to_excel(wr, index=False)
        st.download_button("📥 Bajar Reporte Final", buf3.getvalue(), f"TTR_FINAL_{mes_sel}.xlsx")
