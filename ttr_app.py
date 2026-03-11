import pandas as pd
import numpy as np
import streamlit as st
import io
import zipfile

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR", layout="wide")

# =============================================================================
# 1. TUS FUNCIONES ORIGINALES (CON AJUSTE DE MEMORIA)
# =============================================================================

def proyectar_tarifas(df_nov, nuevas_scn):
    """Tu lógica original para la escala tarifaria."""
    try:
        v1_nov = df_nov.loc[df_nov['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = nuevas_scn['1SCN'] / v1_nov
    except:
        return df_nov, 1.0
    df = df_nov.copy()
    scn_full = {}
    for i in range(1, 6):
        id_scn = f"{i}SCN"
        if id_scn in nuevas_scn and nuevas_scn[id_scn] > 0:
            scn_full[id_scn] = nuevas_scn[id_scn]
        else:
            base_nov = df.loc[df['Id'] == id_scn, 'Limite Inferior'].values[0]
            scn_full[id_scn] = base_nov * factor
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
        elif id_t in scn_full: v_final = scn_full[id_t]
        else: v_final = row['Limite Inferior'] * factor
        df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(v_final, 2)
    return df

def preproceso_dmk_energias(f_csv, nom_gt, df_pme):
    """Tu lógica original de cruce de Nomenclador y Energías."""
    # Manejo de ZIP para el archivo de 215MB
    if f_csv.name.endswith('.zip'):
        with zipfile.ZipFile(f_csv) as z:
            csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f, encoding='ISO-8859-1', sep=None, engine='python')
    else:
        df = pd.read_csv(f_csv, encoding='ISO-8859-1', sep=None, engine='python')

    df.columns = df.columns.str.strip().str.upper()
    nom_gt.columns = nom_gt.columns.str.strip().str.upper()
    df_pme.columns = df_pme.columns.str.strip().str.upper()

    # Limpieza de IDs como hacías al principio
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.replace('.0', '', regex=False)
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.replace('.0', '', regex=False)
    
    # --- EL CRUCE QUE PEDISTE (Tu lógica original) ---
    # Pegamos el Nomenclador a la SUBE para traer GT, PROVINCIA, etc.
    _df2 = pd.merge(df, nom_gt[['ID_LINEA', 'GT', 'LINEA SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']], on='ID_LINEA', how='left')
    _df2.loc[_df2['GT'] == 'DF', 'PROVINCIA'] = 'CABA'

    # Pegamos las Energías
    df_pme['DOMINIO'] = df_pme['DOMINIO'].astype(str).str.strip().str.upper()
    _df2['DOMINIO'] = _df2['DOMINIO'].astype(str).str.strip().str.upper()
    
    final = pd.merge(_df2, df_pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
    final['ENERGIA'] = final['ENERGIA'].fillna(3) # Gasoil por defecto

    # Agrupamiento original
    grupo = ['PROVINCIA', 'MUNICIPIO', 'GT', 'LINEA SILAS DNGFF', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION']
    res = final.groupby(grupo, as_index=False).agg({'CANTIDAD_USOS': 'sum', 'MONTO': 'sum'})
    
    # Fórmulas de compensación originales
    res['COMP. ITG'] = res['DESCUENTO X INTEGRACION'] * res['CANTIDAD_USOS']
    res['COMP. ATS'] = res.apply(lambda x: (((x['DEBITADO']/0.45)*0.55)*x['CANTIDAD_USOS'] if x['GT']=='INP' else (x['TARIFA BASE ITG']-x['DEBITADO']-x['DESCUENTO X INTEGRACION'])*x['CANTIDAD_USOS']) if x['CONTRATO']==621 else 0, axis=1)
    
    return res

def determinar_ttr_motor(df_pme, df_tarifas, anio, reso):
    """Ingeniería inversa usando el diccionario generado en la Tab 1."""
    df = df_pme.copy()
    anio_str, reso_str = str(int(anio)), str(reso).strip()
    
    # Macheo de Tarifas
    df['TARIFA BASE ITG'] = pd.to_numeric(df['TARIFA BASE ITG'], errors='coerce').round(2)
    df_tarifas['Limite Inferior'] = pd.to_numeric(df_tarifas['Limite Inferior'], errors='coerce').round(2)
    lookup = df_tarifas.drop_duplicates(subset=['Limite Inferior']).set_index('Limite Inferior')['Id'].to_dict()
    
    df['NODO_ID'] = df['TARIFA BASE ITG'].map(lookup).fillna("S/D")
    df['SEC_NUM'] = df['NODO_ID'].str.extract('(\d+)').fillna('0')
    df['SEC_FINAL'] = np.where((df['GT'] == "SGII") & (df['SEC_NUM'].isin(['1', '2', '3'])), '4', df['SEC_NUM'])
    
    tipo_t = np.where(df['CONTRATO'] == 627, "SN", "N")
    df['CONCAT_MATCHEO3'] = (anio_str + reso_str + df['SEC_FINAL'].astype(str) + df['GT'].astype(str) + df['ID_LINEA'].astype(str) + "S" + pd.Series(tipo_t, index=df.index).astype(str))
    
    df['FACTOR_CORR'] = np.select([df['ENERGIA'] == 1, df['ENERGIA'] == 2], [1.3, 1.5], default=1.0)
    return df

# =============================================================================
# 2. INTERFAZ STREAMLIT
# =============================================================================

st.sidebar.header("📅 Período")
mes_sel = st.sidebar.selectbox("Mes:", ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"], index=1)
anio_sel = st.sidebar.selectbox("Año:", [2025, 2026], index=1)

if 'df_tarifas' not in st.session_state: st.session_state.df_tarifas = None
if 'df_pme' not in st.session_state: st.session_state.df_pme = None

tab1, tab2, tab3 = st.tabs(["💰 1. TARIFAS", "📂 2. PRE-PROCESO", "🚀 3. TTR FINAL"])

with tab1:
    f_nov = st.file_uploader("Subir JN11 (Excel)", type=['xlsx'])
    if f_nov:
        c1, c2 = st.columns(2)
        n1 = c1.number_input("Valor 1SCN:", value=650.0)
        n2 = c2.number_input("Valor 2SCN:", value=724.09)
        if st.button("🔄 Generar Diccionario"):
            st.session_state.df_tarifas = proyectar_tarifas(pd.read_excel(f_nov, sheet_name='JN11'), {'1SCN': n1, '2SCN': n2})
            st.success("Diccionario listo.")

with tab2:
    col1, col2, col3 = st.columns(3)
    f_csv = col1.file_uploader("SUBE (ZIP/CSV)", type=['zip', 'csv'])
    f_gt = col2.file_uploader("Nomenclador (Excel)", type=['xlsx'])
    f_en = col3.file_uploader("Energías (Excel)", type=['xlsx'])
    if f_csv and f_gt and f_en:
        if st.button("🚀 Iniciar Pre-proceso"):
            with st.spinner("Procesando cruces..."):
                st.session_state.df_pme = preproceso_dmk_energias(f_csv, pd.read_excel(f_gt), pd.read_excel(f_en))
                st.success("Datos consolidados.")

with tab3:
    if st.session_state.df_pme is not None and st.session_state.df_tarifas is not None:
        ca, cr = st.columns(2)
        anio = ca.number_input("Año CONCAT:", value=anio_sel)
        reso = cr.text_input("Resolución CONCAT:", value="86")
        f_teorico = st.file_uploader("Excel Resoluciones (Opcional)", type=['xlsx'])
        if st.button("🚀 Liquidar TTR"):
            res = determinar_ttr_motor(st.session_state.df_pme, st.session_state.df_tarifas, anio, reso)
            if f_teorico:
                t_ref = pd.read_excel(f_teorico, sheet_name='TTR')
                res = pd.merge(res, t_ref[['CONCAT', 'TTR E.C.']], left_on='CONCAT_MATCHEO3', right_on='CONCAT', how='left')
                res['PAGO_FINAL'] = (res['TTR E.C.'] * res['CANTIDAD_USOS']) * res['FACTOR_CORR']
            st.dataframe(res.head(10))
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='xlsxwriter') as wr: res.to_excel(wr, index=False)
            st.download_button("📥 Bajar TTR Final", buf.getvalue(), "TTR_Final.xlsx")
    else:
        st.warning("⚠️ Completá pasos 1 y 2.")
