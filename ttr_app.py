import pandas as pd
import numpy as np
import streamlit as st
import io
import zipfile

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Fiscalización TTR v2.2", layout="wide")

# =============================================================================
# 1. FUNCIONES CORE (LÓGICA DE NEGOCIO)
# =============================================================================

def proyectar_tarifas(df_nov, nuevas_scn):
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
            num = id_t[0] if id_t[0].isdigit() else "1"
            base = scn_full.get(f"{num}SCN", scn_full["1SCN"])
            if 'SESN' in id_t: v_final = (base * 1.59) * 1.25
            elif 'SEASN' in id_t: v_final = (base * 1.59) * 1.75
            elif 'SCSN' in id_t: v_final = base * 1.59
            elif 'SEN' in id_t: v_final = base * 1.25
            elif 'SEAN' in id_t: v_final = base * 1.75
        elif id_t in scn_full:
            v_final = scn_full[id_t]
        else:
            v_final = row['Limite Inferior'] * factor
        df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(v_final, 2)
    return df, factor

def preproceso_dmk_energias(f_input, nom_gt, df_en):
    # Manejo de ZIP o CSV
    if f_input.name.endswith('.zip'):
        with zipfile.ZipFile(f_input) as z:
            csv_file = [n for n in z.namelist() if n.endswith('.csv')][0]
            with z.open(csv_file) as f:
                df = pd.read_csv(f, encoding='ISO-8859-1', sep=None, engine='python')
    else:
        df = pd.read_csv(f_input, encoding='ISO-8859-1', sep=None, engine='python')

    df.columns = df.columns.str.strip().str.upper()
    nom_gt.columns = nom_gt.columns.str.upper()
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.strip().str.replace('.0', '', regex=False)
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.strip().str.replace('.0', '', regex=False)
    
    df_f = df[df['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()
    _df2 = pd.merge(df_f, nom_gt[['ID_LINEA', 'GT', 'LINEA SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']], on='ID_LINEA', how='left')
    
    df_en.columns = df_en.columns.str.upper()
    df_en['DOMINIO'] = df_en['DOMINIO'].astype(str).str.strip().str.upper()
    _df2['DOMINIO'] = _df2['DOMINIO'].astype(str).str.strip().str.upper()
    
    dom_esp = df_en['DOMINIO'].unique()
    df_con_en = _df2[_df2['DOMINIO'].isin(dom_esp)].copy()
    df_con_en = df_con_en.merge(df_en[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
    
    df_resto = _df2[~_df2['DOMINIO'].isin(dom_esp)].copy()
    df_resto['ENERGIA'] = 3
    
    final = pd.concat([df_con_en, df_resto], ignore_index=True)
    grupo = ['PROVINCIA', 'MUNICIPIO', 'GT', 'LINEA SILAS DNGFF', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION']
    res = final.groupby(grupo, as_index=False).agg({'CANTIDAD_USOS': 'sum', 'MONTO': 'sum'})
    return res

def determinar_ttr_motor(df_pme, df_tarifas, anio, reso):
    df = df_pme.copy()
    anio_str, reso_str = str(int(anio)), str(reso).strip()
    df['TARIFA BASE ITG'] = pd.to_numeric(df['TARIFA BASE ITG'], errors='coerce').round(2)
    df_tarifas['Limite Inferior'] = pd.to_numeric(df_tarifas['Limite Inferior'], errors='coerce').round(2)
    
    lookup = df_tarifas.drop_duplicates(subset=['Limite Inferior']).set_index('Limite Inferior')['Id'].to_dict()
    df['NODO_ID'] = df['TARIFA BASE ITG'].map(lookup).fillna("S/D")
    df['SEC_NUM'] = df['NODO_ID'].str.extract('(\d+)').fillna('0')
    df['SEC_FINAL'] = np.where((df['GT'] == "SGII") & (df['SEC_NUM'].isin(['1', '2', '3'])), '4', df['SEC_NUM'])
    
    tipo_t = np.where(df['CONTRATO'] == 627, "SN", "N")
    id_linea_clean = df['ID_LINEA'].astype(str).str.replace('.0', '', regex=False)
    
    df['CONCAT_MATCHEO3'] = (anio_str + reso_str + df['SEC_FINAL'].astype(str) + 
                             df['GT'].astype(str) + id_linea_clean + "S" + pd.Series(tipo_t).astype(str))
    df['FACTOR_CORR'] = np.select([df['ENERGIA'] == 1, df['ENERGIA'] == 2], [1.3, 1.5], default=1.0)
    return df

# =============================================================================
# 2. INTERFAZ DE USUARIO
# =============================================================================

st.sidebar.header("📅 Configuración")
meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
mes_sel = st.sidebar.selectbox("Mes de Cálculo:", meses, index=1)
anio_sel = st.sidebar.selectbox("Año:", [2025, 2026], index=1)

st.title(f"Fiscalización TTR - {mes_sel} {anio_sel}")

for key in ['df_tarifas', 'df_pme', 'ttr_final']:
    if key not in st.session_state: st.session_state[key] = None

t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📂 2. PRE-PROCESO PME", "🚀 3. DETERMINACIÓN TTR"])

with t1:
    f_nov = st.file_uploader("Subir Noviembre (Excel)", type=['xlsx'])
    if f_nov:
        c1, c2 = st.columns(2)
        n1 = c1.number_input("1SCN:", value=650.0)
        n2 = c2.number_input("2SCN:", value=724.09)
        if st.button("🔄 Calcular Diccionario"):
            df_n = pd.read_excel(f_nov, sheet_name='JN11')
            st.session_state.df_tarifas, _ = proyectar_tarifas(df_n, {'1SCN': n1, '2SCN': n2})
            st.success("Escala generada.")
    if st.session_state.df_tarifas is not None:
        buf1 = io.BytesIO()
        with pd.ExcelWriter(buf1, engine='xlsxwriter') as wr: st.session_state.df_tarifas.to_excel(wr, index=False)
        st.download_button("📥 Descargar Diccionario", buf1.getvalue(), f"Diccionario_{mes_sel}.xlsx")

with t2:
    st.header("Consolidación SUBE + Energías")
    c1, c2, c3 = st.columns(3)
    f_csv = c1.file_uploader("Archivo SUBE (CSV o ZIP)", type=['csv', 'zip'])
    f_gt = c2.file_uploader("Nomenclador GT (Excel)", type=['xlsx'])
    f_en = c3.file_uploader("Parque Energías (Excel)", type=['xlsx'])
    if f_csv and f_gt and f_en:
        if st.button("🚀 Iniciar Pre-proceso"):
            with st.spinner("Procesando datos pesados..."):
                st.session_state.df_pme = preproceso_dmk_energias(f_csv, pd.read_excel(f_gt), pd.read_excel(f_en))
                st.success(f"Finalizado: {len(st.session_state.df_pme)} filas resumidas.")
    if st.session_state.df_pme is not None:
        buf2 = io.BytesIO()
        with pd.ExcelWriter(buf2, engine='xlsxwriter') as wr: st.session_state.df_pme.to_excel(wr, index=False)
        st.download_button("📥 Descargar Base PME", buf2.getvalue(), f"Base_PME_{mes_sel}.xlsx")

with t3:
    if st.session_state.df_pme is not None and st.session_state.df_tarifas is not None:
        cc = st.columns(2)
        v_anio = cc[0].number_input("Año CONCAT:", value=anio_sel)
        v_reso = cc[1].text_input("Resolución CONCAT:", value="86")
        f_teorico = st.file_uploader("Excel Resoluciones (Opcional)", type=['xlsx'])
        if st.button("🚀 Calcular TTR Final"):
            res = determinar_ttr_motor(st.session_state.df_pme, st.session_state.df_tarifas, v_anio, v_reso)
            if f_teorico:
                ttr_ref = pd.read_excel(f_teorico, sheet_name='TTR')
                res = pd.merge(res, ttr_ref[['CONCAT', 'TTR E.C.']], left_on='CONCAT_MATCHEO3', right_on='CONCAT', how='left')
                res['PAGO_FINAL'] = (res['TTR E.C.'] * res['CANTIDAD_USOS']) * res['FACTOR_CORR']
            st.session_state.ttr_final = res
            st.dataframe(res.head(15))
            buf3 = io.BytesIO()
            with pd.ExcelWriter(buf3, engine='xlsxwriter') as wr: res.to_excel(wr, index=False)
            st.download_button("📥 Descargar TTR Final", buf3.getvalue(), f"TTR_FINAL_{mes_sel}.xlsx")
    else:
        st.warning("⚠️ Completa los pasos 1 y 2 para habilitar esta sección.")
