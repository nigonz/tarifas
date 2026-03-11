import pandas as pd
import numpy as np
import streamlit as st
import io

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="TTR Fiscalización", layout="wide")

# --- FUNCIONES CORE ---

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
        elif id_t in scn_full: v_final = scn_full[id_t]
        else: v_final = row['Limite Inferior'] * factor
        df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(v_final, 2)
    return df, factor

def preproceso_dmk_energias(f_csv, nom_gt, df_en):
    # Lectura robusta: detecta solo si es , o ;
    df = pd.read_csv(f_csv, encoding='ISO-8859-1', sep=None, engine='python')
    df.columns = df.columns.str.strip().str.upper()
    
    # Limpieza de IDs
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.strip().str.replace('.0', '', regex=False)
    nom_gt.columns = nom_gt.columns.str.upper()
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.strip().str.replace('.0', '', regex=False)
    
    # Filtrado y Cruce
    df_f = df[df['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()
    _df2 = pd.merge(df_f, nom_gt[['ID_LINEA', 'GT', 'LINEA SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']], on='ID_LINEA', how='left')
    
    # Energías
    df_en.columns = df_en.columns.str.upper()
    df_en['DOMINIO'] = df_en['DOMINIO'].astype(str).str.strip().str.upper()
    _df2['DOMINIO'] = _df2['DOMINIO'].astype(str).str.strip().str.upper()
    
    df_con_en = _df2[_df2['DOMINIO'].isin(df_en['DOMINIO'].unique())].copy()
    df_con_en = df_con_en.merge(df_en[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
    
    df_resto = _df2[~_df2['DOMINIO'].isin(df_en['DOMINIO'].unique())].copy()
    df_resto['ENERGIA'] = 3
    
    final = pd.concat([df_con_en, df_resto], ignore_index=True)
    
    # Agrupado para ahorrar memoria
    grupo = ['PROVINCIA', 'MUNICIPIO', 'GT', 'LINEA SILAS DNGFF', 'ID_LINEA', 'RAMAL', 'ENERGIA', 'CONTRATO', 'TARIFA BASE ITG']
    res = final.groupby(grupo, as_index=False).agg({'CANTIDAD_USOS': 'sum', 'MONTO': 'sum'})
    return res

def determinar_ttr_motor(df_pme, df_tarifas, anio, reso):
    df = df_pme.copy()
    df['TARIFA BASE ITG'] = pd.to_numeric(df['TARIFA BASE ITG'], errors='coerce').round(2)
    df_tarifas['Limite Inferior'] = pd.to_numeric(df_tarifas['Limite Inferior'], errors='coerce').round(2)
    
    # Ingeniería Inversa
    lookup = df_tarifas.drop_duplicates(subset=['Limite Inferior']).set_index('Limite Inferior')['Id'].to_dict()
    df['NODO_ID'] = df['TARIFA BASE ITG'].map(lookup).fillna("S/D")
    
    df['SEC_NUM'] = df['NODO_ID'].str.extract('(\d+)').fillna('0')
    df['SEC_FINAL'] = np.where((df['GT'] == "SGII") & (df['SEC_NUM'].isin(['1', '2', '3'])), '4', df['SEC_NUM'])
    
    # CONCAT_MATCHEO3
    tipo_t = np.where(df['CONTRATO'] == 627, "SN", "N")
    df['CONCAT_MATCHEO3'] = (str(int(anio)) + str(reso) + df['SEC_FINAL'].astype(str) + 
                             df['GT'].astype(str) + df['ID_LINEA'].astype(str) + "S" + pd.Series(tipo_t).astype(str))
    
    # Factor Energía
    df['FACTOR_CORR'] = np.select([df['ENERGIA'] == 1, df['ENERGIA'] == 2], [1.3, 1.5], default=1.0)
    return df

# --- INTERFAZ ---
st.title("Fiscalización TTR v2.1")
if 'df_tarifas' not in st.session_state: st.session_state.df_tarifas = None
if 'df_pme' not in st.session_state: st.session_state.df_pme = None

t1, t2, t3 = st.tabs(["💰 1. TARIFAS", "📂 2. PRE-PROCESO", "🚀 3. TTR FINAL"])

with t1:
    f_nov = st.file_uploader("Subir Noviembre (Excel)", type=['xlsx'])
    if f_nov:
        c1, c2 = st.columns(2)
        n1 = c1.number_input("1SCN:", value=650.0)
        n2 = c2.number_input("2SCN:", value=724.09)
        if st.button("🔄 Calcular"):
            df_n = pd.read_excel(f_nov, sheet_name='JN11')
            st.session_state.df_tarifas, _ = proyectar_tarifas(df_n, {'1SCN': n1, '2SCN': n2})
            st.success("Tarifas listas.")

with t2:
    col1, col2, col3 = st.columns(3)
    f_csv = col1.file_uploader("DGGI (CSV)", type=['csv'])
    f_gt = col2.file_uploader("Nomenclador GT", type=['xlsx'])
    f_en = col3.file_uploader("Energías", type=['xlsx'])
    if f_csv and f_gt and f_en:
        if st.button("🚀 Iniciar"):
            st.session_state.df_pme = preproceso_dmk_energias(f_csv, pd.read_excel(f_gt), pd.read_excel(f_en))
            st.success(f"Procesado: {len(st.session_state.df_pme)} filas.")

with t3:
    if st.session_state.df_pme is not None and st.session_state.df_tarifas is not None:
        cc = st.columns(2)
        v_anio = cc[0].number_input("Año:", value=2026)
        v_reso = cc[1].text_input("Reso:", value="86")
        if st.button("🚀 Calcular TTR"):
            res = determinar_ttr_motor(st.session_state.df_pme, st.session_state.df_tarifas, v_anio, v_reso)
            st.dataframe(res.head(10))
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='xlsxwriter') as wr: res.to_excel(wr, index=False)
            st.download_button("📥 Bajar Reporte", buf.getvalue(), "TTR_Final.xlsx")
