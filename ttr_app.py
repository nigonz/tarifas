import pandas as pd
import numpy as np
import streamlit as st
import io

# =============================================================================
# 1. MOTOR DE CÁLCULOS
# =============================================================================

def proyectar_tarifas_febrero(df_nov, nuevas_scn):
    try:
        v1_nov = df_nov.loc[df_nov['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = nuevas_scn['1SCN'] / v1_nov
    except:
        return df_nov, 1.0

    df = df_nov.copy()
    for i, row in df.iterrows():
        id_t = str(row['Id'])
        if id_t in nuevas_scn: v_final = nuevas_scn[id_t]
        elif 'SEN' in id_t and 'SESN' not in id_t: v_final = nuevas_scn.get(id_t.replace('SEN', 'SCN'), nuevas_scn['1SCN']) * 1.25
        elif 'SCSN' in id_t: v_final = nuevas_scn.get(id_t.replace('SCSN', 'SCN'), nuevas_scn['1SCN']) * 1.59
        elif 'SEAN' in id_t and 'SEASN' not in id_t: v_final = nuevas_scn.get(id_t.replace('SEAN', 'SCN'), nuevas_scn['1SCN']) * 1.75
        elif 'SESN' in id_t: v_final = (nuevas_scn.get(id_t.replace('SESN', 'SCN'), nuevas_scn['1SCN']) * 1.59) * 1.25
        elif 'SEASN' in id_t: v_final = (nuevas_scn.get(id_t.replace('SEASN', 'SCN'), nuevas_scn['1SCN']) * 1.59) * 1.75
        else: v_final = row['Limite Inferior'] * factor
        df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(v_final, 2)
    return df, factor

def preproceso_dmk_energias(f_csv, nom_gt, df_pme):
    # 1. Carga con auto-separador
    df = pd.read_csv(f_csv, encoding='ISO-8859-1', sep=None, engine='python')
    
    # Limpieza de columnas
    df.columns = df.columns.str.strip()
    df['ID_LINEA'] = df['ID_LINEA'].astype(str).str.strip()
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.strip()
    
    # 2. Filtrar líneas válidas
    df_ = df[df['ID_LINEA'].isin(nom_gt['ID_LINEA'])].copy()
    
    if df_.empty:
        st.error("No se encontraron coincidencias de ID_LINEA entre el CSV y el Nomenclador.")
        return pd.DataFrame()

    # 3. Traer datos del Nomenclador
    cols_nom = ['ID_LINEA', 'GT', 'Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']
    _df2_ = pd.merge(df_, nom_gt[cols_nom], how='left', on='ID_LINEA')
    _df2_.loc[_df2_['GT'] == 'DF', 'PROVINCIA'] = 'CABA'

    # 4. Separar por Energías
    df_pme['DOMINIO'] = df_pme['DOMINIO'].astype(str).str.strip().str.upper()
    _df2_['DOMINIO'] = _df2_['DOMINIO'].astype(str).str.strip().str.upper()
    dominios_especiales = df_pme['DOMINIO'].unique()

    # CASO A: Dominios que SÍ están en la lista de energías
    df_con_energia = _df2_[_df2_['DOMINIO'].isin(dominios_especiales)].copy()
    df_con_energia = df_con_energia.merge(df_pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')

    # CASO B: Resto (incluye gasoil y negativos)
    df_resto = _df2_[~_df2_['DOMINIO'].isin(dominios_especiales)].copy()
    df_resto['DOMINIO'] = 'NO'
    df_resto['ENERGIA'] = 3 # Gasoil

    # 5. Agrupar y Unir
    final = pd.concat([df_con_energia, df_resto], ignore_index=True)
    
    grupo = ['PROVINCIA', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'Linea SILAS DNGFF', 'ID_LINEA', 
             'RAMAL', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 
             'VIAJE INTEGRADO', 'DESCUENTO X INTEGRACION']
    
    final_res = final.groupby(grupo, as_index=False).agg({'CANTIDAD_USOS': 'sum', 'MONTO': 'sum'})

    # 6. Cálculos de Liquidación
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
st.title("Sistema TTR - Liquidación")

if 'df_tarifas' not in st.session_state: st.session_state['df_tarifas'] = None
if 'df_pme' not in st.session_state: st.session_state['df_pme'] = None

tab1, tab2, tab3 = st.tabs(["💰 TARIFAS", "📂 PRE-PROCESO PME", "🚀 DETERMINACIÓN TTR"])

with tab1:
    st.header("1. Escala Tarifaria")
    f_nov = st.file_uploader("Base Noviembre (Excel)", type=['xlsx'])
    if f_nov:
        c = st.columns(5)
        n1 = c[0].number_input("1SCN", value=650.0); n2 = c[1].number_input("2SCN", value=724.09)
        if st.button("🔄 Calcular"):
            df_n = pd.read_excel(f_nov, sheet_name='JN11')
            res, fac = proyectar_tarifas_febrero(df_n, {'1SCN':n1, '2SCN':n2})
            st.session_state['df_tarifas'] = res
            st.success(f"Diccionario calculado. Factor: {fac:.4f}")

with tab2:
    st.header("2. Pre-proceso DMK Energías")
    col1, col2, col3 = st.columns(3)
    f_csv = col1.file_uploader("Archivo DGGI (SUBE CSV)", type=['csv'])
    f_gt = col2.file_uploader("Nomenclador GT (Excel)", type=['xlsx'])
    f_en = col3.file_uploader("Parque Móvil (Excel)", type=['xlsx'])
    
    if f_csv and f_gt and f_en:
        if st.button("🚀 Iniciar Proceso"):
            with st.spinner("Procesando millones de filas..."):
                nom_gt = pd.read_excel(f_gt)
                df_en_data = pd.read_excel(f_en)
                res = preproceso_dmk_energias(f_csv, nom_gt, df_en_data)
                if not res.empty:
                    st.session_state['df_pme'] = res
                    st.success(f"¡Éxito! {len(res):,} registros procesados.")
                    st.dataframe(res.head())

    if st.session_state['df_pme'] is not None:
        buf = io.BytesIO()
        st.session_state['df_pme'].to_excel(buf, index=False)
        st.download_button("📥 Descargar dggi_DMK_PME_Final.xlsx", buf.getvalue(), "dggi_DMK_PME_Final.xlsx")
