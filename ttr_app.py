import pandas as pd
import numpy as np
import streamlit as st
import xlsxwriter
import io

# --- SISTEMA DE SEGURIDAD (EL PATOVICA) ---
if "acceso_concedido" not in st.session_state:
    st.session_state["acceso_concedido"] = False

if not st.session_state["acceso_concedido"]:
    st.title("🔒 Acceso Restringido")
    
    # Intentamos leer la clave de los secretos
    try:
        clave_maestra = st.secrets["CLAVE_SECRETA"]
    except:
        clave_maestra = "admin123" # Clave temporal si no hay Secrets

    clave_ingresada = st.text_input("Contraseña:", type="password")
    
    if st.button("Entrar"):
        if clave_ingresada == clave_maestra:
            st.session_state["acceso_concedido"] = True
            st.rerun()
        else:
            st.error("❌ Contraseña incorrecta")
    
    # IMPORTANTE: El stop debe estar aquí para frenar el resto de la app
    st.stop()

# =========================================================
# TODO TU CÓDIGO ORIGINAL DEBE EMPEZAR AQUÍ ABAJO
# Asegurate de que NO tenga espacios al principio de cada línea
# =========================================================

st.title("Procedimiento de Macheo TTR")
# ... sigue el resto de tu código ...

# =========================================================
# SI EL CÓDIGO LLEGA HASTA ACÁ, ES PORQUE PUSO LA CLAVE BIEN
# TODO TU CÓDIGO ORIGINAL QUEDA ABAJO DE ESTA LÍNEA INTACTO
# =========================================================
# 
# # =============================================================================================================================================0
# # 1. HERRAMIENTA DF (DISTRITO FEDERAL)
# # =============================================================================
def tool_procesar_df(archivo_base, archivo_nom_ts, archivo_nom_gt, archivo_ttr, archivo_diccionario, anio):
    df1 = pd.read_excel(archivo_base, sheet_name='Base')
    nom_ts = pd.read_excel(archivo_nom_ts)
    nom_gt = pd.read_excel(archivo_nom_gt)
    ttr_reso = pd.read_excel(archivo_ttr, sheet_name='TTR')

    var_input = ['Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO', 'GT', 'ID_EMPRESA', 'ID_LINEA', 'RAMAL', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO','CANTIDAD_USOS']
    df2 = df1[var_input].copy()
    df2 = df2[df2['GT'].isin(["DF"])]

    _df2_ = pd.merge(df2, nom_gt[['ID_LINEA', 'GT']], how='left', left_on='ID_LINEA', right_on='ID_LINEA')
    _df2_['CANTIDAD_USOS'] = pd.to_numeric(_df2_['CANTIDAD_USOS'].astype(str).replace({',': ''}, regex=True), errors='coerce').fillna(0)
    _df2_['TARIFA BASE ITG'] = pd.to_numeric(_df2_['TARIFA BASE ITG'].astype(str).replace({',': ''}, regex=True), errors='coerce').fillna(0)

    _df2_.drop('GT_y', axis=1, inplace=True)
    _df2_.rename(columns={"GT_x": "GT"}, inplace=True)

    _df2_['Linea SILAS DNGFF'] = _df2_['Linea SILAS DNGFF'].astype(str)
    _df2_["RAMAL"] = _df2_["RAMAL"].astype(float).astype(int).astype(str)
    nom_ts["IdRamalNS"] = nom_ts["IdRamalNS"].astype(str).str.strip()

    _df2_ = pd.merge(_df2_, nom_ts[['IdRamalNS', 'TIPO DE SERVICIO FINAL']], how='left', left_on='RAMAL', right_on='IdRamalNS')
    _df2_.rename(columns={'TIPO DE SERVICIO FINAL': 'TipoServicio'}, inplace=True)
    _df2_.drop('IdRamalNS', axis=1, inplace=True)

    _df2_['sin_nominalizar'] = _df2_['CONTRATO'].apply(lambda x: 1 if x == 627 else 0)
    _df2_['PASES'] = _df2_['TARIFA BASE ITG'].apply(lambda x: 1 if 0 <= x <= 0.5 else 0)
    _df2_['FILTRO_1'] = np.where((_df2_['TARIFA BASE ITG'] < 525.65) & (_df2_['TARIFA BASE ITG'] > 0.5), 1, 0)
    _df2_['TARIFA BASE ITG'] = _df2_['TARIFA BASE ITG'].round(3)

    # Diccionario DF
    df_completo = pd.read_excel(archivo_diccionario, sheet_name='DF01')
    df_completo.columns = df_completo.columns.str.strip()
    if 'id' in df_completo.columns: df_completo.rename(columns={'id': 'Id'}, inplace=True)
    df_completo['Id'] = df_completo['Id'].astype(str).str.strip()
    for col in ['Minimo', 'Maximo']:
        df_completo[col] = pd.to_numeric(df_completo[col].astype(str).replace({',': ''}, regex=True), errors='coerce')

    df_tarifas_1 = df_completo.iloc[0:15]
    tarifas_1 = dict(zip(df_tarifas_1['Id'], zip(df_tarifas_1['Minimo'], df_tarifas_1['Maximo'])))
    for col, (lim_inf, lim_sup) in tarifas_1.items():
        _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1), 1, 0)

    df_tarifas_2 = df_completo.iloc[15:30]
    tarifas_2 = dict(zip(df_tarifas_2['Id'], zip(df_tarifas_2['Minimo'], df_tarifas_2['Maximo'])))
    for col, (lim_inf, lim_sup) in tarifas_2.items():
        _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1), 1, 0)

    _df2_['sec_c'] = np.where((_df2_[['1SCN', '2SCN', '3SCN', '4SCN', '5SCN']].sum(axis=1) > 0) | (_df2_[['1SCSN', '2SCSN', '3SCSN', '4SCSN', '5SCSN']].sum(axis=1) > 0), 1, 0)
    _df2_['sec_e'] = np.where((_df2_[['1SEN', '2SEN', '3SEN', '4SEN', '5SEN']].sum(axis=1) > 0) | (_df2_[['1SESN', '2SESN', '3SESN', '4SESN', '5SESN']].sum(axis=1) > 0), 1, 0)
    _df2_['sec_ea'] = np.where((_df2_[['1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']].sum(axis=1) > 0) | (_df2_[['1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']].sum(axis=1) > 0), 1, 0)

    _df2_['norm_por_tarifa'] = np.where((_df2_[['1SCN', '2SCN', '3SCN', '4SCN', '5SCN', '1SEN', '2SEN', '3SEN', '4SEN', '5SEN', '1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']].sum(axis=1) > 0), "N", np.where(_df2_['FILTRO_1'] == 1, "Tarifa Vieja", np.where(_df2_['PASES'] == 1, "N", "SN")))
    _df2_['tarifa_PASE'] = np.where((_df2_['PASES'] == 1), 1, 0)
    _df2_['compilado_tt'] = np.where(_df2_['tarifa_PASE'] != 0, 'P', 'S')

    _df2_['sec_1'] = np.where((_df2_[['1SCN', '1SCSN', '1SEN', '1SESN', '1SEAN', '1SEASN']].sum(axis=1) > 0), 1, 0)
    _df2_['sec_2'] = np.where((_df2_[['2SCN', '2SCSN', '2SEN', '2SESN', '2SEAN', '2SEASN']].sum(axis=1) > 0), 2, 0)
    _df2_['sec_3'] = np.where((_df2_[['3SCN', '3SCSN', '3SEN', '3SESN', '3SEAN', '3SEASN']].sum(axis=1) > 0), 3, 0)
    _df2_['sec_4'] = np.where((_df2_[['4SCN', '4SCSN', '4SEN', '4SESN', '4SEAN', '4SEASN']].sum(axis=1) > 0), 4, 0)
    _df2_['sec_5'] = np.where((_df2_[['5SCN', '5SCSN', '5SEN', '5SESN', '5SEAN', '5SEASN']].sum(axis=1) > 0), 5, 0)

    _df2_['seccionadas_final'] = np.where(_df2_['PASES'] == 1, 1, _df2_[['sec_1', 'sec_2', 'sec_3', 'sec_4', 'sec_5']].sum(axis=1))
    _df2_['compilado_seccion'] = np.where(_df2_['compilado_tt'] == "S", _df2_['seccionadas_final'], np.where(_df2_['compilado_tt'] == "P", 1, 0))

    _df2_.rename(columns={'compilado_seccion': "final_seccion", 'TipoServicio': "compilado_ts"}, inplace=True)
    _df2_['CONCAT_MACHEO'] = (_df2_['final_seccion'].astype(int).astype(str) + _df2_['GT'].astype(str) + _df2_['compilado_ts'].astype(str) + _df2_['norm_por_tarifa'].astype(str))

    _df2_['Año'] = '2025' # O el año que diga el Excel de la Reso 36
    _df2_['Resolucion'] = '36'
    _df2_['CONCAT_MACHEO2'] = (_df2_['Año'].astype(str) + _df2_['Resolucion'].astype(str) + _df2_['final_seccion'].astype(int).astype(str) + _df2_['GT'].astype(str) + _df2_['compilado_ts'].astype(str) + _df2_['norm_por_tarifa'].astype(str))
    _df2_['CONCAT_MACHEO3'] = (_df2_['Año'].astype(str) + _df2_['Resolucion'].astype(str) + _df2_['final_seccion'].astype(int).astype(str) + _df2_['GT'].astype(str) + _df2_['ID_LINEA'].astype(str) + _df2_['compilado_ts'].astype(str) + _df2_['norm_por_tarifa'].astype(str))

    _df2_ = pd.merge(_df2_, ttr_reso[['CONCAT', 'TTR E.C.']], how='left', left_on='CONCAT_MACHEO2', right_on='CONCAT').fillna({'TTR E.C.': 0})
    _df2_.rename(columns={"TTR E.C.": "Tarifa TRSUBE"}, inplace=True)
    _df2_.drop(columns=['CONCAT'], inplace=True)
    _df2_['Recaudacion_TRSUBE'] = _df2_['Tarifa TRSUBE'] * _df2_['CANTIDAD_USOS']

    return _df2_
# 
# # =====================================================================================================================================================================================================
# 
# # 2. HERRAMIENTA JN (NACIÓN) - CORREGIDA
# # =============================================================================
def tool_procesar_jn(archivo_base, archivo_nom_ts, archivo_nom_gt, archivo_ttr, archivo_diccionario, anio):
    df1 = pd.read_excel(archivo_base, sheet_name="Base")
    nom_ts = pd.read_excel(archivo_nom_ts)
    nom_gt = pd.read_excel(archivo_nom_gt)
    ttr_reso = pd.read_excel(archivo_ttr, sheet_name='TTR')
    ttr_sgii_uma2 = pd.read_excel(archivo_ttr, sheet_name='SGII-UMA2')

    var_input = ['Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO', 'GT', 'ID_EMPRESA', 'ID_LINEA', 'RAMAL', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO','CANTIDAD_USOS']
    _df2 = df1[var_input][df1['GT'].isin(["SGI", "SGII", "SGIKM"])].copy()

    _df2_ = pd.merge(_df2, nom_gt[['ID_LINEA', 'GT']], how='left', left_on='ID_LINEA', right_on='ID_LINEA')
    _df2_['CANTIDAD_USOS'] = pd.to_numeric(_df2_['CANTIDAD_USOS'].astype(str).replace({',': ''}, regex=True), errors='coerce').fillna(0)
    _df2_['TARIFA BASE ITG'] = pd.to_numeric(_df2_['TARIFA BASE ITG'].astype(str).replace({',': ''}, regex=True), errors='coerce').fillna(0).round(3)
    _df2_.drop('GT_y', axis=1, inplace=True)
    _df2_.rename(columns={"GT_x": "GT"}, inplace=True)

    _df2_['Linea SILAS DNGFF'] = _df2_['Linea SILAS DNGFF'].astype(str)
    lineas_la_plata = ['LP506', 'LP504', 'LP508', 'LP561', 'LP501', 'LP502', 'LP520', 'LP518', 'LP53A', 'LP53B', '275', '307', '202', '215', '225', '214', '273', '414', '418']
    _df2_['LaPlata'] = _df2_['Linea SILAS DNGFF'].apply(lambda x: 1 if x in lineas_la_plata else 0)

    _df2_["RAMAL"] = _df2_["RAMAL"].astype(str)
    nom_ts["IdRamalNS"] = nom_ts["IdRamalNS"].astype(str)
    _df2_ = pd.merge(_df2_, nom_ts[['IdRamalNS', 'TIPO DE SERVICIO FINAL']], how='left', left_on='RAMAL', right_on='IdRamalNS')
    _df2_.rename(columns={'TIPO DE SERVICIO FINAL': 'TipoServicio'}, inplace=True)
    _df2_['TipoServicio2'] = _df2_['TipoServicio'].replace('SR', 'E')

    _df2_['sin_nominalizar'] = _df2_['CONTRATO'].apply(lambda x: 1 if x == 627 else 0)
    _df2_['PASES'] = _df2_['TARIFA BASE ITG'].apply(lambda x: 1 if 0 <= x <= 0.5 else 0)
    _df2_['FILTRO_1'] = np.where((_df2_['TARIFA BASE ITG'] < 450.51) & (_df2_['TARIFA BASE ITG'] > 0.5), 1, 0)

    # -------------------------------------------------------------
   # RESTAURAMOS LÓGICA MIXTA: EXCEL + HARDCODED (Como tu original)
   # -------------------------------------------------------------
    df_completo = pd.read_excel(archivo_diccionario, sheet_name='JN')
    df_completo.columns = df_completo.columns.str.strip()
    if 'id' in df_completo.columns: df_completo.rename(columns={'id': 'Id'}, inplace=True)
    df_completo['Id'] = df_completo['Id'].astype(str).str.strip()
    for col in ['Minimo', 'Maximo']: df_completo[col] = pd.to_numeric(df_completo[col].astype(str).replace({',': ''}, regex=True), errors='coerce')

    # TARIFA 1 (Desde Excel)
    df_t1 = df_completo.iloc[0:15]
    tarifas_1 = dict(zip(df_t1['Id'], zip(df_t1['Minimo'], df_t1['Maximo'])))
    for col, (lim_inf, lim_sup) in tarifas_1.items():
        _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_['TipoServicio'] != "SR") & (_df2_['LaPlata'] != 1), 1, 0)

    # TARIFA 2 (Desde Excel)
    df_t2 = df_completo.iloc[15:30].round(2)
    tarifas_2 = dict(zip(df_t2['Id'], zip(df_t2['Minimo'], df_t2['Maximo'])))
    for col, (lim_inf, lim_sup) in tarifas_2.items():
        if pd.notna(lim_inf) and pd.notna(lim_sup):
            _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_['TipoServicio'] != "SR") & (_df2_['LaPlata'] != 1), 1, 0)

    # TARIFA 3 (Desde Excel)
    mapa_servicios_t3 = {'1-4KMCN': 'C', '1-4KMEN': 'E', '1-4KMEAN': 'EA'}
    df_t3 = df_completo[df_completo['Id'].isin(mapa_servicios_t3.keys())].drop_duplicates(subset='Id')
    tarifas_3 = {row['Id']: (row['Minimo'], row['Maximo'], mapa_servicios_t3[row['Id']]) for _, row in df_t3.iterrows()}
    for col, (lim_inf, lim_sup, t_serv) in tarifas_3.items():
        if pd.notna(lim_inf) and pd.notna(lim_sup):
            _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_['TipoServicio2'] == t_serv), 1, 0)

    # TARIFA 4 (Desde Excel)
    mapa_servicios_t4 = {'1-4KMCSN': 'C', '1-4KMESN': 'E', '1-4KMEASN': 'EA'}
    df_t4 = df_completo[df_completo['Id'].isin(mapa_servicios_t4.keys())].drop_duplicates(subset='Id')
    tarifas_4 = {row['Id']: (row['Minimo'], row['Maximo'], mapa_servicios_t4[row['Id']]) for _, row in df_t4.iterrows()}
    for col, (lim_inf, lim_sup, t_serv) in tarifas_4.items():
        if pd.notna(lim_inf) and pd.notna(lim_sup):
            _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_['TipoServicio2'] == t_serv), 1, 0)

    # TARIFAS 5 a 10 + Rebotes (100% HARDCODED restauradas de tu código original)
    tarifas_5 = {'1SCNLP': (768.87 ,1181.41), '2SCNLP': (1181.41, 1525.2), '3SCNLP': (1525.2, 1868.99), '4SCNLP': (1868.99, 2212.78), '5SCNLP': (2212.78, 3587.94), '1SENLP': (505.2, 505.24), '2SENLP': (551.56, 551.61), '3SENLP': (596.8, 596.84), '4SENLP': (639.18, 639.2), '5SENLP': (674.4, 674.44)}
    for col, (lim_inf, lim_sup) in tarifas_5.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_['LaPlata'] == 1), 1, 0)

    tarifas_6 = {'1SCSNLP': (642.66, 642.66), '2SCSNLP': (701.66, 701.66), '3SCSNLP': (759.18, 759.2), '4SCSNLP': (813, 813.03), '5SCSNLP': (857.9, 857.92), '1SESNLP': (803.3, 803.33), '2SESNLP': (877, 877.06), '3SESNLP': (948.98, 949), '4SESNLP': (1016.27, 1016.3), '5SESNLP': (1072.36, 1072.36)}
    for col, (lim_inf, lim_sup) in tarifas_6.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_['LaPlata'] == 1), 1, 0)

    mapa_secciones = {'5KPCN': ('C', 5), '6KPCN': ('C', 6), '7KPCN': ('C', 7), '8KPCN': ('C', 8), '9KPCN': ('C', 9), '5KPEN': ('E', 5), '6KPEN': ('E', 6), '7KPEN': ('E', 7), '8KPEN': ('E', 8), '9KPEN': ('E', 9), '5KPEAN': ('EA', 5), '6KPEAN': ('EA', 6), '7KPEAN': ('EA', 7), '8KPEAN': ('EA', 8), '9KPEAN': ('EA', 9)}
    df_t7 = df_completo[df_completo['Id'].isin(mapa_secciones.keys())].drop_duplicates(subset='Id')
    tarifas_7 = {row['Id']: (row['Minimo'], row['Maximo'], mapa_secciones[row['Id']][0], mapa_secciones[row['Id']][1]) for _, row in df_t7.iterrows()}
    for col, (lim_inf, lim_sup, t_serv, val_asig) in tarifas_7.items():
        if pd.notna(lim_inf) and pd.notna(lim_sup): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] < lim_sup - 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_['TipoServicio2'] == t_serv), val_asig, 0)

    mapa_secciones_t8 = {'5KPCSN': ('C', 5), '6KPCSN': ('C', 6), '7KPCSN': ('C', 7), '8KPCSN': ('C', 8), '9KPCSN': ('C', 9), '5KPESN': ('E', 5), '6KPESN': ('E', 6), '7KPESN': ('E', 7), '8KPESN': ('E', 8), '9KPESN': ('E', 9), '5KPEASN': ('EA', 5), '6KPEASN': ('EA', 6), '7KPEASN': ('EA', 7), '8KPEASN': ('EA', 8), '9KPEASN': ('EA', 9)}
    df_t8 = df_completo[df_completo['Id'].isin(mapa_secciones_t8.keys())].drop_duplicates(subset='Id')
    tarifas_8 = {row['Id']: (row['Minimo'], row['Maximo'], mapa_secciones_t8[row['Id']][0], mapa_secciones_t8[row['Id']][1]) for _, row in df_t8.iterrows()}
    for col, (lim_inf, lim_sup, t_serv, val_asig) in tarifas_8.items():
        if pd.notna(lim_inf) and pd.notna(lim_sup): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] < lim_sup - 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_['TipoServicio2'] == t_serv), val_asig, 0)

    tarifas_9 = {'1SRN': (595.67,595.67, "SR"), '2SRN': (620.15, 620.15, "SR"), '3SRN': (667.9, 667.94, "SR"), '4SRN': (715.76,715.76, "SR"), '5SRN': (763.24, 763.24, "SR")}
    for col, (lim_inf, lim_sup, t_serv) in tarifas_9.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_['TipoServicio'] == t_serv), 1, 0)

    tarifas_10 = {'1SRSN': (885.15, 885.15, "SR"), '2SRSN': (986, 986.05, "SR"), '3SRSN': (1062, 1062.05, "SR"), '4SRSN': (1138, 1138.06, "SR"), '5SRSN': (1213.55, 1213.55, "SR")}
    for col, (lim_inf, lim_sup, t_serv) in tarifas_10.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_['TipoServicio'] == t_serv), 1, 0)

    tarifas_cn = { '1-4KMCN2': (651.02, 843.57) }
    tarifas_en = { '1-4KMEN2': (813.78, 1054.46) }
    tarifas_ean = { '1-4KMEAN2': (1139.29, 1476.25) }
    tarifas_cn_2 = { '1-4KMCSN2': (1035.12,1341.28) }
    tarifas_en_2 = { '1-4KMESN2': (1293.90, 1676.59) }
    tarifas_ean_2 = { '1-4KMEASN2': (1811.46,2347.23) }

#     # -------------------------------------------------------------
#     # CONTINÚA EL RESTO DE LA LÓGICA DE BANDERAS Y AGRUPADORES
#     # -------------------------------------------------------------
    columnas_sn_cn = ['1SCN', '2SCN', '3SCN', '4SCN', '5SCN']
    columnas_kmn_cn = ['1-4KMCN']
    columnas_snlp_cn = ['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP']
    columnas_sn_en = ['1SEN', '2SEN', '3SEN', '4SEN', '5SEN']
    columnas_kmn_en = ['1-4KMEN']
    columnas_kpn_en = ['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN']
    columnas_snlp_en = ['1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP']
    columnas_sn_ean = ['1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']
    columnas_kmn_ean = ['1-4KMEAN']
    columnas_kpn_ean = ['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN', '5KPEN', '6KPEN', '7KPEN', '8KPEN', '9KPEN']
    columnas_snlp_ean = ['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP', '1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP' ,'1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP', '1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']
    columnas_srn_srsn = ['1SRN', '2SRN', '3SRN', '4SRN', '5SRN', '1SRSN', '2SRSN', '3SRSN', '4SRSN', '5SRSN']

    for col, (lim_inf, lim_sup) in tarifas_cn.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_[columnas_sn_cn].sum(axis=1) == 0) & (_df2_[columnas_snlp_cn].sum(axis=1) == 0) & (_df2_[columnas_kmn_cn].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)
    for col, (lim_inf, lim_sup) in tarifas_en.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_[columnas_sn_en].sum(axis=1) == 0) & (_df2_[columnas_snlp_en].sum(axis=1) == 0) & (_df2_[columnas_kmn_en].sum(axis=1) == 0) & (_df2_[columnas_kpn_en].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)
    for col, (lim_inf, lim_sup) in tarifas_ean.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_[columnas_sn_ean].sum(axis=1) == 0) & (_df2_[columnas_snlp_ean].sum(axis=1) == 0) & (_df2_[columnas_kmn_ean].sum(axis=1) == 0) & (_df2_[columnas_kpn_ean].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)

    columnas_sn_cSn = ['1SCSN', '2SCSN', '3SCSN', '4SCSN', '5SCSN']
    columnas_kmn_cSn = ['1-4KMCSN']
    columnas_snlp_cSn = ['1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP']
    columnas_sn_eSn = ['1SESN', '2SESN', '3SESN', '4SESN', '5SESN']
    columnas_kmn_eSn = ['1-4KMESN']
    columnas_kpn_eSn = ['5KPCSN', '6KPCSN','7KPCSN','8KPCSN','9KPCSN']
    columnas_snlp_eSn = ['1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']
    columnas_sn_eaSn = ['1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']
    columnas_kmn_eaSn = ['1-4KMEASN']
    columnas_kpn_eaSn = ['5KPCSN', '6KPCSN', '7KPCSN', '8KPCSN', '9KPCSN', '5KPESN', '6KPESN', '7KPESN', '8KPESN', '9KPESN']
    columnas_snlp_eaSn = ['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP', '1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP' ,'1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP', '1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']

    for col, (lim_inf, lim_sup) in tarifas_cn_2.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_[columnas_sn_cSn].sum(axis=1) == 0) & (_df2_[columnas_snlp_cSn].sum(axis=1) == 0) & (_df2_[columnas_kmn_cSn].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)
    for col, (lim_inf, lim_sup) in tarifas_en_2.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_[columnas_sn_eSn].sum(axis=1) == 0) & (_df2_[columnas_snlp_eSn].sum(axis=1) == 0) & (_df2_[columnas_kmn_eSn].sum(axis=1) == 0) & (_df2_[columnas_kpn_eSn].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)
    for col, (lim_inf, lim_sup) in tarifas_ean_2.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_[columnas_sn_eaSn].sum(axis=1) == 0) & (_df2_[columnas_snlp_eaSn].sum(axis=1) == 0) & (_df2_[columnas_kmn_eaSn].sum(axis=1) == 0) & (_df2_[columnas_kpn_eaSn].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)

    _df2_['Filtro1-4KMCN'] = np.where((_df2_['TipoServicio2'] == 'C') & (_df2_[['1SEN', '2SEN', '3SEN', '4SEN', '5SEN', '1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']].sum(axis=1) != 0) & (_df2_['LaPlata'] == 0) & (_df2_['1-4KMCN2'] == 1), 4, 0)
    _df2_['Filtro1-4KMEN'] = np.where((_df2_['TipoServicio2'] == 'E') & (_df2_[['1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']].sum(axis=1) != 0)& (_df2_['LaPlata'] == 0) & (_df2_['1-4KMEN2'] == 1), 4, 0)
    _df2_['Filtro1-4KMEAN'] = np.where((_df2_['TipoServicio2'] == 'EA') & (_df2_['LaPlata'] == 0) & (_df2_['1-4KMEAN2'] == 1), 4, 0)
    _df2_['Filtro1-4KMCSN'] = np.where((_df2_['TipoServicio2'] == 'C') & (_df2_[['1SESN', '2SESN', '3SESN', '4SESN', '5SESN', '1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']].sum(axis=1) != 0)& (_df2_['LaPlata'] == 0) & (_df2_['1-4KMCSN2'] == 1), 4, 0)
    _df2_['Filtro1-4KMESN'] = np.where((_df2_['TipoServicio2'] == 'E') & (_df2_[['1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']].sum(axis=1) != 0)& (_df2_['LaPlata'] == 0) & (_df2_['1-4KMESN2'] == 1), 4, 0)
    _df2_['Filtro1-4KMEASN'] = np.where((_df2_['TipoServicio2'] == 'EA') & (_df2_['LaPlata'] == 0) & (_df2_['1-4KMEASN2'] == 1), 4, 0)

    _df2_['seccionada_correcta_1'] = np.select([(_df2_['1-4KMCN2'] == 1) & (_df2_['Filtro1-4KMCN'] != 4) & ((_df2_['1SEN'] == 1) | (_df2_['1SENLP'] == 1)), (_df2_['1-4KMCN2'] == 1) & (_df2_['Filtro1-4KMCN'] != 4) & ((_df2_['2SEN'] == 1) | (_df2_['2SENLP'] == 1)), (_df2_['1-4KMCN2'] == 1) & (_df2_['Filtro1-4KMCN'] != 4) & ((_df2_['3SEN'] == 1) | (_df2_['3SENLP'] == 1)), (_df2_['1-4KMCN2'] == 1) & (_df2_['Filtro1-4KMCN'] != 4) & ((_df2_['4SEN'] == 1) | (_df2_['4SENLP'] == 1)), (_df2_['1-4KMCN2'] == 1) & (_df2_['Filtro1-4KMCN'] != 4) & ((_df2_['5SEN'] == 1) | (_df2_['5SENLP'] == 1))], [1, 2, 3, 4, 5], default=0)
    _df2_['seccionada_correcta_3'] = np.select([(_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['1-4KMCSN2'] == 1)& ((_df2_['1SESN'] == 1) | (_df2_['1SESNLP'] == 1)), (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['1-4KMCSN2'] == 1) &  ((_df2_['2SESN'] == 1) | (_df2_['2SESNLP'] == 1)), (_df2_['Filtro1-4KMCSN'] != 4) &(_df2_['1-4KMCSN2'] == 1) &  ((_df2_['3SESN'] == 1) | (_df2_['3SESNLP'] == 1)), (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['1-4KMCSN2'] == 1) & ((_df2_['4SESN'] == 1) | (_df2_['4SESNLP'] == 1)), (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['1-4KMCSN2'] == 1) &((_df2_['5SESN'] == 1) | (_df2_['5SESNLP'] == 1))], [1, 2, 3, 4, 5], default=0)
    _df2_['seccionada_correcta_2'] = np.select([(_df2_['1-4KMEN2'] == 1) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['1SEAN'] == 1), (_df2_['1-4KMEN2'] == 1) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['2SEAN'] == 1) , (_df2_['1-4KMEN2'] == 1) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['3SEAN'] == 1) , (_df2_['1-4KMEN2'] == 1) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['4SEAN'] == 1) , (_df2_['1-4KMEN2'] == 1) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['5SEAN'] == 1)], [1, 2, 3, 4, 5], default=0)
    _df2_['seccionada_correcta_4'] = np.select([(_df2_['1-4KMESN2'] == 1) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['1SEASN'] == 1), (_df2_['1-4KMESN2'] == 1) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['2SEASN'] == 1), (_df2_['1-4KMESN2'] == 1) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['3SEASN'] == 1), (_df2_['1-4KMESN2'] == 1) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['4SEASN'] == 1), (_df2_['1-4KMESN2'] == 1) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['5SEASN'] == 1)], [1, 2, 3, 4, 5], default=0)

    _df2_['sec_c'] = np.where((_df2_[['1SCN', '2SCN', '3SCN', '4SCN', '5SCN']].sum(axis=1) > 0) | (_df2_[['1SCSN', '2SCSN', '3SCSN', '4SCSN', '5SCSN']].sum(axis=1) > 0) | (_df2_['1-4KMCN'] > 0) | (_df2_['1-4KMCSN'] > 0) | (_df2_[['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP']].sum(axis=1) > 0) | (_df2_[['1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP']].sum(axis=1) > 0), 1, 0)
    _df2_['sec_e'] = np.where((_df2_[['1SEN', '2SEN', '3SEN', '4SEN', '5SEN']].sum(axis=1) > 0) | (_df2_[['1SESN', '2SESN', '3SESN', '4SESN', '5SESN']].sum(axis=1) > 0) | (_df2_['1-4KMEN'] > 0) | (_df2_['1-4KMESN'] > 0) | (_df2_[['1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP']].sum(axis=1) > 0) | (_df2_[['1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']].sum(axis=1) > 0), 1, 0)
    _df2_['sec_ea'] = np.where((_df2_[['1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']].sum(axis=1) > 0) | (_df2_[['1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']].sum(axis=1) > 0) | (_df2_['1-4KMEAN'] > 0) | (_df2_['1-4KMEASN'] > 0), 1, 0)

    _df2_['km&p_c'] = np.where((_df2_[['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN']].sum(axis=1) > 0) | (_df2_[['5KPCSN', '6KPCSN', '7KPCSN', '8KPCSN', '9KPCSN']].sum(axis=1) > 0) | (_df2_['1-4KMCN2'] > 0) | (_df2_['1-4KMCSN2'] > 0), 1, 0)
    _df2_['km&p_e'] = np.where((_df2_[['5KPEN', '6KPEN', '7KPEN', '8KPEN', '9KPEN']].sum(axis=1) > 0) | (_df2_[['5KPESN', '6KPESN', '7KPESN', '8KPESN', '9KPESN']].sum(axis=1) > 0) | (_df2_['1-4KMEN2'] > 0) | (_df2_['1-4KMESN2'] > 0), 1, 0)
    _df2_['km&p_ea'] = np.where((_df2_[['5KPEAN', '6KPEAN', '7KPEAN', '8KPEAN', '9KPEAN']].sum(axis=1) > 0) | (_df2_[['5KPEASN', '6KPEASN', '7KPEASN', '8KPEASN', '9KPEASN']].sum(axis=1) > 0) | (_df2_['1-4KMEAN2'] > 0) | (_df2_['1-4KMEASN2'] > 0), 1, 0)

    _df2_['compilado_ts'] = np.select(
        [
            ((_df2_[['km&p_c', 'km&p_e', 'km&p_ea']] == 1).any(axis=1)) & (_df2_[['seccionada_correcta_1', 'seccionada_correcta_2', 'seccionada_correcta_3', 'seccionada_correcta_4']].sum(axis=1) == 0),
            (_df2_['sec_c'] == 1), (_df2_['sec_e'] == 1), (_df2_['sec_ea'] == 1), (_df2_['PASES'] == 1),
            (_df2_[['sec_c', 'sec_e', 'sec_ea', 'km&p_c', 'km&p_e', 'km&p_ea']].sum(axis=1) == 0)
        ],
        [_df2_['TipoServicio2'], 'C', 'E', 'EA', _df2_['TipoServicio2'], _df2_['TipoServicio2']], default="S/D"
    )

    _df2_['norm_por_tarifa'] = np.where(((_df2_[['1SCN', '2SCN', '3SCN', '4SCN', '5SCN', '1SEN', '2SEN', '3SEN', '4SEN', '5SEN', '1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']].sum(axis=1) > 0) | (_df2_[['1-4KMCN', '1-4KMEN', '1-4KMEAN']].sum(axis=1) > 0) | (_df2_[['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP', '1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP']].sum(axis=1) > 0) | (_df2_[['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN', '5KPEN', '6KPEN', '7KPEN', '8KPEN', '9KPEN', '5KPEAN', '6KPEAN', '7KPEAN', '8KPEAN', '9KPEAN']].sum(axis=1) > 0) | (_df2_[['1SRN', '2SRN', '3SRN', '4SRN', '5SRN']].sum(axis=1) > 0) | (_df2_[['1-4KMCN2', '1-4KMEN2', '1-4KMEAN2']].sum(axis=1) > 0)), "N", np.where(_df2_['FILTRO_1'] == 1, "Tarifa Vieja", np.where(_df2_['PASES'] == 1, "N", "SN")))
    _df2_['tarifa_s'] = np.where(((_df2_[['1SCN', '2SCN', '3SCN', '4SCN', '5SCN', '1SEN', '2SEN', '3SEN', '4SEN', '5SEN', '1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN', '1SCSN', '2SCSN', '3SCSN', '4SCSN', '5SCSN', '1SESN', '2SESN', '3SESN', '4SESN', '5SESN', '1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']].sum(axis=1) > 0) | (_df2_[['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP', '1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP', '1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP', '1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']].sum(axis=1) > 0)) & (_df2_[['Filtro1-4KMCN', 'Filtro1-4KMEN', 'Filtro1-4KMEAN', 'Filtro1-4KMCSN', 'Filtro1-4KMESN', 'Filtro1-4KMEASN']].sum(axis=1) == 0) & (((_df2_['compilado_ts'] == 'C') & (_df2_['sec_c'] == 1)) | ((_df2_['compilado_ts'] == 'E') & (_df2_['sec_e'] == 1)) | ((_df2_['compilado_ts'] == 'EA') & (_df2_['sec_ea'] == 1))) & (_df2_[['1SRN', '2SRN', '3SRN', '4SRN', '5SRN', '1SRSN', '2SRSN', '3SRSN', '4SRSN', '5SRSN' ]].sum(axis=1) == 0), 1, 0)
    _df2_['tarifa_km'] = np.where(((_df2_[['1-4KMCN', '1-4KMEN', '1-4KMEAN', '1-4KMCSN','1-4KMESN', '1-4KMEASN']].sum(axis=1) > 0) | (_df2_[['1-4KMCN2', '1-4KMEN2', '1-4KMEAN2', '1-4KMCSN2', '1-4KMESN2' ,'1-4KMEASN2']].sum(axis=1) > 0)) & (_df2_[['seccionada_correcta_1', 'seccionada_correcta_3', 'seccionada_correcta_2', 'seccionada_correcta_4']].sum(axis=1) == 0), 1, 0)
    _df2_['tarifa_kp'] = np.where((_df2_[['5KPCN', '6KPCN', '7KPCN', '8KPCN' ,'9KPCN', '5KPEN', '6KPEN' ,'7KPEN' ,'8KPEN' ,'9KPEN', '5KPEAN', '6KPEAN', '7KPEAN', '8KPEAN', '9KPEAN', '5KPCSN', '6KPCSN', '7KPCSN', '8KPCSN', '9KPCSN', '5KPESN', '6KPESN', '7KPESN', '8KPESN', '9KPESN', '5KPEASN', '6KPEASN', '7KPEASN', '8KPEASN', '9KPEASN']].sum(axis=1) > 0), 1, 0)
    _df2_['tarifa_PASE'] = np.where((_df2_['PASES'] == 1), 1, 0)
    _df2_['tarifa_sr'] = np.where((_df2_[['1SRN', '2SRN', '3SRN', '4SRN', '5SRN', '1SRSN', '2SRSN', '3SRSN', '4SRSN', '5SRSN']].sum(axis=1) > 0), 1, 0)
    _df2_['compilado_tt'] = np.where(_df2_['tarifa_s'] != 0, 'S', np.where(_df2_['tarifa_km'] != 0, 'KM', np.where(_df2_['tarifa_kp'] != 0, 'KP', np.where(_df2_['tarifa_PASE'] != 0, 'P', np.where(_df2_['tarifa_sr'] != 0, 'SR', np.where((_df2_['seccionada_correcta_1'] + _df2_['seccionada_correcta_2'] + _df2_['seccionada_correcta_3'] + _df2_['seccionada_correcta_4']) != 0, 'S', 'S/D'))))))

    _df2_['sec_1'] = np.where((_df2_[['1SCN', '1SCSN', '1SEN', '1SESN', '1SEAN', '1SEASN', '1SCNLP', '1SCSNLP', '1SESNLP', '1SENLP', '1SRN', '1SRSN']].sum(axis=1) > 0) & (_df2_['Filtro1-4KMCN'] != 4) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['Filtro1-4KMEAN'] != 4) & (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['Filtro1-4KMEASN'] != 4), 1, 0)
    _df2_['sec_2'] = np.where((_df2_[['2SCN', '2SCSN', '2SEN', '2SESN', '2SEAN', '2SEASN', '2SCNLP', '2SCSNLP', '2SESNLP', '2SENLP', '2SRN', '2SRSN']].sum(axis=1) > 0) & (_df2_['Filtro1-4KMCN'] != 4) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['Filtro1-4KMEAN'] != 4) & (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['Filtro1-4KMEASN'] != 4), 2, 0)
    _df2_['sec_3'] = np.where((_df2_[['3SCN', '3SCSN', '3SEN', '3SESN', '3SEAN', '3SEASN', '3SCNLP', '3SCSNLP', '3SESNLP', '3SENLP', '3SRN', '3SRSN']].sum(axis=1) > 0) & (_df2_['Filtro1-4KMCN'] != 4) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['Filtro1-4KMEAN'] != 4) & (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['Filtro1-4KMEASN'] != 4), 3, 0)
    _df2_['sec_4'] = np.where((_df2_[['4SCN', '4SCSN', '4SEN', '4SESN', '4SEAN', '4SEASN', '4SCNLP', '4SCSNLP', '4SESNLP', '4SENLP', '4SRN', '4SRSN']].sum(axis=1) > 0) & (_df2_['Filtro1-4KMCN'] != 4) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['Filtro1-4KMEAN'] != 4) & (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['Filtro1-4KMEASN'] != 4), 4, 0)
    _df2_['sec_5'] = np.where((_df2_[['5SCN', '5SCSN', '5SEN', '5SESN', '5SEAN', '5SEASN', '5SCNLP', '5SCSNLP', '5SESNLP', '5SENLP', '5SRN', '5SRSN']].sum(axis=1) > 0) & (_df2_['Filtro1-4KMCN'] != 4) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['Filtro1-4KMEAN'] != 4) & (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['Filtro1-4KMEASN'] != 4), 5, 0)

    _df2_['seccionadas_final'] = np.where(_df2_['PASES'] == 1, 1, _df2_[['sec_1', 'sec_2', 'sec_3', 'sec_4', 'sec_5']].sum(axis=1))
    _df2_['sec_1_4'] = np.where((_df2_[['1-4KMCN', '1-4KMEN', '1-4KMEAN', '1-4KMCSN', '1-4KMESN', '1-4KMEASN']].sum(axis=1) > 0) | (_df2_[['1-4KMCN2', '1-4KMEN2', '1-4KMEAN2', '1-4KMCSN2', '1-4KMESN2', '1-4KMEASN2']].sum(axis=1) > 0) & (_df2_[['seccionada_correcta_1', 'seccionada_correcta_2', 'seccionada_correcta_3', 'seccionada_correcta_4']].eq(0).all(axis=1)), 4, 0)

    _df2_['kilometricas_por_TS'] = np.where((_df2_['TipoServicio'] == 'C') & (_df2_['sin_nominalizar'] == 0), _df2_[['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN']].replace(0, np.nan).min(axis=1).fillna(0), np.where((_df2_['TipoServicio'] == 'E') & (_df2_['sin_nominalizar'] == 0), _df2_[['5KPEN', '6KPEN', '7KPEN', '8KPEN', '9KPEN']].replace(0, np.nan).min(axis=1).fillna(0), np.where((_df2_['TipoServicio'] == 'EA') & (_df2_['sin_nominalizar'] == 0), _df2_[['5KPEAN', '6KPEAN', '7KPEAN', '8KPEAN', '9KPEAN']].replace(0, np.nan).min(axis=1).fillna(0), np.where((_df2_['TipoServicio'] == 'C') & (_df2_['sin_nominalizar'] == 1), _df2_[['5KPCSN', '6KPCSN', '7KPCSN', '8KPCSN', '9KPCSN']].replace(0, np.nan).min(axis=1).fillna(0), np.where((_df2_['TipoServicio'] == 'E') & (_df2_['sin_nominalizar'] == 1), _df2_[['5KPESN', '6KPESN', '7KPESN', '8KPESN', '9KPESN']].replace(0, np.nan).min(axis=1).fillna(0), np.where((_df2_['TipoServicio'] == 'EA') & (_df2_['sin_nominalizar'] == 1), _df2_[['5KPEASN', '6KPEASN', '7KPEASN', '8KPEASN', '9KPEASN']].replace(0, np.nan).min(axis=1).fillna(0), 0))))))
    _df2_['compilado_seccion'] = np.where(_df2_['compilado_tt'] == "S", _df2_['seccionadas_final'], np.where(_df2_['compilado_tt'] == "P", 1, np.where(_df2_['compilado_tt'] == "KM", _df2_['sec_1_4'], np.where(_df2_['compilado_tt'] == "KP", _df2_['kilometricas_por_TS'], np.where(_df2_['compilado_tt'] == "SR", _df2_['seccionadas_final'], 0)))))

    _df2_['final_seccion'] = np.where((_df2_['GT'] == "SGII") & (_df2_['compilado_seccion'].isin([1, 2, 3])), 4, _df2_['compilado_seccion'])

    # Restauramos la generación del final_seccion2 que tenías originalmente
    _df2_['final_seccion2'] = np.where(_df2_['PASES'] == 1, _df2_['final_seccion'], np.where(_df2_['ID_LINEA'].isin([360, 394]), 3, np.where(_df2_['ID_LINEA'].isin([1267, 1270, 1271, 1272, 1273, 1274, 1275, 1276, 1277, 1278, 2667]), 2, _df2_['final_seccion'])))

    # Mantenemos el AÑO 2025 fijo porque de lo contrario las claves CONCAT fallan en TRSUBE
    _df2_['Año'] = 2025
    _df2_['Resolucion'] = '36'
    _df2_['CONCAT_MACHEO2'] = (_df2_['Año'].astype(str) + _df2_['Resolucion'].astype(str) + _df2_['final_seccion'].astype(int).astype(str) + _df2_['GT'].astype(str) + _df2_['compilado_ts'].astype(str) + _df2_['norm_por_tarifa'].astype(str))
    _df2_['CONCAT_MACHEO3'] = (_df2_['Año'].astype(str) + _df2_['Resolucion'].astype(str) + _df2_['final_seccion'].astype(int).astype(str) + _df2_['GT'].astype(str) + _df2_['ID_LINEA'].astype(str) + _df2_['compilado_ts'].astype(str) + _df2_['norm_por_tarifa'].astype(str))

    _df2_ = pd.merge(_df2_, ttr_reso[['CONCAT', 'TTR E.C.']], how='left', left_on='CONCAT_MACHEO2', right_on='CONCAT').fillna({'TTR E.C.': 0})
    _df2_.rename(columns={"TTR E.C.": "Tarifa TRSUBE"}, inplace=True)
    _df2_.drop(columns=['CONCAT'], inplace=True)

    _df2_ = pd.merge(_df2_, ttr_sgii_uma2[['CONCAT', 'TTR E.C.']], how='left', left_on='CONCAT_MACHEO3', right_on='CONCAT').fillna({'TTR E.C.': 0})
    _df2_.rename(columns={"TTR E.C.": "Tarifa TRSUBE2"}, inplace=True)
    _df2_.drop(columns=['CONCAT'], inplace=True)

    _df2_['Tarifa TRSUBE_FINAL'] = np.where(_df2_['Tarifa TRSUBE2'] == 0, _df2_['Tarifa TRSUBE'], _df2_['Tarifa TRSUBE2'])
    _df2_['Recaudacion_TRSUBE'] = _df2_['Tarifa TRSUBE_FINAL'] * _df2_['CANTIDAD_USOS']

    _df2_['SubSeccion'] = None
    def asignar_subsecciones(df, tarifas_dict, filtro_sin_nominalizar):
        for _, (lim_inf, lim_sup, tipo_servicio, seccion) in tarifas_dict.items():
            sub_rangos = np.linspace(lim_inf, lim_sup, 4)
            for i in range(3):
                sub_lim_inf = sub_rangos[i]
                sub_lim_sup = sub_rangos[i+1]
                sub_seccion = f"{seccion}-{i+1}"
                mask = ((df['TARIFA BASE ITG'] >= sub_lim_inf - 0.5) & (df['TARIFA BASE ITG'] < sub_lim_sup - 0.5) & (df['PASES'] == 0) & (df['sin_nominalizar'] == filtro_sin_nominalizar) & (df['TipoServicio2'] == tipo_servicio))
                df.loc[mask, 'SubSeccion'] = sub_seccion

    asignar_subsecciones(_df2_, tarifas_7, filtro_sin_nominalizar=0)
    asignar_subsecciones(_df2_, tarifas_8, filtro_sin_nominalizar=1)
    _df2_['SubSeccion'] = _df2_['SubSeccion'].fillna(_df2_['final_seccion'].astype(str))

    return _df2_

# # =================================================================================================================================================================================================
# # 3. HERRAMIENTA PBA (PROVINCIA)
# # =============================================================================
def tool_procesar_pba(archivo_base, archivo_nom_ts, archivo_nom_gt, archivo_ttr, archivo_diccionario, anio):
    df1 = pd.read_excel(archivo_base, sheet_name= "Base")
    nom_ts = pd.read_excel(archivo_nom_ts)
    nom_gt = pd.read_excel(archivo_nom_gt)
    ttr_reso = pd.read_excel(archivo_ttr, sheet_name='TTR')
    ttr_sgii_uma2 = pd.read_excel(archivo_ttr, sheet_name='SGII-UMA2')

    var_input = ['Linea SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO', 'GT', 'ID_EMPRESA', 'ID_LINEA', 'RAMAL', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO','CANTIDAD_USOS']
    _df2 = df1[var_input][df1['GT'].isin(["UPA", "UPAKM", "UMA1", "UMA2"])].copy()

    _df2_ = pd.merge(_df2, nom_gt[['ID_LINEA', 'GT']], how='left', left_on='ID_LINEA', right_on='ID_LINEA')
    _df2_["ID_LINEA"] = _df2_["ID_LINEA"].astype(float).astype(int).astype(str)

    _df2_['CANTIDAD_USOS'] = pd.to_numeric(_df2_['CANTIDAD_USOS'].astype(str).replace({',': ''}, regex=True), errors='coerce').fillna(0)
    _df2_['TARIFA BASE ITG'] = pd.to_numeric(_df2_['TARIFA BASE ITG'].astype(str).replace({',': ''}, regex=True), errors='coerce').fillna(0).round(3)

    _df2_.drop('GT_y', axis=1, inplace=True)
    _df2_.rename(columns={"GT_x": "GT"}, inplace=True)

    _df2_['Linea SILAS DNGFF'] = _df2_['Linea SILAS DNGFF'].astype(str)
    lineas_la_plata = ['LP506', 'LP504', 'LP508', 'LP561', 'LP501', 'LP502', 'LP520', 'LP518', 'LP53A', 'LP53B', '275', '307', '202', '215', '225', '214', '273', '414', '418']
    _df2_['LaPlata'] = _df2_['Linea SILAS DNGFF'].apply(lambda x: 1 if x in lineas_la_plata else 0)

    _df2_["RAMAL"] = _df2_["RAMAL"].astype(float).astype(int).astype(str)
    nom_ts["IdRamalNS"] = nom_ts["IdRamalNS"].astype(str).str.strip()

    _df2_ = pd.merge(_df2_, nom_ts[['TIPO DE SERVICIO FINAL', 'IdRamalNS']], how='left', left_on='RAMAL', right_on='IdRamalNS')
    _df2_.rename(columns={'TIPO DE SERVICIO FINAL': 'TipoServicio'}, inplace=True)
    _df2_.drop('IdRamalNS', axis=1, inplace=True)

    _df2_['TipoServicio2'] = _df2_['TipoServicio'].replace('SR', 'E')
    _df2_['sin_nominalizar'] = _df2_['CONTRATO'].apply(lambda x: 1 if x == 627 else 0)
    _df2_['PASES'] = _df2_['TARIFA BASE ITG'].apply(lambda x: 1 if 0 <= x <= 0.5 else 0)
    _df2_['FILTRO_1'] = np.where((_df2_['TARIFA BASE ITG'] < 528.95) & (_df2_['TARIFA BASE ITG'] > 0.5), 1, 0)

    # Diccionario PB01
    df_precios = pd.read_excel(archivo_diccionario, sheet_name='PB01').round(2)
    df_precios.columns = df_precios.columns.str.strip()
    if 'id' in df_precios.columns: df_precios.rename(columns={'id': 'Id'}, inplace=True)
    if 'minimo' in df_precios.columns: df_precios.rename(columns={'minimo': 'Minimo'}, inplace=True)
    if 'maximo' in df_precios.columns: df_precios.rename(columns={'maximo': 'Maximo'}, inplace=True)

    keys_1 = ['1SCN', '2SCN', '3SCN', '4SCN', '5SCN', '1SEN', '2SEN', '3SEN', '4SEN', '5SEN', '1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']
    valores_excel_1 = df_precios.iloc[0:15][['Minimo', 'Maximo']].values
    tarifas_1 = dict(zip(keys_1, [tuple(x) for x in valores_excel_1]))
    for col, (lim_inf, lim_sup) in tarifas_1.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_['TipoServicio'] != "SR") & (_df2_['LaPlata'] != 1), 1, 0)

    keys_2 = ['1SCSN', '2SCSN', '3SCSN', '4SCSN', '5SCSN', '1SESN', '2SESN', '3SESN', '4SESN', '5SESN', '1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']
    valores_excel_2 = df_precios.iloc[15:30][['Minimo', 'Maximo']].values
    tarifas_2 = dict(zip(keys_2, [tuple(x) for x in valores_excel_2]))
    for col, (lim_inf, lim_sup) in tarifas_2.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_['TipoServicio'] != "SR") & (_df2_['LaPlata'] != 1), 1, 0)

    config_orden = [('1-4KMCN', 'C'), ('1-4KMEN', 'E'), ('1-4KMEAN', 'EA')]
    valores_excel_3 = df_precios.iloc[[30, 34, 38]][['Minimo', 'Maximo']].values
    tarifas_3 = {clave: (valores_excel_3[i][0], valores_excel_3[i][1], tipo) for i, (clave, tipo) in enumerate(config_orden)}
    for col, (lim_inf, lim_sup, tipo) in tarifas_3.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_['TipoServicio2'] == tipo), 1, 0)

    config_orden_4 = [('1-4KMCSN', 'C'), ('1-4KMESN', 'E'), ('1-4KMEASN', 'EA')]
    valores_excel_4 = df_precios.iloc[[42, 46, 50]][['Minimo', 'Maximo']].values
    tarifas_4 = {clave: (valores_excel_4[i][0], valores_excel_4[i][1], tipo) for i, (clave, tipo) in enumerate(config_orden_4)}
    for col, (lim_inf, lim_sup, tipo) in tarifas_4.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_['TipoServicio2'] == tipo), 1, 0)

    keys_5 = ['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP', '1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP']
    valores_excel_5 = df_precios.iloc[54:64][['Minimo', 'Maximo']].values
    tarifas_5 = dict(zip(keys_5, [tuple(x) for x in valores_excel_5]))
    for col, (lim_inf, lim_sup) in tarifas_5.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_['LaPlata'] == 1), 1, 0)

    keys_6 = ['1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP', '1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']
    valores_excel_6 = df_precios.iloc[64:74][['Minimo', 'Maximo']].values
    tarifas_6 = dict(zip(keys_6, [tuple(x) for x in valores_excel_6]))
    for col, (lim_inf, lim_sup) in tarifas_6.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_['LaPlata'] == 1), 1, 0)

    config_7 = [('5KPCN', 'C', 5), ('6KPCN', 'C', 6), ('7KPCN', 'C', 7), ('8KPCN', 'C', 8), ('9KPCN', 'C', 9), ('5KPEN', 'E', 5), ('6KPEN', 'E', 6), ('7KPEN', 'E', 7), ('8KPEN', 'E', 8), ('9KPEN', 'E', 9), ('5KPEAN', 'EA', 5), ('6KPEAN', 'EA', 6), ('7KPEAN', 'EA', 7), ('8KPEAN', 'EA', 8), ('9KPEAN', 'EA', 9)]
    valores_excel_7 = df_precios.iloc[74:89][['Minimo', 'Maximo']].values
    tarifas_7 = {key: (valores_excel_7[i][0], valores_excel_7[i][1], tipo, valor) for i, (key, tipo, valor) in enumerate(config_7)}
    for col, (lim_inf, lim_sup, t_serv, val_asig) in tarifas_7.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] < lim_sup - 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_['TipoServicio2'] == t_serv), val_asig, 0)

    config_8 = [('5KPCSN', 'C', 5), ('6KPCSN', 'C', 6), ('7KPCSN', 'C', 7), ('8KPCSN', 'C', 8), ('9KPCSN', 'C', 9), ('5KPESN', 'E', 5), ('6KPESN', 'E', 6), ('7KPESN', 'E', 7), ('8KPESN', 'E', 8), ('9KPESN', 'E', 9), ('5KPEASN', 'EA', 5), ('6KPEASN', 'EA', 6), ('7KPEASN', 'EA', 7), ('8KPEASN', 'EA', 8), ('9KPEASN', 'EA', 9)]
    valores_excel_8 = df_precios.iloc[89:104][['Minimo', 'Maximo']].values
    tarifas_8 = {key: (valores_excel_8[i][0], valores_excel_8[i][1], tipo, valor) for i, (key, tipo, valor) in enumerate(config_8)}
    for col, (lim_inf, lim_sup, t_serv, val_asig) in tarifas_8.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] < lim_sup - 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_['TipoServicio2'] == t_serv), val_asig, 0)

    keys_9 = ['1SRN', '2SRN', '3SRN', '4SRN', '5SRN']
    valores_excel_9 = df_precios.iloc[104:109][['Minimo', 'Maximo']].values
    tarifas_9 = {key: (valores_excel_9[i][0], valores_excel_9[i][1], "SR") for i, key in enumerate(keys_9)}
    for col, (lim_inf, lim_sup, t_serv) in tarifas_9.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_['TipoServicio'] == t_serv), 1, 0)

    keys_10 = ['1SRSN', '2SRSN', '3SRSN', '4SRSN', '5SRSN']
    valores_excel_10 = df_precios.iloc[109:114][['Minimo', 'Maximo']].values
    tarifas_10 = {key: (valores_excel_10[i][0], valores_excel_10[i][1], "SR") for i, key in enumerate(keys_10)}
    for col, (lim_inf, lim_sup, t_serv) in tarifas_10.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_['TipoServicio'] == t_serv), 1, 0)

    tarifas_cn = {'1-4KMCN2': tuple(df_precios.iloc[114][['Minimo', 'Maximo']].values)}
    tarifas_en = {'1-4KMEN2': tuple(df_precios.iloc[115][['Minimo', 'Maximo']].values)}
    tarifas_ean = {'1-4KMEAN2': tuple(df_precios.iloc[116][['Minimo', 'Maximo']].values)}
    tarifas_cn_2 = {'1-4KMCSN2': tuple(df_precios.iloc[117][['Minimo', 'Maximo']].values)}
    tarifas_en_2 = {'1-4KMESN2': tuple(df_precios.iloc[118][['Minimo', 'Maximo']].values)}
    tarifas_ean_2 = {'1-4KMEASN2': tuple(df_precios.iloc[119][['Minimo', 'Maximo']].values)}

    columnas_sn_cn = ['1SCN', '2SCN', '3SCN', '4SCN', '5SCN']
    columnas_kmn_cn = ['1-4KMCN']
    columnas_snlp_cn = ['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP']
    columnas_sn_en = ['1SEN', '2SEN', '3SEN', '4SEN', '5SEN']
    columnas_kmn_en = ['1-4KMEN']
    columnas_kpn_en = ['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN']
    columnas_snlp_en = ['1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP']
    columnas_sn_ean = ['1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']
    columnas_kmn_ean = ['1-4KMEAN']
    columnas_kpn_ean = ['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN', '5KPEN', '6KPEN', '7KPEN', '8KPEN', '9KPEN']
    columnas_snlp_ean = ['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP', '1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP' ,'1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP', '1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']
    columnas_srn_srsn = ['1SRN', '2SRN', '3SRN', '4SRN', '5SRN', '1SRSN', '2SRSN', '3SRSN', '4SRSN', '5SRSN']

    for col, (lim_inf, lim_sup) in tarifas_cn.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_[columnas_sn_cn].sum(axis=1) == 0) & (_df2_[columnas_snlp_cn].sum(axis=1) == 0) & (_df2_[columnas_kmn_cn].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)
    for col, (lim_inf, lim_sup) in tarifas_en.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_[columnas_sn_en].sum(axis=1) == 0) & (_df2_[columnas_snlp_en].sum(axis=1) == 0) & (_df2_[columnas_kmn_en].sum(axis=1) == 0) & (_df2_[columnas_kpn_en].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)
    for col, (lim_inf, lim_sup) in tarifas_ean.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] != 1) & (_df2_[columnas_sn_ean].sum(axis=1) == 0) & (_df2_[columnas_snlp_ean].sum(axis=1) == 0) & (_df2_[columnas_kmn_ean].sum(axis=1) == 0) & (_df2_[columnas_kpn_ean].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)

    columnas_sn_cSn = ['1SCSN', '2SCSN', '3SCSN', '4SCSN', '5SCSN']
    columnas_kmn_cSn = ['1-4KMCSN']
    columnas_snlp_cSn = ['1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP']
    columnas_sn_eSn = ['1SESN', '2SESN', '3SESN', '4SESN', '5SESN']
    columnas_kmn_eSn = ['1-4KMESN']
    columnas_kpn_eSn = ['5KPCSN', '6KPCSN','7KPCSN','8KPCSN','9KPCSN']
    columnas_snlp_eSn = ['1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']
    columnas_sn_eaSn = ['1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']
    columnas_kmn_eaSn = ['1-4KMEASN']
    columnas_kpn_eaSn = ['5KPCSN', '6KPCSN', '7KPCSN', '8KPCSN', '9KPCSN', '5KPESN', '6KPESN', '7KPESN', '8KPESN', '9KPESN']
    columnas_snlp_eaSn = ['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP', '1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP' ,'1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP', '1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']

    for col, (lim_inf, lim_sup) in tarifas_cn_2.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_[columnas_sn_cSn].sum(axis=1) == 0) & (_df2_[columnas_snlp_cSn].sum(axis=1) == 0) & (_df2_[columnas_kmn_cSn].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)
    for col, (lim_inf, lim_sup) in tarifas_en_2.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_[columnas_sn_eSn].sum(axis=1) == 0) & (_df2_[columnas_snlp_eSn].sum(axis=1) == 0) & (_df2_[columnas_kmn_eSn].sum(axis=1) == 0) & (_df2_[columnas_kpn_eSn].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)
    for col, (lim_inf, lim_sup) in tarifas_ean_2.items(): _df2_[col] = np.where((_df2_['TARIFA BASE ITG'] >= lim_inf - 0.5) & (_df2_['TARIFA BASE ITG'] <= lim_sup- 0.5) & (_df2_['PASES'] == 0) & (_df2_['sin_nominalizar'] == 1) & (_df2_[columnas_sn_eaSn].sum(axis=1) == 0) & (_df2_[columnas_snlp_eaSn].sum(axis=1) == 0) & (_df2_[columnas_kmn_eaSn].sum(axis=1) == 0) & (_df2_[columnas_kpn_eaSn].sum(axis=1) == 0) & (_df2_['GT'] != "DF") & (_df2_[columnas_srn_srsn].sum(axis=1) == 0), 1, 0)

    _df2_['Filtro1-4KMCN'] = np.where((_df2_['TipoServicio2'] == 'C') & (_df2_[['1SEN', '2SEN', '3SEN', '4SEN', '5SEN', '1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']].sum(axis=1) != 0) & (_df2_['LaPlata'] == 0) & (_df2_['1-4KMCN2'] == 1), 4, 0)
    _df2_['Filtro1-4KMEN'] = np.where((_df2_['TipoServicio2'] == 'E') & (_df2_[['1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']].sum(axis=1) != 0)& (_df2_['LaPlata'] == 0) & (_df2_['1-4KMEN2'] == 1), 4, 0)
    _df2_['Filtro1-4KMEAN'] = np.where((_df2_['TipoServicio2'] == 'EA') & (_df2_['LaPlata'] == 0) & (_df2_['1-4KMEAN2'] == 1), 4, 0)
    _df2_['Filtro1-4KMCSN'] = np.where((_df2_['TipoServicio2'] == 'C') & (_df2_[['1SESN', '2SESN', '3SESN', '4SESN', '5SESN', '1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']].sum(axis=1) != 0)& (_df2_['LaPlata'] == 0) & (_df2_['1-4KMCSN2'] == 1), 4, 0)
    _df2_['Filtro1-4KMESN'] = np.where((_df2_['TipoServicio2'] == 'E') & (_df2_[['1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']].sum(axis=1) != 0)& (_df2_['LaPlata'] == 0) & (_df2_['1-4KMESN2'] == 1), 4, 0)
    _df2_['Filtro1-4KMEASN'] = np.where((_df2_['TipoServicio2'] == 'EA') & (_df2_['LaPlata'] == 0) & (_df2_['1-4KMEASN2'] == 1), 4, 0)

    _df2_['seccionada_correcta_1'] = np.select([(_df2_['1-4KMCN2'] == 1) & (_df2_['Filtro1-4KMCN'] != 4) & ((_df2_['1SEN'] == 1) | (_df2_['1SENLP'] == 1)), (_df2_['1-4KMCN2'] == 1) & (_df2_['Filtro1-4KMCN'] != 4) & ((_df2_['2SEN'] == 1) | (_df2_['2SENLP'] == 1)), (_df2_['1-4KMCN2'] == 1) & (_df2_['Filtro1-4KMCN'] != 4) & ((_df2_['3SEN'] == 1) | (_df2_['3SENLP'] == 1)), (_df2_['1-4KMCN2'] == 1) & (_df2_['Filtro1-4KMCN'] != 4) & ((_df2_['4SEN'] == 1) | (_df2_['4SENLP'] == 1)), (_df2_['1-4KMCN2'] == 1) & (_df2_['Filtro1-4KMCN'] != 4) & ((_df2_['5SEN'] == 1) | (_df2_['5SENLP'] == 1))], [1, 2, 3, 4, 5], default=0)
    _df2_['seccionada_correcta_3'] = np.select([(_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['1-4KMCSN2'] == 1)& ((_df2_['1SESN'] == 1) | (_df2_['1SESNLP'] == 1)), (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['1-4KMCSN2'] == 1) &  ((_df2_['2SESN'] == 1) | (_df2_['2SESNLP'] == 1)), (_df2_['Filtro1-4KMCSN'] != 4) &(_df2_['1-4KMCSN2'] == 1) &  ((_df2_['3SESN'] == 1) | (_df2_['3SESNLP'] == 1)), (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['1-4KMCSN2'] == 1) & ((_df2_['4SESN'] == 1) | (_df2_['4SESNLP'] == 1)), (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['1-4KMCSN2'] == 1) &((_df2_['5SESN'] == 1) | (_df2_['5SESNLP'] == 1))], [1, 2, 3, 4, 5], default=0)
    _df2_['seccionada_correcta_2'] = np.select([(_df2_['1-4KMEN2'] == 1) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['1SEAN'] == 1), (_df2_['1-4KMEN2'] == 1) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['2SEAN'] == 1) , (_df2_['1-4KMEN2'] == 1) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['3SEAN'] == 1) , (_df2_['1-4KMEN2'] == 1) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['4SEAN'] == 1) , (_df2_['1-4KMEN2'] == 1) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['5SEAN'] == 1)], [1, 2, 3, 4, 5], default=0)
    _df2_['seccionada_correcta_4'] = np.select([(_df2_['1-4KMESN2'] == 1) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['1SEASN'] == 1), (_df2_['1-4KMESN2'] == 1) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['2SEASN'] == 1), (_df2_['1-4KMESN2'] == 1) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['3SEASN'] == 1), (_df2_['1-4KMESN2'] == 1) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['4SEASN'] == 1), (_df2_['1-4KMESN2'] == 1) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['5SEASN'] == 1)], [1, 2, 3, 4, 5], default=0)

    _df2_['sec_c'] = np.where((_df2_[['1SCN', '2SCN', '3SCN', '4SCN', '5SCN']].sum(axis=1) > 0) | (_df2_[['1SCSN', '2SCSN', '3SCSN', '4SCSN', '5SCSN']].sum(axis=1) > 0) | (_df2_['1-4KMCN'] > 0) | (_df2_['1-4KMCSN'] > 0) | (_df2_[['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP']].sum(axis=1) > 0) | (_df2_[['1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP']].sum(axis=1) > 0), 1, 0)
    _df2_['sec_e'] = np.where((_df2_[['1SEN', '2SEN', '3SEN', '4SEN', '5SEN']].sum(axis=1) > 0) | (_df2_[['1SESN', '2SESN', '3SESN', '4SESN', '5SESN']].sum(axis=1) > 0) | (_df2_['1-4KMEN'] > 0) | (_df2_['1-4KMESN'] > 0) | (_df2_[['1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP']].sum(axis=1) > 0) | (_df2_[['1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']].sum(axis=1) > 0), 1, 0)
    _df2_['sec_ea'] = np.where((_df2_[['1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']].sum(axis=1) > 0) | (_df2_[['1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']].sum(axis=1) > 0) | (_df2_['1-4KMEAN'] > 0) | (_df2_['1-4KMEASN'] > 0), 1, 0)

    _df2_['km&p_c'] = np.where((_df2_[['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN']].sum(axis=1) > 0) | (_df2_[['5KPCSN', '6KPCSN', '7KPCSN', '8KPCSN', '9KPCSN']].sum(axis=1) > 0) | (_df2_['1-4KMCN2'] > 0) | (_df2_['1-4KMCSN2'] > 0), 1, 0)
    _df2_['km&p_e'] = np.where((_df2_[['5KPEN', '6KPEN', '7KPEN', '8KPEN', '9KPEN']].sum(axis=1) > 0) | (_df2_[['5KPESN', '6KPESN', '7KPESN', '8KPESN', '9KPESN']].sum(axis=1) > 0) | (_df2_['1-4KMEN2'] > 0) | (_df2_['1-4KMESN2'] > 0), 1, 0)
    _df2_['km&p_ea'] = np.where((_df2_[['5KPEAN', '6KPEAN', '7KPEAN', '8KPEAN', '9KPEAN']].sum(axis=1) > 0) | (_df2_[['5KPEASN', '6KPEASN', '7KPEASN', '8KPEASN', '9KPEASN']].sum(axis=1) > 0) | (_df2_['1-4KMEAN2'] > 0) | (_df2_['1-4KMEASN2'] > 0), 1, 0)

    _df2_['compilado_ts'] = np.select(
        [
            ((_df2_[['km&p_c', 'km&p_e', 'km&p_ea']] == 1).any(axis=1)) & (_df2_[['seccionada_correcta_1', 'seccionada_correcta_2', 'seccionada_correcta_3', 'seccionada_correcta_4']].sum(axis=1) == 0),
            (_df2_['sec_c'] == 1), (_df2_['sec_e'] == 1), (_df2_['sec_ea'] == 1), (_df2_['PASES'] == 1),
            (_df2_[['sec_c', 'sec_e', 'sec_ea', 'km&p_c', 'km&p_e', 'km&p_ea']].sum(axis=1) == 0)
        ],
        [_df2_['TipoServicio2'], 'C', 'E', 'EA', _df2_['TipoServicio2'], _df2_['TipoServicio2']], default="S/D"
    )

    _df2_['norm_por_tarifa'] = np.where(((_df2_[['1SCN', '2SCN', '3SCN', '4SCN', '5SCN', '1SEN', '2SEN', '3SEN', '4SEN', '5SEN', '1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN']].sum(axis=1) > 0) | (_df2_[['1-4KMCN', '1-4KMEN', '1-4KMEAN']].sum(axis=1) > 0) | (_df2_[['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP', '1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP']].sum(axis=1) > 0) | (_df2_[['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN', '5KPEN', '6KPEN', '7KPEN', '8KPEN', '9KPEN', '5KPEAN', '6KPEAN', '7KPEAN', '8KPEAN', '9KPEAN']].sum(axis=1) > 0) | (_df2_[['1SRN', '2SRN', '3SRN', '4SRN', '5SRN']].sum(axis=1) > 0) | (_df2_[['1-4KMCN2', '1-4KMEN2', '1-4KMEAN2']].sum(axis=1) > 0)), "N", np.where(_df2_['FILTRO_1'] == 1, "Tarifa Vieja", np.where(_df2_['PASES'] == 1, "N", "SN")))
    _df2_['tarifa_s'] = np.where(((_df2_[['1SCN', '2SCN', '3SCN', '4SCN', '5SCN', '1SEN', '2SEN', '3SEN', '4SEN', '5SEN', '1SEAN', '2SEAN', '3SEAN', '4SEAN', '5SEAN', '1SCSN', '2SCSN', '3SCSN', '4SCSN', '5SCSN', '1SESN', '2SESN', '3SESN', '4SESN', '5SESN', '1SEASN', '2SEASN', '3SEASN', '4SEASN', '5SEASN']].sum(axis=1) > 0) | (_df2_[['1SCNLP', '2SCNLP', '3SCNLP', '4SCNLP', '5SCNLP', '1SENLP', '2SENLP', '3SENLP', '4SENLP', '5SENLP', '1SCSNLP', '2SCSNLP', '3SCSNLP', '4SCSNLP', '5SCSNLP', '1SESNLP', '2SESNLP', '3SESNLP', '4SESNLP', '5SESNLP']].sum(axis=1) > 0)) & (_df2_[['Filtro1-4KMCN', 'Filtro1-4KMEN', 'Filtro1-4KMEAN', 'Filtro1-4KMCSN', 'Filtro1-4KMESN', 'Filtro1-4KMEASN']].sum(axis=1) == 0) & (((_df2_['compilado_ts'] == 'C') & (_df2_['sec_c'] == 1)) | ((_df2_['compilado_ts'] == 'E') & (_df2_['sec_e'] == 1)) | ((_df2_['compilado_ts'] == 'EA') & (_df2_['sec_ea'] == 1))) & (_df2_[['1SRN', '2SRN', '3SRN', '4SRN', '5SRN', '1SRSN', '2SRSN', '3SRSN', '4SRSN', '5SRSN' ]].sum(axis=1) == 0), 1, 0)
    _df2_['tarifa_km'] = np.where(((_df2_[['1-4KMCN', '1-4KMEN', '1-4KMEAN', '1-4KMCSN','1-4KMESN', '1-4KMEASN']].sum(axis=1) > 0) | (_df2_[['1-4KMCN2', '1-4KMEN2', '1-4KMEAN2', '1-4KMCSN2', '1-4KMESN2' ,'1-4KMEASN2']].sum(axis=1) > 0)) & (_df2_[['seccionada_correcta_1', 'seccionada_correcta_3', 'seccionada_correcta_2', 'seccionada_correcta_4']].sum(axis=1) == 0), 1, 0)
    _df2_['tarifa_kp'] = np.where((_df2_[['5KPCN', '6KPCN', '7KPCN', '8KPCN' ,'9KPCN', '5KPEN', '6KPEN' ,'7KPEN' ,'8KPEN' ,'9KPEN', '5KPEAN', '6KPEAN', '7KPEAN', '8KPEAN', '9KPEAN', '5KPCSN', '6KPCSN', '7KPCSN', '8KPCSN', '9KPCSN', '5KPESN', '6KPESN', '7KPESN', '8KPESN', '9KPESN', '5KPEASN', '6KPEASN', '7KPEASN', '8KPEASN', '9KPEASN']].sum(axis=1) > 0), 1, 0)
    _df2_['tarifa_PASE'] = np.where((_df2_['PASES'] == 1), 1, 0)
    _df2_['tarifa_sr'] = np.where((_df2_[['1SRN', '2SRN', '3SRN', '4SRN', '5SRN', '1SRSN', '2SRSN', '3SRSN', '4SRSN', '5SRSN']].sum(axis=1) > 0), 1, 0)
    _df2_['compilado_tt'] = np.where(_df2_['tarifa_s'] != 0, 'S', np.where(_df2_['tarifa_km'] != 0, 'KM', np.where(_df2_['tarifa_kp'] != 0, 'KP', np.where(_df2_['tarifa_PASE'] != 0, 'P', np.where(_df2_['tarifa_sr'] != 0, 'SR', np.where((_df2_['seccionada_correcta_1'] + _df2_['seccionada_correcta_2'] + _df2_['seccionada_correcta_3'] + _df2_['seccionada_correcta_4']) != 0, 'S', 'S/D'))))))

    _df2_['sec_1'] = np.where((_df2_[['1SCN', '1SCSN', '1SEN', '1SESN', '1SEAN', '1SEASN', '1SCNLP', '1SCSNLP', '1SESNLP', '1SENLP', '1SRN', '1SRSN']].sum(axis=1) > 0) & (_df2_['Filtro1-4KMCN'] != 4) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['Filtro1-4KMEAN'] != 4) & (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['Filtro1-4KMEASN'] != 4), 1, 0)
    _df2_['sec_2'] = np.where((_df2_[['2SCN', '2SCSN', '2SEN', '2SESN', '2SEAN', '2SEASN', '2SCNLP', '2SCSNLP', '2SESNLP', '2SENLP', '2SRN', '2SRSN']].sum(axis=1) > 0) & (_df2_['Filtro1-4KMCN'] != 4) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['Filtro1-4KMEAN'] != 4) & (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['Filtro1-4KMEASN'] != 4), 2, 0)
    _df2_['sec_3'] = np.where((_df2_[['3SCN', '3SCSN', '3SEN', '3SESN', '3SEAN', '3SEASN', '3SCNLP', '3SCSNLP', '3SESNLP', '3SENLP', '3SRN', '3SRSN']].sum(axis=1) > 0) & (_df2_['Filtro1-4KMCN'] != 4) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['Filtro1-4KMEAN'] != 4) & (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['Filtro1-4KMEASN'] != 4), 3, 0)
    _df2_['sec_4'] = np.where((_df2_[['4SCN', '4SCSN', '4SEN', '4SESN', '4SEAN', '4SEASN', '4SCNLP', '4SCSNLP', '4SESNLP', '4SENLP', '4SRN', '4SRSN']].sum(axis=1) > 0) & (_df2_['Filtro1-4KMCN'] != 4) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['Filtro1-4KMEAN'] != 4) & (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['Filtro1-4KMEASN'] != 4), 4, 0)
    _df2_['sec_5'] = np.where((_df2_[['5SCN', '5SCSN', '5SEN', '5SESN', '5SEAN', '5SEASN', '5SCNLP', '5SCSNLP', '5SESNLP', '5SENLP', '5SRN', '5SRSN']].sum(axis=1) > 0) & (_df2_['Filtro1-4KMCN'] != 4) & (_df2_['Filtro1-4KMEN'] != 4) & (_df2_['Filtro1-4KMEAN'] != 4) & (_df2_['Filtro1-4KMCSN'] != 4) & (_df2_['Filtro1-4KMESN'] != 4) & (_df2_['Filtro1-4KMEASN'] != 4), 5, 0)

    _df2_['seccionadas_final'] = np.where(_df2_['PASES'] == 1, 1, _df2_[['sec_1', 'sec_2', 'sec_3', 'sec_4', 'sec_5']].sum(axis=1))
    _df2_['sec_1_4'] = np.where((_df2_[['1-4KMCN', '1-4KMEN', '1-4KMEAN', '1-4KMCSN', '1-4KMESN', '1-4KMEASN']].sum(axis=1) > 0) | (_df2_[['1-4KMCN2', '1-4KMEN2', '1-4KMEAN2', '1-4KMCSN2', '1-4KMESN2', '1-4KMEASN2']].sum(axis=1) > 0) & (_df2_[['seccionada_correcta_1', 'seccionada_correcta_2', 'seccionada_correcta_3', 'seccionada_correcta_4']].eq(0).all(axis=1)), 4, 0)

    _df2_['kilometricas'] = np.where((_df2_['compilado_ts'] == 'C') & (_df2_['norm_por_tarifa'] == 'N'), _df2_[['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN']].sum(axis=1), np.where((_df2_['compilado_ts'] == 'E') & (_df2_['norm_por_tarifa'] == 'N'), _df2_[['5KPEN', '6KPEN', '7KPEN', '8KPEN', '9KPEN']].sum(axis=1), np.where((_df2_['compilado_ts'] == 'EA') & (_df2_['norm_por_tarifa'] == 'N'), _df2_[['5KPEAN', '6KPEAN', '7KPEAN', '8KPEAN', '9KPEAN']].sum(axis=1), np.where((_df2_['compilado_ts'] == 'C') & (_df2_['norm_por_tarifa'] == 'SN'), _df2_[['5KPCSN', '6KPCSN', '7KPCSN', '8KPCSN', '9KPCSN']].sum(axis=1), np.where((_df2_['compilado_ts'] == 'E') & (_df2_['norm_por_tarifa'] == 'SN'), _df2_[['5KPESN', '6KPESN', '7KPESN', '8KPESN', '9KPESN']].sum(axis=1), _df2_[['5KPEASN', '6KPEASN', '7KPEASN', '8KPEASN', '9KPEASN']].sum(axis=1))))))
    _df2_['kilometricas_por_TS'] = np.where((_df2_['TipoServicio'] == 'C') & (_df2_['sin_nominalizar'] == 0), _df2_[['5KPCN', '6KPCN', '7KPCN', '8KPCN', '9KPCN']].replace(0, np.nan).min(axis=1).fillna(0), np.where((_df2_['TipoServicio'] == 'E') & (_df2_['sin_nominalizar'] == 0), _df2_[['5KPEN', '6KPEN', '7KPEN', '8KPEN', '9KPEN']].replace(0, np.nan).min(axis=1).fillna(0), np.where((_df2_['TipoServicio'] == 'EA') & (_df2_['sin_nominalizar'] == 0), _df2_[['5KPEAN', '6KPEAN', '7KPEAN', '8KPEAN', '9KPEAN']].replace(0, np.nan).min(axis=1).fillna(0), np.where((_df2_['TipoServicio'] == 'C') & (_df2_['sin_nominalizar'] == 1), _df2_[['5KPCSN', '6KPCSN', '7KPCSN', '8KPCSN', '9KPCSN']].replace(0, np.nan).min(axis=1).fillna(0), np.where((_df2_['TipoServicio'] == 'E') & (_df2_['sin_nominalizar'] == 1), _df2_[['5KPESN', '6KPESN', '7KPESN', '8KPESN', '9KPESN']].replace(0, np.nan).min(axis=1).fillna(0), np.where((_df2_['TipoServicio'] == 'EA') & (_df2_['sin_nominalizar'] == 1), _df2_[['5KPEASN', '6KPEASN', '7KPEASN', '8KPEASN', '9KPEASN']].replace(0, np.nan).min(axis=1).fillna(0), 0))))))
    _df2_['compilado_seccion'] = np.where(_df2_['compilado_tt'] == "S", _df2_['seccionadas_final'], np.where(_df2_['compilado_tt'] == "P", 1, np.where(_df2_['compilado_tt'] == "KM", _df2_['sec_1_4'], np.where(_df2_['compilado_tt'] == "KP", _df2_['kilometricas_por_TS'], np.where(_df2_['compilado_tt'] == "SR", _df2_['seccionadas_final'], 0)))))

    _df2_['final_seccion'] = np.where((_df2_['GT'] == "SGII") & (_df2_['compilado_seccion'].isin([1, 2, 3])), 4, _df2_['compilado_seccion'])
    _df2_['CONCAT_MACHEO'] = (_df2_['final_seccion'].astype(int).astype(str) + _df2_['GT'].astype(str) + _df2_['compilado_ts'].astype(str) + _df2_['norm_por_tarifa'].astype(str))

    _df2_['final_seccion2'] = np.where(_df2_['PASES'] == 1, _df2_['final_seccion'], np.where(_df2_['ID_LINEA'].isin(['360', '394']), 3, _df2_['final_seccion']))

    _df2_['Año'] = '2025'  # O el año que diga el Excel de la Reso 36
    _df2_['Resolucion'] = '36'

    _df2_['CONCAT_MACHEO2'] = (_df2_['Año'].astype(str) + _df2_['Resolucion'].astype(str) + _df2_['final_seccion'].astype(int).astype(str) + _df2_['GT'].astype(str) + _df2_['compilado_ts'].astype(str) + _df2_['norm_por_tarifa'].astype(str))
    _df2_['CONCAT_MACHEO3'] = (_df2_['Año'].astype(str) + _df2_['Resolucion'].astype(str) + _df2_['final_seccion'].astype(int).astype(str) + _df2_['GT'].astype(str) + _df2_['ID_LINEA'].astype(str) + _df2_['compilado_ts'].astype(str) + _df2_['norm_por_tarifa'].astype(str))

    _df2_ = pd.merge(_df2_, ttr_reso[['CONCAT', 'TTR E.C.']], how='left', left_on='CONCAT_MACHEO2', right_on='CONCAT').fillna({'TTR E.C.': 0})
    _df2_.rename(columns={"TTR E.C.": "Tarifa TRSUBE"}, inplace=True)
    _df2_.drop(columns=['CONCAT'], inplace=True)

    _df2_ = pd.merge(_df2_, ttr_sgii_uma2[['CONCAT', 'TTR E.C.']], how='left', left_on='CONCAT_MACHEO3', right_on='CONCAT').fillna({'TTR E.C.': 0})
    _df2_.rename(columns={"TTR E.C.": "Tarifa TRSUBE2"}, inplace=True)
    _df2_.drop(columns=['CONCAT'], inplace=True)

    _df2_['Tarifa TRSUBE_FINAL'] = np.where(_df2_['Tarifa TRSUBE2'] == 0, _df2_['Tarifa TRSUBE'], _df2_['Tarifa TRSUBE2'])
    _df2_['Recaudacion_TRSUBE'] = _df2_['Tarifa TRSUBE'] * _df2_['CANTIDAD_USOS']

    _df2_['SubSeccion'] = None
    def asignar_subsecciones(df, tarifas_dict, filtro_sin_nominalizar):
        for _, (lim_inf, lim_sup, tipo_servicio, seccion) in tarifas_dict.items():
            sub_rangos = np.linspace(lim_inf, lim_sup, 4)
            for i in range(3):
                sub_lim_inf = sub_rangos[i]
                sub_lim_sup = sub_rangos[i+1]
                sub_seccion = f"{seccion}-{i+1}"
                mask = ((df['TARIFA BASE ITG'] >= sub_lim_inf - 0.5) & (df['TARIFA BASE ITG'] < sub_lim_sup - 0.5) & (df['PASES'] == 0) & (df['sin_nominalizar'] == filtro_sin_nominalizar) & (df['TipoServicio2'] == tipo_servicio))
                df.loc[mask, 'SubSeccion'] = sub_seccion

    asignar_subsecciones(_df2_, tarifas_7, filtro_sin_nominalizar=0)
    asignar_subsecciones(_df2_, tarifas_8, filtro_sin_nominalizar=1)
    _df2_['SubSeccion'] = _df2_['SubSeccion'].fillna(_df2_['final_seccion'].astype(str))

    return _df2_

    # *************************************************************************************************************************
    def consolidar_excels(df_caba, df_jn, df_pba):
        # Les agregamos una columna para saber de dónde vino cada dato (opcional pero útil)
        df_caba['Jurisdicción'] = 'CABA'
        df_jn['Jurisdicción'] = 'JN'
        df_pba['Jurisdicción'] = 'PBA'
    
    # Los pegamos uno debajo del otro
    df_final = pd.concat([df_caba, df_jn, df_pba], ignore_index=True)
    return df_final
# =============================================================================
# 4. EL ORQUESTADOR (FRONTEND WEB - STREAMLIT)
# =============================================================================

st.set_page_config(page_title="Data TTR -", page_icon="🤖", layout="wide")

st.title("🤖 Tarifas Teóricas de Referencia")
st.markdown("Subí los Excel de este mes y seleccioná la Jurisdicción. El sistema ejecutará toda la lógica automáticamente.")
st.divider()

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("⚙️ Contexto del Proceso")
    tipo_ttr = st.selectbox("Jurisdicción:", ["DF (Distrito Federal)", "JN (Nación)", "PBA (Provincia de Bs. As.)"])
    mes_seleccionado = st.selectbox("Mes a procesar:", ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"])
    anio_seleccionado = st.selectbox("Año:", [2024, 2025, 2026, 2027], index=2)

with col2:
    st.subheader("📁 Archivos Requeridos")
    file_base = st.file_uploader(f"1. Base DGGI ({mes_seleccionado} {anio_seleccionado})", type=["xlsx"])
    file_nom_ts = st.file_uploader("2. Nomenclador TS", type=["xlsx"])
    file_nom_gt = st.file_uploader("3. Nomenclador GT", type=["xlsx"])
    file_ttr = st.file_uploader("4. TTR Resoluciones", type=["xlsx"])
    file_diccionario = st.file_uploader("5. Diccionario de Tarifas Comerciales", type=["xlsx"])

st.divider()

def consolidar_excels(df_caba, df_jn, df_pba):
    df_caba['Jurisdicción'] = 'CABA'
    df_jn['Jurisdicción'] = 'JN'
    df_pba['Jurisdicción'] = 'PBA'
    df_final = pd.concat([df_caba, df_jn, df_pba], ignore_index=True)
    return df_final

if st.button("🚀 Procesar", type="primary"):

    if not (file_base and file_nom_ts and file_nom_gt and file_ttr and file_diccionario):
        st.error("⚠️ Alto ahí. Por favor asegurate de cargar los 5 archivos Excel antes de continuar.")
    else:
        with st.spinner(f'Procesando las TTR de {tipo_ttr} para {mes_seleccionado} {anio_seleccionado}...'):
            try:
                # 1. Ejecutamos el proceso según la elección del menú
                if tipo_ttr == "DF (Distrito Federal)":
                    st.session_state['df_resultado_caba'] = tool_procesar_df(file_base, file_nom_ts, file_nom_gt, file_ttr, file_diccionario, anio_seleccionado)
                    st.success("✅ CABA procesado y guardado en memoria.")
                
                elif tipo_ttr == "JN (Nación)":
                    st.session_state['df_resultado_jn'] = tool_procesar_jn(file_base, file_nom_ts, file_nom_gt, file_ttr, file_diccionario, anio_seleccionado)
                    st.success("✅ JN procesado y guardado en memoria.")
                
                else:
                    st.session_state['df_resultado_pba'] = tool_procesar_pba(file_base, file_nom_ts, file_nom_gt, file_ttr, file_diccionario, anio_seleccionado)
                    st.success("✅ PBA procesado y guardado en memoria.")

                # 2. Verificamos si ya tenemos los tres para mostrar el botón de descarga unificada
                if all(k in st.session_state for k in ['df_resultado_caba', 'df_resultado_jn', 'df_resultado_pba']):
                    st.balloons() # ¡Festejo porque terminaste los tres!
                    
                    # Consolidamos
                    df_final = consolidar_excels(
                        st.session_state['df_resultado_caba'], 
                        st.session_state['df_resultado_jn'], 
                        st.session_state['df_resultado_pba']
                    )
                    
                    # Preparamos el Excel
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df_final.to_excel(writer, index=False, sheet_name='Consolidado_TTR')
                    output.seek(0)
                    
                    st.divider()
                    st.download_button(
                        label="📥 DESCARGAR REPORTE UNIFICADO (CABA+JN+PBA)",
                        data=output,
                        file_name=f"TTR_Consolidado_{mes_seleccionado}_{anio_seleccionado}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        type="primary"
                    )
                else:
                    faltan = []
                    if 'df_resultado_caba' not in st.session_state: faltan.append("CABA")
                    if 'df_resultado_jn' not in st.session_state: faltan.append("JN")
                    if 'df_resultado_pba' not in st.session_state: faltan.append("PBA")
                    st.info(f"💡 Te falta procesar: {', '.join(faltan)} para habilitar la descarga unificada.")

            except Exception as e:
                st.error(f"❌ Error en el proceso: {str(e)}")

