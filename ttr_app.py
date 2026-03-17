import streamlit as st
import pandas as pd
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Procesador DMK JN", layout="wide")

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# =============================================================================
# BLOQUE 1: PROYECCIÓN DE TARIFAS (PASO 1)
# =============================================================================

def motor_tarifas_original(df_nov, manuales):
    try:
        df = df_nov.copy()
        df.columns = [str(c).upper().strip() for c in df.columns]
        
        # Buscamos columnas por palabras clave (ID y Tarifa/Limite)
        c_ids = [c for c in df.columns if any(x in c for x in ['ID', 'GT'])][0]
        c_precios = [c for c in df.columns if any(x in c for x in ['LIMITE', 'TARIFA', 'PRECIO'])][0]
        
        # Limpieza de números (comas por puntos)
        df[c_precios] = pd.to_numeric(df[c_precios].astype(str).str.replace(',', '.'), errors='coerce')
        
        val_1scn = df.loc[df[c_ids].astype(str).str.contains('1SCN', na=False), c_precios].values
        v1_ant = val_1scn[0] if len(val_1scn) > 0 else 270.0
        factor = manuales['1SCN'] / v1_ant if v1_ant > 0 else 1.0
        
        res = []
        for _, row in df.iterrows():
            id_t = str(row[c_ids]).strip().upper()
            v_ant = row[c_precios]
            v_nue = manuales.get(id_t, v_ant * factor if pd.notnull(v_ant) else manuales['1SCN'])
            # Lógica especial para SGI/UPA si no están en los 5 manuales
            if any(x in id_t for x in ['SGI', 'UPA']) and id_t not in manuales:
                v_nue = manuales['1SCN']
            res.append({'GT': id_t, 'TARIFA_FEB': round(v_nue, 2)})
            
        return pd.DataFrame(res)
    except Exception as e:
        st.error(f"Error en Tarifas: {e}")
        return None

# =============================================================================
# BLOQUE 2: PROCESAMIENTO DMK (PASO 2 - LÓGICA PANDAS PURA)
# =============================================================================

def procesar_dmk_v16_1(fz, df_v, df_tarifas, fe):
    try:
        # 1. CARGAR DMK (Desde ZIP o CSV)
        if fz.name.endswith('.zip'):
            with zipfile.ZipFile(fz) as z:
                with z.open(z.namelist()[0]) as f:
                    df = pd.read_csv(f, sep=';', encoding='iso-8859-1', dtype=str)
        else:
            df = pd.read_csv(fz, sep=';', encoding='iso-8859-1', dtype=str)

        # Normalizamos nombres del DMK
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
        
        # 2. LIMPIEZA DE NOMENCLADOR (Eliminamos las columnas conflictivas de entrada)
        df_v.columns = [str(c).upper().strip() for c in df_v.columns]
        # Borramos Silas/DNGFF para evitar errores alfanuméricos
        cols_borrar = [c for c in df_v.columns if 'SILAS' in c or 'DNGFF' in c]
        df_v = df_v.drop(columns=cols_borrar)
        
        # 3. NORMALIZAR IDs PARA EL CRUCE (ID_LINEA como Texto limpio)
        def clean_id(x):
            return str(x).strip().replace('.0', '')

        df['ID_LINEA'] = df['ID_LINEA'].apply(clean_id)
        df_v['ID_LINEA'] = df_v['ID_LINEA'].apply(clean_id)

        # 4. CRUCE CON NOMENCLADOR (LEFT JOIN = NO SE PIERDEN FILAS)
        df = df.merge(df_v[['ID_LINEA', 'GT', 'PROVINCIA', 'MUNICIPIO']], on='ID_LINEA', how='left')

        # 5. CRUCE CON TARIFAS PROYECTADAS
        df_tarifas['GT'] = df_tarifas['GT'].astype(str).str.strip()
        df = df.merge(df_tarifas, on='GT', how='left')

        # 6. CONVERSIÓN NUMÉRICA PARA CÁLCULOS
        cols_n = ['DEBITADO', 'DESCUENTO_X_INTEGRACION', 'CANTIDAD_USOS', 'TARIFA_FEB', 'TARIFA_BASE_ITG']
        for c in cols_n:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)

        # 7. FÓRMULAS NOTEBOOK
        df['BE'] = df['CONTRATO'].isin(['830', '831', '832', '833']).map({True: 'SI', False: 'NO'})
        df['PROV_FINAL'] = df.apply(lambda x: 'CABA' if x['GT'] == 'DF' else x['PROVINCIA'], axis=1)
        
        df['COMP_ITG'] = df['DESCUENTO_X_INTEGRACION'] * df['CANTIDAD_USOS']
        
        # Cálculo ATS (Lógica Original)
        def calc_ats(row):
            if str(row['CONTRATO']) == '621':
                if row['GT'] == 'INP':
                    return (row['DEBITADO'] / 0.45 * 0.55) * row['CANTIDAD_USOS']
                else:
                    # Usamos TARIFA_FEB si existe, sino la BASE_ITG
                    t_ref = row['TARIFA_FEB'] if row['TARIFA_FEB'] > 0 else row['TARIFA_BASE_ITG']
                    return (t_ref - row['DEBITADO'] - row['DESCUENTO_X_INTEGRACION']) * row['CANTIDAD_USOS']
            return 0.0

        df['COMP_ATS'] = df.apply(calc_ats, axis=1)

        # 8. ENERGÍAS
        df_e = pd.read_excel(fe)
        df_e.columns = [str(c).upper().strip() for c in df_e.columns]
        df_e['DOMINIO'] = df_e['DOMINIO'].astype(str).str.strip().str.upper()
        
        df = df.merge(df_e[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df['ENERGIA'] = df['ENERGIA'].fillna(3)
        df['DOMINIO_REAL'] = df.apply(lambda x: x['DOMINIO'] if pd.notnull(x['ENERGIA']) and x['DOMINIO'] != 'nan' else 'NO', axis=1)

        # 9. NETOS Y AGRUPACIÓN
        df['COMP. ATS s/IVA'] = df['COMP_ATS'] / 1.105
        df['COMP. ITG s/IVA'] = df['COMP_ITG'] / 1.105

        agrupadores = ['PROV_FINAL', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'ID_LINEA', 'DOMINIO_REAL', 'ENERGIA', 'CONTRATO', 'BE', 'TARIFA_FEB', 'DEBITADO', 'DESCUENTO_X_INTEGRACION']
        
        res_final = df.groupby(agrupadores, as_index=False).agg({
            'CANTIDAD_USOS': 'sum', 'COMP_ITG': 'sum', 'COMP_ATS': 'sum', 'COMP. ATS s/IVA': 'sum', 'COMP. ITG s/IVA': 'sum'
        })
        
        return res_final

    except Exception as e:
        st.error(f"Error en Procesamiento DMK: {e}")
        return None

# =============================================================================
# INTERFAZ STREAMLIT
# =============================================================================

st.title("📂 Procesador DMK - Lógica Original")

if 'memo_tar' not in st.session_state: st.session_state.memo_tar = None

tabs = st.tabs(["💰 1. TARIFAS", "🚀 2. PROCESAR DMK"])

with tabs[0]:
    f_tar = st.file_uploader("Subir Cuadro Noviembre", key="f_t")
    if f_tar:
        c = st.columns(5)
        m = {
            '1SCN': c[0].number_input("1SCN", 494.33),
            '2SCN': c[1].number_input("2SCN", 551.24),
            '3SCN': c[2].number_input("3SCN", 593.70),
            '4SCN': c[3].number_input("4SCN", 636.21),
            '5SCN': c[4].number_input("5SCN", 678.42)
        }
        if st.button("📊 Calcular Proyección"):
            st.session_state.memo_tar = motor_tarifas_original(pd.read_excel(f_tar), m)
    if st.session_state.memo_tar is not None:
        st.dataframe(st.session_state.memo_tar)

with tabs[1]:
    if st.session_state.memo_tar is not None:
        c1, c2 = st.columns(2)
        fv = c1.file_uploader("Nomenclador V", key="f_v")
        fe = c2.file_uploader("Parque Móvil (Energías)", key="f_e")
        fz = st.file_uploader("Archivo DMK (CSV/ZIP)", key="f_z")
        
        if fv and fe and fz and st.button("🚀 GENERAR ARCHIVO DMK"):
            with st.spinner("Procesando..."):
                resultado = procesar_dmk_v16_1(fz, pd.read_excel(fv), st.session_state.memo_tar, fe)
                if resultado is not None:
                    st.success("Proceso completado.")
                    st.download_button("📥 DESCARGAR EXCEL", preparar_descarga(resultado), "DMK_Procesado.xlsx")
                    st.dataframe(resultado.head(15))
    else:
        st.warning("Primero configurá las tarifas en la pestaña 1.")
