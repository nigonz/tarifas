import streamlit as st
import pandas as pd
import io
import zipfile

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Procesador DMK JN", layout="wide")

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# =============================================================================
# BLOQUE 1: PROYECCIÓN DE TARIFAS (PASO 1) - MOTOR MIXTO (ESTRUCTURAL + INDEXADO)
# =============================================================================

def motor_tarifas_definitivo(df_nov, df_multiplicadores, manuales, coef_oficial=None):
    try:
        # 1. Preparar tabla histórica (El Excel viejo)
        df = df_nov.copy()
        df.columns = [str(c).upper().strip() for c in df.columns]
        
        # Localizar columnas clave
        c_ids = [c for c in df.columns if any(x in c for x in ['ID', 'GT'])][0]
        c_precios = [c for c in df.columns if 'LIMITE SUPERIOR' in c or 'TARIFA' in c or 'PRECIO' in c][0]
        
        # Limpieza de importes (comas a puntos) para que la matemática funcione
        df[c_precios] = pd.to_numeric(df[c_precios].astype(str).str.replace(',', '.'), errors='coerce')
        
        # 2. El Porcentaje / Coeficiente de Actualización
        val_1scn = df.loc[df[c_ids].astype(str).str.contains('1SCN', na=False), c_precios].values
        v1_ant = val_1scn[0] if len(val_1scn) > 0 else 494.33
        
        # Si no le pasamos el porcentaje exacto en la UI, lo calcula automáticamente
        if coef_oficial is None or coef_oficial == 0.0:
            coeficiente = manuales['1SCN'] / v1_ant if v1_ant > 0 else 1.0
        else:
            coeficiente = coef_oficial
            
        # 3. Diccionario de Multiplicadores (Para las tarifas de la "Familia A")
        dict_mult = {}
        if df_multiplicadores is not None:
            df_m = df_multiplicadores.copy()
            df_m.columns = [str(c).upper().strip() for c in df_m.columns]
            
            # Busca automáticamente las columnas correctas en el CSV de multiplicadores
            c_ids_m = [c for c in df_m.columns if any(x in c for x in ['ID', 'GT'])][0]
            c_val_m = [c for c in df_m.columns if any(x in c for x in ['MULT', 'COEF', 'PORCENTAJE'])][0]
            
            df_m[c_val_m] = pd.to_numeric(df_m[c_val_m].astype(str).str.replace(',', '.'), errors='coerce')
            dict_mult = dict(zip(df_m[c_ids_m].astype(str).str.strip().str.upper(), df_m[c_val_m]))

        # 4. El Bucle de Liquidación (Fila por fila)
        res = []
        bases_manuales = ['1SCN', '2SCN', '3SCN', '4SCN', '5SCN']
        
        for _, row in df.iterrows():
            id_t = str(row[c_ids]).strip().upper()
            v_ant = row[c_precios]
            
            # REGLA 1: Las 5 bases intocables (Se imponen a cualquier cálculo)
            if id_t in bases_manuales and id_t in manuales:
                v_nue = manuales[id_t]
                
            # REGLA 2: Estructurales (Si existe en el Excel de multiplicadores)
            elif id_t in dict_mult and pd.notnull(dict_mult[id_t]):
                # Aplica el redondeo estricto inmediatamente
                v_nue = round(manuales['1SCN'] * dict_mult[id_t], 2)
                
            # REGLA 3: Indexadas (La fórmula de Excel que me mandaste: Valor Viejo * % )
            elif pd.notnull(v_ant):
                # Réplica exacta de la fórmula de tu captura: ROUND(Valor * Coef, 2)
                v_nue = round(v_ant * coeficiente, 2)
                
            # Fallback de seguridad por si una celda viene vacía
            else:
                v_nue = manuales['1SCN']
            
            res.append({'GT': id_t, 'TARIFA_FEB': v_nue})
            
        return pd.DataFrame(res)
        
    except Exception as e:
        st.error(f"Error Crítico en Motor de Tarifas: {e}")
        return None
# =============================================================================
# BLOQUE 2: MOTOR DMK (V16.4 - ELIMINACIÓN PREVENTIVA DE COLUMNAS)
# =============================================================================

def procesar_dmk_v16_4(fz, df_v, df_tarifas, fe):
    try:
        # 1. CARGAR DMK (Forzamos todo a TEXTO para que LM622 no rompa nada)
        if fz.name.endswith('.zip'):
            with zipfile.ZipFile(fz) as z:
                with z.open(z.namelist()[0]) as f:
                    df = pd.read_csv(f, sep=';', encoding='iso-8859-1', dtype=str)
        else:
            df = pd.read_csv(fz, sep=';', encoding='iso-8859-1', dtype=str)

        # Normalizamos nombres de columnas
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

        # --- FILTRO QUIRÚRGICO ---
        # Solo nos quedamos con las columnas que sirven para liquidar. 
        # CUALQUIER OTRA COLUMNA (con basura alfanumérica) SE BORRA ACÁ.
        cols_dmk_utiles = ["ID_EMPRESA", "ID_LINEA", "DOMINIO", "DEBITADO", "CONTRATO", "DESCUENTO_X_INTEGRACION", "CANTIDAD_USOS", "TARIFA_BASE_ITG"]
        df = df[[c for c in cols_dmk_utiles if c in df.columns]]

        # 2. PREPARAR NOMENCLADOR (ID_LINEA, GT, PROVINCIA, MUNICIPIO)
        df_v.columns = [str(c).upper().strip() for c in df_v.columns]
        df_v = df_v[["ID_LINEA", "GT", "PROVINCIA", "MUNICIPIO"]] # Ignoramos Silas/DNGFF

        # 3. NORMALIZACIÓN DE IDS PARA EL CRUCE
        def clean_id(x):
            try: return str(int(float(str(x).strip())))
            except: return str(x).strip()

        df['ID_LINEA'] = df['ID_LINEA'].apply(clean_id)
        df_v['ID_LINEA'] = df_v['ID_LINEA'].apply(clean_id)

        # 4. CRUCES (Merge original de Pandas)
        df = pd.merge(df, df_v, on='ID_LINEA', how='left')
        df = pd.merge(df, df_tarifas, on='GT', how='left')

        # 5. CONVERSIÓN NUMÉRICA (Solo para las columnas de plata y usos)
        cols_calc = ['DEBITADO', 'DESCUENTO_X_INTEGRACION', 'CANTIDAD_USOS', 'TARIFA_FEB', 'TARIFA_BASE_ITG']
        for c in [x for x in cols_calc if x in df.columns]:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)

        # 6. REGLAS Y FÓRMULAS
        df['BE'] = df['CONTRATO'].isin(['830', '831', '832', '833']).map({True: 'SI', False: 'NO'})
        df['PROV_FINAL'] = df.apply(lambda x: 'CABA' if x['GT'] == 'DF' else x['PROVINCIA'], axis=1)
        df['COMP_ITG'] = df['DESCUENTO_X_INTEGRACION'] * df['CANTIDAD_USOS']
        
        def calc_ats(row):
            if str(row['CONTRATO']) == '621':
                if row['GT'] == 'INP': return (row['DEBITADO'] / 0.45 * 0.55) * row['CANTIDAD_USOS']
                t_ref = row['TARIFA_FEB'] if row['TARIFA_FEB'] > 0 else row['TARIFA_BASE_ITG']
                return (t_ref - row['DEBITADO'] - row['DESCUENTO_X_INTEGRACION']) * row['CANTIDAD_USOS']
            return 0.0

        df['COMP_ATS'] = df.apply(calc_ats, axis=1)

        # 7. ENERGÍAS
        df_e = pd.read_excel(fe)
        df_e.columns = [str(c).upper().strip() for c in df_e.columns]
        df_e['DOMINIO'] = df_e['DOMINIO'].astype(str).str.strip().str.upper()
        df = pd.merge(df, df_e[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df['ENERGIA'] = df['ENERGIA'].fillna(3)
        df['DOMINIO_REAL'] = df.apply(lambda x: x['DOMINIO'] if pd.notnull(x['ENERGIA']) else 'NO', axis=1)

        # 8. AGRUPACIÓN FINAL
        df['COMP. ATS s/IVA'] = df['COMP_ATS'] / 1.105
        df['COMP. ITG s/IVA'] = df['COMP_ITG'] / 1.105

        agrupadores = ['PROV_FINAL', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'ID_LINEA', 'DOMINIO_REAL', 'ENERGIA', 'CONTRATO', 'BE', 'TARIFA_FEB', 'DEBITADO', 'DESCUENTO_X_INTEGRACION']
        
        return df.groupby(agrupadores, as_index=False).agg({
            'CANTIDAD_USOS': 'sum', 'COMP_ITG': 'sum', 'COMP_ATS': 'sum', 'COMP. ATS s/IVA': 'sum', 'COMP. ITG s/IVA': 'sum'
        })

    except Exception as e:
        st.error(f"Error en Paso 2: {e}")
        return None

st.title("📂 Procesador DMK v16.5 - Liquidación JN")

if 'm_tar' not in st.session_state: st.session_state.m_tar = None

tabs = st.tabs(["💰 1. TARIFAS", "🚀 2. PROCESAR DMK"])

with tabs[0]:
    col1, col2 = st.columns(2)
    f_tar = col1.file_uploader("Cuadro Anterior (Ej: Noviembre)", key="up_t")
    f_mult = col2.file_uploader("CSV Multiplicadores (Opcional)", key="up_m")
    
    st.markdown("---")
    
    # Campo para inyectar la celda maestra de tu Excel
    st.info("💡 Si el Excel tiene un porcentaje oficial en la primera fila, cargalo acá para mayor precisión.")
    coef_input = st.number_input("Coeficiente de Actualización (Ej: 1.3149)", value=0.0000, format="%.4f")
    
    st.markdown("---")
    c = st.columns(5)
    # Valores seteados por defecto según tu última grilla de febrero
    m = {
        '1SCN': c[0].number_input("1SCN", 650.00), 
        '2SCN': c[1].number_input("2SCN", 724.09), 
        '3SCN': c[2].number_input("3SCN", 779.87), 
        '4SCN': c[3].number_input("4SCN", 835.71), 
        '5SCN': c[4].number_input("5SCN", 891.16)
    }
    
    if f_tar and st.button("📊 Generar Cuadro Exacto"):
        with st.spinner("Compilando matriz tarifaria..."):
            df_nov = pd.read_excel(f_tar)
            
            # Carga condicional del archivo de multiplicadores si decidís usarlo
            df_multiplicadores = None
            if f_mult is not None:
                try:
                    df_multiplicadores = pd.read_csv(f_mult, sep=';', encoding='utf-8')
                except:
                    f_mult.seek(0)
                    df_multiplicadores = pd.read_csv(f_mult, sep=',', encoding='utf-8')
            
            # Ejecutamos el motor definitivo
            st.session_state.m_tar = motor_tarifas_definitivo(
                df_nov=df_nov, 
                df_multiplicadores=df_multiplicadores, 
                manuales=m, 
                coef_oficial=coef_input
            )
            
    if st.session_state.m_tar is not None: 
        st.success("✅ Matriz calculada con éxito.")
        st.dataframe(st.session_state.m_tar)

# =============================================================================
# INTERFAZ (UI)
# =============================================================================

st.title("📂 Procesador DMK v16.4")

if 'm_tar' not in st.session_state: st.session_state.m_tar = None

tabs = st.tabs(["💰 1. TARIFAS", "🚀 2. PROCESAR DMK"])

with tabs[0]:
    f_tar = st.file_uploader("Cuadro Noviembre", key="up_t")
    if f_tar:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"):
            st.session_state.m_tar = motor_tarifas_original(pd.read_excel(f_tar), m)
    if st.session_state.m_tar is not None: st.dataframe(st.session_state.m_tar)

with tabs[1]:
    if st.session_state.m_tar is not None:
        ca, cb = st.columns(2)
        fv = ca.file_uploader("Nomenclador V", key="up_v")
        fe = cb.file_uploader("Energías", key="up_e")
        fz = st.file_uploader("Archivo DMK", key="up_z")
        
        if fv and fe and fz and st.button("🚀 INICIAR PROCESO"):
            with st.spinner("Liquidando DMK..."):
                res = procesar_dmk_v16_4(fz, pd.read_excel(fv), st.session_state.m_tar, fe)
                if res is not None:
                    st.success(f"¡Listo! Se procesaron {len(res)} registros.")
                    st.download_button("📥 DESCARGAR EXCEL", preparar_descarga(res), "DMK_Liquidacion.xlsx")
                    st.dataframe(res.head(15))
    else:
        st.warning("Configurá las tarifas primero.")
