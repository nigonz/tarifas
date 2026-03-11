import streamlit as st
import polars as pl
import pandas as pd
import numpy as np
import io
import zipfile

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="TTR Fiscalización Pro", layout="wide")

# =============================================================================
# 1. FUNCIONES DE LÓGICA (EL MOTOR)
# =============================================================================

def motor_tarifas(df_nov, n1, n2):
    """Genera el diccionario de tarifas proyectado."""
    try:
        v1_nov = df_nov.loc[df_nov['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = n1 / v1_nov
        df = df_nov.copy()
        
        # Bases para nodos escolares
        bases = {'1SCN': n1, '2SCN': n2}
        for i in range(3, 6):
            id_scn = f"{i}SCN"
            base_nov = df.loc[df['Id'] == id_scn, 'Limite Inferior'].values[0]
            bases[id_scn] = base_nov * factor

        for i, row in df.iterrows():
            id_t = str(row['Id'])
            val = 0
            if any(x in id_t for x in ['SEN', 'SCSN', 'SEAN', 'SESN', 'SEASN']):
                n = id_t[0] if id_t[0].isdigit() else "1"
                b = bases.get(f"{n}SCN", bases['1SCN'])
                if 'SESN' in id_t:    val = (b * 1.59) * 1.25
                elif 'SEASN' in id_t: val = (b * 1.59) * 1.75
                elif 'SCSN' in id_t:  val = b * 1.59
                elif 'SEN' in id_t:   val = b * 1.25
                elif 'SEAN' in id_t:  val = b * 1.75
            elif id_t in bases:
                val = bases[id_t]
            else:
                val = row['Limite Inferior'] * factor
            df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(val, 2)
        return df
    except Exception as e:
        st.error(f"Error en Tarifas: {e}")
        return None

def motor_preproceso(f_sube, f_gt, f_en):
    """Procesamiento pesado con Polars (soporta ZIP)."""
    try:
        # Cargar Nomencladores
        gt = pd.read_excel(f_gt)
        gt.columns = gt.columns.str.strip().str.upper()
        gt['ID_LINEA'] = gt['ID_LINEA'].astype(str).str.replace('.0', '', regex=False)
        
        en = pd.read_excel(f_en)
        en.columns = en.columns.str.strip().str.upper()
        en['DOMINIO'] = en['DOMINIO'].astype(str).str.strip().str.upper()
        map_en = dict(zip(en['DOMINIO'], en['ENERGIA']))

        # Leer SUBE (ZIP o CSV)
        if f_sube.name.endswith('.zip'):
            with zipfile.ZipFile(f_sube) as z:
                csv_n = [n for n in z.namelist() if n.endswith('.csv')][0]
                data = z.read(csv_n)
                lf = pl.read_csv(data, encoding='iso-8859-1', infer_schema_length=10000).lazy()
        else:
            lf = pl.read_csv(f_sube.getvalue(), encoding='iso-8859-1', infer_schema_length=10000).lazy()

        # Limpieza y Agrupado Lazy
        lf = lf.rename({c: c.strip().upper() for c in lf.columns})
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))
        
        # Filtrar solo líneas del nomenclador para ahorrar RAM
        ids_v = gt['ID_LINEA'].unique().tolist()
        lf = lf.filter(pl.col("ID_LINEA").is_in(ids_v))

        # Agrupar
        lf_agg = lf.group_by(['GT', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION']).agg([
            pl.col('CANTIDAD_USOS').sum(),
            pl.col('MONTO').sum()
        ])

        res = lf_agg.collect().to_pandas()
        
        # Cruces finales
        res = res.merge(gt[['ID_LINEA', 'LINEA SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']].drop_duplicates(), on='ID_LINEA', how='left')
        res['ENERGIA'] = res['DOMINIO'].str.strip().str.upper().map(map_en).fillna(3)
        res['COMP. ITG'] = res['DESCUENTO X INTEGRACION'] * res['CANTIDAD_USOS']
        res['COMP. ATS'] = res.apply(lambda x: (((x['DEBITADO']/0.45)*0.55)*x['CANTIDAD_USOS'] if x['GT']=='INP' else (x['TARIFA BASE ITG']-x['DEBITADO']-x['DESCUENTO X INTEGRACION'])*x['CANTIDAD_USOS']) if x['CONTRATO']==621 else 0, axis=1)
        
        return res
    except Exception as e:
        st.error(f"Error en Pre-proceso: {e}")
        return None

def motor_ttr(df_pme, df_tarifas, anio, reso):
    """Ingeniería Inversa y CONCAT."""
    try:
        df = df_pme.copy()
        # Macheo
        df['TARIFA BASE ITG'] = pd.to_numeric(df['TARIFA BASE ITG']).round(2)
        df_tarifas['Limite Inferior'] = pd.to_numeric(df_tarifas['Limite Inferior']).round(2)
        
        lookup = df_tarifas.drop_duplicates(subset=['Limite Inferior']).set_index('Limite Inferior')['Id'].to_dict()
        df['NODO_ID'] = df['TARIFA BASE ITG'].map(lookup).fillna("S/D")
        
        # Seccionado
        df['SEC_NUM'] = df['NODO_ID'].str.extract('(\d+)').fillna('0')
        df['SEC_FINAL'] = np.where((df['GT'] == "SGII") & (df['SEC_NUM'].isin(['1', '2', '3'])), '4', df['SEC_NUM'])
        
        # Llave CONCAT_MATCHEO3
        tipo_t = np.where(df['CONTRATO'] == 627, "SN", "N")
        df['CONCAT_MATCHEO3'] = (str(int(anio)) + str(reso) + df['SEC_FINAL'].astype(str) + 
                                 df['GT'].astype(str) + df['ID_LINEA'].astype(str) + "S" + pd.Series(tipo_t, index=df.index).astype(str))
        
        df['FACTOR_CORR'] = np.select([df['ENERGIA'] == 1, df['ENERGIA'] == 2], [1.3, 1.5], default=1.0)
        return df
    except Exception as e:
        st.error(f"Error en TTR: {e}")
        return None

# =============================================================================
# 2. INTERFAZ (UI)
# =============================================================================

st.title("Fiscalización TTR v4.0 🚀")

if 'df_tarifas' not in st.session_state: st.session_state.df_tarifas = None
if 'df_pme' not in st.session_state: st.session_state.df_pme = None

tab1, tab2, tab3 = st.tabs(["💰 1. TARIFAS", "📂 2. PRE-PROCESO", "🚀 3. TTR FINAL"])

with tab1:
    f_nov = st.file_uploader("Subir JN11 (Excel)", type=['xlsx'])
    if f_nov:
        col_t1, col_t2 = st.columns(2)
        n1 = col_t1.number_input("Valor 1SCN:", value=650.0)
        n2 = col_t2.number_input("Valor 2SCN:", value=724.09)
        if st.button("🔄 Calcular Diccionario"):
            st.session_state.df_tarifas = motor_tarifas(pd.read_excel(f_nov, sheet_name='JN11'), n1, n2)
            st.success("Diccionario guardado en memoria.")
            st.dataframe(st.session_state.df_tarifas.head())

with tab2:
    st.info("💡 Sube el archivo de 215MB comprimido en .ZIP")
    c1, c2, c3 = st.columns(3)
    f_sube = c1.file_uploader("SUBE (ZIP/CSV)", type=['zip', 'csv'])
    f_gt = c2.file_uploader("GT (Excel)", type=['xlsx'])
    f_en = c3.file_uploader("Energías (Excel)", type=['xlsx'])
    
    if f_sube and f_gt and f_en:
        if st.button("🔥 Iniciar Procesamiento Polars"):
            with st.spinner("Procesando..."):
                st.session_state.df_pme = motor_preproceso(f_sube, f_gt, f_en)
                if st.session_state.df_pme is not None:
                    st.success(f"Finalizado: {len(st.session_state.df_pme)} filas resumidas.")

with tab3:
    if st.session_state.df_pme is not None and st.session_state.df_tarifas is not None:
        ca, cr = st.columns(2)
        anio = ca.number_input("Año:", value=2026)
        reso = cr.text_input("Resolución:", value="86")
        f_teorico = st.file_uploader("Excel Resoluciones (Opcional)", type=['xlsx'])
        
        if st.button("🚀 Calcular TTR"):
            res = motor_ttr(st.session_state.df_pme, st.session_state.df_tarifas, anio, reso)
            if res is not None:
                if f_teorico:
                    t_ref = pd.read_excel(f_teorico, sheet_name='TTR')
                    res = pd.merge(res, t_ref[['CONCAT', 'TTR E.C.']], left_on='CONCAT_MATCHEO3', right_on='CONCAT', how='left')
                    res['PAGO_FINAL'] = (res['TTR E.C.'] * res['CANTIDAD_USOS']) * res['FACTOR_CORR']
                
                st.dataframe(res.head(10))
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine='xlsxwriter') as wr: res.to_excel(wr, index=False)
                st.download_button("📥 Descargar Reporte Final", buf.getvalue(), "TTR_Final.xlsx")
    else:
        st.warning("⚠️ Debes completar los pasos 1 y 2.")
