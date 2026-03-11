import streamlit as st
import polars as pl
import pandas as pd
import numpy as np
import io
import zipfile

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="TTR Fiscalización Pro v4.1", layout="wide")

# =============================================================================
# 1. FUNCIONES DE LÓGICA
# =============================================================================

def motor_tarifas(df_nov, n1, n2):
    try:
        v1_nov = df_nov.loc[df_nov['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = n1 / v1_nov
        df = df_nov.copy()
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
            elif id_t in bases: val = bases[id_t]
            else: val = row['Limite Inferior'] * factor
            df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(val, 2)
        return df
    except Exception as e:
        st.error(f"Error en Tarifas: {e}")
        return None

def motor_preproceso(f_sube, f_gt, f_en):
    try:
        # 1. Cargar Nomencladores (Pandas)
        gt = pd.read_excel(f_gt)
        gt.columns = gt.columns.str.strip().str.upper()
        gt['ID_LINEA'] = gt['ID_LINEA'].astype(str).str.replace('.0', '', regex=False)
        
        en = pd.read_excel(f_en)
        en.columns = en.columns.str.strip().str.upper()
        en['DOMINIO'] = en['DOMINIO'].astype(str).str.strip().str.upper()
        map_en = dict(zip(en['DOMINIO'], en['ENERGIA']))

        # 2. Leer SUBE con Polars (Corrigiendo el Separador a ;)
        if f_sube.name.endswith('.zip'):
            with zipfile.ZipFile(f_sube) as z:
                csv_n = [n for n in z.namelist() if n.endswith('.csv')][0]
                data = z.read(csv_n)
                # Agregamos separator=";" para que no falle como en la imagen
                lf = pl.read_csv(data, encoding='iso-8859-1', separator=";", infer_schema_length=10000).lazy()
        else:
            lf = pl.read_csv(f_sube.getvalue(), encoding='iso-8859-1', separator=";", infer_schema_length=10000).lazy()

        # 3. Limpieza de columnas
        lf = lf.rename({c: c.strip().upper() for c in lf.columns})
        
        # Filtramos columnas que SI existen en el CSV antes del merge
        cols_csv = ["ID_LINEA", "GT", "RAMAL", "DOMINIO", "CONTRATO", "TARIFA BASE ITG", "DEBITADO", "DESCUENTO X INTEGRACION", "CANTIDAD_USOS", "MONTO"]
        lf = lf.select([pl.col(c) for c in cols_csv if c in lf.columns])
        
        # Normalizar ID_LINEA
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))
        
        # Filtrar solo líneas del nomenclador
        ids_v = gt['ID_LINEA'].unique().tolist()
        lf = lf.filter(pl.col("ID_LINEA").is_in(ids_v))

        # 4. Agrupar datos pesados
        lf_agg = lf.group_by(['GT', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION']).agg([
            pl.col('CANTIDAD_USOS').sum(),
            pl.col('MONTO').sum()
        ])

        # 5. Convertir a Pandas para el cruce final (ya es pequeño)
        res = lf_agg.collect().to_pandas()
        
        # 6. Pegar datos del Nomenclador (Aquí aparece PROVINCIA y MUNICIPIO)
        res = res.merge(gt[['ID_LINEA', 'LINEA SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']].drop_duplicates(), on='ID_LINEA', how='left')
        
        # 7. Energías y Compensaciones
        res['ENERGIA'] = res['DOMINIO'].str.strip().str.upper().map(map_en).fillna(3)
        res['COMP. ITG'] = res['DESCUENTO X INTEGRACION'] * res['CANTIDAD_USOS']
        res['COMP. ATS'] = res.apply(lambda x: (((x['DEBITADO']/0.45)*0.55)*x['CANTIDAD_USOS'] if x['GT']=='INP' else (x['TARIFA BASE ITG']-x['DEBITADO']-x['DESCUENTO X INTEGRACION'])*x['CANTIDAD_USOS']) if x['CONTRATO']==621 else 0, axis=1)
        
        return res
    except Exception as e:
        st.error(f"Error en Pre-proceso: {e}")
        return None

def motor_ttr(df_pme, df_tarifas, anio, reso):
    try:
        df = df_pme.copy()
        df['TARIFA BASE ITG'] = pd.to_numeric(df['TARIFA BASE ITG'], errors='coerce').round(2)
        df_tarifas['Limite Inferior'] = pd.to_numeric(df_tarifas['Limite Inferior'], errors='coerce').round(2)
        
        lookup = df_tarifas.drop_duplicates(subset=['Limite Inferior']).set_index('Limite Inferior')['Id'].to_dict()
        df['NODO_ID'] = df['TARIFA BASE ITG'].map(lookup).fillna("S/D")
        
        df['SEC_NUM'] = df['NODO_ID'].str.extract('(\d+)').fillna('0')
        df['SEC_FINAL'] = np.where((df['GT'] == "SGII") & (df['SEC_NUM'].isin(['1', '2', '3'])), '4', df['SEC_NUM'])
        
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

st.sidebar.header("📅 Período")
mes_sel = st.sidebar.selectbox("Mes:", ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"], index=1)
anio_sel = st.sidebar.selectbox("Año:", [2025, 2026], index=1)

if 'df_tarifas' not in st.session_state: st.session_state.df_tarifas = None
if 'df_pme' not in st.session_state: st.session_state.df_pme = None

tab1, tab2, tab3 = st.tabs(["💰 1. TARIFAS", "📂 2. PRE-PROCESO", "🚀 3. TTR FINAL"])

with tab1:
    f_nov = st.file_uploader("Subir Base Tarifas (Excel)", type=['xlsx'])
    if f_nov:
        c1, c2 = st.columns(2)
        n1 = c1.number_input("Valor 1SCN:", value=650.0)
        n2 = c2.number_input("Valor 2SCN:", value=724.09)
        if st.button("🔄 Generar Diccionario"):
            st.session_state.df_tarifas = motor_tarifas(pd.read_excel(f_nov, sheet_name='JN11'), n1, n2)
            st.success("Diccionario guardado.")

with tab2:
    st.info("💡 Subí el archivo ZIP (CSV con separador ';')")
    c1, c2, c3 = st.columns(3)
    f_sube = c1.file_uploader("SUBE (ZIP/CSV)", type=['zip', 'csv'])
    f_gt = c2.file_uploader("Nomenclador (Excel)", type=['xlsx'])
    f_en = c3.file_uploader("Energías (Excel)", type=['xlsx'])
    
    if f_sube and f_gt and f_en:
        if st.button("🔥 Iniciar Proceso Pesado"):
            with st.spinner("Procesando con Polars..."):
                st.session_state.df_pme = motor_preproceso(f_sube, f_gt, f_en)
                if st.session_state.df_pme is not None:
                    st.success(f"Finalizado: {len(st.session_state.df_pme)} registros.")
                    st.dataframe(st.session_state.df_pme.head())

with tab3:
    if st.session_state.df_pme is not None and st.session_state.df_tarifas is not None:
        ca, cr = st.columns(2)
        anio = ca.number_input("Año CONCAT:", value=anio_sel)
        reso = cr.text_input("Resolución CONCAT:", value="86")
        f_teorico = st.file_uploader("Excel Resoluciones (Opcional)", type=['xlsx'])
        
        if st.button("🚀 Liquidar TTR"):
            res = motor_ttr(st.session_state.df_pme, st.session_state.df_tarifas, anio, reso)
            if res is not None:
                if f_teorico:
                    t_ref = pd.read_excel(f_teorico, sheet_name='TTR')
                    res = pd.merge(res, t_ref[['CONCAT', 'TTR E.C.']], left_on='CONCAT_MATCHEO3', right_on='CONCAT', how='left')
                    res['PAGO_FINAL'] = (res['TTR E.C.'] * res['CANTIDAD_USOS']) * res['FACTOR_CORR']
                
                st.dataframe(res.head(10))
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine='xlsxwriter') as wr: res.to_excel(wr, index=False)
                st.download_button("📥 Bajar Reporte Final", buf.getvalue(), f"TTR_FINAL_{mes_sel}.xlsx")
    else:
        st.warning("⚠️ Completá pasos 1 y 2.")
