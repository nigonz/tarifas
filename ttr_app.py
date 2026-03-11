import pandas as pd
import numpy as np
import streamlit as st
import polars as pl
import io
import zipfile

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR v5.0", layout="wide")

# =============================================================================
# 1. TUS FUNCIONES (LÓGICA ORIGINAL PROTEGIDA)
# =============================================================================

def proyectar_tarifas(df_nov, nuevas_scn):
    """Tu lógica original de Tarifas."""
    try:
        v1_nov = df_nov.loc[df_nov['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor = nuevas_scn['1SCN'] / v1_nov
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
            v_f = 0
            if any(x in id_t for x in ['SEN', 'SCSN', 'SEAN', 'SESN', 'SEASN']):
                n = id_t[0] if id_t[0].isdigit() else "1"
                b = scn_full.get(f"{n}SCN", scn_full["1SCN"])
                if 'SESN' in id_t: v_f = (b * 1.59) * 1.25
                elif 'SEASN' in id_t: v_f = (b * 1.59) * 1.75
                elif 'SCSN' in id_t: v_f = b * 1.59
                elif 'SEN' in id_t: v_f = b * 1.25
                elif 'SEAN' in id_t: v_f = b * 1.75
            elif id_t in scn_full: v_f = scn_full[id_t]
            else: v_f = row['Limite Inferior'] * factor
            df.at[i, 'Limite Inferior'] = df.at[i, 'Limite Superior'] = round(v_f, 2)
        return df
    except Exception as e:
        st.error(f"Error en Tarifas: {e}")
        return None

def motor_preproceso_estable(f_sube, f_gt, f_en):
    """Lógica de Preproceso usando Polars para no agotar la RAM."""
    # 1. Preparar Nomencladores (Archivos chicos en Pandas)
    nom_gt = pd.read_excel(f_gt)
    nom_gt.columns = nom_gt.columns.str.strip().str.upper()
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.replace('.0', '', regex=False)
    
    en = pd.read_excel(f_en)
    en.columns = en.columns.str.strip().str.upper()
    en['DOMINIO'] = en['DOMINIO'].astype(str).str.strip().str.upper()

    # 2. Leer SUBE (ZIP/CSV) con Polars (Streaming)
    if f_sube.name.endswith('.zip'):
        with zipfile.ZipFile(f_sube) as z:
            csv_n = [n for n in z.namelist() if n.endswith('.csv')][0]
            data = z.read(csv_n)
            lf = pl.read_csv(data, encoding='iso-8859-1', separator=";", infer_schema_length=10000).lazy()
    else:
        lf = pl.read_csv(f_sube.getvalue(), encoding='iso-8859-1', separator=";", infer_schema_length=10000).lazy()

    # 3. Limpiar y Unir con Nomenclador (Para traer el GT antes de agrupar)
    lf = lf.rename({c: c.strip().upper() for c in lf.columns})
    lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", ""))
    
    # Convertimos Nomenclador a Polars para el Join eficiente
    gt_pl = pl.from_pandas(nom_gt[['ID_LINEA', 'GT', 'LINEA SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']].drop_duplicates()).lazy()
    
    # El Cruce (Merge) - Aquí es donde Pandas estallaba, Polars no.
    lf = lf.join(gt_pl, on="ID_LINEA", how="left")
    
    # 4. Agrupar datos pesados
    lf_agg = lf.group_by(['GT', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION', 'LINEA SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']).agg([
        pl.col('CANTIDAD_USOS').sum(),
        pl.col('MONTO').sum()
    ])

    # 5. Volver a Pandas para el final (Ya es una tabla chiquita)
    res = lf_agg.collect().to_pandas()
    
    # Cruce final con Energías
    res['DOMINIO'] = res['DOMINIO'].astype(str).str.strip().str.upper()
    res = res.merge(en[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
    res['ENERGIA'] = res['ENERGIA'].fillna(3)
    
    # Tus fórmulas originales
    res['COMP. ITG'] = res['DESCUENTO X INTEGRACION'] * res['CANTIDAD_USOS']
    res['COMP. ATS'] = res.apply(lambda x: (((x['DEBITADO']/0.45)*0.55)*x['CANTIDAD_USOS'] if x['GT']=='INP' else (x['TARIFA BASE ITG']-x['DEBITADO']-x['DESCUENTO X INTEGRACION'])*x['CANTIDAD_USOS']) if x['CONTRATO']==621 else 0, axis=1)
    
    return res

def motor_ttr(df_pme, df_tarifas, anio, reso):
    """Tu lógica de Ingeniería Inversa y CONCAT."""
    df = df_pme.copy()
    anio_str, reso_str = str(int(anio)), str(reso).strip()
    
    df['TARIFA BASE ITG'] = pd.to_numeric(df['TARIFA BASE ITG'], errors='coerce').round(2)
    df_tarifas['Limite Inferior'] = pd.to_numeric(df_tarifas['Limite Inferior'], errors='coerce').round(2)
    
    lookup = df_tarifas.drop_duplicates(subset=['Limite Inferior']).set_index('Limite Inferior')['Id'].to_dict()
    df['NODO_ID'] = df['TARIFA BASE ITG'].map(lookup).fillna("S/D")
    
    df['SEC_NUM'] = df['NODO_ID'].str.extract('(\d+)').fillna('0')
    df['SEC_FINAL'] = np.where((df['GT'] == "SGII") & (df['SEC_NUM'].isin(['1', '2', '3'])), '4', df['SEC_NUM'])
    
    tipo_t = np.where(df['CONTRATO'] == 627, "SN", "N")
    df['CONCAT_MATCHEO3'] = (anio_str + reso_str + df['SEC_FINAL'].astype(str) + 
                             df['GT'].astype(str) + df['ID_LINEA'].astype(str) + "S" + pd.Series(tipo_t, index=df.index).astype(str))
    
    df['FACTOR_CORR'] = np.select([df['ENERGIA'] == 1, df['ENERGIA'] == 2], [1.3, 1.5], default=1.0)
    return df

# =============================================================================
# 2. INTERFAZ (UI)
# =============================================================================

st.title("Fiscalización TTR v5.0 🚀")

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
            st.success("Diccionario listo en memoria.")

with tab2:
    st.info("💡 Subí el ZIP con el CSV adentro (separado por ;)")
    c1, c2, c3 = st.columns(3)
    f_csv = c1.file_uploader("SUBE (ZIP/CSV)", type=['zip', 'csv'])
    f_gt = c2.file_uploader("Nomenclador (Excel)", type=['xlsx'])
    f_en = c3.file_uploader("Energías (Excel)", type=['xlsx'])
    
    if f_csv and f_gt and f_en:
        if st.button("🚀 Iniciar Proceso (Motor Polars)"):
            with st.spinner("Procesando datos pesados..."):
                st.session_state.df_pme = motor_preproceso_estable(f_csv, f_gt, f_en)
                if st.session_state.df_pme is not None:
                    st.success(f"¡Hecho! Resumido a {len(st.session_state.df_pme)} filas.")
                    st.dataframe(st.session_state.df_pme.head())

with tab3:
    if st.session_state.df_pme is not None and st.session_state.df_tarifas is not None:
        ca, cr = st.columns(2)
        anio = ca.number_input("Año:", value=2026)
        reso = cr.text_input("Reso:", value="86")
        f_teorico = st.file_uploader("Excel Resoluciones (Opcional)", type=['xlsx'])
        
        if st.button("🚀 Liquidar TTR"):
            res = motor_ttr(st.session_state.df_pme, st.session_state.df_tarifas, anio, reso)
            if f_teorico:
                t_ref = pd.read_excel(f_teorico, sheet_name='TTR')
                res = pd.merge(res, t_ref[['CONCAT', 'TTR E.C.']], left_on='CONCAT_MATCHEO3', right_on='CONCAT', how='left')
                res['PAGO_FINAL'] = (res['TTR E.C.'] * res['CANTIDAD_USOS']) * res['FACTOR_CORR']
            
            st.dataframe(res.head(10))
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='xlsxwriter') as wr: res.to_excel(wr, index=False)
            st.download_button("📥 Bajar Reporte TTR", buf.getvalue(), "TTR_Final.xlsx")
    else:
        st.warning("⚠️ Completá los pasos 1 y 2.")
