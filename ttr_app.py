import streamlit as st
import polars as pl
import pandas as pd
import io
import zipfile

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR v3.0", layout="wide")

# =============================================================================
# 1. MOTOR DE LÓGICA CON POLARS (EL SECRETO PARA ARCHIVOS GRANDES)
# =============================================================================

def procesar_con_polars(f_input, f_gt, f_en):
    """Procesamiento ultra-rápido y eficiente en memoria."""
    # 1. Cargar Nomenclador y Energías (archivos pequeños en Pandas)
    nom_gt = pd.read_excel(f_gt)
    nom_gt.columns = nom_gt.columns.str.upper()
    nom_gt['ID_LINEA'] = nom_gt['ID_LINEA'].astype(str).str.strip().str.replace('.0', '', regex=False)
    ids_validos = nom_gt['ID_LINEA'].unique().tolist()

    df_en = pd.read_excel(f_en)
    df_en.columns = df_en.columns.str.upper()
    df_en['DOMINIO'] = df_en['DOMINIO'].astype(str).str.strip().str.upper()
    map_energia = dict(zip(df_en['DOMINIO'], df_en['ENERGIA']))

    # 2. Leer el CSV gigante con Polars (Streaming mode)
    # Si es ZIP, extraemos el contenido primero en memoria
    if f_input.name.endswith('.zip'):
        with zipfile.ZipFile(f_input) as z:
            csv_file = [n for n in z.namelist() if n.endswith('.csv')][0]
            data = z.read(csv_file)
            # Polars lee el contenido binario directamente
            lf = pl.read_csv(data, encoding='iso-8859-1', infer_schema_length=10000).lazy()
    else:
        lf = pl.read_csv(f_input.getvalue(), encoding='iso-8859-1', infer_schema_length=10000).lazy()

    # 3. Limpieza y Filtrado "Lazy" (No consume RAM hasta el final)
    lf = lf.rename({c: c.strip().upper() for c in lf.columns})
    
    # Filtrar columnas de interés para reducir el ancho de la tabla
    cols_necesarias = ['PROVINCIA', 'MUNICIPIO', 'GT', 'ID_LINEA', 'RAMAL', 'DOMINIO', 
                       'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION', 
                       'CANTIDAD_USOS', 'MONTO']
    
    lf = lf.select([pl.col(c) for c in cols_necesarias])
    
    # Limpiar ID_LINEA y filtrar
    lf = lf.with_columns(
        pl.col("ID_LINEA").cast(pl.Utf8).str.strip_chars().str.replace(r"\.0$", "")
    ).filter(pl.col("ID_LINEA").is_in(ids_validos))

    # 4. Agrupamiento pesado (Se hace en el motor de Polars, no en RAM de Python)
    lf_grouped = lf.group_by(['GT', 'ID_LINEA', 'RAMAL', 'DOMINIO', 'CONTRATO', 'TARIFA BASE ITG', 'DEBITADO', 'DESCUENTO X INTEGRACION']).agg([
        pl.col('CANTIDAD_USOS').sum(),
        pl.col('MONTO').sum()
    ])

    # 5. Ejecutar el plan y convertir a Pandas para el final (aquí ya es pequeño)
    df_res = lf_grouped.collect().to_pandas()

    # 6. Cruces finales (ya con datos resumidos)
    df_res = df_res.merge(nom_gt[['ID_LINEA', 'LINEA SILAS DNGFF', 'PROVINCIA', 'MUNICIPIO']].drop_duplicates(), on='ID_LINEA', how='left')
    df_res['ENERGIA'] = df_res['DOMINIO'].str.strip().str.upper().map(map_energia).fillna(3)
    
    # Cálculos de compensación
    df_res['COMP. ITG'] = df_res['DESCUENTO X INTEGRACION'] * df_res['CANTIDAD_USOS']
    df_res['COMP. ATS'] = df_res.apply(lambda x: (((x['DEBITADO']/0.45)*0.55)*x['CANTIDAD_USOS'] if x['GT']=='INP' else (x['TARIFA BASE ITG']-x['DEBITADO']-x['DESCUENTO X INTEGRACION'])*x['CANTIDAD_USOS']) if x['CONTRATO']==621 else 0, axis=1)
    
    return df_res

# =============================================================================
# 2. INTERFAZ STREAMLIT
# =============================================================================

st.title("🚀 TTR Engine v3.0 (Polars Edition)")
st.info("Esta versión está diseñada para procesar archivos de +200MB sin caídas.")

if 'df_pme' not in st.session_state: st.session_state.df_pme = None

# Solo pongo la Tab 2 para probar la carga pesada
tab1, tab2, tab3 = st.tabs(["Tarifas", "Pre-Proceso", "Resultado"])

with tab2:
    c1, c2, c3 = st.columns(3)
    f_csv = c1.file_uploader("SUBE (CSV o ZIP)", type=['csv', 'zip'])
    f_gt = c2.file_uploader("Nomenclador (Excel)", type=['xlsx'])
    f_en = c3.file_uploader("Energías (Excel)", type=['xlsx'])

    if f_csv and f_gt and f_en:
        if st.button("🔥 Iniciar Procesamiento"):
            with st.spinner("Polars está procesando el archivo gigante..."):
                try:
                    st.session_state.df_pme = procesar_con_polars(f_csv, f_gt, f_en)
                    st.success(f"¡Éxito! Base resumida a {len(st.session_state.df_pme)} filas.")
                    st.dataframe(st.session_state.df_pme.head())
                except Exception as e:
                    st.error(f"Error: {e}")

if st.session_state.df_pme is not None:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as wr:
        st.session_state.df_pme.to_excel(wr, index=False)
