import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fiscalización TTR JN", layout="wide")

def preparar_descarga(df):
    if df is None: return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# =============================================================================
# BLOQUE 1: MOTOR DE TARIFAS (CON VISUALIZACIÓN DE COLUMNAS)
# =============================================================================

def motor_tarifas_proyeccion(df_nov, manuales):
    try:
        df = df_nov.copy()
        # Normalización total
        df.columns = [str(c).upper().strip() for c in df.columns]
        
        # DEBUG: Mostramos las columnas en la App para que Natalia las vea
        st.write("🔍 Columnas detectadas en Noviembre:", list(df.columns))
        
        # Buscadores con redundancia
        c_ids = [c for c in df.columns if any(x in c for x in ['ID', 'GT', 'GRUPO'])]
        c_precios = [c for c in df.columns if any(x in c for x in ['LIMITE', 'TARIFA', 'PRECIO', 'MONTO'])]
        
        if not c_ids:
            st.error("❌ No encontré la columna de 'ID' o 'GT'.")
            return None
        if not c_precios:
            st.error("❌ No encontré la columna de 'Tarifa' o 'Limite'.")
            return None
            
        col_id, col_p = c_ids[0], c_precios[0]
        
        # Buscamos el valor de 1SCN
        val_raw = df.loc[df[col_id].str.contains('1SCN', na=False), col_p].values
        
        def clean_num(x):
            try:
                return float(str(x).replace(',', '.'))
            except:
                return 0.0

        v1_ant = clean_num(val_raw[0]) if len(val_raw) > 0 else 270.0
        factor = manuales['1SCN'] / v1_ant if v1_ant > 0 else 1.0
        
        res = []
        for _, row in df.iterrows():
            id_t = str(row[col_id]).strip().upper()
            v_ant = clean_num(row[col_p])
            v_nue = manuales.get(id_t, v_ant * factor if v_ant > 0 else manuales['1SCN'])
            if any(x in id_t for x in ['SGI', 'UPA']) and id_t not in manuales: v_nue = manuales['1SCN']
            res.append({'GT': id_t, 'TARIFA_FEB': round(v_nue, 2)})
        
        return pd.DataFrame(res)
    except Exception as e:
        st.error(f"Error en Paso 1: {e}")
        return None

# =============================================================================
# BLOQUE 2: MOTOR DMK (V15.3 - EVITAR EL INDEXERROR)
# =============================================================================

def procesar_dmk_v15_3(fz, df_v, df_tarifas, fe):
    try:
        cols_dmk = ["ID_EMPRESA", "ID_LINEA", "DOMINIO", "DEBITADO", "CONTRATO", "DESCUENTO X INTEGRACION", "CANTIDAD_USOS", "TARIFA BASE ITG"]
        
        # Leemos el archivo asegurando que no explote
        if fz.name.endswith('.zip'):
            with zipfile.ZipFile(fz) as z:
                with z.open(z.namelist()[0]) as f:
                    lf = pl.read_csv(f.read(), separator=';', encoding='iso-8859-1', columns=cols_dmk, infer_schema_length=0).lazy()
        else:
            lf = pl.read_csv(fz, separator=';', encoding='iso-8859-1', columns=cols_dmk, infer_schema_length=0).lazy()

        # Normalización de nombres en Polars
        nombres = lf.collect_schema().names()
        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in nombres})

        # Nomenclador V (Sin las columnas borradas)
        df_v.columns = [str(c).upper().strip() for c in df_v.columns]
        v_cols = [c for c in ["ID_LINEA", "GT", "PROVINCIA", "MUNICIPIO"] if c in df_v.columns]
        v_pl = pl.from_pandas(df_v[v_cols]).lazy()
        v_pl = v_pl.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", "").alias("ID_LINEA_KEY"))

        # Cruce y Cálculos
        lf = lf.with_columns(pl.col("ID_LINEA").cast(pl.Utf8).str.replace(r"\.0$", "").str.strip_chars())
        lf = lf.join(v_pl, left_on="ID_LINEA", right_on="ID_LINEA_KEY", how="inner")

        for c in ["TARIFA_BASE_ITG", "DEBITADO", "DESCUENTO_X_INTEGRACION", "CANTIDAD_USOS"]:
            lf = lf.with_columns(pl.col(c).cast(pl.Float64, strict=False).fill_null(0))

        tar_pl = pl.from_pandas(df_tarifas).lazy().with_columns(pl.col("GT").cast(pl.Utf8).alias("GT_TAR"))
        lf = lf.join(tar_pl, left_on="GT", right_on="GT_TAR", how="left")

        # Fórmulas
        lf = lf.with_columns([
            (pl.col("DESCUENTO_X_INTEGRACION") * pl.col("CANTIDAD_USOS")).alias("COMP_ITG"),
            pl.when(pl.col("CONTRATO") == "621")
              .then(
                  pl.when(pl.col("GT") == "INP")
                    .then((pl.col("DEBITADO") / 0.45 * 0.55) * pl.col("CANTIDAD_USOS"))
                    .otherwise((pl.col("TARIFA_FEB") - pl.col("DEBITADO") - pl.col("DESCUENTO_X_INTEGRACION")) * pl.col("CANTIDAD_USOS"))
              ).otherwise(0).alias("COMP_ATS"),
            pl.when(pl.col("CONTRATO").is_in(["830", "831", "832", "833"])).then(pl.lit("SI")).otherwise(pl.lit("NO")).alias("BE"),
            pl.when(pl.col("GT") == "DF").then(pl.lit("CABA")).otherwise(pl.col("PROVINCIA")).alias("PROV_FINAL")
        ])

        # Reporte final
        df_res = lf.collect().to_pandas()
        pme = pd.read_excel(fe)
        pme.columns = [str(c).upper().strip() for c in pme.columns]
        pme['DOMINIO'] = pme['DOMINIO'].astype(str).str.strip().str.upper()
        
        df_pm = df_res[df_res['DOMINIO'].isin(pme['DOMINIO'])].merge(pme[['DOMINIO', 'ENERGIA']].drop_duplicates(), on='DOMINIO', how='left')
        df_resto = df_res[~df_res['DOMINIO'].isin(pme['DOMINIO'])].copy()
        df_resto['DOMINIO'], df_resto['ENERGIA'] = 'NO', 3
        df_final = pd.concat([df_pm, df_resto], ignore_index=True)

        df_final['COMP. ATS s/IVA'] = df_final['COMP_ATS'] / 1.105
        df_final['COMP. ITG s/IVA'] = df_final['COMP_ITG'] / 1.105

        agrupadores = [c for c in ['PROV_FINAL', 'MUNICIPIO', 'ID_EMPRESA', 'GT', 'ID_LINEA', 'DOMINIO', 'ENERGIA', 'CONTRATO', 'BE', 'TARIFA_FEB', 'DEBITADO', 'DESCUENTO_X_INTEGRACION'] if c in df_final.columns]
        
        return df_final.groupby(agrupadores, as_index=False).agg({
            'CANTIDAD_USOS': 'sum', 'COMP_ITG': 'sum', 'COMP_ATS': 'sum', 'COMP. ATS s/IVA': 'sum', 'COMP. ITG s/IVA': 'sum'
        })
    except Exception as e:
        st.error(f"Error en Paso 2: {e}")
        return None

# =============================================================================
# UI
# =============================================================================

st.title("🏛️ Sistema de Fiscalización TTR")

if 'memo_tar' not in st.session_state: st.session_state.memo_tar = None
if 'memo_ttr' not in st.session_state: st.session_state.memo_ttr = None

t = st.tabs(["💰 1. TARIFAS", "⚡ 2. PROCESAR TTR"])

with t[0]:
    f_n = st.file_uploader("Cuadro Noviembre", key="f_n_153")
    if f_n:
        c = st.columns(5)
        m = {'1SCN': c[0].number_input("1SCN", 494.33), '2SCN': c[1].number_input("2SCN", 551.24), '3SCN': c[2].number_input("3SCN", 593.70), '4SCN': c[3].number_input("4SCN", 636.21), '5SCN': c[4].number_input("5SCN", 678.42)}
        if st.button("📊 Proyectar"):
            st.session_state.memo_tar = motor_tarifas_proyeccion(pd.read_excel(f_n), m)
    if st.session_state.memo_tar is not None: st.dataframe(st.session_state.memo_tar)

with t[1]:
    if st.session_state.memo_tar is not None:
        ca, cb = st.columns(2)
        fv = ca.file_uploader("Nomenclador V", key="fv_153")
        fe = cb.file_uploader("Energías", key="fe_153")
        fz = st.file_uploader("DMK (ZIP/CSV)", key="fz_153")
        
        if fv and fe and fz and st.button("🚀 INICIAR PROCESO"):
            st.session_state.memo_ttr = procesar_dmk_v15_3(fz, pd.read_excel(fv), st.session_state.memo_tar, fe)
            if st.session_state.memo_ttr is not None:
                st.success("TTR Finalizado.")
                st.download_button("📥 DESCARGAR EXCEL", preparar_descarga(st.session_state.memo_ttr), "TTR_Final.xlsx")
                st.dataframe(st.session_state.memo_ttr.head(10))
