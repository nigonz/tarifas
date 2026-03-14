import streamlit as st
import pandas as pd
import polars as pl
import io
import zipfile
from datetime import datetime

# --- CONFIGURACIÓN DE NIVEL PRODUCCIÓN ---
st.set_page_config(page_title="Fiscalización TTR Natalia v8.6", layout="wide")

# =============================================================================
# BLOQUE 0: EL ESCUDO (NORMALIZACIÓN Y LIMPIEZA)
# =============================================================================

def blindar_nombres(df):
    """Estandariza encabezados para procesos internos."""
    if df is None: return None
    df.columns = [str(c).upper().strip().replace(" ", "_").split('_X')[0].split('_Y')[0] for c in df.columns]
    return df

def formatear_ids(df, columnas_id):
    """Asegura que los IDs sean texto puro y limpio."""
    for col in columnas_id:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.upper()
    return df

def motor_tarifas_v8(df_base, manuales):
    df = blindar_nombres(df_base.copy())
    col_id = [c for c in df.columns if 'ID' in c][0]
    for col in ['LIMITE_INFERIOR', 'LIMITE_SUPERIOR']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
    
    v1_ant = df.loc[df[col_id] == '1SCN', 'LIMITE_INFERIOR'].values[0]
    factor = manuales['1SCN'] / v1_ant
    
    res = []
    for _, row in df.iterrows():
        id_t = str(row[col_id]).strip().upper()
        v_min_ant, v_max_ant = row['LIMITE_INFERIOR'], row['LIMITE_SUPERIOR']
        
        if id_t in manuales: v_min = v_max = manuales[id_t]
        elif 'SEN' in id_t and 'SESN' not in id_t: v_min = v_max = manuales.get(id_t.replace('SEN', 'SCN'), manuales['1SCN']) * 1.25
        elif 'SEAN' in id_t and 'SEASN' not in id_t: v_min = v_max = manuales.get(id_t.replace('SEAN', 'SCN'), manuales['1SCN']) * 1.75
        elif 'SCSN' in id_t: v_min = v_max = manuales.get(id_t.replace('SCSN', 'SCN'), manuales['1SCN']) * 1.59
        elif 'SESN' in id_t: v_min = v_max = (manuales.get(id_t.replace('SESN', 'SCN'), manuales['1SCN']) * 1.59) * 1.25
        elif 'SEASN' in id_t: v_min = v_max = (manuales.get(id_t.replace('SEASN', 'SCN'), manuales['1SCN']) * 1.59) * 1.75
        else: v_min, v_max = v_min_ant * factor, v_max_ant * factor

        res.append({
            'ID': id_t, 'ANTERIOR': round(v_min_ant, 2), 'NUEVO': round(v_min, 2),
            'VAR_%': round(((v_min/v_min_ant)-1)*100, 2) if v_min_ant > 0 else 0,
            'LIMITE_SUPERIOR': round(v_max, 2)
        })
    return pd.DataFrame(res)
def motor_maestro_v8_6(df_v2, df_elr, df_ts):
    # 1. Guardar el molde original (Nombres de columnas y orden exacto)
    columnas_originales = df_v2.columns.tolist()
    ids_originales = df_v2[df_v2.columns[0]].astype(str).str.strip().str.upper().tolist()


# =============================================================================
# BLOQUE 1: MOTOR MAESTRO (FIX 16 COLUMNAS / 443 FILAS)
# =============================================================================

def motor_maestro_v8_6(df_v2, df_elr, df_ts):
    # 1. Guardar el molde original (Nombres de columnas y orden exacto)
    columnas_originales = df_v2.columns.tolist()
    ids_originales = df_v2[df_v2.columns[0]].astype(str).str.strip().str.upper().tolist()

    # 2. Normalización interna
    df_v2_int = blindar_nombres(df_v2.copy())
    df_elr_int = blindar_nombres(df_elr.copy())
    
    id_l_base = df_v2_int.columns[0] # Usualmente ID_LINEA
    id_l_elr = [c for c in df_elr_int.columns if 'ID_LINEA_BO' in c or ('ID' in c and 'LINEA' in c)][0]
    col_gt_elr = [c for c in df_elr_int.columns if 'GRUPO_TARIF' in c][0]

    df_v2_int = formatear_ids(df_v2_int, [id_l_base])
    df_elr_int = formatear_ids(df_elr_int, [id_l_elr])

    # 3. Limpieza de ELR para evitar duplicados (Evita pasar de 443 a 444 filas)
    elr_map = df_elr_int[[id_l_elr, col_gt_elr]].drop_duplicates(subset=[id_l_elr])

    # 4. Actualización Quirúrgica (Merge)
    # Solo nos traemos el GT nuevo, sin agregar columnas basura
    v3_final = df_v2_int.merge(elr_map, left_on=id_l_base, right_on=id_l_elr, how='left')

    # 5. Pisar el GT original con el nuevo
    if 'GT' in v3_final.columns:
        v3_final['GT'] = v3_final[col_gt_elr].fillna(v3_final['GT'])
    
    # 6. Restauración del Molde (Volver a 16 columnas y nombres originales)
    # Filtramos para que solo queden las IDs que estaban en el molde original (Elimina la 4220)
    v3_final = v3_final[v3_final[id_l_base].isin(ids_originales)]
    
    # Reasignamos los nombres de columna originales
    v3_final = v3_final[df_v2_int.columns] # Quitamos las columnas del merge
    v3_final.columns = columnas_originales # Restauramos espacios y mayúsculas originales

    # Auditoría simple
    audit = pd.DataFrame({
        "ID_LINEA": ids_originales,
        "ESTADO": ["✅ OK" if id in v3_final[v3_final.columns[0]].values else "❌ Perdido" for id in ids_originales]
    })

    return v3_final, audit

# =============================================================================
# BLOQUE 2: MOTOR DMK (SOLUCIÓN ERROR 'LM622' Y POLARS)
# =============================================================================

def motor_dmk_v8_6(f_sube, df_v3, df_en, fecha_corte="2026-02-14"):
    try:
        if f_sube.name.endswith('.zip'):
            with zipfile.ZipFile(f_sube) as z:
                csv_f = [n for n in z.namelist() if n.endswith('.csv')][0]
                with z.open(csv_f) as f: data = f.read()
        else: data = f_sube.getvalue()

        # SOLUCIÓN POLARS: infer_schema_length=0 obliga a leer TODO como texto (evita error LM622)
        lf = pl.read_csv(io.BytesIO(data), 
                         encoding='iso-8859-1', 
                         separator=";", 
                         infer_schema_length=0).lazy()

        # Limpieza de columnas
        lf = lf.rename({c: c.strip().upper().replace(" ", "_") for c in lf.collect_schema().names()})
        
        # Casteo seguro de IDs a String
        lf = lf.with_columns(pl.col("ID_LINEA").str.replace(r"\.0$", "").str.strip_chars().str.to_uppercase())
        
        # --- LÓGICA DE MES PARTIDO (TTR) ---
        # Convertimos fecha para el switch
        lf = lf.with_columns(pl.col("FECHA").str.to_date("%d/%m/%Y"))
        
        # Aquí se aplicaría el cruce con tarifas según la fecha
        # (Lógica simplificada para no romper el flujo actual)
        
        v3_pl = pl.from_pandas(formatear_ids(df_v3.copy(), [df_v3.columns[0]])).lazy()
        v3_pl = v3_pl.rename({df_v3.columns[0]: "ID_LINEA"})

        # Join y resultado
        res = lf.join(v3_pl, on="ID_LINEA", how="inner").collect().to_pandas()
        return res
    except Exception as e:
        st.error(f"Error crítico en DMK: {e}")
        return None

# =============================================================================
# INTERFAZ DE USUARIO
# =============================================================================

st.title("Fiscalización TTR Natalia v8.6")

if 'v3_pers' not in st.session_state: st.session_state.v3_pers = None

t1, t2, t3 = st.tabs(["💰 TARIFAS", "📋 NOMENCLADORES", "📂 PROCESO DMK"])

with t2:
    st.header("Actualización de Nomenclador (Modo Espejo)")
    col1, col2, col3 = st.columns(3)
    f_v2 = col1.file_uploader("Nomenclador Molde (16 col)", key="v2")
    f_elr = col2.file_uploader("ELR Febrero", key="elr")
    f_ts = col3.file_uploader("TS Febrero", key="ts")
    
    if f_v2 and f_elr and f_ts and st.button("🔄 Ejecutar Actualización"):
        v3, audit = motor_maestro_v8_6(pd.read_excel(f_v2), pd.read_excel(f_elr), pd.read_excel(f_ts))
        st.session_state.v3_pers = v3
        st.success(f"Proceso terminado. Filas: {len(v3)} | Columnas: {len(v3.columns)}")
        st.dataframe(v3.head())
        
        # Descarga
        buf = io.BytesIO()
        v3.to_excel(buf, index=False)
        st.download_button("📥 Descargar Nomenclador V3 Limpio", buf.getvalue(), "Nomenclador_V3_Final.xlsx")

with t3:
    if st.session_state.v3_pers is not None:
        st.header("Liquidación con Mes Partido")
        corte = st.date_input("Fecha de cambio de tarifa (inclusive)", datetime(2026, 2, 14))
        f_dmk = st.file_uploader("Archivo DMK", key="dmk")
        f_en = st.file_uploader("Energías", key="en")
        
        if f_dmk and f_en and st.button("⚡ Procesar"):
            res = motor_dmk_v8_6(f_dmk, st.session_state.v3_pers, pd.read_excel(f_en), str(corte))
            if res is not None:
                st.dataframe(res.head())
    else:
        st.warning("Primero generá el Nomenclador V3 en la pestaña anterior.")
