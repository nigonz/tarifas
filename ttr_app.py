import streamlit as st
import pandas as pd
import io

# =============================================================================
# MÓDULO 1: GENERADOR DE DICCIONARIO (Lógica "Tarifas comerciales.ipynb")
# =============================================================================

def motor_tarifas_comerciales(df_prev, dic_manuales):
    """
    Proyecta las tarifas basándose exclusivamente en las columnas Id, 
    Limite Superior y Limite Inferior.
    """
    # 1. Limpieza de nombres de columnas (quita espacios y asegura nombres correctos)
    df_prev.columns = [str(c).strip() for c in df_prev.columns]
    
    # 2. Conversión a numérico de los límites
    for col in ['Limite Inferior', 'Limite Superior']:
        if col in df_prev.columns:
            df_prev[col] = pd.to_numeric(df_prev[col].astype(str).str.replace(',', '.'), errors='coerce')

    # 3. Cálculo del Factor de Ajuste basado en 1SCN
    try:
        # Buscamos '1SCN' en la columna 'Id'
        v1_anterior = df_prev.loc[df_prev['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor_ajuste = dic_manuales['1SCN'] / v1_anterior
    except (IndexError, KeyError):
        st.error("❌ No se encontró la tarifa '1SCN' en la columna 'Id' para calcular el factor.")
        return None

    resultados = []

    for _, row in df_prev.iterrows():
        id_t = str(row['Id']).strip()
        v_min_ant = row['Limite Inferior']
        v_max_ant = row['Limite Superior']

        # REGLAS DE NEGOCIO (Acuerdo de Cod)

        # A. Bloque SCN (Las 5 manuales)
        if id_t in dic_manuales:
            v_min = v_max = dic_manuales[id_t]

        # B. Bloques Proporcionales (Basados en las nuevas SCN)
        elif 'SEN' in id_t and 'SESN' not in id_t:
            base = dic_manuales.get(id_t.replace('SEN', 'SCN'), dic_manuales['1SCN'])
            v_min = v_max = base * 1.25
        elif 'SEAN' in id_t and 'SEASN' not in id_t:
            base = dic_manuales.get(id_t.replace('SEAN', 'SCN'), dic_manuales['1SCN'])
            v_min = v_max = base * 1.75
        elif 'SCSN' in id_t:
            base = dic_manuales.get(id_t.replace('SCSN', 'SCN'), dic_manuales['1SCN'])
            v_min = v_max = base * 1.59

        # C. Bloques SESN / SEASN (Derivados de SCSN)
        elif 'SESN' in id_t:
            base_scsn = dic_manuales.get(id_t.replace('SESN', 'SCN'), dic_manuales['1SCN']) * 1.59
            v_min = v_max = base_scsn * 1.25
        elif 'SEASN' in id_t:
            base_scsn = dic_manuales.get(id_t.replace('SEASN', 'SCN'), dic_manuales['1SCN']) * 1.59
            v_min = v_max = base_scsn * 1.75

        # D. Resto de tarifas (Ajuste por factor de incremento)
        else:
            v_min = v_min_ant * factor_ajuste
            v_max = v_max_ant * factor_ajuste

        resultados.append({
            'Id': id_t,
            'Limite Superior': round(v_max, 2),
            'Limite Inferior': round(v_min, 2)
        })

    return pd.DataFrame(resultados)

# =============================================================================
# INTERFAZ TAB 1 (AISLADA)
# =============================================================================

st.title("Módulo de Tarifas Natalia v6.0")

if 'df_tarifas_2026' not in st.session_state:
    st.session_state.df_tarifas_2026 = None

# Subida del archivo de referencia (Noviembre)
f_ref = st.file_uploader("Subir Archivo de Referencia (Noviembre)", type=['xlsx', 'csv'])

if f_ref:
    # Parámetro dinámico para evitar error de JN07
    nombre_hoja = st.text_input("Nombre de la hoja en el Excel:", value="JN11")
    
    st.markdown("### Ingrese las 5 Tarifas SCN 2026")
    c1, c2, c3, c4, c5 = st.columns(5)
    t1 = c1.number_input("1SCN", value=494.33)
    t2 = c2.number_input("2SCN", value=551.24)
    t3 = c3.number_input("3SCN", value=593.70)
    t4 = c4.number_input("4SCN", value=636.21)
    t5 = c5.number_input("5SCN", value=678.42)

    if st.button("🚀 Calcular Proyección 2026"):
        try:
            # Lectura flexible (CSV o Excel)
            if f_ref.name.endswith('.csv'):
                df_nov = pd.read_csv(f_ref, sep=None, engine='python')
            else:
                df_nov = pd.read_excel(f_ref, sheet_name=nombre_hoja)
            
            tarifas_nuevas = {'1SCN': t1, '2SCN': t2, '3SCN': t3, '4SCN': t4, '5SCN': t5}
            
            # Ejecución del motor
            resultado = motor_tarifas_comerciales(df_nov, tarifas_nuevas)
            
            if resultado is not None:
                st.session_state.df_tarifas_2026 = resultado
                st.success("✅ Tarifas proyectadas correctamente.")
                st.dataframe(resultado)
        except Exception as e:
            st.error(f"Error al leer el archivo: {e}. Verifique el nombre de la hoja.")

# Botón de descarga (solo aparece si el cálculo fue exitoso)
if st.session_state.df_tarifas_2026 is not None:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
        st.session_state.df_tarifas_2026.to_excel(writer, index=False, sheet_name='Tarifas_2026')
    
    st.download_button(
        label="📥 Descargar Tarifas_Calculadas_2026.xlsx",
        data=buf.getvalue(),
        file_name="Tarifas_Calculadas_2026.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
