import streamlit as st
import pandas as pd
import io

# =============================================================================
# MOTOR DE CÁLCULO Y AUDITORÍA
# =============================================================================

def motor_tarifas_con_auditoria(df_prev, dic_manuales):
    """
    Proyecta tarifas y genera una tabla comparativa para validación inmediata.
    """
    df_prev.columns = [str(c).strip() for c in df_prev.columns]
    
    for col in ['Limite Inferior', 'Limite Superior']:
        if col in df_prev.columns:
            df_prev[col] = pd.to_numeric(df_prev[col].astype(str).str.replace(',', '.'), errors='coerce')

    try:
        v1_anterior = df_prev.loc[df_prev['Id'] == '1SCN', 'Limite Inferior'].values[0]
        factor_ajuste = dic_manuales['1SCN'] / v1_anterior
    except (IndexError, KeyError):
        st.error("No se encontró la tarifa '1SCN' en la columna 'Id'.")
        return None

    resultados = []
    
    for _, row in df_prev.iterrows():
        id_t = str(row['Id']).strip()
        v_min_ant = row['Limite Inferior']
        v_max_ant = row['Limite Superior']
        regla = ""

        # APLICACIÓN DE REGLAS SEGÚN ACUERDO
        if id_t in dic_manuales:
            v_min = v_max = dic_manuales[id_t]
            regla = "Ingreso Manual (SCN)"
        elif 'SEN' in id_t and 'SESN' not in id_t:
            base = dic_manuales.get(id_t.replace('SEN', 'SCN'), dic_manuales['1SCN'])
            v_min = v_max = base * 1.25
            regla = "Factor 1.25 (SEN)"
        elif 'SEAN' in id_t and 'SEASN' not in id_t:
            base = dic_manuales.get(id_t.replace('SEAN', 'SCN'), dic_manuales['1SCN'])
            v_min = v_max = base * 1.75
            regla = "Factor 1.75 (SEAN)"
        elif 'SCSN' in id_t:
            base = dic_manuales.get(id_t.replace('SCSN', 'SCN'), dic_manuales['1SCN'])
            v_min = v_max = base * 1.59
            regla = "Factor 1.59 (SCSN)"
        elif 'SESN' in id_t:
            base_scsn = dic_manuales.get(id_t.replace('SESN', 'SCN'), dic_manuales['1SCN']) * 1.59
            v_min = v_max = base_scsn * 1.25
            regla = "Compuesta (SCSN * 1.25)"
        elif 'SEASN' in id_t:
            base_scsn = dic_manuales.get(id_t.replace('SEASN', 'SCN'), dic_manuales['1SCN']) * 1.59
            v_min = v_max = base_scsn * 1.75
            regla = "Compuesta (SCSN * 1.75)"
        else:
            v_min, v_max = v_min_ant * factor_ajuste, v_max_ant * factor_ajuste
            regla = f"Ajuste General (%)"

        variacion = ((v_min / v_min_ant) - 1) * 100 if v_min_ant > 0 else 0

        resultados.append({
            'Id': id_t,
            'Noviembre (Base)': round(v_min_ant, 2),
            'Enero 2026 (Nuevo)': round(v_min, 2),
            'Variación %': round(variacion, 2),
            'Regla Aplicada': regla,
            'Limite Superior': round(v_max, 2)
        })

    return pd.DataFrame(resultados)

# =============================================================================
# INTERFAZ DE USUARIO
# =============================================================================

st.title("Módulo de Tarifas con Auditoría Automática 🛡️")

f_ref = st.file_uploader("Subir Tarifas de Referencia (Excel/CSV)", type=['xlsx', 'csv'])

if f_ref:
    nombre_hoja = st.text_input("Nombre de la hoja (ej: JN11):", value="JN11")
    
    st.subheader("Configuración de Tarifas SCN 2026")
    c1, c2, c3, c4, c5 = st.columns(5)
    t1 = c1.number_input("1SCN", value=494.33)
    t2 = c2.number_input("2SCN", value=551.24)
    t3 = c3.number_input("3SCN", value=593.70)
    t4 = c4.number_input("4SCN", value=636.21)
    t5 = c5.number_input("5SCN", value=678.42)

    if st.button("⚡ Calcular y Auditar"):
        if f_ref.name.endswith('.csv'):
            df_base = pd.read_csv(f_ref, sep=None, engine='python')
        else:
            df_base = pd.read_excel(f_ref, sheet_name=nombre_hoja)
        
        manuales = {'1SCN': t1, '2SCN': t2, '3SCN': t3, '4SCN': t4, '5SCN': t5}
        df_audit = motor_tarifas_con_auditoria(df_base, manuales)

        if df_audit is not None:
            st.success("Cálculos completados. Revise la tabla de auditoría antes de descargar.")
            
            # --- TABLA DE AUDITORÍA VISUAL ---
            st.subheader("📋 Tabla de Auditoría (Comparativa vs. Noviembre)")
            st.dataframe(df_audit[['Id', 'Noviembre (Base)', 'Enero 2026 (Nuevo)', 'Variación %', 'Regla Aplicada']], 
                         use_container_width=True)

            # Botón de Descarga
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
                df_audit[['Id', 'Enero 2026 (Nuevo)', 'Limite Superior']].rename(
                    columns={'Enero 2026 (Nuevo)': 'Limite Inferior'}
                ).to_excel(writer, index=False, sheet_name='Tarifas_Proyectadas')
            
            st.download_button(
                label="📥 Descargar Tarifas_Calculadas_2026.xlsx",
                data=buf.getvalue(),
                file_name="Tarifas_Calculadas_2026.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
