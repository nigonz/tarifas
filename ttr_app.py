def generar_diccionario_actualizado(df_referencia, s1, s2, s3, s4, s5):
    """
    Implementa la Acordada 1: Toma la hoja del periodo anterior (ej. JN07)
    y genera las 90 tarifas actuales basadas en las 5 bases manuales.
    """
    # 1. Calcular Factor de Incremento (AI)
    # Buscamos 1SCN en la referencia (limpiando formatos si es necesario)
    v1_prev = float(str(df_referencia.loc[df_referencia['Id'] == '1SCN', 'Minimo'].values[0]).replace(',', '.'))
    factor_ajuste = s1 / v1_prev
    
    # 2. Crear el nuevo diccionario
    bases_manuales = {'1SCN': s1, '2SCN': s2, '3SCN': s3, '4SCN': s4, '5SCN': s5}
    nuevo_dic = []
    
    for _, row in df_referencia.iterrows():
        id_t = str(row['Id'])
        v_min_prev = float(str(row['Minimo']).replace(',', '.'))
        v_max_prev = float(str(row['Maximo']).replace(',', '.'))
        
        # REGLAS ACORDADA 1
        if id_t in bases_manuales:
            # SCN: Valor manual
            v_min, v_max = bases_manuales[id_t], bases_manuales[id_t]
        elif 'SEN' in id_t and 'SESN' not in id_t:
            # SEN: SCN Actual * 1.25
            base = bases_manuales.get(id_t.replace('SEN', 'SCN'), s1)
            v_min = v_max = base * 1.25
        elif 'SEAN' in id_t and 'SEASN' not in id_t:
            # SEAN: SCN Actual * 1.75
            base = bases_manuales.get(id_t.replace('SEAN', 'SCN'), s1)
            v_min = v_max = base * 1.75
        elif 'SCSN' in id_t:
            # SCSN: SCN Actual * 1.59
            base = bases_manuales.get(id_t.replace('SCSN', 'SCN'), s1)
            v_min = v_max = base * 1.59
        elif 'KM' in id_t or 'KP' in id_t:
            # KM y KP: Periodo Anterior * Factor
            v_min, v_max = v_min_prev * factor_ajuste, v_max_prev * factor_ajuste
        else:
            v_min, v_max = v_min_prev * factor_ajuste, v_max_prev * factor_ajuste
            
        nuevo_dic.append({'Id': id_t, 'Minimo': round(v_min, 2), 'Maximo': round(v_max, 2)})
        
    return pd.DataFrame(nuevo_dic)
#2. Modificación en la Interfaz (Tab 1)
En la sección de col_menu, agregamos los campos para que cargues tus 5 tarifas del periodo actual:

Python
with col_menu:
    st.subheader("Configuración")
    mes = st.selectbox("Mes", ["Enero", "Febrero", ...])
    anio = st.number_input("Año", value=2026)
    
    # --- BLOQUE ACORDADA 1 ---
    st.divider()
    st.markdown("### 💰 Tarifas Manuales (SCN)")
    s1 = st.number_input("1SCN Actual", value=494.33, format="%.2f")
    s2 = st.number_input("2SCN Actual", value=551.24, format="%.2f")
    s3 = st.number_input("3SCN Actual", value=593.70, format="%.2f")
    s4 = st.number_input("4SCN Actual", value=636.21, format="%.2f")
    s5 = st.number_input("5SCN Actual", value=678.42, format="%.2f")
    
    # Selector de hoja de referencia
    hoja_ref = st.text_input("Hoja de Referencia (Anterior)", value="JN07")
3. Integración en el Botón de Procesado
Dentro del if btn_procesar_todo:, antes de llamar a las funciones de TTR, generamos el diccionario dinámico:

Python
if btn_procesar_todo:
    # 1. Cargar la referencia del Excel de diccionarios
    df_ref_excel = pd.read_excel(f_dic, sheet_name=hoja_ref)
    
    # 2. Ejecutar Acordada 1 para obtener el diccionario de HOY
    df_diccionario_hoy = generar_diccionario_actualizado(df_ref_excel, s1, s2, s3, s4, s5)
    
    # 3. Pasar este nuevo dataframe a tus funciones (en lugar de que ellas lean el Excel)
    df_jn = tool_procesar_jn(f_base, f_nom_ts, f_nom_gt, f_ttr, df_diccionario_hoy, anio
