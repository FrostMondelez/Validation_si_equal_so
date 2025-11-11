#-----------------------------------IMPORTACION DE LIBRERIAS-----------------------------------#
import pandas as pd 
import numpy as np
import shutil

def validar_reglas_manual_file_si_igual_so(df, nombre_archivo):
   """
   Validar las reglas de negocio y estrutura del manual file SI=SO, con el fin de dar aprobacion o no a la revision.

   Parámetros
   df (pandas) :es el manual file de SI=SO que se va a realizar su respectiva validacion.
   nombre_archivo (str) :nombre del manual file en revision.

   Retorna
   Dataframe con los resultados de la revision y se envia al correo electronico las observaciones, el dataframe tiene como estructura 
   (Manual_file	Regla	Indicador	Resultado	Hallazgo).
   """
   resultados = []
   columnas_requeridas = [
       'Country_Key', 'Customer_Cod_Ship_to',
       'Valid_From_Period', 'Valid_To_Period',
       'Calculation_Type'
   ]
   valores_country_key = {"AE", "BO", "CL", "PE", "CO", "EC", "NI", "HN", "SV", "CR", "PA", "GT", "PR", "DO"}
   valores_calculation = {"SO = SI", "SO = AVG(3 month SI)"}
   regex_periodo = r"^\d{4} P\d{2}$"
   errores = 0
   def add(indicador, resultado, hallazgo, regla='Reglas de estructura'):
       resultados.append({
           'Manual_file': nombre_archivo,
           'Regla': regla,
           'Indicador': indicador,
           'Resultado': resultado,
           'Hallazgo': hallazgo
       })
   # 1. Estructura
   requeridas = columnas_requeridas
   faltantes = [c for c in requeridas if c not in df.columns]
   sobrantes = [c for c in df.columns if c not in requeridas]
   if faltantes or sobrantes:
    errores += 1
    mensajes = []
    if faltantes:
       mensajes.append("Faltan columnas: " + ", ".join(faltantes))
    if sobrantes:
       mensajes.append("Columnas no permitidas: " + ", ".join(sobrantes))
    add('Estructura', 'Error', " ; ".join(mensajes))
   else:
    # Revision de orden de columnas 
    if list(df.columns) != list(requeridas):
       errores += 1
       add('Estructura', 'Error', 'Orden de columnas incorrecto')
    else:
       add('Estructura', 'Estructura OK', 'Exacta y en orden')
   # 2. Duplicados
   duplicados = df.duplicated()
   if duplicados.any():
       errores += 1
       filas = (duplicados[duplicados].index + 2).tolist()
       add('Duplicados', f'{len(filas)} fila(s) duplicada(s)', f'Filas: {filas}')
   else:
       add('Duplicados', 'OK', 'No hay duplicados')
   # 3. Nulos
   nulos_total = 0
   for col in columnas_requeridas:
       if col in df.columns:
           nulos = df[df[col].isnull()]
           nulos_total += nulos.shape[0]
           for idx in nulos.index:
               errores += 1
               add('Nulos', f'Nulo en {col}', f'Fila {idx+2} / {col} = NaN')
   if nulos_total == 0:
       add('Nulos', 'OK', 'No hay nulos en columnas requeridas')
   # 4. Tipo de dato
   tipo_error = False
   for col in columnas_requeridas:
       if col in df.columns:
           no_string = df[~df[col].apply(lambda x: isinstance(x, str))]
           for idx, val in no_string[col].items():
               tipo_error = True
               errores += 1
               add('Tipo de dato', f'{col} no es string', f'Fila {idx+2} / {col} = {val} ({type(val).__name__})')
   if not tipo_error:
       add('Tipo de dato', 'OK', 'Todas las columnas requeridas son string')
   # 5. Validación Country_Key
   if 'Country_Key' in df.columns:
       no_validos = df[~df['Country_Key'].isin(valores_country_key)]
       if no_validos.empty:
           add('Country_Key', 'OK', 'Todos los valores válidos')
       else:
           errores += 1
           for idx, row in no_validos.iterrows():
               add('Country_Key', 'Valor inválido', f'Fila {idx+2} / Country_Key = {row["Country_Key"]}')
   # 6. Validación Calculation_Type
   if 'Calculation_Type' in df.columns:
       no_validos = df[~df['Calculation_Type'].isin(valores_calculation)]
       if no_validos.empty:
           add('Calculation_Type', 'OK', 'Todos los valores válidos')
       else:
           errores += 1
           for idx, row in no_validos.iterrows():
               add('Calculation_Type', 'Valor inválido', f'Fila {idx+2} / Calculation_Type = {row["Calculation_Type"]}')
   # 7. Validación formato de periodos
   for col in ['Valid_From_Period', 'Valid_To_Period']:
       if col in df.columns:
           no_validos = df[~df[col].astype(str).str.match(regex_periodo)]
           if no_validos.empty:
               add(col, 'OK', 'Formato correcto en todos')
           else:
               errores += 1
               for idx, row in no_validos.iterrows():
                   add(col, 'Formato inválido', f'Fila {idx+2} / {col} = {row[col]}')
   # 8. Regla de negocio: transiciones sin solapamiento
   def periodo_a_orden(p):
       if isinstance(p, str) and 'P' in p:
           y, m = p.strip().split('P')
           return int(y.strip()) * 100 + int(m.strip())
       return None
   df['Orden_From'] = df['Valid_From_Period'].apply(periodo_a_orden)
   df['Orden_To'] = df['Valid_To_Period'].apply(periodo_a_orden)
   for (country, client), grupo in df.groupby(['Country_Key', 'Customer_Cod_Ship_to']):
       grupo = grupo.sort_values('Orden_From').reset_index()
       for i in range(len(grupo) - 1):
           actual = grupo.loc[i]
           siguiente = grupo.loc[i + 1]
           if actual['Orden_To'] >= siguiente['Orden_From'] and actual['Calculation_Type'] != siguiente['Calculation_Type']:
               errores += 1
               add(
                   'Transición inválida',
                   'Error',
                   f'{country}-{client}: solapamiento entre {actual["Valid_To_Period"]} y {siguiente["Valid_From_Period"]}',
                   regla='Regla de negocio'
               )
   # 9. Resultado general
   estado = 'Archivo conforme' if errores == 0 else 'Archivo con errores'
   add('Resultado general', estado, None, regla ="Consolidado")

   return pd.DataFrame(resultados)
