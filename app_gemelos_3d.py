import requests
import json
import time
from datetime import datetime, timedelta
import os
import streamlit as st
import pandas as pd
import base64
from streamlit.components.v1 import html

# --- CONFIGURACIÓN ---
# ¡¡¡IMPORTANTE!!! CAMBIO AQUÍ: Ahora el token se carga desde st.secrets
# NO HARDCODEES TU TOKEN AQUÍ SI LO SUBIRÁS A UN REPOSITORIO PÚBLICO
try:
    SAMSARA_API_TOKEN = st.secrets["SAMSARA_API_TOKEN"]
except KeyError:
    st.error("Error: La clave 'SAMSARA_API_TOKEN' no se encontró en los secretos de Streamlit. "
             "Por favor, configura tu token de Samsara en Streamlit Cloud o en un archivo .streamlit/secrets.toml localmente.")
    st.stop()


BASE_URL = "https://api.samsara.com/fleet"
HEADERS = {
    "Authorization": f"Bearer {SAMSARA_API_TOKEN}",
    "Content-Type": "application/json"
}

# --- ¡¡¡IMPORTANTE!!! CONVERSIÓN DEL MODELO 3D ---
# Según tus capturas (image_ffe444.png, image_ffc90c.png), tu modelo es 'truck.obj'.
# El visor 3D de Streamlit (model-viewer) NO SOPORTA .OBJ DIRECTAMENTE.
# DEBES CONVERTIR 'truck.obj' a 'truck.glb' o 'truck.gltf' para que funcione.
# Herramientas recomendadas para la conversión:
# - Online: https://www.greentoken.de/onlineconv/ o https://anyconv.com/obj-to-glb-converter/
# - Software: Blender (Import .obj, Export .glb/gltf)
# Una vez convertido, asegúrate de que el nuevo archivo se llame 'truck.glb'
# y esté en la MISMA CARPETA que este script de Python.
TRUCK_MODEL_PATH = "truck.glb"

# --- ¡¡¡IMPORTANTE!!! REEMPLAZA ESTA LISTA CON IDs REALES DE VEHÍCULOS ACTIVOS DE TU FLOTA. ---
# Incluye un ID de vehículo que sepas que tiene FALLAS ACTIVAS (DTCs o luces de check engine encendidas)
# para poder ver los datos de mantenimiento. El endpoint /v1/fleet/maintenance/list solo devuelve
# vehículos con fallas activas.
HARDCODED_VEHICLE_IDS = [
    "281474986130035", # ID del vehículo que ya estabas usando (PR1889)
    "281474987148134", # Segundo vehículo de tus logs (PR1563)
    "281474987157622", # Tercer vehículo de tus logs (PR1567) (Este es el que aparece con fallas en tu consola)
    "281474987159128"  # ID del vehículo "1157" de tus logs
]

# Umbrales para alertas
FUEL_LOW_THRESHOLD_PERCENT = 15
COOLANT_TEMP_HIGH_THRESHOLD_C = 100
IDLE_TIME_THRESHOLD_MINUTES = 10 # No usado directamente en el código actual, pero útil para futuro
IDLE_THRESHOLD_SPEED_MPH = 1

# --- Función para obtener datos de MÚLTIPLES vehículos ---
@st.cache_data(ttl=300)
def fetch_samsara_data_multiple_vehicles(vehicle_ids_to_fetch):
    st.write(f"Obteniendo datos de Samsara para {len(vehicle_ids_to_fetch)} vehículos...")

    all_vehicle_details_map = {}
    all_vehicle_locations = {}
    all_vehicle_stats = {}
    all_vehicle_maintenance_data = {}

    st.write(f"DEBUG: Intentando obtener ubicaciones para todos los vehículos desde /vehicles/locations...")
    locations_data_from_api = get_vehicle_locations(vehicle_ids_to_fetch)
    all_vehicle_locations.update(locations_data_from_api)
    if locations_data_from_api:
        st.success(f"DEBUG: Ubicaciones obtenidas correctamente para {len(locations_data_from_api)} vehículos.")
    else:
        st.warning(f"DEBUG: No se pudo obtener ubicación para ninguno de los vehículos desde /vehicles/locations. Esto puede deberse a permisos o que los vehículos no estén reportando GPS.")

    for vehicle_id_to_fetch in vehicle_ids_to_fetch:
        st.write(f"--- Procesando vehículo: {vehicle_id_to_fetch} ---")

        # 1. Obtener detalles básicos del vehículo (nombre, etc.)
        st.write(f"DEBUG: Intentando obtener detalles del vehículo {vehicle_id_to_fetch} desde /vehicles/{{id}}...")
        vehicle_data = get_single_vehicle_details(vehicle_id_to_fetch)
        if vehicle_data:
            all_vehicle_details_map[vehicle_id_to_fetch] = vehicle_data
            st.success(f"DEBUG: Detalles del vehículo {vehicle_data.get('name', vehicle_id_to_fetch)} obtenidos correctamente.")
        else:
            st.error(f"ERROR: No se pudieron obtener los detalles para el vehículo ID: {vehicle_id_to_fetch}. Verifique el ID y su estado en Samsara y los permisos de su token.")
            continue

        # 2. Obtener estadísticas para el vehículo (separadas por tipos para evitar el error 400 con fuelPercent)
        st.write(f"DEBUG: Intentando obtener stats para el vehículo {vehicle_id_to_fetch} usando múltiples llamadas a /fleet/vehicles/stats...")

        # ELIMINADO fuelPercent de este grupo por problemas de la API de Samsara en tu entorno
        samsara_stat_types_temps = ['engineCoolantTemperatureMilliC', 'ambientAirTemperatureMilliC']
        samsara_stat_types_engine = ['engineRpm', 'obdEngineSeconds', 'engineOilPressureKPa']

        # Nuevo grupo solo para fuelPercent si lo necesitas en el futuro, pero lo estamos omitiendo por ahora
        # samsara_stat_types_fuel = ['fuelPercent']

        combined_stats = {}

        # Omitiendo la llamada de fuelPercent por ahora
        # st.write(f"DEBUG: Realizando llamada para stats de combustible de {vehicle_id_to_fetch}: {samsara_stat_types_fuel}")
        # stats_fuel = get_all_vehicle_stats_and_filter(vehicle_id_to_fetch, samsara_stat_types_fuel)
        # if stats_fuel:
        #    combined_stats.update(stats_fuel)
        #    st.success(f"DEBUG: Combustible para {vehicle_id_to_fetch} obtenido correctamente.")
        # else:
        #    st.warning(f"DEBUG: No se pudo obtener el combustible para {vehicle_id_to_fetch} (problema conocido con 'fuelPercent').")

        st.write(f"DEBUG: Realizando llamada para stats de temperaturas de {vehicle_id_to_fetch}: {samsara_stat_types_temps}")
        stats_temps = get_all_vehicle_stats_and_filter(vehicle_id_to_fetch, samsara_stat_types_temps)
        if stats_temps:
            combined_stats.update(stats_temps)
            st.success(f"DEBUG: Temperaturas para {vehicle_id_to_fetch} obtenidas correctamente.")
        else:
            st.warning(f"DEBUG: No se pudieron obtener las temperaturas para {vehicle_id_to_fetch}.")

        st.write(f"DEBUG: Realizando llamada para otras stats de motor de {vehicle_id_to_fetch}: {samsara_stat_types_engine}")
        stats_engine = get_all_vehicle_stats_and_filter(vehicle_id_to_fetch, samsara_stat_types_engine)
        if stats_engine:
            combined_stats.update(stats_engine)
            st.success(f"DEBUG: Otras estadísticas de motor para {vehicle_id_to_fetch} obtenidas correctamente.")
        else:
            st.warning(f"DEBUG: No se pudieron obtener otras estadísticas de motor para {vehicle_id_to_fetch}.")

        if combined_stats:
            all_vehicle_stats[vehicle_id_to_fetch] = combined_stats
            st.success(f"DEBUG: Todas las estadísticas combinadas para {vehicle_id_to_fetch} procesadas.")
        else:
            st.warning(f"ADVERTENCIA: No se pudo obtener NINGUNA estadística para el vehículo ID: {vehicle_id_to_fetch}. Esto puede afectar la visualización de datos.")

        # 3. Obtener datos de mantenimiento (DTCs y Check Engine Light)
        st.write(f"DEBUG: Intentando obtener datos de mantenimiento para el vehículo {vehicle_id_to_fetch} desde /v1/fleet/maintenance/list (con paginación)...")
        maintenance_data = get_vehicle_maintenance_data(vehicle_id_to_fetch)
        if maintenance_data:
            all_vehicle_maintenance_data[vehicle_id_to_fetch] = maintenance_data
            st.success(f"DEBUG: Datos de mantenimiento para {vehicle_id_to_fetch} OBTENIDOS CORRECTAMENTE. Se encontró un objeto de mantenimiento.")
            # st.json(maintenance_data) # <--- IMPRIME EL JSON DE MANTENIMIENTO PARA VERIFICACIÓN
        else:
            st.info(f"INFO: No se encontraron datos de mantenimiento (DTCs/luces de Check Engine) para el vehículo {vehicle_id_to_fetch}. Esto es normal si el vehículo NO TIENE FALLAS activas o si el API token NO TIENE los permisos de lectura de mantenimiento.")

    return all_vehicle_details_map, all_vehicle_locations, all_vehicle_stats, all_vehicle_maintenance_data

def get_single_vehicle_details(vehicle_id):
    endpoint = f"{BASE_URL}/vehicles/{vehicle_id}"
    try:
        response = requests.get(endpoint, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json().get('data')
        # st.write(f"DEBUG: Respuesta RAW de detalles de vehículo {vehicle_id}:")
        # st.json(data)
        return data
    except requests.exceptions.RequestException as e:
        st.error(f"ERROR_LOG: Fallo al obtener detalles para el vehículo {vehicle_id}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_json = e.response.json()
                st.error(f"ERROR_LOG: Cuerpo JSON de la respuesta de error de detalles: {json.dumps(error_json, indent=2)}")
            except json.JSONDecodeError:
                st.error(f"ERROR_LOG: Cuerpo RAW de la respuesta de error de detalles: {e.response.text}")
        return None

def get_vehicle_locations(vehicle_ids):
    endpoint = f"{BASE_URL}/vehicles/locations"
    # Convertir a string para el parámetro 'ids'
    ids_str = ",".join(vehicle_ids)
    params = {'ids': ids_str}
    try:
        response = requests.get(endpoint, headers=HEADERS, params=params, timeout=10)
        # st.write(f"DEBUG: Solicitando ubicaciones desde {endpoint} con params: {params}")
        # st.write(f"DEBUG: Código de estado de la respuesta de ubicaciones: {response.status_code}")

        response.raise_for_status()
        locations_data = response.json().get('data', [])
        # st.write(f"DEBUG: Respuesta RAW de ubicaciones:")
        # st.json(locations_data) # <--- IMPRIME EL JSON DE UBICACIONES
        return {loc['id']: loc['location'] for loc in locations_data}
    except requests.exceptions.RequestException as e:
        st.warning(f"ERROR_LOG: Error al obtener ubicaciones desde /vehicles/locations: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_json = e.response.json()
                st.warning(f"ERROR_LOG: Cuerpo JSON de la respuesta de error de ubicaciones: {json.dumps(error_json, indent=2)}")
            except json.JSONDecodeError:
                st.warning(f"ERROR_LOG: Cuerpo RAW de la respuesta de error de ubicaciones: {e.response.text}")
        return {}

def get_all_vehicle_stats_and_filter(target_vehicle_id, stat_types):
    endpoint = f"{BASE_URL}/vehicles/stats"
    params = {"types": ",".join(stat_types)}

    try:
        response = requests.get(endpoint, headers=HEADERS, params=params, timeout=10)

        # st.write(f"DEBUG: Solicitando stats ESPECÍFICAS desde {endpoint}")
        # st.write(f"DEBUG: URL de la solicitud: {response.url}")
        # st.write(f"DEBUG: Código de estado de la respuesta: {response.status_code}")

        response.raise_for_status()

        data = response.json().get('data', [])

        # st.write(f"DEBUG: Respuesta RAW de stats para tipos {stat_types}:")
        # st.json(data) # <--- IMPRIME EL JSON DE STATS

        for item in data:
            if item.get('id') == target_vehicle_id:
                all_stats_for_vehicle = {}
                for stat_type in stat_types:
                    if stat_type in item:
                        # Para stats que vienen anidados bajo 'value'
                        if isinstance(item[stat_type], dict) and 'value' in item[stat_type]:
                            all_stats_for_vehicle[stat_type] = item[stat_type]['value']
                        # Para stats que vienen directamente
                        else:
                            all_stats_for_vehicle[stat_type] = item[stat_type]

                # st.write(f"DEBUG: Estadísticas obtenidas y filtradas para {target_vehicle_id} y tipos {stat_types}: {all_stats_for_vehicle}")
                return all_stats_for_vehicle

        st.warning(f"DEBUG: Vehículo con ID {target_vehicle_id} no encontrado en la respuesta del endpoint {endpoint} para los tipos de stats solicitados o no reporta estos stats.")
        return None

    except requests.exceptions.Timeout:
        st.error(f"ERROR_LOG: Timeout al obtener stats generales.")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"ERROR_LOG: Fallo al obtener stats generales: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_json = e.response.json()
                st.error(f"ERROR_LOG: Cuerpo JSON de la respuesta de error de stats generales: {json.dumps(error_json, indent=2)}")
            except json.JSONDecodeError:
                st.error(f"ERROR_LOG: Cuerpo RAW de la respuesta de error de stats generales: {e.response.text}")
        return None

def get_vehicle_maintenance_data(target_vehicle_id):
    endpoint = "https://api.samsara.com/v1/fleet/maintenance/list"
    all_maintenance_items = []
    next_cursor = None
    page_count = 0

    st.write(f"DEBUG: Iniciando paginación para obtener datos de mantenimiento del vehículo {target_vehicle_id} desde {endpoint}...")

    while True:
        page_count += 1
        params = {}
        if next_cursor:
            params['after'] = next_cursor

        st.write(f"DEBUG: Realizando llamada de mantenimiento - Página {page_count} desde {endpoint} con params: {params}")
        try:
            response = requests.get(endpoint, headers=HEADERS, params=params, timeout=10)
            # st.write(f"DEBUG: Código de estado de la respuesta de mantenimiento (Página {page_count}): {response.status_code}")

            response.raise_for_status()

            response_data = response.json()
            current_page_items = response_data.get('vehicleMaintenance', []) # Asegúrate de que el key es 'vehicleMaintenance'

            # Si 'vehicleMaintenance' no existe, puede que esté directamente en 'vehicles' o algún otro key
            # Revisando tu log, parece que la respuesta de este endpoint tiene un key principal 'vehicles'
            # Vamos a intentar con 'vehicles' si 'vehicleMaintenance' está vacío.
            if not current_page_items:
                current_page_items = response_data.get('vehicles', [])

            all_maintenance_items.extend(current_page_items)

            pagination_info = response_data.get('pagination', {})
            next_cursor = pagination_info.get('endCursor')

            # st.write(f"DEBUG: Items recibidos en la Página {page_count}: {len(current_page_items)}")
            # st.write(f"DEBUG: Next cursor para mantenimiento (Página {page_count}): {next_cursor}")
            # st.write(f"DEBUG: Respuesta RAW de mantenimiento (Página {page_count}):")
            # st.json(response_data) # <--- IMPRIME EL JSON DE MANTENIMIENTO POR PÁGINA

            if not next_cursor:
                st.write(f"DEBUG: Paginación de mantenimiento completada. No hay más páginas.")
                break

        except requests.exceptions.Timeout:
            st.error(f"ERROR_LOG: Timeout al obtener datos de mantenimiento (Página {page_count}).")
            return None
        except requests.exceptions.RequestException as e:
            st.error(f"ERROR_LOG: Fallo al obtener datos de mantenimiento (Página {page_count}): {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_json = e.response.json()
                    st.error(f"ERROR_LOG: Cuerpo JSON de la respuesta de error de mantenimiento: {json.dumps(error_json, indent=2)}")
                except json.JSONDecodeError:
                    st.error(f"ERROR_LOG: Cuerpo RAW de la respuesta de error de mantenimiento: {e.response.text}")
            return None

    st.write(f"DEBUG: Total de ítems de mantenimiento recopilados de todas las páginas: {len(all_maintenance_items)}")

    # Después de obtener todas las páginas, buscar el vehículo objetivo y sus datos de J1939 (DTCs y Check Engine)
    found_vehicle_data = None
    for vehicle_item in all_maintenance_items:
        # Asegúrate de comparar como strings por si acaso los IDs vienen en diferentes tipos
        if str(vehicle_item.get('id')) == str(target_vehicle_id):
            found_vehicle_data = vehicle_item
            break

    if found_vehicle_data:
        st.write(f"DEBUG: Vehículo con ID {target_vehicle_id} encontrado en los datos de mantenimiento consolidados.")
        return found_vehicle_data
    else:
        st.warning(f"DEBUG: Vehículo con ID {target_vehicle_id} no encontrado en los datos de mantenimiento consolidados. Esto puede significar que NO TIENE FALLAS ACTIVAS actualmente, ya que este endpoint solo devuelve vehículos con mantenimiento activo.")
        return None


# --- LÓGICA DEL GEMELO DIGITAL Y DETECCIÓN DE ALERTA ---
def process_vehicle_data(vehicle_details, vehicle_locations, vehicle_stats, vehicle_maintenance_data):
    gemelo_digital = {
        'vehicle_id': vehicle_details.get('id', ''),
        'vehicle_name': vehicle_details.get('name', 'N/A'),
        'make': vehicle_details.get('make', 'N/A'),
        'model': vehicle_details.get('model', 'N/A'),
        'license_plate': vehicle_details.get('licensePlate', 'N/A'),
        'latitude': 'N/A', 'longitude': 'N/A', 'speed_mph': 'N/A', 'current_address': 'N/A',
        'gps_odometer_meters': 'N/A', 'location_updated_at': 'N/A',
        'engine_hours': 'N/A',
        'fuel_perc_remaining': 'N/A', # Puede que no se obtenga si la API sigue dando problemas
        'engine_oil_pressure_kpa': 'N/A',
        'engine_coolant_temperature_c': 'N/A',
        'engine_rpm': 'N/A',
        'ambient_air_temperature_c': 'N/A',
        'engine_check_light_warning': False,
        'engine_check_light_emissions': False,
        'engine_check_light_protect': False,
        'engine_check_light_stop': False,
        'diagnostic_trouble_codes': [],
        'last_data_sync': datetime.now().isoformat(),
        'status_alert': 'OPERANDO NORMALMENTE',
        'alert_color': 'green'
    }

    stats_data = vehicle_stats.get(gemelo_digital['vehicle_id'], {})
    maintenance_data = vehicle_maintenance_data.get(gemelo_digital['vehicle_id'], {})

    # Priorizar ubicación de /vehicles/locations
    loc_data = vehicle_locations.get(gemelo_digital['vehicle_id'])
    if loc_data:
        gemelo_digital['latitude'] = loc_data.get('latitude', 'N/A')
        gemelo_digital['longitude'] = loc_data.get('longitude', 'N/A')
        speed_value_loc = loc_data.get('speed')
        if isinstance(speed_value_loc, (int, float)):
            gemelo_digital['speed_mph'] = round(speed_value_loc, 2)
        else:
            gemelo_digital['speed_mph'] = 'N/A'
        gemelo_digital['current_address'] = loc_data.get('reverseGeo', {}).get('formattedLocation', 'N/A')
        gemelo_digital['location_updated_at'] = loc_data.get('time', 'N/A')

    # Horas de Motor
    engine_seconds = stats_data.get('obdEngineSeconds')
    if isinstance(engine_seconds, (int, float)):
        gemelo_digital['engine_hours'] = round(engine_seconds / 3600, 2)
    else:
        gemelo_digital['engine_hours'] = 'N/A'

    # Combustible (si se logró obtener, si no, quedará N/A)
    fuel_perc = stats_data.get('fuelPercent')
    if isinstance(fuel_perc, (int, float)):
        gemelo_digital['fuel_perc_remaining'] = round(fuel_perc, 2)
    else:
        gemelo_digital['fuel_perc_remaining'] = 'N/A'

    # Presión de Aceite
    oil_pressure = stats_data.get('engineOilPressureKPa')
    if isinstance(oil_pressure, (int, float)):
        gemelo_digital['engine_oil_pressure_kpa'] = round(oil_pressure, 2)
    else:
        gemelo_digital['engine_oil_pressure_kpa'] = 'N/A'

    # Temperatura del Motor
    temp_c_milli = stats_data.get('engineCoolantTemperatureMilliC')
    if isinstance(temp_c_milli, (int, float)):
        gemelo_digital['engine_coolant_temperature_c'] = round(temp_c_milli / 1000, 2)
    else:
        gemelo_digital['engine_coolant_temperature_c'] = 'N/A'

    # Temperatura del Aire Ambiente
    temp_ambient_milli = stats_data.get('ambientAirTemperatureMilliC')
    if isinstance(temp_ambient_milli, (int, float)):
        gemelo_digital['ambient_air_temperature_c'] = round(temp_ambient_milli / 1000, 2)
    else:
        gemelo_digital['ambient_air_temperature_c'] = 'N/A'

    # RPM del Motor
    engine_rpm_val = stats_data.get('engineRpm')
    if isinstance(engine_rpm_val, (int, float)):
        gemelo_digital['engine_rpm'] = engine_rpm_val
    else:
        gemelo_digital['engine_rpm'] = 'N/A'

    # Procesar datos de mantenimiento (DTCs y Check Engine Light) desde maintenance_data
    if maintenance_data:
        j1939_data = maintenance_data.get('j1939', {})
        check_engine_light_data = j1939_data.get('checkEngineLight', {})

        gemelo_digital['engine_check_light_warning'] = check_engine_light_data.get('warningIsOn', False)
        gemelo_digital['engine_check_light_emissions'] = check_engine_light_data.get('emissionsIsOn', False)
        gemelo_digital['engine_check_light_protect'] = check_engine_light_data.get('protectIsOn', False)
        gemelo_digital['engine_check_light_stop'] = check_engine_light_data.get('stopIsOn', False)

        dtcs_from_maintenance = j1939_data.get('diagnosticTroubleCodes', [])
        if isinstance(dtcs_from_maintenance, list):
            gemelo_digital['diagnostic_trouble_codes'] = dtcs_from_maintenance
        else:
            gemelo_digital['diagnostic_trouble_codes'] = []

    # Lógica de alertas
    alerts = []
    if isinstance(gemelo_digital['fuel_perc_remaining'], (int, float)) and \
       gemelo_digital['fuel_perc_remaining'] < FUEL_LOW_THRESHOLD_PERCENT:
        alerts.append(f"Bajo combustible ({gemelo_digital['fuel_perc_remaining']}%)")

    if gemelo_digital['diagnostic_trouble_codes'] and isinstance(gemelo_digital['diagnostic_trouble_codes'], list) and len(gemelo_digital['diagnostic_trouble_codes']) > 0:
        dtc_codes_info = []
        for code in gemelo_digital['diagnostic_trouble_codes']:
            spn = code.get('spnId', 'N/A')
            fmi = code.get('fmiId', 'N/A')
            fmi_text = code.get('fmiText', '')
            dtc_codes_info.append(f"SPN: {spn} (FMI: {fmi} - {fmi_text})")

        alerts.append(f"Fallas de motor (DTCs: {'; '.join(dtc_codes_info)})")

    check_light_alerts = []
    if gemelo_digital['engine_check_light_warning']:
        check_light_alerts.append("Advertencia (Warning)")
    if gemelo_digital['engine_check_light_emissions']:
        check_light_alerts.append("Emisiones (Emissions)")
    if gemelo_digital['engine_check_light_protect']:
        check_light_alerts.append("Protección (Protect)")
    if gemelo_digital['engine_check_light_stop']:
        check_light_alerts.append("Detener (Stop)")

    if check_light_alerts:
        alerts.append(f"Luz de Check Engine ON ({', '.join(check_light_alerts)})")


    if isinstance(gemelo_digital['engine_coolant_temperature_c'], (int, float)) and \
       gemelo_digital['engine_coolant_temperature_c'] > COOLANT_TEMP_HIGH_THRESHOLD_C:
        alerts.append(f"Sobrecalentamiento ({gemelo_digital['engine_coolant_temperature_c']}°C)")

    current_speed = gemelo_digital['speed_mph']
    is_engine_on = (gemelo_digital['engine_rpm'] is not None and isinstance(gemelo_digital['engine_rpm'], (int, float)) and gemelo_digital['engine_rpm'] > 0)

    if isinstance(current_speed, (int, float)) and current_speed <= IDLE_THRESHOLD_SPEED_MPH and is_engine_on:
        alerts.append(f"Ralentí (velocidad {current_speed} mph)")

    if alerts:
        gemelo_digital['status_alert'] = "ALERTA: " + '; '.join(alerts)
        gemelo_digital['alert_color'] = 'red'
    elif gemelo_digital['status_alert'] != 'OFFLINE o SIN DATOS':
        gemelo_digital['status_alert'] = 'OPERANDO NORMALMENTE'
        gemelo_digital['alert_color'] = 'green'

    return gemelo_digital

# --- Función para mostrar el visor 3D ---
def display_gltf_viewer(model_path, height=400):
    if not os.path.exists(model_path):
        st.error(f"Error: El archivo del modelo 3D '{model_path}' no se encontró en la ruta: {os.path.abspath(model_path)}")
        st.warning("Asegúrate de que el archivo del modelo 3D (ej. 'truck.glb') esté en la misma carpeta que este script.")
        st.warning(f"**¡IMPORTANTE!** Si tu archivo es '.obj' (como '{os.path.basename(model_path).replace('.glb', '.obj')}'), necesitas **CONVERTIRLO** a '.glb' o '.gltf' para que funcione con el visor 3D en Streamlit.")
        st.warning("Puedes usar herramientas online como: https://www.greentoken.de/onlineconv/ o https://anyconv.com/obj-to-glb-converter/")
        st.warning("Después de la conversión, asegúrate de **RENOMBRAR** el archivo resultante a `truck.glb` y que sea el único `truck.glb` en la carpeta.")
        return

    try:
        if not model_path.lower().endswith(('.glb', '.gltf')):
            st.error(f"Error: El archivo '{os.path.basename(model_path)}' no es un modelo GLB o GLTF.")
            st.warning("El visor 3D en Streamlit solo soporta modelos en formato '.glb' o '.gltf'.")
            st.warning("Por favor, convierte tu modelo 3D a uno de estos formatos y renombra el archivo a 'truck.glb'.")
            return

        with open(model_path, "rb") as f:
            model_bytes = f.read()
        model_b64 = base64.b64encode(model_bytes).decode("utf-8")
        data_url = f"data:model/gltf-binary;base64,{model_b64}"

        html_code = f"""
        <script type="module" src="https://unpkg.com/@google/model-viewer/dist/model-viewer.min.js"></script>
        <style>
          model-viewer {{
            width: 100%;
            height: {height}px;
            background-color: #F0F0F0;
            --poster-color: #F0F0F0;
          }}
        </style>
        <model-viewer
          src="{data_url}"
          alt="Modelo 3D de Camión"
          auto-rotate
          camera-controls
          shadow-intensity="1"
          exposure="1"
          ar
          ar-modes="webxr scene-viewer quick-look"
        ></model-viewer>
        """
        html(html_code, height=height, width=None, scrolling=False)
    except Exception as e:
        st.error(f"Error al cargar o mostrar el modelo 3D: {e}")
        st.info("Asegúrate de que el archivo GLB no esté corrupto y que el nombre del archivo en el código sea exactamente igual al del archivo en tu disco.")

# --- APLICACIÓN STREAMLIT ---
st.set_page_config(layout="wide")
st.title("🚚 Gemelos Digitales de tu Flota (Samsara) con 3D")

if st.button("Actualizar Datos"):
    st.cache_data.clear() # Limpia la caché para obtener datos frescos
    st.rerun() # Recarga la aplicación para re-ejecutar la lógica

# Llama a la función de obtención de datos para múltiples vehículos
# Se añaden print statements para depuración
st.write("Iniciando la obtención de datos de Samsara...")
vehicle_details_map, vehicle_locations, vehicle_stats, vehicle_maintenance_data = fetch_samsara_data_multiple_vehicles(HARDCODED_VEHICLE_IDS)
st.write("Obtención de datos de Samsara finalizada.")

all_gemelos_digitales = {}
if vehicle_details_map:
    for vehicle_id, details in vehicle_details_map.items():
        st.write(f"DEBUG: Procesando gemelo digital para {vehicle_id}...")
        gemelo = process_vehicle_data(details, vehicle_locations, vehicle_stats, vehicle_maintenance_data)
        all_gemelos_digitales[vehicle_id] = gemelo
        st.write(f"DEBUG: Gemelo digital para {vehicle_id} procesado.")
        # st.json(gemelo) # <--- IMPRIME EL GEMELO DIGITAL COMPLETO
else:
    st.info("No se pudieron cargar datos de vehículos. Asegúrate de que los IDs de vehículos hardcodeados sean válidos y estén activos y que tu token de API tenga los permisos correctos.")

df_fleet = pd.DataFrame(list(all_gemelos_digitales.values()))

st.subheader("Resumen de la Flota")
if not df_fleet.empty:
    df_fleet['speed_mph'] = pd.to_numeric(df_fleet['speed_mph'], errors='coerce')

    st.dataframe(df_fleet[['vehicle_name', 'status_alert', 'fuel_perc_remaining',
                           'engine_coolant_temperature_c', 'speed_mph', 'current_address', 'last_data_sync']], use_container_width=True)
else:
    st.warning("No hay datos de vehículos disponibles para mostrar en el resumen de la flota.")


st.markdown("---")

st.subheader("Detalle del Camión y Visualización 3D")
if not df_fleet.empty:
    vehicle_names = [v.get('vehicle_name', v.get('vehicle_id')) for v in all_gemelos_digitales.values()]
    selected_vehicle_name = st.selectbox("Selecciona un vehículo para ver detalles:", vehicle_names)

    selected_vehicle_data = None
    for gemelo_id, gemelo_data in all_gemelos_digitales.items():
        if gemelo_data.get('vehicle_name', gemelo_data.get('vehicle_id')) == selected_vehicle_name:
            selected_vehicle_data = gemelo_data
            break

    if selected_vehicle_data:
        col1, col2 = st.columns([1, 2])

        with col1:
            st.write(f"### Gemelo Digital de {selected_vehicle_name}")
            st.write(f"**Estado:** <span style='color:{selected_vehicle_data.get('alert_color', 'gray')}; font-weight:bold;'>{selected_vehicle_data.get('status_alert', 'N/A')}</span>", unsafe_allow_html=True)
            st.write(f"**Última Sincronización:** {selected_vehicle_data.get('last_data_sync', 'N/A')}")
            st.write(f"**Ubicación:** ({selected_vehicle_data.get('latitude', 'N/A')}, {selected_vehicle_data.get('longitude', 'N/A')})")
            st.write(f"**Dirección Actual:** {selected_vehicle_data.get('current_address', 'N/A')}")
            st.write(f"**Velocidad:** {selected_vehicle_data.get('speed_mph', 'N/A')} MPH")
            st.write(f"**Combustible:** {selected_vehicle_data.get('fuel_perc_remaining', 'N/A')}%")
            st.write(f"**Temperatura Motor:** {selected_vehicle_data.get('engine_coolant_temperature_c', 'N/A')}°C")
            st.write(f"**Presión Aceite:** {selected_vehicle_data.get('engine_oil_pressure_kpa', 'N/A')} KPa")
            st.write(f"**RPM Motor:** {selected_vehicle_data.get('engine_rpm', 'N/A')}")
            st.write(f"**Horas de Motor:** {selected_vehicle_data.get('engine_hours', 'N/A')} hrs")

            dtcs = selected_vehicle_data.get('diagnostic_trouble_codes')
            if dtcs and isinstance(dtcs, list) and len(dtcs) > 0:
                st.warning(f"**DTCs:**")
                for dtc in dtcs:
                    spn = dtc.get('spnId', 'N/A')
                    fmi = dtc.get('fmiId', 'N/A')
                    fmi_text = dtc.get('fmiText', 'N/A')
                    occurrence = dtc.get('occurrenceCount', 'N/A')
                    st.write(f"- SPN: `{spn}` (FMI: `{fmi}` - _{fmi_text}_). Ocurrencias: `{occurrence}`")
            else:
                st.info("**DTCs:** Ninguno activo")

            st.write("**Luces de Check Engine:**")
            if selected_vehicle_data['engine_check_light_warning'] or \
               selected_vehicle_data['engine_check_light_emissions'] or \
               selected_vehicle_data['engine_check_light_protect'] or \
               selected_vehicle_data['engine_check_light_stop']:
                if selected_vehicle_data['engine_check_light_warning']:
                    st.warning("- Advertencia (Warning) ON")
                if selected_vehicle_data['engine_check_light_emissions']:
                    st.warning("- Emisiones (Emissions) ON")
                if selected_vehicle_data['engine_check_light_protect']:
                    st.warning("- Protección (Protect) ON")
                if selected_vehicle_data['engine_check_light_stop']:
                    st.error("- ¡Detener (Stop) ON!")
            else:
                st.info("- Ninguna luz de Check Engine activa.")


        with col2:
            st.write(f"### Visualización 3D de {selected_vehicle_name}")
            display_gltf_viewer(TRUCK_MODEL_PATH, height=400)
            st.markdown(f"**Estado del Modelo 3D:** <span style='color:{selected_vehicle_data.get('alert_color', 'gray')}; font-weight:bold;'>{selected_vehicle_data.get('status_alert', 'N/A')}</span>", unsafe_allow_html=True)
    else:
        st.warning("No se pudieron encontrar datos para el vehículo seleccionado.")
else:
    st.warning("No hay datos de vehículos disponibles para mostrar el detalle del camión y la visualización 3D.")