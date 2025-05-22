import requests
import json
import time
from datetime import datetime, timedelta
import os
import streamlit as st
import pandas as pd
import base64
from streamlit.components.v1 import html
import pydeck as pdk

# --- CONFIGURACIÓN DE PÁGINA (¡DEBE SER LO PRIMERO!) ---
st.set_page_config(layout="wide", page_title="Gemelos Digitales de Flota")

# --- CONFIGURACIÓN GENERAL ---
try:
    SAMSARA_API_TOKEN = st.secrets["SAMSARA_API_TOKEN"]
except KeyError:
    st.error("Error: La clave 'SAMSARA_API_TOKEN' no se encontró en los secretos de Streamlit. "
             "Por favor, configura tu token de Samsara en Streamlit Cloud o en un archivo .streamlit/secrets.toml localmente.")
    st.stop()

# Si deseas usar un token de Mapbox para estilos de mapa más avanzados (satélite, híbrido, etc.),
# puedes descomentar y configurar la siguiente línea en tus secretos de Streamlit:
# MAPBOX_API_TOKEN = st.secrets.get("MAPBOX_API_TOKEN", None)
# Si no lo configuras, pydeck usará un estilo de mapa básico (calles) por defecto.

BASE_URL = "https://api.samsara.com/fleet"
HEADERS = {
    "Authorization": f"Bearer {SAMSARA_API_TOKEN}",
    "Content-Type": "application/json"
}

TRUCK_MODEL_PATH = "truck5.glb" # Asegúrate de que tu modelo 3D esté en formato .glb o .gltf

# --- Cargar las definiciones de DTCs ---
DTC_DEFINITIONS = {}
try:
    with open("dtc_definitions.json", "r", encoding='utf-8') as f:
        DTC_DEFINITIONS = json.load(f)
except FileNotFoundError:
    st.warning("Advertencia: El archivo 'dtc_definitions.json' no se encontró. Las descripciones de DTCs no estarán disponibles.")
except json.JSONDecodeError:
    st.error("Error: El archivo 'dtc_definitions.json' está mal formateado. No se pudieron cargar las descripciones de DTCs. Por favor, revisa su sintaxis JSON.")
except Exception as e:
    st.error(f"Error inesperado al cargar dtc_definitions.json: {e}")


# --- ¡¡¡IMPORTANTE!!! REEMPLAZA ESTA LISTA CON IDS REALES DE VEHÍCULOS ACTIVOS DE TU FLOTA. ---
HARDCODED_VEHICLE_IDS = [
    "281474986130035", # ID del vehículo que ya estabas usando (PR1889)
    "281474987148134", # Segundo vehículo de tus logs (PR1563)
    "281474987157622", # Tercer vehículo de tus logs (PR1567) (Este es el que aparece con fallas en tu consola)
    "281474987159128",
    "281474994357352",
    "281474987052920",
    "281474987048760"
]

# Umbrales para alertas (Mantengo las variables por si se deciden usar más adelante, pero no se usan en la lógica de alerta principal)
COOLANT_TEMP_HIGH_THRESHOLD_C = 100
IDLE_THRESHOLD_SPEED_MPH = 1

# --- Función para obtener datos de MÚLTIPLES vehículos ---
@st.cache_data(ttl=300)
def fetch_samsara_data_multiple_vehicles(vehicle_ids_to_fetch):
    all_vehicle_details_map = {}
    all_vehicle_locations = {}
    all_vehicle_stats = {}
    all_vehicle_maintenance_data = {}

    locations_data_from_api = get_vehicle_locations(vehicle_ids_to_fetch)
    all_vehicle_locations.update(locations_data_from_api)

    # Definir todos los tipos de estadísticas que necesitamos
    all_desired_stat_types = [
        'engineCoolantTemperatureMilliC',
        'ambientAirTemperatureMilliC',
        'engineRpm',
        'obdEngineSeconds',
        'engineOilPressureKPa'
    ]
    
    # Dividir los tipos de estadísticas en lotes de 4
    stat_type_batches = [all_desired_stat_types[i:i + 4] for i in range(0, len(all_desired_stat_types), 4)]


    for vehicle_id_to_fetch in vehicle_ids_to_fetch:
        vehicle_data = get_single_vehicle_details(vehicle_id_to_fetch)
        if vehicle_data:
            all_vehicle_details_map[vehicle_id_to_fetch] = vehicle_data
        else:
            st.error(f"ERROR: No se pudieron obtener los detalles para el vehículo ID: {vehicle_id_to_fetch}. Verifique el ID y su estado en Samsara y los permisos de su token.")
            continue

        combined_stats = {}
        for batch in stat_type_batches:
            stats_data_fetched_batch = get_all_vehicle_stats_and_filter(vehicle_id_to_fetch, batch)
            if stats_data_fetched_batch:
                combined_stats.update(stats_data_fetched_batch)

        if combined_stats:
            all_vehicle_stats[vehicle_id_to_fetch] = combined_stats
        else:
            st.warning(f"ADVERTENCIA: No se pudo obtener NINGUNA estadística para el vehículo ID: {vehicle_id_to_fetch}. Esto puede afectar la visualización de datos.")

        maintenance_data = get_vehicle_maintenance_data(vehicle_id_to_fetch)
        if maintenance_data:
            all_vehicle_maintenance_data[vehicle_id_to_fetch] = maintenance_data

    return all_vehicle_details_map, all_vehicle_locations, all_vehicle_stats, all_vehicle_maintenance_data

# --- Nueva función para actualizar un solo vehículo ---
@st.cache_data(ttl=300)
def fetch_samsara_data_single_vehicle(vehicle_id_to_fetch):
    vehicle_details_map = {}
    vehicle_locations = {}
    vehicle_stats = {}
    vehicle_maintenance_data = {}

    # Get location for the single vehicle
    location_data = get_vehicle_locations([vehicle_id_to_fetch])
    if location_data:
        vehicle_locations.update(location_data)

    vehicle_data = get_single_vehicle_details(vehicle_id_to_fetch)
    if vehicle_data:
        vehicle_details_map[vehicle_id_to_fetch] = vehicle_data
    else:
        st.error(f"ERROR: No se pudieron obtener los detalles para el vehículo ID: {vehicle_id_to_fetch}. Verifique el ID y su estado en Samsara y los permisos de su token.")
        return {}, {}, {}, {} # Return empty if details can't be fetched

    # Define all stat types we need for a single vehicle
    all_desired_stat_types = [
        'engineCoolantTemperatureMilliC',
        'ambientAirTemperatureMilliC',
        'engineRpm',
        'obdEngineSeconds',
        'engineOilPressureKPa'
    ]
    
    # Divide stat types into batches of 4
    stat_type_batches = [all_desired_stat_types[i:i + 4] for i in range(0, len(all_desired_stat_types), 4)]

    combined_stats = {}
    for batch in stat_type_batches:
        stats_data_fetched_batch = get_all_vehicle_stats_and_filter(vehicle_id_to_fetch, batch)
        if stats_data_fetched_batch:
            combined_stats.update(stats_data_fetched_batch)

    if combined_stats:
        vehicle_stats[vehicle_id_to_fetch] = combined_stats
    else:
        st.warning(f"ADVERTENCIA: No se pudo obtener NINGUNA estadística para el vehículo ID: {vehicle_id_to_fetch}. Esto puede afectar la visualización de datos.")

    maintenance_data = get_vehicle_maintenance_data(vehicle_id_to_fetch)
    if maintenance_data:
        vehicle_maintenance_data[vehicle_id_to_fetch] = maintenance_data

    return vehicle_details_map, vehicle_locations, vehicle_stats, vehicle_maintenance_data


def get_single_vehicle_details(vehicle_id):
    endpoint = f"{BASE_URL}/vehicles/{vehicle_id}"
    try:
        response = requests.get(endpoint, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json().get('data')
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
    ids_str = ",".join(vehicle_ids)
    params = {'ids': ids_str}
    try:
        response = requests.get(endpoint, headers=HEADERS, params=params, timeout=10)
        response.raise_for_status()
        locations_data = response.json().get('data', [])
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
    if not stat_types or len(stat_types) > 4:
        st.error(f"ERROR: get_all_vehicle_stats_and_filter recibió una lista de stat_types inválida: {stat_types}. Debe ser entre 1 y 4 elementos.")
        return None

    endpoint = f"{BASE_URL}/vehicles/stats"
    params = {"types": ",".join(stat_types)}

    try:
        response = requests.get(endpoint, headers=HEADERS, params=params, timeout=10)
        response.raise_for_status()

        data = response.json().get('data', [])

        for item in data:
            if item.get('id') == target_vehicle_id:
                all_stats_for_vehicle = {}
                for stat_type in stat_types:
                    if stat_type in item:
                        if isinstance(item[stat_type], dict) and 'value' in item[stat_type]:
                            all_stats_for_vehicle[stat_type] = item[stat_type]['value']
                        else:
                            all_stats_for_vehicle[stat_type] = item[stat_type]
                return all_stats_for_vehicle
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

    while True:
        page_count += 1
        params = {}
        if next_cursor:
            params['after'] = next_cursor

        try:
            response = requests.get(endpoint, headers=HEADERS, params=params, timeout=10)
            response.raise_for_status()

            response_data = response.json()
            current_page_items = response_data.get('vehicleMaintenance', [])
            if not current_page_items:
                current_page_items = response_data.get('vehicles', [])

            all_maintenance_items.extend(current_page_items)

            pagination_info = response_data.get('pagination', {})
            next_cursor = pagination_info.get('endCursor')

            if not next_cursor:
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

    found_vehicle_data = None
    for vehicle_item in all_maintenance_items:
        if str(vehicle_item.get('id')) == str(target_vehicle_id):
            found_vehicle_data = vehicle_item
            break

    if found_vehicle_data:
        return found_vehicle_data
    else:
        return None


# --- LÓGICA DEL GEMELO DIGITAL Y DETECCIÓN DE ALERTA ---
def process_vehicle_data(vehicle_details, vehicle_locations, vehicle_stats, vehicle_maintenance_data):
    gemelo_digital = {
        'vehicle_id': vehicle_details.get('id', ''),
        'vehicle_name': vehicle_details.get('name', 'N/A'),
        'make': vehicle_details.get('make', 'N/A'),
        'model': vehicle_details.get('model', 'N/A'),
        'year': vehicle_details.get('year', 'N/A'),
        'license_plate': vehicle_details.get('licensePlate', 'N/A'),
        'latitude': 'N/A', 'longitude': 'N/A', 'speed_mph': 'N/A', 'current_address': 'N/A',
        'gps_odometer_meters': 'N/A', 'location_updated_at': 'N/A',
        'engine_hours': 'N/A',
        'fuel_perc_remaining': 'N/A',
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
        loc_time_str = loc_data.get('time', 'N/A')
        if loc_time_str != 'N/A':
            try:
                gemelo_digital['location_updated_at'] = datetime.fromisoformat(loc_time_str.replace('Z', '+00:00')).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                gemelo_digital['location_updated_at'] = loc_time_str
        else:
            gemelo_digital['location_updated_at'] = 'N/A'


    engine_seconds = stats_data.get('obdEngineSeconds')
    if isinstance(engine_seconds, (int, float)):
        gemelo_digital['engine_hours'] = round(engine_seconds / 3600, 2)
    else:
        gemelo_digital['engine_hours'] = 'N/A'

    gemelo_digital['fuel_perc_remaining'] = 'N/A'


    oil_pressure = stats_data.get('engineOilPressureKPa')
    if isinstance(oil_pressure, (int, float)):
        gemelo_digital['engine_oil_pressure_kpa'] = round(oil_pressure, 2)
    else:
        gemelo_digital['engine_oil_pressure_kpa'] = 'N/A'

    temp_c_milli = stats_data.get('engineCoolantTemperatureMilliC')
    if isinstance(temp_c_milli, (int, float)):
        gemelo_digital['engine_coolant_temperature_c'] = round(temp_c_milli / 1000, 2)
    else:
        gemelo_digital['engine_coolant_temperature_c'] = 'N/A'

    temp_ambient_milli = stats_data.get('ambientAirTemperatureMilliC')
    if isinstance(temp_ambient_milli, (int, float)):
        gemelo_digital['ambient_air_temperature_c'] = round(temp_ambient_milli / 1000, 2)
    else:
        gemelo_digital['ambient_air_temperature_c'] = 'N/A'

    engine_rpm_val = stats_data.get('engineRpm')
    if isinstance(engine_rpm_val, (int, float)):
        gemelo_digital['engine_rpm'] = engine_rpm_val
    else:
        gemelo_digital['engine_rpm'] = 'N/A'

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

    alerts = []

    if gemelo_digital['diagnostic_trouble_codes'] and isinstance(gemelo_digital['diagnostic_trouble_codes'], list) and len(gemelo_digital['diagnostic_trouble_codes']) > 0:
        dtc_codes_info_for_alert = []
        for code in gemelo_digital['diagnostic_trouble_codes']:
            spn = code.get('spnId', 'N/A')
            fmi = code.get('fmiId', 'N/A')
            dtc_codes_info_for_alert.append(f"SPN: {spn} (FMI: {fmi})")
        alerts.append(f"Fallas de motor (DTCs: {'; '.join(dtc_codes_info_for_alert)})")

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

    if alerts:
        gemelo_digital['status_alert'] = "ALERTA: " + '; '.join(alerts)
        gemelo_digital['alert_color'] = 'red'
    elif gemelo_digital['status_alert'] != 'OFFLINE o SIN DATOS':
        gemelo_digital['status_alert'] = 'OPERANDO NORMALMENTE'
        gemelo_digital['alert_color'] = 'green'

    return gemelo_digital

# --- Función para mostrar el visor 3D ---
def display_gltf_viewer(model_path, height=500):
    if not os.path.exists(model_path):
        st.error(f"Error: El archivo del modelo 3D '{model_path}' no se encontró en la ruta: {os.path.abspath(model_path)}")
        st.warning("Asegúrate de que el archivo del modelo 3D (ej. 'truck.glb') esté en la misma carpeta que este script.")
        st.warning(f"**¡IMPORTANTÍSIMO!** Si tu archivo es '.obj' (como '{os.path.basename(model_path).replace('.glb', '.obj')}'), necesitas **CONVERTIRLO** a '.glb' o '.gltf' para que funcione con el visor 3D en Streamlit.")
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
            margin: 0;
            padding: 0;
            display: block;
            /* Cambiar el color de fondo para que no sea un blanco plano */
            background-color: #333333; /* Un gris oscuro suave */
            --poster-color: #333333; /* También para el color del póster antes de cargar el modelo */
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
          /* Ajustes de cámara para acercar y centrar el modelo */
          camera-orbit="0deg 90deg 100%" /* Posición inicial de la cámara: 0deg azimut, 90deg elevación, 100% radio. Puedes ajustar el radio para acercar o alejar. */
          field-of-view="30deg" /* Un campo de visión más estrecho puede hacer que el modelo parezca más grande. Prueba con 30deg, 45deg, etc. */
          min-field-of-view="20deg" /* Evita que el usuario aleje demasiado */
          max-field-of-view="60deg" /* Evita que el usuario acerque demasiado */
          interpolation-decay="200" /* Hace que la rotación y zoom sean más suaves */
          shadow-softness="0.5" /* Ajusta la suavidad de las sombras */
          auto-rotate-delay="1000" /* Retraso antes de que empiece a rotar automáticamente */
          interaction-prompt="none" /* Quita el texto de "haz clic para interactuar" */
          interaction-prompt-style="basic"
          camera-target="0.0m 0.5m 0.0m" /* Si tu modelo está descentrado, puedes ajustar esto (x y z) para enfocar un punto específico */
        ></model-viewer>
        """
        html(html_code, height=height, width=None, scrolling=False)
    except Exception as e:
        st.error(f"Error al cargar o mostrar el modelo 3D: {e}")
        st.info("Asegúrate de que el archivo GLB no esté corrupto y que el nombre del archivo en el código sea exactamente igual al del archivo en tu disco.")

# --- APLICACIÓN STREAMLIT ---
st.title("🚚 Gemelos Digitales de tu Flota (Samsara) con 3D y Mapa")

# Initialize session state for all_gemelos_digitales if not already present
if 'all_gemelos_digitales' not in st.session_state:
    st.session_state.all_gemelos_digitales = {}

if st.button("Actualizar Datos de toda la Flota"):
    st.cache_data.clear() # Clear the cache for fresh data
    with st.spinner("Cargando datos de Samsara para toda la flota..."):
        vehicle_details_map, vehicle_locations, vehicle_stats, vehicle_maintenance_data = fetch_samsara_data_multiple_vehicles(HARDCODED_VEHICLE_IDS)
        st.session_state.all_gemelos_digitales = {} # Reset to update all
        if vehicle_details_map:
            for vehicle_id, details in vehicle_details_map.items():
                gemelo = process_vehicle_data(details, vehicle_locations, vehicle_stats, vehicle_maintenance_data)
                st.session_state.all_gemelos_digitales[vehicle_id] = gemelo
        else:
            st.warning("No se pudieron cargar datos de vehículos. Asegúrate de que los IDs de vehículos hardcodeados sean válidos y estén activos y que tu token de API tenga los permisos correctos.")
    st.success("Datos de Samsara cargados para toda la flota.")
    st.rerun() # Rerun to refresh the display

# Initial load of data if session state is empty (first run or after a full clear)
if not st.session_state.all_gemelos_digitales:
    with st.spinner("Cargando datos iniciales de Samsara para toda la flota..."):
        vehicle_details_map, vehicle_locations, vehicle_stats, vehicle_maintenance_data = fetch_samsara_data_multiple_vehicles(HARDCODED_VEHICLE_IDS)
        if vehicle_details_map:
            for vehicle_id, details in vehicle_details_map.items():
                gemelo = process_vehicle_data(details, vehicle_locations, vehicle_stats, vehicle_maintenance_data)
                st.session_state.all_gemelos_digitales[vehicle_id] = gemelo
        else:
            st.warning("No se pudieron cargar datos de vehículos. Asegúrate de que los IDs de vehículos hardcodeados sean válidos y estén activos y que tu token de API tenga los permisos correctos.")
    if st.session_state.all_gemelos_digitales:
        st.success("Datos de Samsara cargados inicialmente.")


df_fleet = pd.DataFrame(list(st.session_state.all_gemelos_digitales.values()))

st.subheader("Resumen de la Flota")
if not df_fleet.empty:
    df_fleet['speed_mph'] = pd.to_numeric(df_fleet['speed_mph'], errors='coerce')

    summary_cols = ['vehicle_name', 'make', 'model', 'year', 'status_alert',
                    'engine_coolant_temperature_c',
                    'speed_mph', 'current_address', 'last_data_sync']

    st.dataframe(df_fleet[summary_cols], use_container_width=True)
else:
    st.warning("No hay datos de vehículos disponibles para mostrar en el resumen de la flota.")

st.markdown("---")

st.subheader("Detalle del Camión y Visualización 3D")
if not df_fleet.empty:
    vehicle_names = [v.get('vehicle_name', v.get('vehicle_id')) for v in st.session_state.all_gemelos_digitales.values()]
    selected_vehicle_name = st.selectbox("Selecciona un vehículo para ver detalles:", vehicle_names, key='selected_vehicle_detail')

    selected_vehicle_data = None
    selected_vehicle_id = None
    for gemelo_id, gemelo_data in st.session_state.all_gemelos_digitales.items():
        if gemelo_data.get('vehicle_name', gemelo_data.get('vehicle_id')) == selected_vehicle_name:
            selected_vehicle_data = gemelo_data
            selected_vehicle_id = gemelo_id
            break

    if selected_vehicle_data:
        # Aquí definimos las 3 columnas principales para el detalle del vehículo
        # Proporciones ajustadas para simetría visual del 3D y el mapa
        col_details, col_3d_model, col_map = st.columns([2, 1.5, 1.5]) 

        with col_details:
            st.write(f"### Gemelo Digital de {selected_vehicle_name}")
            st.write(f"**Estado:** <span style='color:{selected_vehicle_data.get('alert_color', 'gray')}; font-weight:bold;'>{selected_vehicle_data.get('status_alert', 'N/A')}</span>", unsafe_allow_html=True)
            st.write(f"**Última Sincronización:** {selected_vehicle_data.get('last_data_sync', 'N/A')}")
            st.write(f"**Marca:** {selected_vehicle_data.get('make', 'N/A')}")
            st.write(f"**Modelo:** {selected_vehicle_data.get('model', 'N/A')}")
            st.write(f"**Año:** {selected_vehicle_data.get('year', 'N/A')}")
            st.write(f"🌍 **Ubicación:** ({selected_vehicle_data.get('latitude', 'N/A')}, {selected_vehicle_data.get('longitude', 'N/A')})")
            st.write(f"📍 **Dirección Actual:** {selected_vehicle_data.get('current_address', 'N/A')}")
            st.write(f"⚡ **Velocidad:** {selected_vehicle_data.get('speed_mph', 'N/A')} MPH")

            st.write(f"🌡️ **Temperatura Motor:** {selected_vehicle_data.get('engine_coolant_temperature_c', 'N/A')}°C")
            st.write(f"💧 **Presión Aceite:** {selected_vehicle_data.get('engine_oil_pressure_kpa', 'N/A')} KPa")
            st.write(f"🔄 **RPM Motor:** {selected_vehicle_data.get('engine_rpm', 'N/A')}")
            st.write(f"⏱️ **Horas de Motor:** {selected_vehicle_data.get('engine_hours', 'N/A')} hrs")
            
            # --- Nuevo botón de actualización para la unidad específica ---
            if st.button(f"Actualizar Solo {selected_vehicle_name}", key=f"update_single_{selected_vehicle_id}"):
                with st.spinner(f"Actualizando datos para {selected_vehicle_name}..."):
                    st.cache_data.clear() # Simplest way, but will clear all cache
                    
                    details_map_single, locations_single, stats_single, maintenance_single = fetch_samsara_data_single_vehicle(selected_vehicle_id)
                    
                    if selected_vehicle_id in details_map_single:
                        updated_gemelo = process_vehicle_data(
                            details_map_single[selected_vehicle_id],
                            locations_single,
                            stats_single,
                            maintenance_single
                        )
                        st.session_state.all_gemelos_digitales[selected_vehicle_id] = updated_gemelo
                        st.success(f"Datos de {selected_vehicle_name} actualizados.")
                        st.rerun() # Rerun to refresh the display with updated single vehicle data
                    else:
                        st.error(f"No se pudieron actualizar los datos para {selected_vehicle_name}.")


        with col_3d_model: # Contenido del modelo 3D
            st.write(f"### Modelo 3D")
            display_gltf_viewer(TRUCK_MODEL_PATH, height=500) 
        
        with col_map: # Contenido del mapa
            st.write(f"### Ubicación") # Título conciso
            latitude = selected_vehicle_data.get('latitude')
            longitude = selected_vehicle_data.get('longitude')

            if latitude != 'N/A' and longitude != 'N/A' and isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
                map_df = pd.DataFrame([{'lat': latitude, 'lon': longitude, 'name': selected_vehicle_name}])

                view_state = pdk.ViewState(
                    latitude=latitude,
                    longitude=longitude,
                    zoom=14, # Nivel de zoom
                    pitch=0 # Ángulo de la cámara, 0 es vista cenital
                )

                # Capa para el punto del vehículo
                layer = pdk.Layer(
                    "ScatterplotLayer",
                    map_df,
                    get_position="[lon, lat]",
                    get_color=[255, 0, 0, 160], # Rojo para el punto
                    get_radius=50, # Radio del punto en metros (ajustado para que sea visible)
                    pickable=True,
                    tooltip={
                        "html": "<b>Vehículo:</b> {name}<br/><b>Lat:</b> {lat}<br/><b>Lon:</b> {lon}",
                        "style": {"backgroundColor": "steelblue", "color": "white"}
                    }
                )

                # Renderizar el mapa con pydeck.Deck
                st.pydeck_chart(pdk.Deck(
                    map_style="mapbox://styles/mapbox/streets-v11", # Estilo de mapa (calles)
                    initial_view_state=view_state,
                    layers=[layer],
                    tooltip={
                        "html": "<b>{name}</b><br/>{current_address}",
                        "style": {"color": "white"}
                    },
                    height=500
                ), use_container_width=True)
            else:
                st.warning("Ubicación no disponible para este vehículo o datos inválidos.")
            # --- FIN DEL MAPA PYDECK ---
        
        # Nueva sección para DTCs y luces de Check Engine, ocupando el ancho completo
        # Esta sección se mantiene debajo de las 3 columnas para evitar desbordamientos
        st.markdown("---") # Una línea para separar visualmente

        st.subheader("Códigos de Falla y Luces de Advertencia")

        # DTCs
        dtcs = selected_vehicle_data.get('diagnostic_trouble_codes')
        if dtcs and isinstance(dtcs, list) and len(dtcs) > 0:
            st.warning(f"🚨 **DTCs Activos:**")
            
            num_columns_dtcs = 4 # Ahora 4 columnas para DTCs
            cols_dtc = st.columns(num_columns_dtcs)
            col_idx_dtc = 0

            for dtc in dtcs:
                with cols_dtc[col_idx_dtc]:
                    spn = dtc.get('spnId', 'N/A')
                    fmi = dtc.get('fmiId', 'N/A')
                    occurrence = dtc.get('occurrenceCount', 'N/A')

                    dtc_key = f"SPN:{spn} FMI:{fmi}"
                    dtc_info = DTC_DEFINITIONS.get(dtc_key, {})
                    description = dtc_info.get('description', f"Descripción no disponible para {dtc_key}")
                    external_link = dtc_info.get('link')
                    suggestion = dtc_info.get('suggestion', 'No hay sugerencia de solución disponible.')

                    with st.popover(f"**{dtc_key}** (Ocurrencias: `{occurrence}`)", use_container_width=True):
                        st.markdown(f"**Código:** {dtc_key}")
                        st.markdown(f"**Ocurrencias:** `{occurrence}`")
                        st.markdown(f"**Descripción:** {description}")
                        st.markdown(f"**Sugerencia de Solución:** {suggestion}")
                        if external_link and external_link != "":
                            st.markdown(f"[Más detalles aquí]({external_link})")
                
                col_idx_dtc = (col_idx_dtc + 1) % num_columns_dtcs
        else:
            st.info("✅ **DTCs:** Ninguno activo")

        # Luces de Check Engine
        st.write("🚦 **Luces de Check Engine:**")
        check_light_alerts = []
        if selected_vehicle_data['engine_check_light_warning']:
            check_light_alerts.append("- 🟠 Advertencia (Warning) ON")
        if selected_vehicle_data['engine_check_light_emissions']:
            check_light_alerts.append("- 💨 Emisiones (Emissions) ON")
        if selected_vehicle_data['engine_check_light_protect']:
            check_light_alerts.append("- 🛡️ Protección (Protect) ON")
        if selected_vehicle_data['engine_check_light_stop']:
            check_light_alerts.append("- 🛑 ¡Detener (Stop) ON!")
        
        if check_light_alerts:
            num_columns_lights = 2 # Puedes ajustar este número para las luces, 2 es un buen inicio
            cols_lights = st.columns(num_columns_lights)
            col_idx_lights = 0
            for alert_text in check_light_alerts:
                with cols_lights[col_idx_lights]:
                    st.warning(alert_text) # O usar st.error para la de Stop
                col_idx_lights = (col_idx_lights + 1) % num_columns_lights
        else:
            st.info("- 🟢 Ninguna luz de Check Engine activa.")

    else:
        st.warning("No se pudieron encontrar datos para el vehículo seleccionado.")
else:
    st.warning("No hay datos de vehículos disponibles para mostrar el detalle del camión y la visualización 3D.")