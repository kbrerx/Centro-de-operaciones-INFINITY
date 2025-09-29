import streamlit as st
import pandas as pd
import datetime
import time
import locale
import json
import pyrebase
import firebase_admin
from firebase_admin import credentials, firestore

# --- Configuración Inicial de la Página ---
st.set_page_config(
    page_title="Centro de Mando de Ofertas",
    page_icon="🚀",
    layout="wide"
)

# --- Configuración de Localismo para Español ---
try:
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
except locale.Error:
    pass

# --- INICIALIZACIÓN DE FIREBASE (MÉTODO JSON DIRECTO) ---
def init_firebase_admin():
    """Inicializa Firebase Admin SDK leyendo todo el JSON desde los secretos."""
    try:
        creds_json_str = st.secrets["firebase_secrets"]["credentials_json"]
        creds_dict = json.loads(creds_json_str)
        creds_dict['private_key'] = creds_dict['private_key'].replace('\\n', '\n')
        creds = credentials.Certificate(creds_dict)
        firebase_admin.initialize_app(creds)
    except Exception as e:
        st.error(f"Error crítico al inicializar Firebase Admin: {e}")
        st.info("Verifica que el contenido del JSON en .streamlit/secrets.toml sea correcto.")
        st.stop()

def init_firebase_auth():
    """Inicializa Pyrebase para la autenticación del cliente."""
    try:
        return pyrebase.initialize_app(st.secrets["firebase_auth"])
    except Exception as e:
        st.error(f"Error crítico al inicializar la autenticación de Firebase: {e}")
        st.error("Verifica la sección [firebase_auth] en tu archivo .streamlit/secrets.toml.")
        st.stop()

if not firebase_admin._apps:
    init_firebase_admin()

db = firestore.client()
auth_client = init_firebase_auth()


# --- FUNCIONES DE MANEJO DE DATOS CON FIRESTORE (VERSIÓN SOCIOS) ---
def df_to_json(df):
    """Convierte un DataFrame a formato JSON compatible con Firestore."""
    return df.to_json(orient='split', date_format='iso')

def json_to_df(json_str):
    """Convierte un string JSON de vuelta a un DataFrame."""
    if not json_str or not isinstance(json_str, str):
        return pd.DataFrame()
    df = pd.read_json(json_str, orient='split')
    if 'Fecha' in df.columns:
        df['Fecha'] = pd.to_datetime(df['Fecha'])
    return df

def save_data_to_firestore():
    """Guarda todo el estado de la sesión del equipo en una ruta compartida en Firestore."""
    # MODIFICACIÓN CLAVE: Se usa una ruta compartida definida en secrets.toml
    try:
        workspace_id = st.secrets["team_config"]["workspace_id"]
        doc_ref = db.collection('socios').document(workspace_id).collection('app_data').document('main')
    except KeyError:
        st.error("Error de configuración: No se encontró 'team_config' o 'workspace_id' en los secretos.")
        return

    data_to_save = {
        'ofertas': {},
        'boveda': st.session_state.get('boveda', []),
        'plantillas': st.session_state.get('plantillas', {})
    }
    for offer_id, offer_data in st.session_state.get('ofertas', {}).items():
        processed_offer = offer_data.copy()
        if 'testeos' in processed_offer:
            processed_offer['testeos'] = df_to_json(processed_offer['testeos'])
        if 'escala' in processed_offer:
            for camp_id, camp_data in processed_offer['escala'].items():
                if 'registros' in camp_data:
                    camp_data['registros'] = df_to_json(camp_data['registros'])
        data_to_save['ofertas'][offer_id] = processed_offer
    try:
        doc_ref.set(data_to_save)
    except Exception as e:
        st.error(f"Error al guardar los datos en la nube: {e}")

def load_data_from_firestore():
    """Carga los datos del equipo desde la ruta compartida de Firestore."""
    # MODIFICACIÓN CLAVE: Se usa una ruta compartida definida en secrets.toml
    try:
        workspace_id = st.secrets["team_config"]["workspace_id"]
        doc_ref = db.collection('socios').document(workspace_id).collection('app_data').document('main')
    except KeyError:
        st.error("Error de configuración: No se encontró 'team_config' o 'workspace_id' en los secretos.")
        return
        
    doc = doc_ref.get()
    if doc.exists:
        data = doc.to_dict()
        st.session_state.boveda = data.get('boveda', [])
        st.session_state.plantillas = data.get('plantillas', {})
        loaded_ofertas = {}
        for offer_id, offer_data in data.get('ofertas', {}).items():
            processed_offer = offer_data.copy()
            if 'testeos' in processed_offer:
                processed_offer['testeos'] = json_to_df(processed_offer['testeos'])
            if 'escala' in processed_offer:
                for camp_id, camp_data in processed_offer['escala'].items():
                    if 'registros' in camp_data:
                        camp_data['registros'] = json_to_df(camp_data['registros'])
            loaded_ofertas[offer_id] = processed_offer
        st.session_state.ofertas = loaded_ofertas
    else:
        st.session_state.ofertas = {}
        st.session_state.boveda = []
        st.session_state.plantillas = {}
        save_data_to_firestore()

# --- LÓGICA DE AUTENTICACIÓN Y PANTALLA DE LOGIN (VERSIÓN SOCIOS) ---
def show_login_page():
    st.title("🚀 Centro de Mando de Ofertas")
    # MODIFICACIÓN: Título actualizado para la versión de socios.
    st.subheader("Bienvenido a la Versión para Socios")
    
    # Cargar la lista de correos autorizados desde los secretos.
    try:
        authorized_emails = st.secrets["team_config"]["authorized_emails"]
    except KeyError:
        st.error("Error de configuración: La lista de correos autorizados no está definida en secrets.toml.")
        st.stop()


    if 'auth_form' not in st.session_state:
        st.session_state.auth_form = 'Login'
    
    if st.session_state.auth_form == 'Login':
        with st.form("login_form"):
            st.write("#### Iniciar Sesión")
            email = st.text_input("Email")
            password = st.text_input("Contraseña", type="password")
            submit_button = st.form_submit_button("Entrar")
            if submit_button:
                # MODIFICACIÓN CLAVE: Verificar si el email está en la whitelist ANTES de proceder.
                if email not in authorized_emails:
                    st.error("Acceso denegado. Tu correo no está en la lista de usuarios autorizados.")
                else:
                    try:
                        user = auth_client.auth().sign_in_with_email_and_password(email, password)
                        st.session_state.logged_in = True
                        st.session_state.user_id = user['localId']
                        st.session_state.user_email = user['email']
                        load_data_from_firestore()
                        st.rerun()
                    except Exception as e:
                        st.error("Email o contraseña incorrectos. Por favor, inténtalo de nuevo.")

        if st.button("¿No tienes cuenta? Regístrate aquí"):
            st.session_state.auth_form = 'Register'
            st.rerun()
    else: # Formulario de Registro
        with st.form("register_form"):
            st.write("#### Crear Nueva Cuenta")
            email = st.text_input("Email")
            password = st.text_input("Contraseña", type="password")
            confirm_password = st.text_input("Confirmar Contraseña", type="password")
            submit_button = st.form_submit_button("Registrarse")
            if submit_button:
                # MODIFICACIÓN CLAVE: Verificar si el email está en la whitelist ANTES de crear la cuenta.
                if email not in authorized_emails:
                    st.error("Este correo electrónico no está autorizado para registrarse.")
                elif password == confirm_password:
                    try:
                        user = auth_client.auth().create_user_with_email_and_password(email, password)
                        st.success("¡Cuenta creada con éxito! Ahora puedes iniciar sesión.")
                        st.session_state.auth_form = 'Login'
                        st.rerun()
                    except Exception as e:
                        st.error("No se pudo crear la cuenta. Es posible que el email ya esté en uso.")
                else:
                    st.error("Las contraseñas no coinciden.")
        if st.button("¿Ya tienes cuenta? Inicia sesión aquí"):
            st.session_state.auth_form = 'Login'
            st.rerun()

# --- FIN DE LA LÓGICA DE AUTENTICACIÓN ---

# --- Inicialización del Estado de la Aplicación ---
if 'vista_actual' not in st.session_state: st.session_state['vista_actual'] = 'dashboard'
if 'oferta_seleccionada' not in st.session_state: st.session_state['oferta_seleccionada'] = None
if 'anuncio_para_escalar' not in st.session_state: st.session_state['anuncio_para_escalar'] = None
if 'accion_de_escala' not in st.session_state: st.session_state['accion_de_escala'] = None
if 'editing_record' not in st.session_state: st.session_state['editing_record'] = None
if 'offer_to_delete' not in st.session_state: st.session_state['offer_to_delete'] = None
if 'editing_boveda_id' not in st.session_state: st.session_state['editing_boveda_id'] = None
if 'boveda_view_mode' not in st.session_state: st.session_state['boveda_view_mode'] = '🖼️ Tarjetas'
if 'editing_plantilla_id' not in st.session_state: st.session_state['editing_plantilla_id'] = None
if 'editing_checklist_oferta_id' not in st.session_state: st.session_state['editing_checklist_oferta_id'] = None 

# --- (El resto del código de la aplicación (funciones, UI, etc.) sigue aquí sin cambios) ---
# --- No se requieren modificaciones en la lógica principal de la aplicación. ---

# --- Funciones para la Bóveda ---
def eliminar_entrada_boveda(id_entrada):
    st.session_state.boveda = [entrada for entrada in st.session_state.boveda if entrada['id'] != id_entrada]
    save_data_to_firestore()
    st.success("Oferta eliminada de la Bóveda.")

def render_rating_stars(rating):
    return "⭐" * rating + "☆" * (5 - rating)

def update_entrada_boveda(id_entrada, nuevos_datos):
    for i, entrada in enumerate(st.session_state.boveda):
        if entrada['id'] == id_entrada:
            st.session_state.boveda[i] = nuevos_datos
            break
    save_data_to_firestore()
    st.success("¡Oferta actualizada en la Bóveda!")

def update_boveda_status(entrada_id, key):
    nuevo_estatus = st.session_state[key]
    for i, entrada in enumerate(st.session_state.boveda):
        if entrada['id'] == entrada_id:
            st.session_state.boveda[i]['estatus'] = nuevo_estatus
            break
    save_data_to_firestore()

# --- Funciones para Plantillas y Checklists ---
def parse_checklist(raw_text):
    parsed = []
    lines = raw_text.strip().split('\n')
    for line in lines:
        stripped_line = line.strip()
        if not stripped_line:
            continue
        if stripped_line.startswith('-'):
            parsed.append({
                "type": "task",
                "text": stripped_line[1:].strip(),
                "completed": False
            })
        else:
            parsed.append({
                "type": "phase",
                "text": stripped_line
            })
    return parsed

def unparse_checklist(tareas):
    raw_text = []
    for item in tareas:
        if item['type'] == 'phase':
            raw_text.append(item['text'])
        elif item['type'] == 'task':
            raw_text.append(f"- {item['text']}")
    return "\n".join(raw_text)

def merge_checklists(old_tareas, new_raw_text):
    new_tareas = parse_checklist(new_raw_text)
    old_task_status = {task['text']: task['completed'] for task in old_tareas if task['type'] == 'task'}
    
    for i, new_item in enumerate(new_tareas):
        if new_item['type'] == 'task':
            if new_item['text'] in old_task_status:
                new_tareas[i]['completed'] = old_task_status[new_item['text']]
    return new_tareas

def update_plantilla(id_plantilla, nombre, checklist_raw):
    st.session_state.plantillas[id_plantilla]['nombre'] = nombre
    st.session_state.plantillas[id_plantilla]['checklist_raw'] = checklist_raw
    save_data_to_firestore()
    st.success("¡Plantilla actualizada con éxito!")

# --- FUNCIONES DE CÁLCULO Y LÓGICA ---
def get_safe_column_name(alias):
    return f"Ventas: {alias}"

def calcular_metricas_diarias(registro, funnel, comision_pp=0.0):
    facturacion_bruta = 0
    ventas_pp = 0
    col_pp = get_safe_column_name("PP")
    
    for item_details in funnel.values():
        col_name = get_safe_column_name(item_details['alias'])
        if col_name in registro and pd.notna(registro[col_name]) and registro[col_name] > 0:
            facturacion_bruta += registro[col_name] * item_details['precio']
            if col_name == col_pp:
                ventas_pp += registro[col_name]

    total_comisiones = ventas_pp * comision_pp
    facturacion_neta = facturacion_bruta - total_comisiones

    registro['Facturación Total'] = facturacion_bruta
    registro['Ganancia Bruta'] = facturacion_bruta - registro['Inversión']
    registro['Ganancia Neta'] = facturacion_neta - registro['Inversión']
    registro['ROAS Bruto'] = facturacion_bruta / registro['Inversión'] if registro['Inversión'] > 0 else 0
    registro['ROAS Neto'] = facturacion_neta / registro['Inversión'] if registro['Inversión'] > 0 else 0
    return registro

def analizar_sugerencias_anuncios(df_testeos_global):
    if df_testeos_global.empty: return {}
    df_testeos_global['Fecha'] = pd.to_datetime(df_testeos_global['Fecha'])
    sugerencias = {}
    for anuncio, grupo in df_testeos_global.groupby('Anuncio'):
        grupo = grupo.sort_values(by='Fecha')
        inversion_total = grupo['Inversión'].sum()
        roas_acumulado = grupo['ROAS Neto'].sum() if 'ROAS Neto' in grupo else 0
        if inversion_total > 0 and 'Facturación Total' in grupo and st.session_state.oferta_seleccionada in st.session_state.ofertas:
                roas_acumulado = (grupo['Facturación Total'].sum() - (grupo[get_safe_column_name("PP")].sum() * st.session_state.ofertas[st.session_state.oferta_seleccionada].get('comision_pp', 0))) / inversion_total

        racha_ventas = 0
        grupo_invertido = grupo.iloc[::-1]
        columna_ventas_pp = get_safe_column_name("PP")
        for _, row in grupo_invertido.iterrows():
            if columna_ventas_pp in row and row[columna_ventas_pp] > 0:
                racha_ventas += 1
            else: break
        if roas_acumulado < 1.2 and inversion_total > 0:
            sugerencias[anuncio] = f"❄️ Apagar (ROAS Neto: {roas_acumulado:.2f})"
        elif roas_acumulado >= 1.7 and racha_ventas >= 4:
            sugerencias[anuncio] = f"🏆 GANADOR (ROAS Neto: {roas_acumulado:.2f}, Racha: {racha_ventas})"
        else:
            sugerencias[anuncio] = f"🧪 Testeando (ROAS Neto: {roas_acumulado:.2f}, Racha: {racha_ventas})"
    return sugerencias

# --- FUNCIONES DE MANEJO DE ESTADO ---
def crear_nueva_oferta(nombre, tipo_embudo, precio_principal, plantilla_id=None):
    id_oferta = f"oferta_{nombre.replace(' ', '_').lower()}_{int(time.time())}"
    if any(d['nombre'] == nombre for d in st.session_state.ofertas.values()):
        st.error(f"Ya existe una oferta con el nombre '{nombre}'."); return
    
    oferta_data = {
        "nombre": nombre, "tipo_embudo": tipo_embudo, "estado": "🧪 En Testeo",
        "funnel": {"principal": {"nombre": "Producto Principal", "precio": precio_principal, "alias": "PP", "estado": "🟢 Activo"}},
        "anuncios_testeo": [], "testeos": pd.DataFrame(columns=["Fecha", "Anuncio", "Inversión", "Pagos Iniciados", get_safe_column_name("PP"), "Facturación Total", "Ganancia Bruta", "Ganancia Neta", "ROAS Bruto", "ROAS Neto"]),
        "escala": {},
        "comision_pp": 0.0, "cpa_objetivo": 0.0
    }
    
    if plantilla_id and plantilla_id in st.session_state.plantillas:
        plantilla = st.session_state.plantillas[plantilla_id]
        oferta_data['checklist'] = {
            "plantilla_nombre": plantilla['nombre'],
            "tareas": parse_checklist(plantilla['checklist_raw'])
        }

    st.session_state.ofertas[id_oferta] = oferta_data
    save_data_to_firestore()
    st.success(f"¡Oferta '{nombre}' creada con éxito!")
    st.session_state.oferta_seleccionada = id_oferta
    st.session_state.vista_actual = 'dashboard'

def seleccionar_oferta(id_oferta):
    st.session_state.oferta_seleccionada = id_oferta
    st.session_state.vista_actual = 'dashboard'
    st.session_state['anuncio_para_escalar'] = None
    st.session_state['accion_de_escala'] = None
    st.session_state['editing_record'] = None
    st.session_state['offer_to_delete'] = None

def cambiar_estado_oferta(id_oferta, nuevo_estado):
    st.session_state.ofertas[id_oferta]['estado'] = nuevo_estado
    save_data_to_firestore()
    st.success(f"El estado de la oferta ha sido actualizado a: {nuevo_estado}")
    
def eliminar_oferta(id_oferta):
    if id_oferta in st.session_state.ofertas:
        nombre_oferta = st.session_state.ofertas[id_oferta]['nombre']
        del st.session_state.ofertas[id_oferta]
        save_data_to_firestore()
        st.session_state.oferta_seleccionada = None
        st.session_state.offer_to_delete = None
        st.success(f"¡Oferta '{nombre_oferta}' eliminada permanentemente!")

def actualizar_configuracion_financiera(id_oferta, comision, cpa):
    st.session_state.ofertas[id_oferta]['comision_pp'] = comision
    st.session_state.ofertas[id_oferta]['cpa_objetivo'] = cpa
    save_data_to_firestore()
    st.success("Configuración financiera actualizada.")

def eliminar_registro_testeo(id_oferta, index):
    df = st.session_state.ofertas[id_oferta]['testeos']
    st.session_state.ofertas[id_oferta]['testeos'] = df.drop(index).reset_index(drop=True)
    save_data_to_firestore()
    st.success("Registro eliminado con éxito.")

def eliminar_registro_escala(id_oferta, id_campana, index):
    df = st.session_state.ofertas[id_oferta]['escala'][id_campana]['registros']
    st.session_state.ofertas[id_oferta]['escala'][id_campana]['registros'] = df.drop(index).reset_index(drop=True)
    save_data_to_firestore()
    st.success("Registro de escala eliminado con éxito.")

def actualizar_registro_escala(id_oferta, id_campana, index, nuevo_registro):
    oferta = st.session_state.ofertas[id_oferta]
    campana = oferta['escala'][id_campana]
    registro_calculado = calcular_metricas_diarias(nuevo_registro, oferta['funnel'], oferta.get('comision_pp', 0.0))
    df_registro_actualizado = pd.DataFrame([registro_calculado])
    for col in df_registro_actualizado.columns:
        if col in campana['registros'].columns:
            campana['registros'].loc[index, col] = df_registro_actualizado.iloc[0][col]
    save_data_to_firestore()
    st.success("Registro de escala actualizado con éxito.")
    st.session_state['editing_record'] = None

def actualizar_registro_testeo(id_oferta, index, nuevo_registro):
    oferta = st.session_state.ofertas[id_oferta]
    registro_calculado = calcular_metricas_diarias(nuevo_registro, oferta['funnel'], oferta.get('comision_pp', 0.0))
    df_registro_actualizado = pd.DataFrame([registro_calculado])
    for col in df_registro_actualizado.columns:
        if col in oferta['testeos'].columns:
            oferta['testeos'].loc[index, col] = df_registro_actualizado.iloc[0][col]
    save_data_to_firestore()
    st.success("Registro actualizado con éxito.")
    st.session_state['editing_record'] = None

def agregar_item_funnel(id_oferta, tipo, nombre, precio):
    oferta = st.session_state.ofertas[id_oferta]
    count = len([k for k in oferta['funnel'] if k.startswith(tipo.lower())]) + 1
    alias_map = {"Bump": "B", "Upsell": "U", "Downsell": "D"}
    alias = f"{alias_map.get(tipo, 'E')}{count}"
    item_id = f"{tipo.lower()}_{count}"
    oferta['funnel'][item_id] = {"nombre": nombre, "precio": precio, "alias": alias, "estado": "🟢 Activo"}
    col_name = get_safe_column_name(alias)
    if col_name not in oferta['testeos'].columns:
        pos = oferta['testeos'].columns.get_loc("Facturación Total")
        oferta['testeos'].insert(pos, col_name, 0)
    if col_name not in [c for camp in oferta['escala'].values() for c in camp['registros'].columns]:
       for camp in oferta['escala'].values():
            if col_name not in camp['registros'].columns:
                camp['registros'].insert(pos, col_name, 0)
    save_data_to_firestore()

def agregar_anuncio_testeo(id_oferta, nombre_anuncio):
    anuncios = st.session_state.ofertas[id_oferta]['anuncios_testeo']
    if nombre_anuncio and not any(d['nombre'] == nombre_anuncio for d in anuncios):
        anuncios.append({"nombre": nombre_anuncio, "estado": "🟢 Activo"})
        save_data_to_firestore()
        st.success(f"Anuncio '{nombre_anuncio}' añadido y activado.")
    elif not nombre_anuncio: st.warning("El nombre del anuncio no puede estar vacío.")
    else: st.warning(f"El anuncio '{nombre_anuncio}' ya existe.")

def toggle_estado_anuncio(id_oferta, nombre_anuncio):
    for ad in st.session_state.ofertas[id_oferta]['anuncios_testeo']:
        if ad['nombre'] == nombre_anuncio:
            ad['estado'] = "🔴 Inactivo" if ad['estado'] == "🟢 Activo" else "🟢 Activo"
            break
    save_data_to_firestore()

def toggle_estado_funnel_item(id_oferta, item_id):
    item = st.session_state.ofertas[id_oferta]['funnel'][item_id]
    item['estado'] = "📁 Archivado" if item['estado'] == "🟢 Activo" else "🟢 Activo"
    save_data_to_firestore()

def toggle_estado_campana_escala(id_oferta, id_campana):
    campana = st.session_state.ofertas[id_oferta]['escala'][id_campana]
    estado_actual = campana.get('estado', '🟢 Activa') 
    campana['estado'] = "🔴 Inactiva" if estado_actual == "🟢 Activa" else "🟢 Activa"
    save_data_to_firestore()

def toggle_estado_componente_escala(id_oferta, id_campana, nombre_componente):
    for comp in st.session_state.ofertas[id_oferta]['escala'][id_campana]['componentes']:
        if comp['nombre'] == nombre_componente:
            comp['estado'] = "🔴 Inactivo" if comp['estado'] == "🟢 Activo" else "🟢 Activo"
            save_data_to_firestore()
            st.success(f"Estado de '{nombre_componente}' actualizado.")
            break

def crear_campana_escala(id_oferta, nombre_campana, anuncio_ganador, estrategia, presupuesto, valor_x=None):
    id_campana = f"escala_{int(time.time())}"
    columnas_escala = ["Fecha", "Componente", "Inversión", "Pagos Iniciados"] + \
                      [get_safe_column_name(v['alias']) for v in st.session_state.ofertas[id_oferta]['funnel'].values()] + \
                      ["Facturación Total", "Ganancia Neta", "ROAS Neto"]
    
    componentes = []
    if estrategia == '1-1-X' and valor_x:
        for i in range(1, valor_x + 1): componentes.append({"nombre": f"[AD {i}] {anuncio_ganador}", "estado": "🟢 Activo"})
    elif estrategia == '1-X-1' and valor_x:
        for i in range(1, valor_x + 1): componentes.append({"nombre": f"Conjunto de Anuncios {i}", "estado": "🟢 Activo"})
    else:
        componentes.append({"nombre": anuncio_ganador, "estado": "🟢 Activo"})

    st.session_state.ofertas[id_oferta]['escala'][id_campana] = {
        "nombre_campana": nombre_campana, "anuncio_base": anuncio_ganador, "estrategia": estrategia,
        "valor_x": valor_x, "presupuesto_diario": presupuesto,
        "registros": pd.DataFrame(columns=columnas_escala), "componentes": componentes,
        "estado": "🟢 Activa"
    }
    save_data_to_firestore()
    st.success(f"¡Campaña de escala '{nombre_campana}' creada con éxito!")
    st.session_state['anuncio_para_escalar'] = None
    st.session_state['accion_de_escala'] = None

def agregar_componente_a_campana(id_oferta, id_campana, nombre_componente):
    if 'componentes' not in st.session_state.ofertas[id_oferta]['escala'][id_campana]:
         st.session_state.ofertas[id_oferta]['escala'][id_campana]['componentes'] = []
    
    st.session_state.ofertas[id_oferta]['escala'][id_campana]['componentes'].append({
        "nombre": nombre_componente, "estado": "🟢 Activo"
    })
    save_data_to_firestore()
    st.success(f"'{nombre_componente}' añadido a la campaña '{st.session_state.ofertas[id_oferta]['escala'][id_campana]['nombre_campana']}'")
    st.session_state['anuncio_para_escalar'] = None
    st.session_state['accion_de_escala'] = None

def agregar_registro_escala(id_oferta, id_campana, nuevo_registro):
    oferta = st.session_state.ofertas[id_oferta]
    campana = oferta['escala'][id_campana]
    
    registro_calculado = calcular_metricas_diarias(nuevo_registro, oferta['funnel'], oferta.get('comision_pp', 0.0))
    df_nuevo = pd.DataFrame([registro_calculado])
    
    campana['registros'] = pd.concat([campana['registros'], df_nuevo], ignore_index=True)
    save_data_to_firestore()
    st.success("Registro de escala guardado con éxito.")


# --- FLUJO PRINCIPAL DE LA APLICACIÓN ---
def main_app():
    with st.sidebar:
        st.title("Panel de Control")
        st.write(f"Conectado como: **{st.session_state.user_email}**")
        
        if st.button("Cerrar Sesión", use_container_width=True, type="secondary"):
            st.session_state.logged_in = False
            st.session_state.user_id = None
            st.session_state.user_email = None
            for key in list(st.session_state.keys()):
                if key not in ['logged_in']:
                    del st.session_state[key]
            st.rerun()

        st.divider()
        
        if st.button("📈 Ofertas y Dashboard", use_container_width=True):
            st.session_state.vista_actual = 'dashboard'
            st.session_state.oferta_seleccionada = None
            st.rerun()
            
        if st.button("🕵️ Bóveda de Ofertas", use_container_width=True):
            st.session_state.vista_actual = 'boveda'
            st.session_state.oferta_seleccionada = None
            st.rerun()

        if st.button("✅ Plantillas de Proyectos", use_container_width=True):
            st.session_state.vista_actual = 'plantillas'
            st.session_state.oferta_seleccionada = None
            st.rerun()

        st.divider()
        st.info("Los datos del equipo se guardan automáticamente en la nube.")

        if st.session_state.vista_actual == 'dashboard':
            with st.expander("➕ CREAR NUEVA OFERTA", expanded=True):
                with st.form("nueva_oferta_form", clear_on_submit=True):
                    nuevo_nombre = st.text_input("Nombre de la Oferta", placeholder="Ej: Curso de Jardinería")
                    nuevo_tipo = st.selectbox("Tipo de Embudo", ["VSL", "TSL", "QUIZ"])
                    nuevo_precio = st.number_input("Precio del Producto Principal ($)", min_value=0.01, format="%.2f")
                    
                    plantillas_disponibles = {"ninguna": "Ninguna"}
                    for pid, pdata in st.session_state.get('plantillas', {}).items():
                        plantillas_disponibles[pid] = pdata['nombre']
                    
                    plantilla_seleccionada = st.selectbox(
                        "Usar Plantilla de Lanzamiento (Opcional)",
                        options=list(plantillas_disponibles.keys()),
                        format_func=lambda x: plantillas_disponibles[x]
                    )

                    if st.form_submit_button("Crear Oferta"):
                        if nuevo_nombre and nuevo_precio:
                            pid_to_pass = None if plantilla_seleccionada == "ninguna" else plantilla_seleccionada
                            crear_nueva_oferta(nuevo_nombre, nuevo_tipo, nuevo_precio, pid_to_pass)
            st.divider()
            st.header("🗂️ Ofertas del Equipo")
            if not st.session_state.get('ofertas'): st.info("Aún no se ha creado ninguna oferta.")
            else:
                filtro = st.radio("Filtrar por:", ["Todas", "🧪 En Testeo", "✅ Validada", "🗄️ Archivada"], horizontal=True)
                for id_oferta, detalles in sorted(st.session_state.ofertas.items()):
                    if filtro == "Todas" or detalles['estado'] == filtro:
                        if st.button(f"{detalles['estado']} {detalles['nombre']}", key=f"btn_{id_oferta}", use_container_width=True):
                            seleccionar_oferta(id_oferta)

    # --- El resto del código de la interfaz de usuario no necesita cambios ---
    # --- ... (código UI pegado sin modificaciones) ... ---
    if st.session_state.vista_actual == 'plantillas':
        st.title("✅ Administrador de Plantillas de Proyectos")
        st.info("Define tus procesos de lanzamiento una vez y reutilízalos para cada nueva oferta.", icon="💡")

        editing_id_plantilla = st.session_state.get('editing_plantilla_id', None)
        plantilla_to_edit = st.session_state.get('plantillas', {}).get(editing_id_plantilla)

        with st.expander("📝 Formulario de Plantilla", expanded=True if editing_id_plantilla else False):
            with st.form("form_plantilla", clear_on_submit=False):
                st.subheader("Crear Nueva Plantilla" if not editing_id_plantilla else f"✏️ Editando: {plantilla_to_edit['nombre']}")
                
                nombre_plantilla = st.text_input("Nombre de la Plantilla", value=plantilla_to_edit['nombre'] if plantilla_to_edit else "")
                
                st.markdown("##### Estructura del Checklist")
                st.caption("Define las fases y tareas. Usa un guion (-) para cada tarea.")
                
                checklist_texto = st.text_area(
                    "Fases y Tareas", 
                    height=300,
                    value=plantilla_to_edit['checklist_raw'] if plantilla_to_edit else "",
                    placeholder="Ejemplo:\n\nFase 1: Investigación\n- Tarea 1.1\n- Tarea 1.2\n\nFase 2: Creación de Activos\n- Tarea 2.1\n- Tarea 2.2"
                )

                btn_col1, btn_col2 = st.columns([3, 1])
                with btn_col1:
                    if st.form_submit_button("💾 Guardar Plantilla" if not editing_id_plantilla else "💾 Actualizar Plantilla", use_container_width=True):
                        if nombre_plantilla and checklist_texto:
                            if editing_id_plantilla:
                                update_plantilla(editing_id_plantilla, nombre_plantilla, checklist_texto)
                                st.session_state.editing_plantilla_id = None
                                st.rerun()
                            else:
                                id_plantilla = f"plantilla_{nombre_plantilla.replace(' ', '_').lower()}"
                                if id_plantilla in st.session_state.get('plantillas', {}):
                                    st.error("Ya existe una plantilla con un nombre similar.")
                                else:
                                    st.session_state.plantillas[id_plantilla] = {
                                        "nombre": nombre_plantilla,
                                        "checklist_raw": checklist_texto
                                    }
                                    save_data_to_firestore()
                                    st.success(f"¡Plantilla '{nombre_plantilla}' guardada con éxito!")
                        else:
                            st.warning("El nombre y el checklist no pueden estar vacíos.")
                
                with btn_col2:
                    if editing_id_plantilla:
                        if st.form_submit_button("❌ Cancelar", use_container_width=True, type="secondary"):
                            st.session_state.editing_plantilla_id = None
                            st.rerun()
        st.divider()
        st.header("🗂️ Plantillas Guardadas")
        if not st.session_state.get('plantillas'):
            st.info("Aún no has creado ninguna plantilla.")
        else:
            for id_plantilla, detalles in st.session_state.plantillas.items():
                with st.expander(f"**{detalles['nombre']}**"):
                    st.text(detalles['checklist_raw'])
                    
                    col1, col2 = st.columns(2)
                    if col1.button("✏️ Editar", key=f"edit_{id_plantilla}", use_container_width=True):
                        st.session_state.editing_plantilla_id = id_plantilla
                        st.rerun()
                    if col2.button("🗑️ Eliminar", key=f"del_{id_plantilla}", use_container_width=True):
                        del st.session_state.plantillas[id_plantilla]
                        save_data_to_firestore()
                        st.rerun()

    elif st.session_state.vista_actual == 'boveda':
        st.title("🕵️ Bóveda de Inteligencia de Mercado")
        st.info("Registra y analiza ofertas del mercado para tomar mejores decisiones estratégicas.", icon="💡")
        
        for entrada in st.session_state.boveda:
            if 'estatus' not in entrada:
                entrada['estatus'] = '💡 Idea'

        editing_id = st.session_state.get('editing_boveda_id', None)
        entry_to_edit = next((item for item in st.session_state.boveda if item['id'] == editing_id), None) if editing_id else None

        with st.expander("📝 Formulario de Oferta", expanded=True if editing_id else False):
            with st.form("form_boveda", clear_on_submit=False):
                st.subheader("Captura Rápida de Oferta" if not editing_id else "✏️ Editando Oferta")
                
                c1, c2 = st.columns(2)
                nombre = c1.text_input("**Nombre de la Oferta***", value=entry_to_edit['nombre'] if entry_to_edit else "")
                tipos = ["VSL", "QUIZ", "TSL"]
                tipo_oferta = c2.selectbox("**Tipo de Oferta***", tipos, index=tipos.index(entry_to_edit['tipo_oferta']) if entry_to_edit and entry_to_edit['tipo_oferta'] in tipos else 0)

                link_anuncios = st.text_input("Link a Biblioteca de Anuncios", value=entry_to_edit['link_anuncios'] if entry_to_edit else "")
                link_oferta = st.text_input("Link a la Oferta (Página de Venta)", value=entry_to_edit['link_oferta'] if entry_to_edit else "")

                c3, c4, c5 = st.columns(3)
                nicho = c3.text_input("Nicho", value=entry_to_edit['nicho'] if entry_to_edit else "")
                idioma = c4.text_input("Idioma", value=entry_to_edit['idioma'] if entry_to_edit else "Português")
                num_anuncios = c5.number_input("Nº Anuncios Activos", min_value=0, step=1, value=entry_to_edit['num_anuncios'] if entry_to_edit else 0)
                
                c6, c7 = st.columns(2)
                calificacion = c6.slider("Calificación (1-5)", 1, 5, value=entry_to_edit['calificacion'] if entry_to_edit else 3)
                testear_opts = ["Sí", "No", "Indeciso"]
                testear = c7.radio("¿Vale la pena testear?", testear_opts, index=testear_opts.index(entry_to_edit['testear']) if entry_to_edit and entry_to_edit['testear'] in testear_opts else 2, horizontal=True)
                
                comentarios = st.text_area("Comentarios y Observaciones", value=entry_to_edit['comentarios'] if entry_to_edit else "")

                form_btn1, form_btn2 = st.columns([3,1])
                with form_btn1:
                    if st.form_submit_button("💾 Guardar en la Bóveda" if not editing_id else "💾 Actualizar Cambios", use_container_width=True):
                        if nombre and tipo_oferta:
                            datos_entrada = {
                                "id": editing_id or f"boveda_{int(time.time())}",
                                "nombre": nombre, "tipo_oferta": tipo_oferta,
                                "link_anuncios": link_anuncios, "link_oferta": link_oferta,
                                "nicho": nicho, "idioma": idioma, "num_anuncios": num_anuncios,
                                "calificacion": calificacion, "testear": testear, "comentarios": comentarios,
                                "fecha_registro": entry_to_edit['fecha_registro'] if entry_to_edit else datetime.date.today().strftime("%Y-%m-%d"),
                                "estatus": entry_to_edit['estatus'] if entry_to_edit else '💡 Idea'
                            }
                            if editing_id:
                                update_entrada_boveda(editing_id, datos_entrada)
                                st.session_state.editing_boveda_id = None
                            else:
                                st.session_state.boveda.insert(0, datos_entrada)
                                save_data_to_firestore()
                                st.success(f"¡Oferta '{nombre}' guardada!")
                            st.rerun()
                        else:
                            st.warning("El Nombre y el Tipo de Oferta son obligatorios.")
                with form_btn2:
                    if editing_id:
                        if st.form_submit_button("❌ Cancelar", use_container_width=True, type="secondary"):
                            st.session_state.editing_boveda_id = None
                            st.rerun()
        st.divider()
        st.header("📚 Tablero de Inteligencia")
        
        st.markdown("##### Filtros del Tablero")
        f_col1, f_col2, f_col3 = st.columns([2, 2, 1])
        with f_col1:
            tipos_unicos = sorted(list(set(o['tipo_oferta'] for o in st.session_state.boveda if 'tipo_oferta' in o)))
            filtro_tipo = st.multiselect("Filtrar por Tipo:", options=tipos_unicos, placeholder="Todos los tipos")
        with f_col2:
            estatus_unicos = ['💡 Idea', '⚙️ Modelando', '🧪 En Pruebas', '🗄️ Archivada']
            filtro_estatus = st.multiselect("Filtrar por Estatus:", options=estatus_unicos, placeholder="Todos los estatus")
        with f_col3:
            st.write("") 
            ocultar_archivadas = st.checkbox("Ocultar archivadas", value=True)
        view_options = ['🖼️ Tarjetas', '📋 Tabla']
        st.session_state.boveda_view_mode = st.radio("Ver como:", view_options, horizontal=True, key="boveda_view_selector")
        ofertas_a_mostrar = st.session_state.boveda
        if ocultar_archivadas:
            ofertas_a_mostrar = [o for o in ofertas_a_mostrar if o.get('estatus') != '🗄️ Archivada']
        if filtro_tipo:
            ofertas_a_mostrar = [o for o in ofertas_a_mostrar if o.get('tipo_oferta') in filtro_tipo]
        if filtro_estatus:
            ofertas_a_mostrar = [o for o in ofertas_a_mostrar if o.get('estatus') in filtro_estatus]
        if not ofertas_a_mostrar:
            st.info("Tu bóveda está vacía o ninguna oferta coincide con los filtros actuales.")
        else:
            tipo_map = { "VSL": {"color": "#007bff", "emoji": "🟦"}, "QUIZ": {"color": "#6f42c1", "emoji": "🟪"}, "TSL": {"color": "#28a745", "emoji": "🟩"} }
            
            if st.session_state.boveda_view_mode == '🖼️ Tarjetas':
                num_columnas = 3
                columnas = st.columns(num_columnas)
                for index, entrada in enumerate(ofertas_a_mostrar):
                    columna_actual = columnas[index % num_columnas]
                    with columna_actual:
                        tipo_info = tipo_map.get(entrada['tipo_oferta'], {"color": "#6c757d", "emoji": "❔"})
                        with st.container():
                            estatus_actual = entrada.get('estatus', '💡 Idea')
                            extra_class = ''
                            if estatus_actual == '⚙️ Modelando':
                                extra_class = 'glow-modelando'
                            elif estatus_actual == '🧪 En Pruebas':
                                extra_class = 'glow-pruebas'
                            elif estatus_actual == '🗄️ Archivada':
                                extra_class = 'fade-archivada'

                            st.markdown(f"""
                            <div class="card-boveda {extra_class}" style="border-left: 5px solid {tipo_info['color']};">
                                <h4 style="color: black;">{entrada['nombre']} <span style="font-size: 1.2em;">{render_rating_stars(entrada['calificacion'])}</span></h4>
                                <p class="fecha-registro">Registrado el: {entrada['fecha_registro']}</p>
                                <hr>
                                <div class="card-body">
                                    <div class="card-col">
                                        <p><strong>{tipo_info['emoji']} Tipo:</strong> {entrada['tipo_oferta']}</p>
                                        <p><strong>🎯 Nicho:</strong> {entrada.get('nicho', 'N/A')}</p>
                                        <p><strong>🌎 Idioma:</strong> {entrada.get('idioma', 'N/A')}</p>
                                    </div>
                                    <div class="card-col">
                                        <p><strong>📊 Ads Activos:</strong> {entrada.get('num_anuncios', 'N/A')}</p>
                                        <p><strong>💡 ¿Testear?:</strong> {entrada.get('testear', 'N/A')}</p>
                                    </div>
                                </div>
                                <div class="card-footer">
                                    <strong>🔗 Links:</strong>
                                    {'<a href="' + entrada["link_anuncios"] + '" target="_blank">Ver Anuncios</a>' if entrada.get("link_anuncios") else ''}
                                    {' | <a href="' + entrada["link_oferta"] + '" target="_blank">Ver Oferta</a>' if entrada.get("link_oferta") else ''}
                                    <details>
                                        <summary>Ver Comentarios</summary>
                                        <p class="comentarios">
                                            {entrada.get('comentarios') if entrada.get('comentarios') else 'Sin comentarios.'}
                                        </p>
                                    </details>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            estatus_opciones = ['💡 Idea', '⚙️ Modelando', '🧪 En Pruebas', '🗄️ Archivada']
                            indice_actual = estatus_opciones.index(estatus_actual) if estatus_actual in estatus_opciones else 0
                            key = f"status_select_{entrada['id']}"
                            
                            st.selectbox(
                                "Estatus:",
                                options=estatus_opciones,
                                index=indice_actual,
                                key=key,
                                on_change=update_boveda_status,
                                args=(entrada['id'], key)
                            )
                            
                            btn_col1, btn_col2 = st.columns(2)
                            if btn_col1.button("✏️ Editar", key=f"edit_boveda_{entrada['id']}", use_container_width=True):
                                st.session_state.editing_boveda_id = entrada['id']
                                st.rerun()
                            if btn_col2.button("🗑️ Eliminar", key=f"del_boveda_{entrada['id']}", use_container_width=True):
                                eliminar_entrada_boveda(entrada['id'])
                                st.rerun()

            elif st.session_state.boveda_view_mode == '📋 Tabla':
                df_boveda = pd.DataFrame(ofertas_a_mostrar)
                if not df_boveda.empty:
                    df_display = df_boveda[['nombre', 'tipo_oferta', 'nicho', 'estatus', 'calificacion', 'testear', 'fecha_registro']].copy()
                    df_display.rename(columns={'nombre': 'Nombre', 'tipo_oferta': 'Tipo', 'nicho': 'Nicho', 'estatus': 'Estatus', 'calificacion': 'Calificación', 'testear': 'Testear?', 'fecha_registro': 'Fecha'}, inplace=True)
                    st.dataframe(df_display, use_container_width=True)
                else:
                    st.info("No hay datos para mostrar en la tabla con los filtros actuales.")
                st.markdown("---")
                st.subheader("Acciones en Tabla")
                if not st.session_state.boveda:
                    st.info("No hay ofertas para seleccionar.")
                else:
                    nombres_ofertas = {entry['id']: entry['nombre'] for entry in st.session_state.boveda}
                    id_seleccionado = st.selectbox("Selecciona una oferta para realizar una acción", options=nombres_ofertas.keys(), format_func=lambda x: nombres_ofertas[x])
                    
                    if id_seleccionado:
                        action_col1, action_col2 = st.columns(2)
                        if action_col1.button("✏️ Editar Oferta Seleccionada", use_container_width=True):
                            st.session_state.editing_boveda_id = id_seleccionado
                            st.rerun()
                        if action_col2.button("🗑️ Eliminar Oferta Seleccionada", use_container_width=True):
                            eliminar_entrada_boveda(id_seleccionado)
                            st.rerun()
        st.markdown("""
        <style>
            .card-boveda { border-radius: 5px; padding: 15px; margin-bottom: 10px; background-color: #f8f9fa; color: #333; height: 100%; display: flex; flex-direction: column; justify-content: space-between; transition: all 0.3s ease-in-out;}
            .card-boveda:hover { transform: translateY(-5px); box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
            .glow-modelando { box-shadow: 0 0 25px 8px #ffc107aa; }
            .glow-pruebas { box-shadow: 0 0 25px 8px #28a745aa; }
            .fade-archivada { opacity: 0.6; }
            .fecha-registro { margin: 0; font-size: 0.9em; color: #6c757d; }
            .card-body { display: flex; justify-content: space-between; }
            .card-col { flex: 1; }
            .card-col:first-child { padding-right: 5px; }
            .card-col:last-child { padding-left: 5px; }
            .card-footer { margin-top: 15px; }
            .card-footer details summary { cursor: pointer; font-size: 0.9em; margin-top: 10px; }
            .comentarios { background-color: #e9ecef; padding: 10px; border-radius: 5px; margin-top: 5px; color: black; }
        </style>
        """, unsafe_allow_html=True)

    elif not st.session_state.oferta_seleccionada:
        st.title("📈 Dashboard Principal del Equipo")
        st.markdown("Visión general del rendimiento de todas las ofertas activas.")
        if not st.session_state.ofertas:
            st.info("Crea la primera oferta en la barra lateral para empezar a ver datos aquí.")
        else:
            all_dfs = []
            for id_oferta, oferta_data in st.session_state.ofertas.items():
                if oferta_data['estado'] in ["🧪 En Testeo", "✅ Validada"]:
                    df_testeo = oferta_data['testeos'].copy()
                    if not df_testeo.empty:
                        df_testeo['Oferta'] = oferta_data['nombre']
                        df_testeo['Comision PP'] = oferta_data.get('comision_pp', 0.0)
                        all_dfs.append(df_testeo)
                    
                    for camp_id, camp_data in oferta_data.get('escala', {}).items():
                        df_escala = camp_data['registros'].copy()
                        if not df_escala.empty:
                            df_escala['Oferta'] = oferta_data['nombre']
                            df_escala['Comision PP'] = oferta_data.get('comision_pp', 0.0)
                            all_dfs.append(df_escala)
            
            if not all_dfs:
                st.warning("No hay datos registrados en ninguna de las ofertas activas.")
            else:
                df_global = pd.concat(all_dfs, ignore_index=True)
                df_global['Fecha'] = pd.to_datetime(df_global['Fecha'])

                st.divider()
                col1, col2 = st.columns(2)
                min_date = df_global['Fecha'].min().date()
                max_date = df_global['Fecha'].max().date()
                start_date_global = col1.date_input("Fecha de Inicio", min_date, min_value=min_date, max_value=max_date, key="global_start")
                end_date_global = col2.date_input("Fecha de Fin", max_date, min_value=min_date, max_value=max_date, key="global_end")
                df_filtrado = df_global[(df_global['Fecha'].dt.date >= start_date_global) & (df_global['Fecha'].dt.date <= end_date_global)]

                if df_filtrado.empty:
                    st.warning("No hay datos en el rango de fechas seleccionado.")
                else:
                    total_inversion = df_filtrado['Inversión'].sum()
                    total_facturacion_bruta = df_filtrado['Facturación Total'].sum()
                    
                    ventas_pp_col = get_safe_column_name("PP")
                    df_filtrado['Comisiones'] = df_filtrado.apply(lambda row: row.get(ventas_pp_col, 0) * row.get('Comision PP', 0), axis=1)
                    total_comisiones = df_filtrado['Comisiones'].sum()
                    total_ganancia_neta = total_facturacion_bruta - total_inversion - total_comisiones
                    roas_neto_global = (total_facturacion_bruta - total_comisiones) / total_inversion if total_inversion > 0 else 0

                    st.divider()
                    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
                    kpi1.metric("💵 Inversión Total", f"${total_inversion:,.2f}")
                    kpi2.metric("📈 Facturación Bruta Total", f"${total_facturacion_bruta:,.2f}")
                    with kpi3:
                        st.write("💰 Ganancia Neta Total")
                        ganancia_color = "green" if total_ganancia_neta >= 0 else "red"
                        st.markdown(f"<p style='font-size: 1.75rem; font-weight: 600; color: {ganancia_color};'>${total_ganancia_neta:,.2f}</p>", unsafe_allow_html=True)
                    kpi4.metric("🎯 ROAS Neto General", f"{roas_neto_global:.2f}")

                    st.divider()
                    st.subheader("Desglose de Rendimiento por Oferta")
                    
                    df_por_oferta = df_filtrado.groupby('Oferta').agg({'Inversión': 'sum', 'Facturación Total': 'sum', 'Ganancia Neta': 'sum'}).reset_index()
                    st.dataframe(df_por_oferta.style.format({'Inversión': "${:,.2f}", 'Facturación Total': "${:,.2f}", 'Ganancia Neta': "${:,.2f}"}), use_container_width=True)
                    st.bar_chart(df_por_oferta.set_index('Oferta'), y='Ganancia Neta')
                    st.divider()
                    with st.expander("📅 Análisis de Rendimiento por Día de la Semana"):
                        st.markdown("Descubre qué días son los más rentables para tu operación en el período seleccionado.")
                        df_analisis_dia = df_filtrado.copy()
                        df_analisis_dia['Día de la Semana'] = df_analisis_dia['Fecha'].dt.day_name().str.capitalize()
                        dias_ordenados = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                        df_analisis_dia['Día de la Semana'] = pd.Categorical(df_analisis_dia['Día de la Semana'], categories=dias_ordenados, ordered=True)
                        df_por_dia = df_analisis_dia.groupby('Día de la Semana', observed=False).agg({'Inversión': 'sum', 'Ganancia Neta': 'sum'}).reset_index()
                        st.subheader("Ganancia Neta por Día")
                        st.bar_chart(df_por_dia.set_index('Día de la Semana'), y='Ganancia Neta')
                        st.subheader("Datos Agregados por Día")
                        st.dataframe(df_por_dia.style.format({'Inversión': "${:,.2f}", 'Ganancia Neta': "${:,.2f}"}).apply(lambda row: ['background-color: #e6ffed; color: black;' if row['Ganancia Neta'] > 0 else 'background-color: #ffe6e6; color: black;' for i in row], axis=1), use_container_width=True)

    elif st.session_state.get('editing_record') is not None:
        id_actual = st.session_state.oferta_seleccionada
        oferta_actual = st.session_state.ofertas[id_actual]
        editing_info = st.session_state.editing_record
        is_test_record = editing_info.get('type') == 'testeo'
        
        try:
            if is_test_record:
                record_index = editing_info['index']
                registro_a_editar = oferta_actual['testeos'].loc[record_index].to_dict()
                componente_nombre = registro_a_editar['Anuncio']
            else:
                id_campana = editing_info['campaign_id']
                record_index = editing_info['index']
                registro_a_editar = oferta_actual['escala'][id_campana]['registros'].loc[record_index].to_dict()
                componente_nombre = registro_a_editar['Componente']

            st.header(f"✏️ Editando Registro ({'Testeo' if is_test_record else 'Escala'})")
            st.info(f"Fecha: {registro_a_editar['Fecha'].strftime('%Y-%m-%d')} | {'Anuncio' if is_test_record else 'Componente'}: {componente_nombre}")

            with st.form("form_edit_record", clear_on_submit=False):
                nuevo_inversion = st.number_input("Inversión ($)", value=registro_a_editar.get('Inversión', 0.0), format="%.2f")
                st.markdown("**Resultados de Ventas**")
                funnel_activo = {k: v for k, v in oferta_actual['funnel'].items() if v['estado'] == "🟢 Activo"}
                form_cols = st.columns(len(funnel_activo) + 1)
                nuevas_ventas = {}
                nuevas_ventas['Pagos Iniciados'] = form_cols[0].number_input("Pagos Iniciados", value=int(registro_a_editar.get('Pagos Iniciados', 0)), min_value=0, step=1)
                i = 1
                for item_details in funnel_activo.values():
                    col_name = get_safe_column_name(item_details['alias'])
                    nuevas_ventas[col_name] = form_cols[i].number_input(item_details['alias'], value=int(registro_a_editar.get(col_name, 0)), min_value=0, step=1, key=f"edit_venta_{item_details['alias']}")
                    i += 1
                col_submit, col_cancel = st.columns(2)
                if col_submit.form_submit_button("💾 Guardar Cambios", use_container_width=True):
                    registro_actualizado = {"Fecha": registro_a_editar['Fecha'], "Inversión": nuevo_inversion, **nuevas_ventas}
                    if is_test_record:
                        registro_actualizado["Anuncio"] = componente_nombre
                        actualizar_registro_testeo(id_actual, record_index, registro_actualizado)
                    else:
                        registro_actualizado["Componente"] = componente_nombre
                        actualizar_registro_escala(id_actual, id_campana, record_index, registro_actualizado)
                    st.rerun()
                if col_cancel.form_submit_button("❌ Cancelar", type="secondary", use_container_width=True):
                    st.session_state['editing_record'] = None
                    st.rerun()
        except (IndexError, KeyError):
            st.error("El registro que intentas editar ya no existe. Volviendo al panel...")
            st.session_state['editing_record'] = None
            time.sleep(2)
            st.rerun()

    elif st.session_state.get('anuncio_para_escalar'):
        id_actual = st.session_state.oferta_seleccionada
        st.header(f"🚀 Módulo de Lanzamiento de Escala")
        st.subheader(f"Anuncio Ganador: {st.session_state['anuncio_para_escalar']}")

        if st.session_state['accion_de_escala'] == 'crear_nueva':
            estrategia = st.selectbox("1. Elige la Estrategia de Escala", ['1-1-1', '1-1-X', '1-X-1', 'Personalizada'], key="estrategia_escala_selector")
            with st.form("form_crear_campana_escala", clear_on_submit=True):
                st.markdown("#### 2. Define los Detalles de tu Campaña")
                nombre_campana = st.text_input("Nombre de la Campaña de Escala", placeholder="Ej: ESC - CBO - Intereses Abiertos")
                valor_x = None
                if estrategia in ['1-1-X', '1-X-1']:
                    st.info("👇 Has elegido una estrategia de duplicación. Por favor, define el número de copias.")
                    valor_x = st.number_input(f"¿Cuántos {'anuncios' if '1-1-X' in estrategia else 'conjuntos'} quieres crear? (Valor de X)", min_value=1, step=1, placeholder="Ej: 3", key="valor_x_escala")
                presupuesto = st.number_input("Presupuesto Diario Total ($)", min_value=1.0, format="%.2f")
                submitted = st.form_submit_button("🚀 Lanzar Campaña", use_container_width=True)
                if submitted:
                    form_valid = True
                    if not nombre_campana or not presupuesto:
                        st.warning("Por favor, completa el nombre de la campaña y el presupuesto."); form_valid = False
                    if estrategia in ['1-1-X', '1-X-1'] and (valor_x is None or valor_x < 1):
                        st.error(f"Para la estrategia '{estrategia}', debes introducir un número de duplicados (Valor de X)."); form_valid = False
                    if form_valid:
                        crear_campana_escala(id_actual, nombre_campana, st.session_state['anuncio_para_escalar'], estrategia, presupuesto, valor_x); st.rerun()
        elif st.session_state['accion_de_escala'] == 'añadir_existente':
            campanas_existentes = st.session_state.ofertas[id_actual].get('escala', {})
            if not campanas_existentes:
                st.warning("No tienes ninguna campaña de escala activa para esta oferta. Por favor, crea una primero.")
            else:
                with st.form("form_add_a_campana", clear_on_submit=True):
                    st.markdown("#### Añadir a una Campaña Existente")
                    opciones_campana = {cid: cdetails['nombre_campana'] for cid, cdetails in campanas_existentes.items()}
                    id_campana_sel = st.selectbox("Selecciona la campaña de destino", options=list(opciones_campana.keys()), format_func=lambda x: opciones_campana[x])
                    nombre_componente = st.text_input("Nombre del nuevo Conjunto de Anuncios / Anuncio", placeholder="Ej: Adset Interes Z - Test Creativo C")
                    if st.form_submit_button("📥 Añadir a Campaña", use_container_width=True):
                        if id_campana_sel and nombre_componente:
                            agregar_componente_a_campana(id_actual, id_campana_sel, nombre_componente); st.rerun()
                        else: st.warning("Por favor, selecciona una campaña y asigna un nombre al nuevo componente.")
        
        if st.button("⬅️ Volver al Panel"):
            st.session_state['anuncio_para_escalar'] = None
            st.session_state['accion_de_escala'] = None
            st.rerun()
    else:
        id_actual = st.session_state.oferta_seleccionada
        oferta_actual = st.session_state.ofertas[id_actual]
        df_testeos_global = oferta_actual['testeos'].copy()
        if not df_testeos_global.empty:
            df_testeos_global['Fecha'] = pd.to_datetime(df_testeos_global['Fecha'])
        st.header(f"Laboratorio de Oferta: {oferta_actual['nombre']} | {oferta_actual.get('tipo_embudo', 'N/A')}")
        comision_pp = oferta_actual.get('comision_pp', 0.0)
        inversion_testeo = df_testeos_global['Inversión'].sum()
        facturacion_bruta_testeo = df_testeos_global['Facturación Total'].sum()
        ventas_pp_testeo = df_testeos_global[get_safe_column_name("PP")].sum() if get_safe_column_name("PP") in df_testeos_global else 0
        comisiones_testeo = ventas_pp_testeo * comision_pp
        ganancia_neta_testeo = facturacion_bruta_testeo - inversion_testeo - comisiones_testeo
        roas_neto_testeo = (facturacion_bruta_testeo - comisiones_testeo) / inversion_testeo if inversion_testeo > 0 else 0
        inversion_escala = 0
        facturacion_bruta_escala = 0
        ventas_pp_escala = 0
        todos_los_registros_escala = []
        campanas_escala_global = oferta_actual.get('escala', {})
        for campana_details in campanas_escala_global.values():
            if not campana_details['registros'].empty:
                registros_campana = campana_details['registros']
                inversion_escala += registros_campana['Inversión'].sum()
                facturacion_bruta_escala += registros_campana['Facturación Total'].sum()
                ventas_pp_escala += registros_campana[get_safe_column_name("PP")].sum() if get_safe_column_name("PP") in registros_campana else 0
                todos_los_registros_escala.append(registros_campana)
        comisiones_escala = ventas_pp_escala * comision_pp
        ganancia_neta_escala = facturacion_bruta_escala - inversion_escala - comisiones_escala
        roas_neto_escala = (facturacion_bruta_escala - comisiones_escala) / inversion_escala if inversion_escala > 0 else 0
        total_inversion_global = inversion_testeo + inversion_escala
        total_facturacion_bruta_global = facturacion_bruta_testeo + facturacion_bruta_escala
        total_ganancia_neta_global = ganancia_neta_testeo + ganancia_neta_escala
        roas_neto_global = (total_facturacion_bruta_global - (comisiones_testeo + comisiones_escala)) / total_inversion_global if total_inversion_global > 0 else 0
        dias_transcurridos_testeo = (df_testeos_global['Fecha'].max() - df_testeos_global['Fecha'].min()).days + 1 if not df_testeos_global.empty else 0
        dias_transcurridos_escala = 0
        if todos_los_registros_escala:
            df_escala_completo = pd.concat(todos_los_registros_escala, ignore_index=True)
            if not df_escala_completo.empty:
                df_escala_completo['Fecha'] = pd.to_datetime(df_escala_completo['Fecha'])
                dias_transcurridos_escala = (df_escala_completo['Fecha'].max() - df_escala_completo['Fecha'].min()).days + 1
        c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
        c1.metric("🗓️ Días (Testeo)", f"{dias_transcurridos_testeo}")
        if dias_transcurridos_escala > 0: c2.metric("🗓️ Días (Escala)", f"{dias_transcurridos_escala}")
        c3.metric("💵 Inversión Total", f"${total_inversion_global:,.2f}")
        c4.metric("📈 Facturación Bruta", f"${total_facturacion_bruta_global:,.2f}")
        with c5:
            st.write("💰 Ganancia Neta"); ganancia_color = "green" if total_ganancia_neta_global >= 0 else "red"
            st.markdown(f"<p style='font-size: 1.75rem; font-weight: 600; color: {ganancia_color};'>${total_ganancia_neta_global:,.2f}</p>", unsafe_allow_html=True)
        c6.metric("🎯 ROAS Neto", f"{roas_neto_global:.2f}")
        c7.metric("📊 Estado", oferta_actual['estado'])
        st.divider()

        tab_resumen, tab_lanzamiento, tab_funnel, tab_campanas = st.tabs(["📊 Resumen", "✅ Lanzamiento", "🛠️ Funnel", "⚔️ Campañas"])
        with tab_resumen:
            st.subheader("Análisis General de la Oferta")
            with st.container(border=True):
                col_test, col_escala = st.columns(2)
                with col_test:
                    st.subheader("🧪 Fase de Testeo")
                    st.metric("💵 Inversión", f"${inversion_testeo:,.2f}")
                    st.metric("📈 Facturación Bruta", f"${facturacion_bruta_testeo:,.2f}")
                    ganancia_color_test = "green" if ganancia_neta_testeo >= 0 else "red"
                    st.markdown(f"**💰 Ganancia Neta**")
                    st.markdown(f"<p style='font-size: 1.5rem; font-weight: 600; color: {ganancia_color_test};'>${ganancia_neta_testeo:,.2f}</p>", unsafe_allow_html=True)
                    st.metric("🎯 ROAS Neto", f"{roas_neto_testeo:.2f}")
                with col_escala:
                    st.subheader("🚀 Fase de Escala")
                    st.metric("💵 Inversión", f"${inversion_escala:,.2f}")
                    st.metric("📈 Facturación Bruta", f"${facturacion_bruta_escala:,.2f}")
                    ganancia_color_escala = "green" if ganancia_neta_escala >= 0 else "red"
                    st.markdown(f"**💰 Ganancia Neta**")
                    st.markdown(f"<p style='font-size: 1.5rem; font-weight: 600; color: {ganancia_color_escala};'>${ganancia_neta_escala:,.2f}</p>", unsafe_allow_html=True)
                    st.metric("🎯 ROAS Neto", f"{roas_neto_escala:.2f}")
            st.subheader("Configuración de la Oferta")
            with st.container(border=True):
                sub_tab_estado, sub_tab_config = st.tabs(["Estado de la Oferta", "Configuración Financiera"])
                with sub_tab_estado:
                    num_anuncios_testeados = len(oferta_actual.get('anuncios_testeo', []))
                    if num_anuncios_testeados > 10 and roas_neto_global > 1.2 and oferta_actual['estado'] == '🧪 En Testeo':
                        st.success(f"🎉 **Sugerencia:** ¡Esta oferta cumple los criterios para ser validada! Se han testeado {num_anuncios_testeados} anuncios y el ROAS neto global es de {roas_neto_global:.2f}.")
                    col1, col2 = st.columns([2,1])
                    with col1:
                        estados_posibles = ['🧪 En Testeo', '✅ Validada', '🗄️ Archivada']
                        estado_actual_idx = estados_posibles.index(oferta_actual['estado']) if oferta_actual['estado'] in estados_posibles else 0
                        nuevo_estado = st.selectbox("Cambiar estado de la oferta:", options=estados_posibles, index=estado_actual_idx)
                    if col2.button("Actualizar Estado", use_container_width=True):
                        if nuevo_estado != oferta_actual['estado']:
                            cambiar_estado_oferta(id_actual, nuevo_estado); st.rerun()
                    st.divider()
                    st.markdown("##### ⚠️ Zona de Peligro")
                    if st.session_state.get('offer_to_delete') != id_actual:
                        if st.button("🗑️ Eliminar Oferta Permanentemente", type="secondary", use_container_width=True):
                            st.session_state.offer_to_delete = id_actual
                            st.rerun()
                    else:
                        st.warning(f"**¿Estás seguro?** Esta acción no se puede deshacer. Se borrará toda la información de la oferta **'{oferta_actual['nombre']}'**.", icon="🚨")
                        col_confirm, col_cancel = st.columns(2)
                        with col_confirm:
                            if st.button("🔴 Sí, eliminar ahora", use_container_width=True):
                                eliminar_oferta(id_actual)
                                st.rerun()
                        with col_cancel:
                            if st.button("✅ No, cancelar", use_container_width=True):
                                st.session_state.offer_to_delete = None
                                st.rerun()
                with sub_tab_config:
                    with st.form("config_financiera_form"):
                        st.markdown("##### Define tus costos y objetivos")
                        c1, c2, c3 = st.columns(3)
                        comision_actual = oferta_actual.get('comision_pp', 0.0)
                        cpa_actual = oferta_actual.get('cpa_objetivo', 0.0)
                        nueva_comision = c1.number_input("Comisión por Venta PP ($)", value=comision_actual, min_value=0.0, format="%.2f")
                        nuevo_cpa = c2.number_input("CPA Objetivo ($)", value=cpa_actual, min_value=0.0, format="%.2f")
                        precio_pp = oferta_actual['funnel']['principal']['precio']
                        ganancia_neta_pp = precio_pp - nueva_comision
                        roas_be = precio_pp / ganancia_neta_pp if ganancia_neta_pp > 0 else 0
                        with c3:
                            st.write("ROAS de Break-Even")
                            st.markdown(f"<p style='font-size: 1.75rem; font-weight: 600; color: #666;'>{roas_be:.2f}</p>", unsafe_allow_html=True)
                        if st.form_submit_button("Guardar Configuración", use_container_width=True):
                            actualizar_configuracion_financiera(id_actual, nueva_comision, nuevo_cpa)
                            st.rerun()
        with tab_lanzamiento:
            editing_checklist = st.session_state.get('editing_checklist_oferta_id') == id_actual
            checklist_data = oferta_actual.get('checklist')

            if not checklist_data:
                st.subheader("Asignar un Plan de Lanzamiento")
                st.info("Esta oferta aún no tiene un checklist. Selecciona una de tus plantillas guardadas para asignarle un plan de acción.")
                
                plantillas_disponibles = {"ninguna": "Selecciona una plantilla..."}
                for pid, pdata in st.session_state.plantillas.items():
                    plantillas_disponibles[pid] = pdata['nombre']
                
                if len(plantillas_disponibles) > 1:
                    plantilla_a_asignar = st.selectbox("Plantillas Disponibles", options=list(plantillas_disponibles.keys()), format_func=lambda x: plantillas_disponibles[x])
                    if st.button("Asignar Plantilla a esta Oferta", use_container_width=True, type="primary"):
                        if plantilla_a_asignar != "ninguna":
                            plantilla = st.session_state.plantillas[plantilla_a_asignar]
                            st.session_state.ofertas[id_actual]['checklist'] = {
                                "plantilla_nombre": plantilla['nombre'],
                                "tareas": parse_checklist(plantilla['checklist_raw'])
                            }
                            save_data_to_firestore()
                            st.success("¡Checklist asignado con éxito!")
                            st.rerun()
                        else:
                            st.warning("Por favor, selecciona una plantilla válida.")
                else:
                    st.warning("No has creado ninguna plantilla de lanzamiento. Ve a '✅ Plantillas de Proyectos' en la barra lateral para crear tu primera plantilla.")
            
            elif editing_checklist:
                st.subheader(f"✏️ Editando Checklist: {checklist_data['plantilla_nombre']}")
                with st.form("form_edit_checklist"):
                    current_raw_text = unparse_checklist(checklist_data['tareas'])
                    new_raw_text = st.text_area("Fases y Tareas", value=current_raw_text, height=300)
                    edit_btn_col1, edit_btn_col2 = st.columns(2)
                    if edit_btn_col1.form_submit_button("💾 Guardar Cambios", use_container_width=True):
                        new_tasks = merge_checklists(checklist_data['tareas'], new_raw_text)
                        st.session_state.ofertas[id_actual]['checklist']['tareas'] = new_tasks
                        save_data_to_firestore()
                        st.session_state.editing_checklist_oferta_id = None
                        st.success("Checklist actualizado.")
                        st.rerun()
                    if edit_btn_col2.form_submit_button("❌ Cancelar", use_container_width=True, type="secondary"):
                        st.session_state.editing_checklist_oferta_id = None
                        st.rerun()
            else: # Vista normal del checklist
                st.subheader(f"Progreso del Lanzamiento: {checklist_data['plantilla_nombre']}")
                tareas = checklist_data.get('tareas', [])
                total_tasks = sum(1 for item in tareas if item['type'] == 'task')
                completed_tasks = sum(1 for item in tareas if item['type'] == 'task' and item['completed'])
                progress = (completed_tasks / total_tasks) if total_tasks > 0 else 0
                st.progress(progress)
                st.metric("Progreso Total", f"{completed_tasks} / {total_tasks} Tareas Completadas", f"{progress:.0%}")
                st.button("✏️ Editar Checklist", on_click=lambda: st.session_state.update(editing_checklist_oferta_id=id_actual))
                st.divider()
                for i, item in enumerate(tareas):
                    if item['type'] == 'phase':
                        st.subheader(item['text'], divider='rainbow')
                    elif item['type'] == 'task':
                        is_checked = st.checkbox(item['text'], value=item['completed'], key=f"task_{id_actual}_{i}")
                        if is_checked != item['completed']:
                            st.session_state.ofertas[id_actual]['checklist']['tareas'][i]['completed'] = is_checked
                            save_data_to_firestore()
                            st.rerun()

        with tab_funnel:
            st.subheader("Añadir Nuevos Elementos al Funnel")
            c1, c2, c3 = st.columns(3)
            with c1:
                with st.form(f"form_bump_{id_actual}", clear_on_submit=True):
                    st.markdown("**Añadir Order Bump**"); nombre = st.text_input("Nombre")
                    precio = st.number_input("Precio ($)", min_value=0.01, format="%.2f")
                    if st.form_submit_button("Añadir Bump"):
                        if nombre and precio: agregar_item_funnel(id_actual, "Bump", nombre, precio); st.rerun()
            with c2:
                with st.form(f"form_upsell_{id_actual}", clear_on_submit=True):
                    st.markdown("**Añadir Upsell**"); nombre = st.text_input("Nombre")
                    precio = st.number_input("Precio ($)", min_value=0.01, format="%.2f")
                    if st.form_submit_button("Añadir Upsell"):
                        if nombre and precio: agregar_item_funnel(id_actual, "Upsell", nombre, precio); st.rerun()
            with c3:
                with st.form(f"form_downsell_{id_actual}", clear_on_submit=True):
                    st.markdown("**Añadir Downsell**"); nombre = st.text_input("Nombre")
                    precio = st.number_input("Precio ($)", min_value=0.01, format="%.2f")
                    if st.form_submit_button("Añadir Downsell"):
                        if nombre and precio: agregar_item_funnel(id_actual, "Downsell", nombre, precio); st.rerun()
            st.divider()
            st.subheader("Gestionar Elementos del Funnel")
            for item_id, item_details in oferta_actual['funnel'].items():
                if item_id == 'principal': continue
                col1, col2 = st.columns([3, 1])
                col1.text(f"{item_details['estado']} {item_details['alias']} - {item_details['nombre']}: ${item_details['precio']:.2f}")
                button_text = "📁 Archivar" if item_details['estado'] == "🟢 Activo" else "✅ Activar"
                if col2.button(button_text, key=f"btn_toggle_{item_id}"):
                    toggle_estado_funnel_item(id_actual, item_id); st.rerun()
        with tab_campanas:
            sub_tab_test, sub_tab_escala, sub_tab_analisis = st.tabs(["🧪 Fase de Testeo", "🚀 Fase de Escala", "🔬 Análisis Global del Funnel"])
            with sub_tab_test:
                st.subheader("1. Registro de Datos de Testeo")
                col1, col2 = st.columns(2)
                with col1:
                    with st.form(f"form_add_anuncio_{id_actual}", clear_on_submit=True):
                        nombre_anuncio = st.text_input("Nombre del Anuncio (Ej: V1-CopyA-CreativoB)")
                        if st.form_submit_button("Añadir Anuncio"): 
                            agregar_anuncio_testeo(id_actual, nombre_anuncio)
                with col2:
                    anuncios_activos = [ad['nombre'] for ad in oferta_actual['anuncios_testeo'] if ad['estado'] == "🟢 Activo"]
                    if not anuncios_activos: st.info("Añade un anuncio para registrar datos.")
                    else:
                        with st.form(f"form_log_data_{id_actual}", clear_on_submit=True):
                            fecha = st.date_input("Fecha", datetime.date.today())
                            anuncio_sel = st.selectbox("Anuncio (Sólo Activos)", options=anuncios_activos)
                            inversion = st.number_input("Inversión ($)", min_value=0.0, format="%.2f")
                            st.markdown("**Resultados de Ventas**")
                            funnel_activo = {k: v for k, v in oferta_actual['funnel'].items() if v['estado'] == "🟢 Activo"}
                            form_cols = st.columns(len(funnel_activo) + 1)
                            ventas_data = {}
                            ventas_data['Pagos Iniciados'] = form_cols[0].number_input("Pagos Iniciados", min_value=0, step=1)
                            i=1
                            for item_details in funnel_activo.values():
                                col_name = get_safe_column_name(item_details['alias'])
                                ventas_data[col_name] = form_cols[i].number_input(item_details['alias'], min_value=0, step=1)
                                i+=1
                            if st.form_submit_button("💾 Guardar Registro Diario", use_container_width=True):
                                nuevo_registro = {"Fecha": fecha, "Anuncio": anuncio_sel, "Inversión": inversion, **ventas_data}
                                registro_calculado = calcular_metricas_diarias(nuevo_registro, oferta_actual['funnel'], oferta_actual.get('comision_pp', 0.0))
                                df_nuevo = pd.DataFrame([registro_calculado])
                                oferta_actual['testeos'] = pd.concat([oferta_actual['testeos'], df_nuevo], ignore_index=True)
                                save_data_to_firestore()
                                st.rerun()
                st.divider()
                st.subheader("2. Panel de Rendimiento por Anuncio")
                if df_testeos_global.empty: st.info("Aún no hay datos para analizar.")
                else:
                    c1, c2 = st.columns(2)
                    start_date = c1.date_input("Fecha Inicio", df_testeos_global['Fecha'].min().date(), key="start_analisis")
                    end_date = c2.date_input("Fecha Fin", df_testeos_global['Fecha'].max().date(), key="end_analisis")
                    df_filtrado_diario = df_testeos_global[(df_testeos_global['Fecha'].dt.date >= start_date) & (df_testeos_global['Fecha'].dt.date <= end_date)]
                    if not df_filtrado_diario.empty:
                        df_agrupado = df_filtrado_diario.groupby("Anuncio").agg({'Inversión': 'sum', 'Pagos Iniciados': 'sum', 'Facturación Total': 'sum', 'Ganancia Neta': 'sum', **{get_safe_column_name(v['alias']): 'sum' for v in oferta_actual['funnel'].values() if get_safe_column_name(v['alias']) in df_filtrado_diario}}).reset_index()
                        precio_pp = oferta_actual['funnel']['principal']['precio']
                        ventas_pp_col = get_safe_column_name("PP")
                        df_agrupado['CPA'] = df_agrupado.apply(lambda row: row['Inversión'] / row[ventas_pp_col] if ventas_pp_col in row and row[ventas_pp_col] > 0 else 0, axis=1)
                        df_agrupado['Facturación FE'] = df_agrupado.apply(lambda row: row[ventas_pp_col] * precio_pp if ventas_pp_col in row else 0, axis=1)
                        df_agrupado['ROAS FE'] = df_agrupado.apply(lambda row: row['Facturación FE'] / row['Inversión'] if 'Facturación FE' in row and row['Inversión'] > 0 else 0, axis=1)
                        df_agrupado['ROAS Total (Neto)'] = df_agrupado.apply(lambda row: (row['Facturación Total'] - (row[ventas_pp_col] * comision_pp)) / row['Inversión'] if row['Inversión'] > 0 else 0, axis=1)
                        
                        mapa_estados = {ad['nombre']: ad['estado'] for ad in oferta_actual['anuncios_testeo']}
                        df_agrupado['Estado'] = df_agrupado['Anuncio'].map(mapa_estados)
                        sugerencias_globales = analizar_sugerencias_anuncios(df_testeos_global)
                        df_agrupado['Sugerencia'] = df_agrupado['Anuncio'].map(sugerencias_globales)
                        st.markdown("##### Rendimiento Agregado del Período")
                        mostrar_solo_activos = st.checkbox("Mostrar solo anuncios activos", value=True, key="cb_testeo_activos")
                        df_para_mostrar = df_agrupado.copy()
                        if mostrar_solo_activos:
                            df_para_mostrar = df_para_mostrar[df_para_mostrar['Estado'] == "🟢 Activo"]
                        def color_roas(val):
                            color = 'inherit'
                            if val >= 1.7: color = 'green'
                            elif val < 1.2: color = 'red'
                            return f'color: {color}'
                        def color_ganancia(val):
                            color = 'inherit'
                            if val > 0: color = 'green'
                            elif val < 0: color = 'red'
                            return f'color: {color}'
                        
                        cols_display_order = ['Anuncio', 'Estado', 'Inversión', 'Ganancia Neta', ventas_pp_col]
                        cols_rename_map = {ventas_pp_col: 'Ventas PP', 'Ganancia Neta': 'Ganancia Neta'}
                        for alias in [v['alias'] for k, v in oferta_actual['funnel'].items() if k != 'principal']:
                            col_name = get_safe_column_name(alias)
                            if col_name in df_para_mostrar.columns:
                                cols_display_order.append(col_name); cols_rename_map[col_name] = f"Ventas {alias}"
                        cols_display_order.extend(['CPA', 'ROAS FE', 'ROAS Total (Neto)', 'Sugerencia'])
                        final_cols_to_show = [col for col in cols_display_order if col in df_para_mostrar.columns]
                        
                        if df_para_mostrar.empty:
                            st.info("No hay anuncios que cumplan con el filtro actual. Desmarca la casilla para ver todos.")
                        else:
                            df_display = df_para_mostrar[final_cols_to_show].rename(columns=cols_rename_map)
                            st.dataframe(df_display.style.apply(lambda x: x.map(color_roas), subset=['ROAS FE', 'ROAS Total (Neto)']).apply(lambda x: x.map(color_ganancia), subset=['Ganancia Neta']).format({'Inversión': "${:,.2f}", 'Ganancia Neta': "${:,.2f}", 'CPA': "${:,.2f}", 'ROAS FE': "{:.2f}", 'ROAS Total (Neto)': "{:.2f}"}), use_container_width=True)
                        
                        st.markdown("---")
                        st.subheader("3. Acciones y Desglose")
                        c1, c2 = st.columns(2)
                        with c1:
                            st.markdown("**Gestión de Estado de Anuncios**")
                            anuncio_a_gestionar = st.selectbox("Seleccionar Anuncio", options=[ad['nombre'] for ad in oferta_actual['anuncios_testeo']], key="sb_gestionar_anuncio")
                            if st.button(f"Cambiar Estado de '{anuncio_a_gestionar}'"): toggle_estado_anuncio(id_actual, anuncio_a_gestionar); st.rerun()
                        with c2:
                            st.markdown("**Ver Desglose Diario**")
                            anuncio_a_desglosar = st.selectbox("Seleccionar Anuncio", options=df_agrupado['Anuncio'].unique(), key="sb_desglosar_anuncio")
                            if anuncio_a_desglosar:
                                with st.expander(f"Desglose para '{anuncio_a_desglosar}'"):
                                    df_desglose = df_filtrado_diario[df_filtrado_diario['Anuncio'] == anuncio_a_desglosar].copy().reset_index()
                                    for idx, row in df_desglose.iterrows():
                                        st.write(f"**Fecha:** {row['Fecha'].strftime('%Y-%m-%d')} | **Inversión:** ${row['Inversión']:.2f} | **Ganancia Neta:** ${row['Ganancia Neta']:.2f} | **ROAS Neto:** {row['ROAS Neto']:.2f}")
                                        action_col1, action_col2 = st.columns([1,1])
                                        if action_col1.button("✏️ Editar", key=f"edit_{row['index']}"):
                                            st.session_state['editing_record'] = {'type': 'testeo', 'index': row['index']}
                                            st.rerun()
                                        if action_col2.button("🗑️ Eliminar", key=f"del_{row['index']}"):
                                            eliminar_registro_testeo(id_actual, row['index'])
                                            st.rerun()
                                        st.divider()
                        st.markdown("---")
                        st.subheader("4. Acciones de Escala")
                        ganadores = df_agrupado[df_agrupado['Sugerencia'].str.contains("GANADOR", na=False)]['Anuncio'].tolist()
                        if not ganadores:
                            st.info("Aún no hay anuncios con la categoría de 'GANADOR' para escalar.")
                        else:
                            anuncio_a_escalar = st.selectbox("Selecciona un anuncio GANADOR para escalar", options=ganadores, index=None, placeholder="Elige un anuncio...")
                            if anuncio_a_escalar:
                                col1, col2 = st.columns(2)
                                if col1.button("➕ Crear Nueva Campaña de Escala", use_container_width=True):
                                    st.session_state['anuncio_para_escalar'] = anuncio_a_escalar; st.session_state['accion_de_escala'] = 'crear_nueva'; st.rerun()
                                if col2.button("📥 Añadir a Campaña Existente", use_container_width=True):
                                    st.session_state['anuncio_para_escalar'] = anuncio_a_escalar; st.session_state['accion_de_escala'] = 'añadir_existente'; st.rerun()
                        
                        with st.container(border=True):
                            st.subheader("🚀 Lanzamiento Directo a Escala")
                            st.info("Usa esta opción si ya tienes un anuncio validado y quieres escalarlo sin esperar la sugerencia automática del sistema.", icon="💡")
                            anuncios_activos_para_escala = [ad['nombre'] for ad in oferta_actual.get('anuncios_testeo', []) if ad['estado'] == "🟢 Activo"]
                            if not anuncios_activos_para_escala:
                                st.warning("Primero debes añadir un anuncio y asegurarte de que esté 'Activo' para poder lanzarlo a escala manualmente.")
                            else:
                                anuncio_manual_a_escalar = st.selectbox("Selecciona cualquier anuncio ACTIVO para escalar", options=anuncios_activos_para_escala, index=None, placeholder="Elige un anuncio para el lanzamiento directo...", key="lanzamiento_directo_sb")
                                if anuncio_manual_a_escalar:
                                    col1_manual, col2_manual = st.columns(2)
                                    if col1_manual.button("➕ Crear Nueva Campaña (Directo)", use_container_width=True, key="crear_directo"):
                                        st.session_state['anuncio_para_escalar'] = anuncio_manual_a_escalar
                                        st.session_state['accion_de_escala'] = 'crear_nueva'
                                        st.rerun()
                                    if col2_manual.button("📥 Añadir a Campaña (Directo)", use_container_width=True, key="anadir_directo"):
                                        st.session_state['anuncio_para_escalar'] = anuncio_manual_a_escalar
                                        st.session_state['accion_de_escala'] = 'añadir_existente'
                                        st.rerun()
                    else: st.warning("No hay datos en el rango de fechas seleccionado.")

                with st.expander("Ver Análisis Gráfico"):
                    if not df_testeos_global.empty:
                        df_filtrado_visual = df_testeos_global.copy()
                        st.markdown("#### 📈 Gráfico de Batalla: ROAS Neto vs. CPA")
                        anuncios_disponibles = df_filtrado_visual['Anuncio'].unique()
                        anuncios_a_mostrar = st.multiselect("Selecciona anuncios para comparar", options=anuncios_disponibles, default=list(anuncios_disponibles[:3]))
                        if anuncios_a_mostrar:
                            df_tendencia = df_filtrado_visual[df_filtrado_visual['Anuncio'].isin(anuncios_a_mostrar)].copy()
                            precio_pp = oferta_actual['funnel']['principal']['precio']
                            ventas_pp_col = get_safe_column_name("PP")
                            df_tendencia['CPA'] = df_tendencia.apply(lambda row: row['Inversión'] / row[ventas_pp_col] if ventas_pp_col in row and row[ventas_pp_col] > 0 else 0, axis=1)
                            st.line_chart(df_tendencia, x='Fecha', y=['ROAS Neto', 'CPA'], color='Anuncio')
                        st.markdown("---"); st.markdown("#### 📊 Gráfico de Volumen: Total Ventas PP")
                        df_volumen = df_filtrado_visual.groupby('Anuncio')[get_safe_column_name("PP")].sum().sort_values(ascending=False)
                        df_volumen.name = "Total Ventas PP"
                        st.bar_chart(df_volumen)
                        st.markdown("---"); st.markdown("#### 🗓️ Calendario de Consistencia (Últimos 7 días)")
                        df_consistencia = df_filtrado_visual.copy()
                        dias_a_mostrar = sorted(df_consistencia['Fecha'].dt.date.unique())[-7:]
                        df_consistencia = df_consistencia[df_consistencia['Fecha'].dt.date.isin(dias_a_mostrar)]
                        if not df_consistencia.empty:
                            df_pivot = df_consistencia.pivot_table(index='Anuncio', columns=df_consistencia['Fecha'].dt.strftime('%Y-%m-%d'), values=get_safe_column_name("PP"), aggfunc='sum').fillna(0)
                            df_visual_consistencia = df_pivot.applymap(lambda x: "✅" if x > 0 else "❌")
                            st.dataframe(df_visual_consistencia)
            with sub_tab_escala:
                st.header("📊 Panel de Control de Campañas de Escala")
                campanas_escala = oferta_actual.get('escala', {})
                st.markdown("---")
                st.subheader("✍️ Registrar Datos Diarios de Escala")
                if not campanas_escala:
                    st.info("Crea tu primera campaña de escala desde la 'Fase de Testeo' para poder registrar datos.")
                else:
                    opciones_campana = {cid: cdetails['nombre_campana'] for cid, cdetails in campanas_escala.items()}
                    id_campana_sel = st.selectbox("1. Selecciona la Campaña para registrar datos", options=list(opciones_campana.keys()), format_func=lambda x: opciones_campana[x], index=None, placeholder="Elige una campaña...", key="selector_campana_registro_escala")
                    if id_campana_sel:
                        with st.form("form_log_data_escala", clear_on_submit=True):
                            st.markdown(f"**Registrando para la campaña: __{opciones_campana[id_campana_sel]}__**")
                            componentes_activos = [comp['nombre'] for comp in campanas_escala[id_campana_sel].get('componentes', []) if comp['estado'] == '🟢 Activo']
                            c1, c2, c3 = st.columns(3)
                            componente_sel = c1.selectbox("2. Selecciona el Componente", options=componentes_activos, key="sel_comp_escala")
                            fecha_escala = c2.date_input("Fecha", datetime.date.today(), key="date_escala")
                            inversion_escala = c3.number_input("Inversión ($)", min_value=0.0, format="%.2f", key="inv_escala")
                            st.markdown("**Resultados de Ventas**")
                            funnel_activo = {k: v for k, v in oferta_actual['funnel'].items() if v['estado'] == "🟢 Activo"}
                            form_cols_escala = st.columns(len(funnel_activo) + 1)
                            ventas_data_escala = {}
                            ventas_data_escala['Pagos Iniciados'] = form_cols_escala[0].number_input("Pagos Iniciados", min_value=0, step=1, key="pi_escala")
                            i=1
                            for item_details in funnel_activo.values():
                                col_name = get_safe_column_name(item_details['alias'])
                                ventas_data_escala[col_name] = form_cols_escala[i].number_input(item_details['alias'], min_value=0, step=1, key=f"venta_{item_details['alias']}_escala")
                                i+=1
                            if st.form_submit_button("💾 Guardar Registro de Escala", use_container_width=True):
                                if componente_sel:
                                    nuevo_registro = {"Fecha": fecha_escala, "Componente": componente_sel, "Inversión": inversion_escala, **ventas_data_escala}
                                    agregar_registro_escala(id_actual, id_campana_sel, nuevo_registro); st.rerun()
                                else: st.warning("Asegúrate de seleccionar un componente.")
                st.markdown("---")
                if not campanas_escala:
                    st.info("Aquí se mostrarán tus campañas de escala una vez que las lances.")
                else:
                    def color_roas(val):
                        color = 'inherit'
                        if val >= 1.7: color = 'green'
                        elif val < 1.2: color = 'red'
                        return f'color: {color}'
                    def color_ganancia(val):
                        color = 'inherit'
                        if val > 0: color = 'green'
                        elif val < 0: color = 'red'
                        return f'color: {color}'
                    for cid, cdetails in campanas_escala.items():
                        ganancia_neta_campana_header = 0
                        if not cdetails['registros'].empty:
                            ganancia_neta_campana_header = cdetails['registros']['Ganancia Neta'].sum()
                        estado_campana = cdetails.get("estado", "🟢 Activa")
                        expander_title = f"**{cdetails['nombre_campana']}** (Estrategia: {cdetails['estrategia']}) | Estado: {estado_campana} | Ganancia Neta: ${ganancia_neta_campana_header:,.2f}"
                        with st.expander(expander_title, expanded=True):
                            df_escala_raw = cdetails['registros'].copy()
                            c1, c2, c3 = st.columns([2,2,1])
                            if c3.button("🔴 Apagar Campaña" if estado_campana == "🟢 Activa" else "✅ Activar Campaña", key=f"toggle_camp_{cid}"):
                                toggle_estado_campana_escala(id_actual, cid); st.rerun()
                            if df_escala_raw.empty:
                                st.info("Aún no hay datos registrados para esta campaña.");
                            if not df_escala_raw.empty:
                                df_escala_raw['Fecha'] = pd.to_datetime(df_escala_raw['Fecha'])
                            with c1:
                                start_date_escala = st.date_input("Fecha Inicio", df_escala_raw['Fecha'].min().date() if not df_escala_raw.empty else datetime.date.today(), key=f"start_escala_{cid}")
                            with c2:
                                end_date_escala = st.date_input("Fecha Fin", df_escala_raw['Fecha'].max().date() if not df_escala_raw.empty else datetime.date.today(), key=f"end_escala_{cid}")
                            st.markdown("---")
                            st.markdown("##### ⚙️ Gestión de Componentes")
                            col_gest_1, col_gest_2 = st.columns([2,1])
                            with col_gest_1:
                                componentes_totales = [comp['nombre'] for comp in cdetails.get('componentes', [])]
                                if not componentes_totales:
                                    st.info("No hay componentes en esta campaña.")
                                else:
                                    componente_a_gestionar = st.selectbox("Seleccionar componente para cambiar estado", options=componentes_totales, key=f"sb_gest_comp_{cid}")
                            with col_gest_2:
                                if componentes_totales:
                                    st.write(" ") 
                                    if st.button("Activar/Desactivar Componente", key=f"btn_gest_comp_{cid}", use_container_width=True):
                                        toggle_estado_componente_escala(id_actual, cid, componente_a_gestionar)
                                        st.rerun()
                            if df_escala_raw.empty: continue
                            df_filtrado_escala = df_escala_raw[(df_escala_raw['Fecha'].dt.date >= start_date_escala) & (df_escala_raw['Fecha'].dt.date <= end_date_escala)]
                            if not df_filtrado_escala.empty:
                                df_agrupado_escala = df_filtrado_escala.groupby("Componente").agg({'Inversión': 'sum', 'Pagos Iniciados': 'sum', 'Facturación Total': 'sum', 'Ganancia Neta': 'sum', **{get_safe_column_name(v['alias']): 'sum' for v in oferta_actual['funnel'].values() if get_safe_column_name(v['alias']) in df_filtrado_escala}}).reset_index()
                                precio_pp = oferta_actual['funnel']['principal']['precio']
                                ventas_pp_col = get_safe_column_name("PP")
                                df_agrupado_escala['CPA'] = df_agrupado_escala.apply(lambda row: row['Inversión'] / row[ventas_pp_col] if ventas_pp_col in row and row[ventas_pp_col] > 0 else 0, axis=1)
                                df_agrupado_escala['Facturación FE'] = df_agrupado_escala.apply(lambda row: row[ventas_pp_col] * precio_pp if ventas_pp_col in row else 0, axis=1)
                                df_agrupado_escala['ROAS FE'] = df_agrupado_escala.apply(lambda row: row['Facturación FE'] / row['Inversión'] if 'Facturación FE' in row and row['Inversión'] > 0 else 0, axis=1)
                                df_agrupado_escala['ROAS Total (Neto)'] = df_agrupado_escala.apply(lambda row: (row['Facturación Total'] - (row[ventas_pp_col] * comision_pp)) / row['Inversión'] if row['Inversión'] > 0 else 0, axis=1)
                                mapa_estados_escala = {comp['nombre']: comp['estado'] for comp in cdetails.get('componentes', [])}
                                df_agrupado_escala['Estado'] = df_agrupado_escala['Componente'].map(mapa_estados_escala)
                                st.markdown("##### Rendimiento por Componente")
                                mostrar_solo_activos_escala = st.checkbox("Mostrar solo componentes activos", value=True, key=f"cb_escala_activos_{cid}")
                                df_para_mostrar_escala = df_agrupado_escala.copy()
                                if mostrar_solo_activos_escala:
                                    df_para_mostrar_escala = df_para_mostrar_escala[df_para_mostrar_escala['Estado'] == "🟢 Activo"]
                                cols_display_order = ['Componente', 'Estado', 'Inversión', 'Ganancia Neta', ventas_pp_col]
                                cols_rename_map = {ventas_pp_col: 'Ventas PP', 'Componente': 'Conjunto/Anuncio'}
                                for alias in [v['alias'] for k, v in oferta_actual['funnel'].items() if k != 'principal']:
                                    col_name = get_safe_column_name(alias)
                                    if col_name in df_para_mostrar_escala.columns:
                                        cols_display_order.append(col_name); cols_rename_map[col_name] = f"Ventas {alias}"
                                cols_display_order.extend(['CPA', 'ROAS FE', 'ROAS Total (Neto)'])
                                final_cols_to_show = [col for col in cols_display_order if col in df_para_mostrar_escala.columns]
                                
                                if df_para_mostrar_escala.empty:
                                    st.info("No hay componentes que cumplan con el filtro actual. Desmarca la casilla para ver todos.")
                                else:
                                    df_display_escala = df_para_mostrar_escala[final_cols_to_show].rename(columns=cols_rename_map)
                                    st.dataframe(df_display_escala.style.apply(lambda x: x.map(color_roas), subset=['ROAS FE', 'ROAS Total (Neto)']).apply(lambda x: x.map(color_ganancia), subset=['Ganancia Neta']).format({'Inversión': "${:,.2f}", 'Ganancia Neta': "${:,.2f}", 'CPA': "${:,.2f}", 'ROAS FE': "{:.2f}", 'ROAS Total (Neto)': "{:.2f}"}), use_container_width=True)
                                
                                st.markdown("##### Totales de la Campaña (Período Seleccionado)")
                                total_inversion_campana = df_agrupado_escala['Inversión'].sum()
                                total_ganancia_neta_campana = df_agrupado_escala['Ganancia Neta'].sum()
                                total_facturacion_bruta_campana = df_agrupado_escala['Facturación Total'].sum()
                                roas_neto_campana = (total_facturacion_bruta_campana - (df_agrupado_escala.get(ventas_pp_col, 0).sum() * comision_pp)) / total_inversion_campana if total_inversion_campana > 0 else 0
                                
                                kpi1, kpi2, kpi3, kpi4 = st.columns(4)
                                kpi1.metric("Inversión Total", f"${total_inversion_campana:,.2f}")
                                kpi2.metric("Facturación Bruta", f"${total_facturacion_bruta_campana:,.2f}")
                                kpi3.metric("Ganancia Neta", f"${total_ganancia_neta_campana:,.2f}")
                                kpi4.metric("ROAS Neto", f"{roas_neto_campana:.2f}")
                                st.markdown("---")
                                st.subheader("Desglose y Acciones por Componente")
                                componentes_con_datos = df_filtrado_escala['Componente'].unique()
                                
                                if len(componentes_con_datos) == 0:
                                    st.info("No hay componentes con datos en el período seleccionado.")
                                else:
                                    componente_a_desglosar = st.selectbox("Selecciona un Componente para ver su desglose diario", options=componentes_con_datos, key=f"sb_desglose_escala_{cid}")
                                    if componente_a_desglosar:
                                        with st.expander(f"Desglose para '{componente_a_desglosar}'"):
                                            df_desglose_escala = df_filtrado_escala[df_filtrado_escala['Componente'] == componente_a_desglosar].copy().reset_index()
                                            for idx, row in df_desglose_escala.iterrows():
                                                st.write(f"**Fecha:** {row['Fecha'].strftime('%Y-%m-%d')} | **Inversión:** ${row['Inversión']:.2f} | **Ganancia Neta:** ${row['Ganancia Neta']:.2f} | **ROAS Neto:** {row['ROAS Neto']:.2f}")
                                                action_col1, action_col2 = st.columns([1,1])
                                                if action_col1.button("✏️ Editar", key=f"edit_escala_{row['index']}_{cid}"):
                                                    st.session_state['editing_record'] = {'type': 'escala', 'campaign_id': cid, 'index': row['index']}
                                                    st.rerun()
                                                if action_col2.button("🗑️ Eliminar", key=f"del_escala_{row['index']}_{cid}"):
                                                    eliminar_registro_escala(id_actual, cid, row['index'])
                                                    st.rerun()
                                                st.divider()
                            else:
                                st.warning("No hay datos en el rango de fechas seleccionado para esta campaña.")
            with sub_tab_analisis:
                st.subheader("🔬 Monitor de Signos Vitales del Embudo (Global)")
                df_funnel_completo = [df_testeos_global.copy()]
                for camp_details in oferta_actual.get('escala', {}).values():
                    if not camp_details['registros'].empty:
                        registros_escala = camp_details['registros'].copy()
                        registros_escala['Fecha'] = pd.to_datetime(registros_escala['Fecha'])
                        df_funnel_completo.append(registros_escala)
                
                df_consolidado_funnel = pd.concat(df_funnel_completo, ignore_index=True)

                if df_consolidado_funnel.empty:
                    st.info("Aún no hay datos registrados en ninguna fase para analizar el funnel.")
                else:
                    df_consolidado_funnel['Fecha'] = pd.to_datetime(df_consolidado_funnel['Fecha'])
                    c1, c2 = st.columns(2)
                    start_date_f = c1.date_input("Fecha Inicio", df_consolidado_funnel['Fecha'].min().date(), key="start_funnel_total")
                    end_date_f = c2.date_input("Fecha Fin", df_consolidado_funnel['Fecha'].max().date(), key="end_funnel_total")
                    
                    df_filtrado_funnel = df_consolidado_funnel[(df_consolidado_funnel['Fecha'].dt.date >= start_date_f) & (df_consolidado_funnel['Fecha'].dt.date <= end_date_f)]

                    if not df_filtrado_funnel.empty:
                        st.markdown("---")
                        st.markdown("#### Tasas de Conversión y Adopción del Backend (Global)")
                        total_pagos_iniciados = df_filtrado_funnel['Pagos Iniciados'].sum()
                        ventas_pp_col = get_safe_column_name("PP")
                        total_ventas_pp = df_filtrado_funnel[ventas_pp_col].sum() if ventas_pp_col in df_filtrado_funnel else 0
                        
                        checkout_cr = (total_ventas_pp / total_pagos_iniciados) * 100 if total_pagos_iniciados > 0 else 0
                        st.metric(label="Tasa de Conversión (Pagos Iniciados a Ventas PP)", value=f"{checkout_cr:.2f}%")
                        st.progress(int(checkout_cr)); st.caption(f"Pagos Iniciados: {int(total_pagos_iniciados)} | Ventas PP: {int(total_ventas_pp)}")
                        
                        funnel_items = [v for k, v in oferta_actual['funnel'].items() if k != 'principal']
                        if funnel_items:
                            cols = st.columns(len(funnel_items))
                            i = 0
                            for item in funnel_items:
                                alias = item['alias']; col_name = get_safe_column_name(alias)
                                if col_name in df_filtrado_funnel.columns:
                                    total_ventas_item = df_filtrado_funnel[col_name].sum()
                                    item_cr = (total_ventas_item / total_ventas_pp) * 100 if total_ventas_pp > 0 else 0
                                    with cols[i]:
                                        st.metric(label=f"Adopción de {alias} ({item['nombre']})", value=f"{item_cr:.2f}%")
                                        st.progress(int(item_cr)); st.caption(f"Ventas {alias}: {int(total_ventas_item)}")
                                    i+=1
                        
                        st.divider()
                        st.subheader("📈 Análisis de Rendimiento Temporal")
                        agrupacion = st.radio("Agrupar por:", ["Día", "Semana", "Mes"], horizontal=True, key="agrupacion_temporal")
                        df_analisis_temp = df_filtrado_funnel.copy()
                        df_analisis_temp['Día de la Semana'] = df_analisis_temp['Fecha'].dt.day_name()
                        precio_pp = oferta_actual['funnel']['principal']['precio']
                        comision_pp = oferta_actual.get('comision_pp', 0.0)
                        ventas_pp_col = get_safe_column_name("PP")
                        def calcular_metricas_temporales(df):
                            df['Facturación FE'] = df.get(ventas_pp_col, 0) * precio_pp
                            df['Ganancia Neta FE'] = df['Facturación FE'] - df['Inversión'] - (df.get(ventas_pp_col, 0) * comision_pp)
                            df['ROAS FE'] = df['Facturación FE'].div(df['Inversión']).where(df['Inversión'] != 0, 0)
                            df['ROAS Neto'] = (df['Facturación Total'] - (df.get(ventas_pp_col, 0) * comision_pp)).div(df['Inversión']).where(df['Inversión'] != 0, 0)
                            return df
                        agg_dict = {'Inversión': ('Inversión', 'sum'), 'Ganancia Neta': ('Ganancia Neta', 'sum'), 'Facturación Total': ('Facturación Total', 'sum'), ventas_pp_col: (ventas_pp_col, 'sum')}
                        if agrupacion == "Día":
                            df_agrupado_temp = df_analisis_temp.groupby(['Fecha', 'Día de la Semana']).agg(**agg_dict).reset_index()
                            df_agrupado_temp = calcular_metricas_temporales(df_agrupado_temp)
                            st.markdown("##### Rendimiento Diario")
                            display_cols = ['Fecha', 'Día de la Semana', 'Inversión', 'Facturación Total', 'Facturación FE', 'Ganancia Neta', 'Ganancia Neta FE', 'ROAS Neto', 'ROAS FE']
                            st.dataframe(df_agrupado_temp[display_cols].style.format({'Inversión': "${:,.2f}", 'Facturación Total': "${:,.2f}", 'Facturación FE': "${:,.2f}",'Ganancia Neta': "${:,.2f}", 'Ganancia Neta FE': "${:,.2f}", 'ROAS Neto': "{:.2f}", 'ROAS FE': "{:.2f}"}), use_container_width=True)
                        else: # Semana o Mes
                            if agrupacion == "Semana":
                                df_analisis_temp['Periodo'] = df_analisis_temp['Fecha'].dt.to_period('W').apply(lambda r: r.start_time.strftime('%Y-%m-%d'))
                            else: # Mes
                                try:
                                    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
                                except locale.Error:
                                    pass
                                df_analisis_temp['Periodo'] = df_analisis_temp['Fecha'].dt.strftime('%B %Y').str.capitalize()
                            df_agrupado_periodo = df_analisis_temp.groupby('Periodo').agg(**agg_dict).reset_index()
                            df_agrupado_periodo = calcular_metricas_temporales(df_agrupado_periodo)
                            st.markdown(f"##### Rendimiento por {agrupacion}")
                            for index, row in df_agrupado_periodo.iterrows():
                                expander_title = f"**{agrupacion}: {row['Periodo']}** | Inv: ${row['Inversión']:,.2f} | Gan. Neta: ${row['Ganancia Neta']:,.2f} | ROAS Neto: {row['ROAS Neto']:.2f}"
                                with st.expander(expander_title):
                                    df_desglose_periodo = df_analisis_temp[df_analisis_temp['Periodo'] == row['Periodo']]
                                    df_desglose_diario = df_desglose_periodo.groupby(['Fecha', 'Día de la Semana']).agg(**agg_dict).reset_index()
                                    df_desglose_diario = calcular_metricas_temporales(df_desglose_diario)
                                    display_cols_desglose = ['Fecha', 'Día de la Semana', 'Inversión', 'Facturación Total', 'Facturación FE', 'Ganancia Neta', 'Ganancia Neta FE', 'ROAS Neto', 'ROAS FE']
                                    st.dataframe(df_desglose_diario[display_cols_desglose].sort_values(by="Fecha").style.format({'Inversión': "${:,.2f}", 'Facturación Total': "${:,.2f}", 'Facturación FE': "${:,.2f}", 'Ganancia Neta': "${:,.2f}", 'Ganancia Neta FE': "${:,.2f}", 'ROAS Neto': "{:.2f}", 'ROAS FE': "{:.2f}"}), use_container_width=True)
                    else: st.warning("No hay datos en el rango de fechas seleccionado.")

# --- PUNTO DE ENTRADA ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

if st.session_state.logged_in:
    main_app()
else:
    show_login_page()
