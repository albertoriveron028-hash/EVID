import os
import json
import hashlib
import getpass
from datetime import datetime


# ============================================================
# CONFIGURACIÓN
# ============================================================

BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

USUARIOS_DIR = os.path.join(
    BASE_DIR,
    "usuarios"
)

PERFILES_DIR = os.path.join(
    USUARIOS_DIR,
    "perfiles"
)

USUARIOS_FILE = os.path.join(
    USUARIOS_DIR,
    "usuarios.json"
)


os.makedirs(
    PERFILES_DIR,
    exist_ok=True
)


# ============================================================
# UTILIDADES
# ============================================================

def hash_pin(pin):

    return hashlib.sha256(
        pin.encode("utf-8")
    ).hexdigest()


def crear_id(nombre):

    base = (
        nombre.lower()
        .strip()
        .replace(" ", "_")
    )

    return base


# ============================================================
# ARCHIVO PRINCIPAL DE USUARIOS
# ============================================================

def cargar_usuarios():

    if not os.path.exists(
        USUARIOS_FILE
    ):

        datos = {
            "usuarios": []
        }

        guardar_usuarios(
            datos
        )

        return datos


    try:

        with open(
            USUARIOS_FILE,
            "r",
            encoding="utf-8"
        ) as archivo:

            datos = json.load(
                archivo
            )


        if "usuarios" not in datos:

            datos["usuarios"] = []


        return datos


    except Exception as error:

        print(
            f"⚠️ Error leyendo usuarios: {error}"
        )

        return {
            "usuarios": []
        }


def guardar_usuarios(datos):

    try:

        with open(
            USUARIOS_FILE,
            "w",
            encoding="utf-8"
        ) as archivo:

            json.dump(
                datos,
                archivo,
                ensure_ascii=False,
                indent=4
            )


        return True


    except Exception as error:

        print(
            f"❌ Error guardando usuarios: {error}"
        )

        return False


# ============================================================
# PERFIL VACÍO
# ============================================================

def crear_perfil_vacio(
    usuario_id,
    nombre,
    tratamiento
):

    return {

        "id": usuario_id,

        "nombre": nombre,

        "tratamiento": tratamiento,

        "pin": "",

        "voz_registrada": False,

        "voz_archivos": [],

        "preferencias": [],

        "informacion_importante": [],

        "memorias": [],

        "contexto_emocional": [],

        "historial": [],

        "creado": datetime.now().isoformat(),

        "ultimo_acceso": None
    }


# ============================================================
# CREAR USUARIO
# ============================================================

def crear_usuario(
    nombre,
    tratamiento="señor",
    pin=None
):

    nombre = nombre.strip()

    if not nombre:

        return None


    usuarios = cargar_usuarios()


    # ========================================================
    # VERIFICAR SI YA EXISTE
    # ========================================================

    for usuario in usuarios["usuarios"]:

        if (
            usuario["nombre"]
            .lower()
            ==
            nombre.lower()
        ):

            print(
                "⚠️ Ese usuario ya existe."
            )

            return usuario


    # ========================================================
    # ID
    # ========================================================

    usuario_id = crear_id(
        nombre
    )


    # Evitar conflicto de IDs

    contador = 2

    id_original = usuario_id


    while os.path.exists(
        os.path.join(
            PERFILES_DIR,
            f"{usuario_id}.json"
        )
    ):

        usuario_id = (
            f"{id_original}_{contador}"
        )

        contador += 1


    # ========================================================
    # PIN
    # ========================================================

    pin_hash = ""

    if pin:

        pin_hash = hash_pin(
            str(pin)
        )


    # ========================================================
    # PERFIL
    # ========================================================

    perfil = crear_perfil_vacio(

        usuario_id,

        nombre,

        tratamiento
    )


    perfil["pin"] = pin_hash


    # ========================================================
    # GUARDAR PERFIL
    # ========================================================

    archivo_perfil = os.path.join(

        PERFILES_DIR,

        f"{usuario_id}.json"
    )


    try:

        with open(
            archivo_perfil,
            "w",
            encoding="utf-8"
        ) as archivo:

            json.dump(
                perfil,
                archivo,
                ensure_ascii=False,
                indent=4
            )


    except Exception as error:

        print(
            f"❌ Error creando perfil: {error}"
        )

        return None


    # ========================================================
    # REGISTRO GENERAL
    # ========================================================

    usuarios["usuarios"].append({

        "id": usuario_id,

        "nombre": nombre,

        "tratamiento": tratamiento,

        "voz_registrada": False,

        "creado": perfil["creado"]
    })


    guardar_usuarios(
        usuarios
    )


    print(
        f"✅ Usuario creado: {nombre}"
    )


    return perfil


# ============================================================
# OBTENER USUARIO
# ============================================================

def obtener_usuario(
    usuario_id
):

    archivo = os.path.join(

        PERFILES_DIR,

        f"{usuario_id}.json"
    )


    if not os.path.exists(
        archivo
    ):

        return None


    try:

        with open(
            archivo,
            "r",
            encoding="utf-8"
        ) as archivo_json:

            return json.load(
                archivo_json
            )


    except Exception as error:

        print(
            f"❌ Error leyendo perfil: {error}"
        )

        return None


# ============================================================
# GUARDAR PERFIL
# ============================================================

def guardar_usuario(
    perfil
):

    if not perfil:

        return False


    usuario_id = perfil.get(
        "id"
    )


    if not usuario_id:

        return False


    archivo = os.path.join(

        PERFILES_DIR,

        f"{usuario_id}.json"
    )


    try:

        with open(
            archivo,
            "w",
            encoding="utf-8"
        ) as archivo_json:

            json.dump(
                perfil,
                archivo_json,
                ensure_ascii=False,
                indent=4
            )


        return True


    except Exception as error:

        print(
            f"❌ Error guardando perfil: {error}"
        )

        return False


# ============================================================
# BUSCAR POR NOMBRE
# ============================================================

def buscar_usuario_por_nombre(
    nombre
):

    nombre = nombre.strip().lower()


    usuarios = cargar_usuarios()


    for usuario in usuarios["usuarios"]:

        if (
            usuario["nombre"]
            .lower()
            ==
            nombre
        ):

            return obtener_usuario(
                usuario["id"]
            )


    return None


# ============================================================
# LISTAR USUARIOS
# ============================================================

def listar_usuarios():
    """
    Devuelve una lista de usuarios registrados.

    Compatible con:
    {
        "usuarios": []
    }

    y también protege contra datos antiguos
    que hayan quedado guardados como strings.
    """

    try:
        if not os.path.exists(USUARIOS_FILE):
            return []

        with open(
            USUARIOS_FILE,
            "r",
            encoding="utf-8"
        ) as archivo:
            datos = json.load(archivo)

        # ----------------------------------------------------
        # Estructura esperada
        # ----------------------------------------------------

        if not isinstance(datos, dict):
            print("⚠️ usuarios.json no contiene un objeto válido.")
            return []

        usuarios = datos.get("usuarios", [])

        if not isinstance(usuarios, list):
            print("⚠️ 'usuarios' no es una lista.")
            return []

        usuarios_validos = []

        # ----------------------------------------------------
        # Revisar cada usuario
        # ----------------------------------------------------

        for i, usuario in enumerate(usuarios):

            # Si quedó un usuario como texto,
            # lo convertimos a estructura válida.
            if isinstance(usuario, str):

                usuario = {
                    "id": f"usuario_{i + 1:03d}",
                    "nombre": usuario,
                    "genero": "desconocido",
                    "voz": {
                        "registrada": False
                    },
                    "password": "",
                    "memoria": {
                        "recuerdos": [],
                        "preferencias": [],
                        "informacion_importante": []
                    },
                    "emociones": []
                }

            # ------------------------------------------------
            # Si no es diccionario, ignorarlo
            # ------------------------------------------------

            if not isinstance(usuario, dict):

                print(
                    f"⚠️ Usuario {i + 1} "
                    f"tiene un formato inválido. Se ignora."
                )

                continue

            # ------------------------------------------------
            # ID
            # ------------------------------------------------

            if not usuario.get("id"):

                usuario["id"] = (
                    f"usuario_{i + 1:03d}"
                )

            # ------------------------------------------------
            # Nombre
            # ------------------------------------------------

            if not usuario.get("nombre"):

                usuario["nombre"] = (
                    f"Usuario {i + 1}"
                )

            # ------------------------------------------------
            # Género
            # ------------------------------------------------

            if "genero" not in usuario:

                usuario["genero"] = (
                    "desconocido"
                )

            # ------------------------------------------------
            # Voz
            # ------------------------------------------------

            if not isinstance(
                usuario.get("voz"),
                dict
            ):

                usuario["voz"] = {
                    "registrada": False
                }

            if "registrada" not in usuario["voz"]:

                usuario["voz"]["registrada"] = False

            # ------------------------------------------------
            # Contraseña
            # ------------------------------------------------

            if "password" not in usuario:

                usuario["password"] = ""

            # ------------------------------------------------
            # Memoria privada
            # ------------------------------------------------

            if not isinstance(
                usuario.get("memoria"),
                dict
            ):

                usuario["memoria"] = {}

            if "recuerdos" not in usuario["memoria"]:

                usuario["memoria"]["recuerdos"] = []

            if "preferencias" not in usuario["memoria"]:

                usuario["memoria"]["preferencias"] = []

            if "informacion_importante" not in usuario["memoria"]:

                usuario["memoria"][
                    "informacion_importante"
                ] = []

            # ------------------------------------------------
            # Emociones privadas
            # ------------------------------------------------

            if "emociones" not in usuario:

                usuario["emociones"] = []

            if not isinstance(
                usuario["emociones"],
                list
            ):

                usuario["emociones"] = []

            usuarios_validos.append(usuario)

        return usuarios_validos

    except json.JSONDecodeError:

        print(
            "❌ usuarios.json contiene JSON inválido."
        )

        return []

    except Exception as error:

        print(
            f"❌ Error leyendo usuarios: {error}"
        )

        return []


# ============================================================
# VERIFICAR PIN
# ============================================================

def verificar_pin(
    perfil,
    pin
):

    if not perfil:

        return False


    pin_guardado = perfil.get(
        "pin",
        ""
    )


    # ========================================================
    # USUARIO SIN PIN
    # ========================================================

    if not pin_guardado:

        return True


    pin_hash = hash_pin(
        str(pin)
    )


    return pin_hash == pin_guardado


# ============================================================
# CAMBIAR PIN
# ============================================================

def cambiar_pin(
    perfil,
    nuevo_pin
):

    if not perfil:

        return False


    perfil["pin"] = hash_pin(
        str(nuevo_pin)
    )


    return guardar_usuario(
        perfil
    )


# ============================================================
# ELIMINAR USUARIO
# ============================================================

def eliminar_usuario(
    usuario_id
):

    usuarios = cargar_usuarios()


    encontrado = False


    nuevos = []


    for usuario in usuarios["usuarios"]:

        if usuario["id"] == usuario_id:

            encontrado = True

        else:

            nuevos.append(
                usuario
            )


    if not encontrado:

        return False


    usuarios["usuarios"] = nuevos


    guardar_usuarios(
        usuarios
    )


    archivo = os.path.join(

        PERFILES_DIR,

        f"{usuario_id}.json"
    )


    if os.path.exists(
        archivo
    ):

        try:

            os.remove(
                archivo
            )

        except Exception as error:

            print(
                f"⚠️ No se pudo eliminar "
                f"el perfil: {error}"
            )


    return True


# ============================================================
# ACTUALIZAR ÚLTIMO ACCESO
# ============================================================

def actualizar_acceso(
    perfil
):

    if not perfil:

        return


    perfil["ultimo_acceso"] = (
        datetime.now().isoformat()
    )


    guardar_usuario(
        perfil
    )


# ============================================================
# AGREGAR PREFERENCIA
# ============================================================

def agregar_preferencia(
    perfil,
    preferencia
):

    if not perfil:

        return False


    preferencia = preferencia.strip()


    if not preferencia:

        return False


    preferencias = perfil.setdefault(

        "preferencias",

        []
    )


    for existente in preferencias:

        if (
            existente.lower()
            ==
            preferencia.lower()
        ):

            return True


    preferencias.append(
        preferencia
    )


    return guardar_usuario(
        perfil
    )


# ============================================================
# AGREGAR INFORMACIÓN IMPORTANTE
# ============================================================

def agregar_informacion_importante(
    perfil,
    informacion
):

    if not perfil:

        return False


    informacion = informacion.strip()


    if not informacion:

        return False


    lista = perfil.setdefault(

        "informacion_importante",

        []
    )


    for existente in lista:

        if (
            existente.lower()
            ==
            informacion.lower()
        ):

            return True


    lista.append(
        informacion
    )


    return guardar_usuario(
        perfil
    )


# ============================================================
# AGREGAR MEMORIA
# ============================================================

def agregar_memoria_usuario(
    perfil,
    memoria
):

    if not perfil:

        return False


    memoria = memoria.strip()


    if not memoria:

        return False


    memorias = perfil.setdefault(

        "memorias",

        []
    )


    for existente in memorias:

        if (
            existente.lower()
            ==
            memoria.lower()
        ):

            return True


    memorias.append(
        memoria
    )


    # Máximo 100 recuerdos
    if len(memorias) > 100:

        perfil["memorias"] = (
            memorias[-100:]
        )


    return guardar_usuario(
        perfil
    )


# ============================================================
# AGREGAR CONTEXTO EMOCIONAL
# ============================================================

def agregar_emocion_usuario(
    perfil,
    emocion,
    texto
):

    if not perfil:

        return False


    registro = {

        "emocion": emocion,

        "texto": texto,

        "fecha":
            datetime.now().isoformat()
    }


    emociones = perfil.setdefault(

        "contexto_emocional",

        []
    )


    emociones.append(
        registro
    )


    # Guardar máximo 100 estados
    if len(emociones) > 100:

        perfil["contexto_emocional"] = (
            emociones[-100:]
        )


    return guardar_usuario(
        perfil
    )


# ============================================================
# AGREGAR HISTORIAL
# ============================================================

def agregar_historial_usuario(
    perfil,
    rol,
    contenido
):

    if not perfil:

        return False


    historial = perfil.setdefault(

        "historial",

        []
    )


    historial.append({

        "role": rol,

        "content": contenido,

        "fecha":
            datetime.now().isoformat()
    })


    # Mantener solamente las últimas 30
    if len(historial) > 30:

        perfil["historial"] = (
            historial[-30:]
        )


    return guardar_usuario(
        perfil
    )


# ============================================================
# OBTENER SALUDO
# ============================================================

def obtener_saludo(
    perfil,
    momento="buen dia"
):

    if not perfil:

        return (
            "Buen día. "
            "¿Con quién tengo el gusto?"
        )


    tratamiento = perfil.get(

        "tratamiento",

        "señor"
    )


    nombre = perfil.get(

        "nombre",

        ""
    )


    if momento == "buenas tardes":

        saludo = "Buenas tardes"

    elif momento == "buenas noches":

        saludo = "Buenas noches"

    else:

        saludo = "Buen día"


    if nombre:

        return (
            f"{saludo}, "
            f"{tratamiento} "
            f"{nombre}."
        )


    return (
        f"{saludo}, "
        f"{tratamiento}."
    )


# ============================================================
# INFORMACIÓN DEL PERFIL
# ============================================================

def obtener_resumen_usuario(
    perfil
):

    if not perfil:

        return (
            "No hay ningún usuario activo."
        )


    nombre = perfil.get(
        "nombre",
        ""
    )


    tratamiento = perfil.get(
        "tratamiento",
        "señor"
    )


    preferencias = perfil.get(

        "preferencias",

        []
    )


    memorias = perfil.get(

        "memorias",

        []
    )


    informacion = perfil.get(

        "informacion_importante",

        []
    )


    resultado = (

        f"Usuario: "
        f"{tratamiento} "
        f"{nombre}.\n"
    )


    if preferencias:

        resultado += (

            "Preferencias: "

            +
            "; ".join(
                preferencias
            )

            +
            ".\n"
        )


    if informacion:

        resultado += (

            "Información importante: "

            +
            "; ".join(
                informacion
            )

            +
            ".\n"
        )


    if memorias:

        resultado += (

            "Memorias: "

            +
            "; ".join(
                memorias
            )

            +
            "."
        )


    return resultado


# ============================================================
# PRUEBA DIRECTA
# ============================================================

if __name__ == "__main__":

    print()
    print("=" * 60)
    print("       PRUEBA DEL SISTEMA DE USUARIOS EVID")
    print("=" * 60)
    print()


    usuarios = listar_usuarios()


    if not usuarios:

        print(
            "No hay usuarios registrados."
        )

        print()

        print(
            "Crearemos uno de prueba."
        )

        print()


        nombre = input(
            "Nombre: "
        ).strip()


        tratamiento = input(

            "¿Señor o señorita?: "

        ).strip().lower()


        if tratamiento not in [

            "señor",
            "señora",
            "señorita"

        ]:

            tratamiento = "señor"


        pin = getpass.getpass(

            "PIN (Enter para ninguno): "

        )


        if not pin:

            pin = None


        perfil = crear_usuario(

            nombre,

            tratamiento,

            pin
        )


        if perfil:

            print()

            print(
                "✅ Usuario creado correctamente."
            )

            print()

            print(
                obtener_resumen_usuario(
                    perfil
                )
            )


    else:

        print(
            "👤 Usuarios registrados:"
        )

        print()


        for usuario in usuarios:

            print(

                f"- "
                f"{usuario['nombre']} "
                f"({usuario['tratamiento']})"

            )


    print()

    print(
        "📁 Sistema de usuarios funcionando."
    )
