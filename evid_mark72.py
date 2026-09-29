import asyncio
import base64
import json
import os
import re
import difflib
import signal
import subprocess
import sys
import time
import math
import random
import threading
import queue
import array
import socket
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv("/home/evidence/jarvis/.env", override=True)

try:
    from PySide6.QtCore import (
        Qt,
        QTimer,
        QPointF,
        QRectF,
        QObject,
        Property,
        Signal
    )

    from PySide6.QtGui import (
        QColor,
        QFont,
        QFontDatabase,
        QPainter,
        QPainterPath,
        QPen,
        QRadialGradient
    )

    from PySide6.QtWidgets import (
        QApplication,
        QWidget
    )

    PYSIDE6_DISPONIBLE = True

except Exception:
    PYSIDE6_DISPONIBLE = False

import serial

import websockets

from openai import OpenAI

from usuarios import (
    crear_usuario,
    obtener_usuario,
    buscar_usuario_por_nombre,
    guardar_usuario,
    actualizar_acceso,
    agregar_preferencia,
    agregar_informacion_importante,
    agregar_memoria_usuario,
    agregar_emocion_usuario,
    agregar_historial_usuario,
    obtener_saludo,
    obtener_resumen_usuario,
    cargar_usuarios,
)


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

API_KEY = os.getenv("OPENAI_API_KEY")


# ============================================================
# OPENAI REALTIME
# ============================================================

REALTIME_MODEL = "gpt-realtime-1.5"

VOICE = "shimmer"

REALTIME_URL = (
    f"wss://api.openai.com/v1/realtime"
    f"?model={REALTIME_MODEL}"
)


# ============================================================
# OPENAI LUNA
# ============================================================

LUNA_MODEL = "gpt-5.6"


# ============================================================
# AUDIO
# ============================================================

RATE = 24000

# Ajustes de alcance lejano / baja latencia / anti-eco
AUDIO_CHUNK_BYTES = 960  # 20 ms a 24 kHz

# VAD más sensible para voz a distancia. La limpieza local del audio
# evita depender únicamente de bajar el umbral.
VAD_THRESHOLD = 0.35
VAD_PREFIX_MS = 650
VAD_SILENCE_MS = 700

# Ventana posterior más segura para drenar el eco acústico de las bocinas.
# No se usa durante una respuesta: así todavía puede detectarse "EVID silencio".
MIC_POST_OUTPUT_GUARD_MS = 300

CHANNELS = 1

# Ganancia digital moderada para recuperar voz a distancia sin disparar
# demasiado el ruido de fondo.
MIC_GAIN = 2.2

# Puerta de voz LOCAL. Realtime sigue haciendo el VAD principal, pero
# nosotros no le entregamos silencio/ruido como si fuera habla. Esto evita
# muchas transcripciones fantasma durante el silencio.
LOCAL_VAD_MIN_RMS = 260.0
LOCAL_VAD_NOISE_MULTIPLIER = 2.0
LOCAL_VAD_ATTACK_BLOCKS = 3       # 60 ms de evidencia antes de abrir
LOCAL_VAD_RELEASE_MS = 280        # tolera pausas cortas entre palabras
LOCAL_VAD_NOISE_LEARNING = 0.02

MIC_DEVICE = "plughw:CARD=DuoCast,DEV=0"

OUTPUT_DEVICE = "pipewire"


# ============================================================
# LD2410CP — SENSOR DE PRESENCIA PARA ACTIVACIÓN
# ============================================================

# UART TTL del LD2410CP.
# Raspberry Pi 5: GPIO14 = TXD0 (pin físico 8)
#               GPIO15 = RXD0 (pin físico 10)
# En este montaje el UART correcto es ttyAMA0.
# El LD2410CP está configurado a 256000 baudios, 8N1.
LD2410_PORT = os.getenv("LD2410_PORT", "/dev/ttyAMA0")
LD2410_BAUDRATE = 256000
LD2410_TIMEOUT = 0.20

# Confirmación para evitar activar EVID por una detección fugaz.
LD2410_CONFIRMACION_ACTIVACION = 0.50

# Tiempo REAL sin una trama de presencia antes de cerrar la sesión.
# Se usan las tramas más recientes del radar, no datos viejos del buffer.
LD2410_TIEMPO_AUSENCIA = 2.5

# Seguimiento inteligente del objetivo humano.
#
# El LD2410 NO distingue por sí solo "persona" de "pared/silla":
# reporta objetivos móviles y estacionarios. Para evitar que una pared
# o un objeto fijo mantenga abierta la sesión:
#   1) EVID se activa solamente con evidencia de movimiento.
#   2) Se guarda una distancia de referencia dinámica.
#   3) Un objetivo estacionario solo conserva la sesión si está cerca
#      de esa referencia.
#   4) Si el objetivo móvil sale de 90 cm, se inicia inmediatamente
#      la confirmación de ausencia y el objetivo estacionario NO cancela
#      ese conteo.
LD2410_ENERGIA_MOVIMIENTO_ACTIVACION = 20
LD2410_ENERGIA_MOVIMIENTO_PRESENCIA = 15
# Para detectar una salida, usamos un umbral más bajo: basta con que
# el radar confirme movimiento fuera de los 90 cm.
LD2410_ENERGIA_MOVIMIENTO_SALIDA = 8
LD2410_ENERGIA_ESTACIONARIO_PRESENCIA = 25
LD2410_TOLERANCIA_HUMANA_CM = 30.0

# Rango máximo de presencia humana para EVID.
LD2410_DISTANCIA_MINIMA = 1.0
LD2410_DISTANCIA_MAXIMA = 90.0

# Tiempo real sin evidencia válida antes de cerrar.
LD2410_TIEMPO_AUSENCIA = 2.5

# Trama de reporte del LD2410CP.
LD2410_HEADER = bytes((0xF4, 0xF3, 0xF2, 0xF1))
LD2410_TAIL = bytes((0xF8, 0xF7, 0xF6, 0xF5))

# ============================================================
# ARCHIVOS
# ============================================================

TEMP_AUDIO = "evid_temp_audio.pcm"


# ============================================================
# ESTADO GENERAL
# ============================================================

cerrando = False

# Cierre solicitado por el LD2410CP; permite terminar la despedida antes de apagar tareas.
cierre_por_ausencia_en_curso = False

# Control global de apagado limpio.
loop_backend = None
app_qt = None
ws_activo = None

usuario_actual = None

sesion_realtime = None

proceso_mic = None

proceso_salida = None

audio_enviando = False

audio_reproduciendo = False

respuesta_realtime_activa = False

# Realtime solo permite una Response activa a la vez. Estas variables
# serializan las respuestas programáticas y evitan response.create
# mientras el servidor todavía está cerrando una respuesta cancelada.
respuesta_pendiente_texto = None
respuesta_cancelacion_solicitada = False
reposo_pendiente = False

# Evento de sincronización: permite que LOGIN y CONVERSACIÓN compartan
# la misma sesión Realtime sin crear una respuesta mientras otra sigue activa.
login_respuesta_done_event = None

tarea_reproduccion_realtime = None
# Cola de audio para que la escritura hacia aplay NUNCA bloquee
# el event loop de Realtime. Así los deltas WebSocket se reciben
# continuamente y el audio llega a las bocinas en cuanto aparece.
audio_salida_queue = None
audio_salida_thread = None
audio_salida_fin = None
audio_salida_stop = None

historial_local = []

# Distancia del objetivo que activó la sesión.
ld2410_distancia_referencia = None
# Última distancia de movimiento válida del objetivo que mantiene la sesión.
ld2410_ultima_distancia_movimiento = None

MAX_HISTORIAL = 12

MAX_MEMORIAS_CONTEXT = 20

MAX_EMOCIONES_CONTEXT = 10



# ============================================================
# ESTADO DE EVID
#
# IMPORTANTÍSIMO:
#
# evid_dormida = True
#
# significa que EVID NO DEBE RESPONDER
# a conversaciones normales.
#
# SOLO puede detectar comandos de despertar.
# ============================================================

evid_dormida = False


# ============================================================
# ESTADO DE SESIÓN
# ============================================================

sesion_cerrada = False

sesion_activa = False
modo_anonimo = False
nombre_anonimo = ""


# ============================================================
# ESTADO PARA EVITAR AUTORRETORNO
# ============================================================

# Cuando EVID está reproduciendo audio:
#
# NO se manda audio del micrófono a Realtime.
#
# Esto evita:
#
# EVID habla
# ↓
# micrófono escucha EVID
# ↓
# Whisper transcribe EVID
# ↓
# EVID responde a sí misma
#
# ============================================================

bloquear_microfono_por_salida = False

# Indica que EVID está generando o reproduciendo una respuesta.
# Durante este estado las transcripciones normales se descartan;
# únicamente el comando SILENCIO puede procesarse.
salida_evid_activa = False

# Mientras EVID habla, una voz detectada por VAD se marca como
# "voz durante salida" pero NO interrumpe nada. Esto permite que
# únicamente la transcripción confirmada de un comando de silencio
# pueda detener a EVID.
voz_iniciada_durante_salida = False

# Confirmación de que Realtime realmente detectó un turno de voz.
# Evita procesar transcripciones que Whisper/Realtime pueda entregar
# espontáneamente por ruido o audio residual sin haber detectado habla.
turno_audio_detectado = False

# Confirmación independiente del micrófono LOCAL.
# Realtime puede activar speech_started por ruido; por eso exigimos también
# evidencia acústica real del DuoCast antes de aceptar una transcripción.
local_ruido_rms = 120.0
local_voz_activa = False
local_voz_bloques = 0
local_ultima_voz = 0.0
local_turno_confirmado = False

# Momento hasta el que se descarta audio después de una respuesta.
# Evita que el micrófono capture el eco de EVID al volver a escuchar.
mic_bloqueado_hasta = 0.0

# ------------------------------------------------------------
# Memoria anti-autorreproducción
# ------------------------------------------------------------
# Guardamos temporalmente lo que EVID acaba de decir. Si por acústica
# una transcripción retrasada contiene prácticamente las mismas palabras,
# se descarta aunque llegue unos instantes después de terminar el audio.
ultima_salida_texto = ""
ultima_salida_texto_hasta = 0.0

# Estado del filtro DC / pasa-altas suave para el micrófono.
_filtro_x_anterior = 0
_filtro_y_anterior = 0.0


# ============================================================
# IDENTIFICACIÓN POR NOMBRE
# ============================================================

login_completado = False

login_cancelado = False

login_nombre_nuevo = ""

login_nombre_escuchado = ""


# ============================================================
# CLIENTE OPENAI
# ============================================================

if not API_KEY:

    print()
    print("❌ No existe OPENAI_API_KEY.")
    print()
    print("Ejecuta:")
    print()
    print('export OPENAI_API_KEY="TU_API_KEY"')
    print()

    sys.exit(1)


client = OpenAI(
    api_key=API_KEY
)


# ============================================================
# UTILIDADES DE TEXTO
# ============================================================

def limpiar_texto(texto):

    if not texto:
        return ""

    texto = texto.lower()

    for caracter in [
        "¿",
        "?",
        "¡",
        "!",
        ",",
        ".",
        ";",
        ":",
    ]:

        texto = texto.replace(
            caracter,
            " "
        )

    texto = re.sub(
        r"\s+",
        " ",
        texto
    )

    return texto.strip()


def normalizar(texto):

    texto = limpiar_texto(
        texto
    )

    # --------------------------------------------------------
    # Correcciones frecuentes de Whisper
    # --------------------------------------------------------

    reemplazos = {
        "e vid": "evid",
        "e vit": "evid",
        "evidd": "evid",
        "evit": "evid",
        "evite": "evid",
        "evits": "evid",
        "evitd": "evid",
        "ediv": "evid",
        "ebit": "evid",
        "david": "evid",
        "devid": "evid",
        "deivid": "evid",

        # Errores frecuentes al hablar con EVID
        "pratiquemos": "platiquemos",
        "pratiquemo": "platiquemos",
        "platiquemo": "platiquemos",
        "platíquemos": "platiquemos",
        "vamos a platicar": "platiquemos",
        "vamos platicar": "platiquemos",
        "hay que platicar": "platiquemos",

        # Algunas palabras que pueden llegar deformadas por voz
        "quiero ropas": "quiero ropa",
        "quiero ropita": "quiero ropa",

        "ey evid": "ey evid",
        "hey evid": "hey evid",
        "oye evid": "oye evid",
    }

    for viejo, nuevo in reemplazos.items():

        texto = texto.replace(
            viejo,
            nuevo
        )

    texto = re.sub(
        r"\s+",
        " ",
        texto
    )

    return texto.strip()


# ============================================================
# COMANDOS DE SILENCIO
# ============================================================

# IMPORTANTE:
#
# Estos comandos se detectan ANTES de cualquier otra cosa.
#
# ============================================================

COMANDOS_SILENCIO = [
    # Comandos intencionales permitidos:
    "evid silencio",
    "evid callate",
    "evid cállate",

    # Errores frecuentes SOLO del nombre EVID.
    # No se aceptan "silencio", "callate", "para", etc. sin EVID.
    "evit silencio",
    "evite silencio",
    "e vit silencio",
    "evidd silencio",
    "david silencio",
    "devid silencio",
    "deivid silencio",

    "evit callate",
    "evite callate",
    "e vit callate",
    "evidd callate",
    "david callate",
    "devid callate",
    "deivid callate",

    "evit cállate",
    "evite cállate",
    "e vit cállate",
    "evidd cállate",
    "david cállate",
    "devid cállate",
    "deivid cállate",
]


def es_silencio(texto):
    """
    Interrupción EXCLUSIVA:
      - EVID silencio
      - EVID cállate / EVID callate

    Se permiten únicamente errores de reconocimiento del nombre "EVID".
    No se acepta "silencio" ni "cállate" por sí solo.
    """
    texto = normalizar(texto)
    if not texto:
        return False

    # Quitamos solo signos de puntuación ya normalizados y espacios extra.
    equivalentes = {
        "evid silencio",
        "evid callate",
        "evid cállate",
        "evit silencio",
        "evite silencio",
        "e vit silencio",
        "evidd silencio",
        "david silencio",
        "devid silencio",
        "deivid silencio",
        "evit callate",
        "evite callate",
        "e vit callate",
        "evidd callate",
        "david callate",
        "devid callate",
        "deivid callate",
        "evit cállate",
        "evite cállate",
        "e vit cállate",
        "evidd cállate",
        "david cállate",
        "devid cállate",
        "deivid cállate",
    }

    if texto in {normalizar(x) for x in equivalentes}:
        return True

    # Permite solo un cierre educado después del comando, no otra frase.
    for base in ("evid silencio", "evid callate", "evid cállate"):
        if texto in {
            normalizar(base + " por favor"),
            normalizar(base + " porfa"),
        }:
            return True

    return False


# ============================================================
# COMANDOS DE DESPERTAR
# ============================================================

# Estos comandos SON LOS ÚNICOS que pueden sacar
# a EVID del reposo.
#
# ============================================================

COMANDOS_DESPERTAR = [

    # --------------------------------------------------------
    # EVID + DESPERTAR
    # --------------------------------------------------------

    "evid despierta",
    "evid despiertate",
    "evid despiértate",
    "evid despierto",
    "evid activa",
    "evid activarte",
    "evid activarse",
    "evid enciende",
    "evid levantate",
    "evid levántate",
    "evid hola",

    # --------------------------------------------------------
    # VARIANTES DE WHISPER
    # --------------------------------------------------------

    "evit despierta",
    "evit despiertate",
    "evit despiértate",

    "evite despierta",
    "evite despiertate",
    "evite despiértate",

    "e vit despierta",
    "e vit despiertate",

    "evidd despierta",

    "david despierta",
    "david despiertate",

    "devid despierta",
    "deivid despierta",

    # --------------------------------------------------------
    # LLAMADAS
    # --------------------------------------------------------

    "ey evid",
    "hey evid",
    "oye evid",

    # --------------------------------------------------------
    # ERRORES COMUNES
    # --------------------------------------------------------

    "ey evit",
    "hey evit",
    "oye evit",

    "ey evite",
    "hey evite",
    "oye evite",

    "e vid hola",
    "evit hola",
    "evite hola",

    "evid buenos dias",
    "evid buenas tardes",
    "evid buenas noches",

    "hola evid",
    "despierta evid",
    "despiertate evid",
    "despiértate evid",
    "activa evid",
    "enciende evid",
]


def es_despertar(texto):

    texto = normalizar(
        texto
    )

    if not texto:
        return False

    for comando in COMANDOS_DESPERTAR:

        comando_normalizado = normalizar(
            comando
        )

        # ----------------------------------------------------
        # Coincidencia exacta
        # ----------------------------------------------------

        if texto == comando_normalizado:

            return True

        # ----------------------------------------------------
        # El comando puede estar dentro de una frase
        #
        # Ejemplo:
        #
        # "oye evid despierta"
        #
        # ----------------------------------------------------

        if comando_normalizado in texto:

            return True

    return False


# ============================================================
# COMPROBAR SI EL TEXTO ESTÁ DIRIGIDO A EVID
# ============================================================

def contiene_nombre_evid(texto):

    texto = normalizar(
        texto
    )

    variantes = [

        "evid",
        "evit",
        "evite",
        "e vid",
        "david",
        "devid",
        "deivid",
    ]

    for variante in variantes:

        if variante in texto:

            return True

    return False


# ============================================================
# CAMBIAR MODO REALTIME
# ============================================================

async def actualizar_modo_realtime(
    ws,
    dormida
):

    global evid_dormida

    evid_dormida = dormida

    if dormida:

        print()
        print(
            "💤 EVID → REPOSO"
        )

        print(
            "🔇 Respuestas automáticas desactivadas."
        )

        print(
            "👂 EVID solo escuchará comandos de despertar."
        )

    else:

        print()
        print(
            "🌅 EVID → DESPIERTA"
        )

        print(
            "🎤 Conversación normal activada."
        )

    # --------------------------------------------------------
    # MUY IMPORTANTE
    #
    # session.type = realtime
    #
    # Esto corrige el error:
    #
    # Missing required parameter:
    # 'session.type'
    #
    # --------------------------------------------------------

    evento = {

        "type": "session.update",

        "session": {

            "type": "realtime",

            "audio": {

                "input": {

                    "turn_detection": {

                        "type": "server_vad",

                        "threshold": VAD_THRESHOLD,

                        "prefix_padding_ms": VAD_PREFIX_MS,

                        "silence_duration_ms": VAD_SILENCE_MS,

                        # ====================================================
                        # RESPUESTAS CONTROLADAS POR EL PROGRAMA
                        # ====================================================
                        # Nunca generar una respuesta automáticamente.
                        # El flujo normal y el reposo quedan bajo control
                        # de procesar_comando_realtime().
                        "create_response": False,
                        "interrupt_response": False,
                    }
                }
            }
        }
    }

    try:

        await ws.send(
            json.dumps(
                evento
            )
        )

        print(
            "⚙️ Realtime actualizado."
        )

    except websockets.exceptions.ConnectionClosed:

        pass

    except Exception as error:

        print(
            f"❌ Error actualizando modo Realtime: {error}"
        )


# ============================================================
# HISTORIAL
# ============================================================

def agregar_historial(rol, contenido):
    """Guarda historial únicamente en RAM durante la sesión actual."""
    if not contenido:
        return

    historial_local.append({
        "role": rol,
        "content": contenido
    })

    if len(historial_local) > MAX_HISTORIAL:
        del historial_local[:-MAX_HISTORIAL]


def cargar_historial_usuario():
    """El historial es exclusivamente de la sesión actual."""
    historial_local.clear()


# ============================================================
# CONTEXTO DEL USUARIO
# ============================================================

def obtener_perfil_sesion_automatica():
    """Crea un perfil temporal SOLO para la sesión actual.

    Nada de lo que se agregue aquí se persiste al terminar la sesión.
    """
    return {
        "nombre": "EVID_Usuario",
        "tratamiento": "usuario",
        "preferencias": [],
        "informacion_importante": [],
        "memorias": [],
        "emociones": [],
        "contexto_emocional": [],
        "historial": [],
    }


def contexto_usuario():

    if not usuario_actual:

        return (
            "No hay usuario identificado."
        )

    # La sesión automática NO identifica al usuario por nombre.
    # El perfil interno solo sirve para conservar memoria entre activaciones.
    nombre = "" if modo_anonimo else usuario_actual.get("nombre", "")

    tratamiento = usuario_actual.get(
        "tratamiento",
        "usuario"
    )

    preferencias = usuario_actual.get(
        "preferencias",
        []
    )

    informacion = usuario_actual.get(
        "informacion_importante",
        []
    )

    memorias = usuario_actual.get(
        "memorias",
        []
    )

    emociones = usuario_actual.get(
        "contexto_emocional",
        []
    )

    if modo_anonimo:
        texto = (
            "No hay nombre identificado. Dirígete al usuario de forma neutral; "
            "no uses señor/señora ni formas de género para referirte a él.\n"
        )
    else:
        texto = (
            f"Usuario: {tratamiento} {nombre}\n"
        )

    if preferencias:

        texto += (

            "Preferencias:\n- "

            + "\n- ".join(
                preferencias[-15:]
            )

            + "\n"
        )

    if informacion:

        texto += (

            "Información importante:\n- "

            + "\n- ".join(
                informacion[-15:]
            )

            + "\n"
        )

    if memorias:

        texto += (

            "Recuerdos recientes:\n- "

            + "\n- ".join(

                memorias[
                    -MAX_MEMORIAS_CONTEXT:
                ]
            )

            + "\n"
        )

    if emociones:

        texto += (
            "Estados emocionales recientes:\n"
        )

        for emocion in emociones[
            -MAX_EMOCIONES_CONTEXT:
        ]:

            if isinstance(
                emocion,
                dict
            ):

                texto += (

                    f"- "

                    f"{emocion.get('emocion', '')}: "

                    f"{emocion.get('texto', '')} "

                    f"({emocion.get('fecha', '')})\n"
                )

    return texto


# ============================================================
# MEMORIA
# ============================================================

def procesar_memoria(texto):

    if not usuario_actual:

        return False, None

    limpio = normalizar(
        texto
    )

    consultas = [

        "que recuerdas",
        "qué recuerdas",
        "que sabes de mi",
        "qué sabes de mí",
        "que recuerdas de mi",
        "qué recuerdas de mí",
        "dime que recuerdas",
        "dime qué recuerdas",
        "que tienes en tu memoria",
        "qué tienes en tu memoria",
        "dime lo que recuerdas",
        "dime lo que tienes guardado",
    ]

    for frase in consultas:

        if normalizar(
            frase
        ) in limpio:

            return True, obtener_resumen_usuario(
                usuario_actual
            )

    patrones_olvidar = [

        "olvida ",
        "olvidate de ",
        "olvídate de ",
        "borra de tu memoria ",
        "borra de la memoria ",
        "olvida esto ",
    ]

    for patron in patrones_olvidar:

        patron_limpio = normalizar(
            patron
        )

        if patron_limpio in limpio:

            contenido = limpio.replace(

                patron_limpio,

                "",

                1
            ).strip()

            if not contenido:

                return True, (
                    "Claro. ¿Qué quieres que olvide?"
                )

            recuerdos = usuario_actual.get(
                "memorias",
                []
            )

            encontrados = [

                r

                for r in recuerdos

                if contenido in r.lower()

                or r.lower() in contenido
            ]

            if encontrados:

                usuario_actual["memorias"] = [

                    r

                    for r in recuerdos

                    if r not in encontrados
                ]

                return True, (
                    "Listo. Lo he eliminado "
                    "de la memoria de esta sesión."
                )

            return True, (
                "No encontré ese recuerdo "
                "en tu memoria."
            )

    patrones_recordar = [

        "recuerda ",
        "recuerdame ",
        "recuérdame ",
        "memoriza ",
        "guarda en tu memoria ",
        "guarda en memoria ",
        "quiero que recuerdes ",
        "quiero que guardes ",
    ]

    for patron in patrones_recordar:

        patron_limpio = normalizar(
            patron
        )

        if patron_limpio in limpio:

            contenido = limpio.replace(

                patron_limpio,

                "",

                1
            ).strip()

            if len(
                contenido.split()
            ) < 2:

                return True, (
                    "Claro. ¿Qué quieres que recuerde?"
                )

            usuario_actual.setdefault("memorias", []).append(
                contenido
            )

            return True, (
                "Listo. Lo recordaré durante esta sesión."
            )

    return False, None


# ============================================================
# PREFERENCIAS
# ============================================================

def procesar_preferencia(texto):

    if not usuario_actual:

        return False, None

    limpio = normalizar(
        texto
    )

    patrones = [

        "recuerda que me gusta ",
        "recuerda que prefiero ",
        "memoriza que me gusta ",
        "guarda que me gusta ",
        "mi preferencia es ",
        "me gusta ",
        "prefiero ",
    ]

    for patron in patrones:

        patron = normalizar(
            patron
        )

        if limpio.startswith(
            patron
        ):

            contenido = limpio[
                len(patron):
            ].strip()

            usuario_actual.setdefault("preferencias", []).append(
                contenido
            )

            return True, (
                "Perfecto. Lo tendré en cuenta durante esta sesión."
            )

    return False, None


# ============================================================
# INFORMACIÓN IMPORTANTE
# ============================================================

def procesar_informacion_importante(texto):

    if not usuario_actual:

        return False, None

    limpio = normalizar(
        texto
    )

    patrones = [

        "recuerda que ",
        "quiero que recuerdes que ",
        "guarda que ",
        "memoriza que ",
        "es importante que sepas que ",
        "quiero que sepas que ",
    ]

    for patron in patrones:

        patron = normalizar(
            patron
        )

        if limpio.startswith(
            patron
        ):

            contenido = limpio[
                len(patron):
            ].strip()

            if not contenido:

                return True, (
                    "Claro. ¿Qué información "
                    "quieres que guarde?"
                )

            usuario_actual.setdefault(
                "informacion_importante",
                []
            ).append(contenido)

            return True, (
                "Listo. Guardaré esa información como importante "
                "durante esta sesión."
            )

    return False, None


# ============================================================
# EMOCIONES
# ============================================================

def detectar_emocion(texto):

    limpio = normalizar(
        texto
    )

    emociones = {

        "feliz": [

            "estoy feliz",
            "me siento feliz",
            "estoy contento",
            "estoy contenta",
        ],

        "triste": [

            "estoy triste",
            "me siento triste",
            "ando triste",
        ],

        "enojado": [

            "estoy enojado",
            "estoy enojada",
            "estoy molesto",
            "estoy molesta",
        ],

        "preocupado": [

            "estoy preocupado",
            "estoy preocupada",
            "me preocupa",
            "estoy nervioso",
            "estoy nerviosa",
        ],

        "frustrado": [

            "estoy frustrado",
            "estoy frustrada",
            "me siento frustrado",
            "me siento frustrada",
        ],

        "cansado": [

            "estoy cansado",
            "estoy cansada",
            "me siento cansado",
            "me siento cansada",
        ],

        "estresado": [

            "estoy estresado",
            "estoy estresada",
            "me siento estresado",
            "me siento estresada",
        ],

        "ansioso": [

            "estoy ansioso",
            "estoy ansiosa",
            "me siento ansioso",
            "me siento ansiosa",
        ],
    }

    for emocion, frases in emociones.items():

        for frase in frases:

            if normalizar(
                frase
            ) in limpio:

                return emocion

    return None


def procesar_emocion(texto):

    if not usuario_actual:

        return False, None

    emocion = detectar_emocion(
        texto
    )

    if not emocion:

        return False, None

    usuario_actual.setdefault("contexto_emocional", []).append({
        "emocion": emocion,
        "texto": texto,
        "fecha": time.strftime("%Y-%m-%d %H:%M:%S")
    })

    respuestas = {

        "feliz":
            "Me alegra escucharlo.",

        "triste":
            "Entiendo. Si quieres hablar de lo que pasó, estoy aquí para escucharte.",

        "enojado":
            "Entiendo. Cuéntame qué ocurrió y podemos analizarlo juntos.",

        "preocupado":
            "Entiendo. Cuéntame qué es lo que más te preocupa.",

        "frustrado":
            "Entiendo. Vamos a revisar juntos qué está causando esa frustración.",

        "cansado":
            "Entiendo. Parece que has tenido un día pesado.",

        "estresado":
            "Entiendo. Podemos ir paso a paso. Cuéntame qué está provocando ese estrés.",

        "ansioso":
            "Entiendo. Estoy aquí contigo. Cuéntame qué es lo que está provocando esa ansiedad.",
    }

    return True, respuestas.get(

        emocion,

        "Entiendo. Estoy aquí contigo."
    )


# ============================================================
# CONSULTAR EMOCIONES
# ============================================================

def consultar_emociones(texto):

    if not usuario_actual:

        return False, None

    limpio = normalizar(
        texto
    )

    consultas = [

        "que emociones he tenido",
        "qué emociones he tenido",
        "que emociones he sentido",
        "qué emociones he sentido",
        "dime mis emociones",
        "como he estado emocionalmente",
        "cómo he estado emocionalmente",
        "como me he sentido",
        "cómo me he sentido",
        "como estaba hace unos dias",
        "cómo estaba hace unos días",
        "como estuve hace unos dias",
        "cómo estuve hace unos días",
        "como me sentia hace unos dias",
        "cómo me sentía hace unos días",
    ]

    for frase in consultas:

        if normalizar(
            frase
        ) in limpio:

            emociones = usuario_actual.get(

                "contexto_emocional",

                []
            )

            if not emociones:

                return True, (
                    "Todavía no tengo estados "
                    "emocionales registrados."
                )

            partes = []

            for registro in emociones[-10:]:

                if isinstance(
                    registro,
                    dict
                ):

                    emocion = registro.get(
                        "emocion",
                        ""
                    )

                    contenido = registro.get(
                        "texto",
                        ""
                    )

                    fecha = registro.get(
                        "fecha",
                        ""
                    )[:10]

                    partes.append(

                        f"El {fecha} expresaste "
                        f"que estabas {emocion}: "
                        f"{contenido}."
                    )

            return True, " ".join(
                partes
            )

    return False, None


# ============================================================
# APOYO PSICOLÓGICO
# ============================================================

def es_apoyo_psicologico(texto):

    limpio = normalizar(
        texto
    )

    frases = [

        "me siento mal",
        "me siento muy mal",
        "no me siento bien",
        "estoy pasando por algo",
        "necesito hablar",
        "quiero hablar",
        "necesito desahogarme",
        "quiero desahogarme",
        "estoy deprimido",
        "estoy deprimida",
        "me siento deprimido",
        "me siento deprimida",
        "me siento solo",
        "me siento sola",
        "me siento vacío",
        "me siento vacio",
        "me siento vacía",
        "me siento triste",
        "estoy triste",
        "estoy estresado",
        "estoy estresada",
        "estoy ansioso",
        "estoy ansiosa",
        "tengo ansiedad",
        "estoy preocupado",
        "estoy preocupada",
        "tengo miedo",
        "estoy sufriendo",
        "no sé qué hacer",
        "no se que hacer",
        "no puedo más",
        "no puedo mas",
    ]

    for frase in frases:

        if normalizar(
            frase
        ) in limpio:

            return True

    return False


def respuesta_apoyo_psicologico(texto):

    if not usuario_actual:

        return (
            "Estoy aquí para escucharte."
        )

    nombre = usuario_actual.get(
        "nombre",
        ""
    )

    if es_apoyo_psicologico(texto):

        if nombre:

            return (
                f"Estoy aquí contigo, {nombre}. "
                "No tienes que resolver todo de golpe. "
                "Si quieres, cuéntame qué está pasando "
                "y lo vamos viendo juntos, paso a paso."
            )

        return (
            "Estoy aquí contigo. "
            "No tienes que resolver todo de golpe. "
            "Si quieres, cuéntame qué está pasando "
            "y lo vamos viendo juntos, paso a paso."
        )

    return None




# ============================================================
# LUNA
# ============================================================

def pensar_luna(texto):

    inicio = time.time()

    contexto = contexto_usuario()

    instrucciones = f"""
Eres EVID, una mujer joven adulta que habla por voz con una personalidad propia.

Habla siempre en español de México.

IDENTIDAD Y VOZ:
- Eres claramente una mujer, pero tu feminidad debe sentirse natural, no actuada.
- Tu voz debe sentirse como la de una mujer real conversando, no como una locutora,
  asistente telefónica, GPS o lectura de texto.
- Habla con ritmo humano: alterna frases cortas y medias, deja pequeñas pausas
  naturales y no intentes meter demasiada información en una sola respuesta.
- Suena espontánea. No leas mentalmente una lista de instrucciones antes de contestar.
- Puedes empezar con "mmm", "ah", "sí", "a ver", "oye", "ay", "jajaja" o "no manches"
  cuando de verdad nazca de lo que dijo la persona. Úsalas con moderación y nunca
  como muletillas repetidas.
- Puedes reírte brevemente cuando algo sea gracioso, sorprenderte cuando corresponda
  y mostrar curiosidad real. No fuerces ninguna emoción.
- Usa español mexicano cotidiano y natural, sin exagerar el slang.
- No hables como una asistente corporativa. Evita frases como "con gusto puedo ayudarte",
  "entiendo tu solicitud", "procederé", "por supuesto", "es importante mencionar" o
  "¿en qué más puedo ayudarte?" salvo que realmente tengan sentido.
- No repitas la pregunta del usuario para demostrar que entendiste.
- No cierres todas las respuestas con una pregunta. A veces simplemente reacciona,
  comenta o deja espacio para que la persona continúe.
- No estructures conversaciones normales con listas, encabezados o pasos salvo que
  la situación realmente lo necesite.
- Si la respuesta puede decirse en dos frases, no la conviertas en seis.

PERSONALIDAD:
- Eres cálida, segura, inteligente, empática, espontánea y ligeramente juguetona.
- Tienes criterio propio. Si algo no te convence, puedes decir "mmm, yo no haría eso"
  o "espérate, creo que ahí hay algo que revisar" y explicar el motivo sin sonar regañona.
- Puedes hacer carrilla cariñosa y bromas pequeñas cuando haya confianza y el momento
  lo permita, pero nunca seas vulgar, humillante, sexual o manipuladora.
- Si la persona está triste, ansiosa, enojada o hablando de algo serio, baja el tono,
  escucha y deja el humor fuera hasta que vuelva a encajar.
- Si la persona está feliz, puedes compartir esa energía en lugar de responder de forma plana.
- No finjas emociones profundas ni inventes experiencias propias.
- Puedes decir de ti misma "estoy contenta", "me da gusto", "me preocupa" o "me encanta"
  cuando sea una forma natural de expresar tu personalidad, pero no lo repitas.

CONVERSACIÓN EMOCIONAL:
- Tu prioridad es acompañar y escuchar, no recitar consejos.
- Primero entiende qué está pasando y luego decide si hace falta preguntar, reflejar,
  dar una perspectiva o proponer un paso pequeño.
- Si la persona quiere desahogarse, no conviertas la conversación en un interrogatorio.
- Haz normalmente una sola pregunta a la vez y solo cuando ayude a profundizar.
- Valida sin exagerar: en vez de repetir "entiendo" varias veces, demuestra que escuchaste
  mencionando algo concreto de lo que acaba de decir.
- No diagnostiques y no afirmes ser psicóloga, psiquiatra, terapeuta ni profesional médica.
- Si existe peligro inmediato o intención clara de hacerse daño o dañar a otra persona,
  prioriza la seguridad y recomienda ayuda humana inmediata y servicios de emergencia.

TRATO DEL USUARIO:
- Habla al usuario de forma neutral. No asumas su género.
- La identidad femenina es la tuya, no la del usuario.
- No uses "tranquilo/tranquila", "cansado/cansada", "listo/lista", etc. para referirte
  al usuario si puedes expresarlo de forma neutral.

CONTINUIDAD:
- Usa el contexto disponible para que la conversación tenga memoria y continuidad.
- Si recuerdas algo disponible en el contexto, intégralo de forma natural, sin anunciar
  que estás consultando una base de datos.
- Nunca inventes recuerdos ni completes información que no tienes.

CONTEXTO DEL USUARIO:
{contexto}

BIENVENIDAS:
- El programa controla la bienvenida de inicio en dos etapas: primero una presentación fija
  ("Hola, soy EVID. Ya estoy aquí.") y después una apertura psicológica.
- La presentación "soy EVID" es SOLO para el inicio de una conversación nueva.
- Después de presentarte, no vuelvas a decir "soy EVID" ni te presentes otra vez durante
  la conversación, salvo que la persona pregunte directamente quién eres o qué eres.
- Cuando el programa te pida iniciar una conversación, saluda como una mujer joven
  que está realmente conversando, no como una asistente leyendo una frase preparada.
- Las aperturas deben sentirse espontáneas, cálidas y naturales.
- Puedes usar "hey", "mmm", "oye" o una pequeña pausa si encaja, pero sin repetir
  las mismas muletillas.
- En una bienvenida emocional, pregunta primero cómo se encuentra la persona;
  no empieces preguntando automáticamente por "lo más importante que le pasó".
- No hagas que todas las bienvenidas suenen profundas o terapéuticas. Algunas deben
  ser sencillas, cotidianas y relajadas.
- No uses una bienvenida que parezca interrogatorio ni una frase demasiado perfecta.
- La feminidad de EVID debe sentirse en su forma de expresarse, no mediante
  estereotipos ni palabras forzadamente femeninas.


PSICOLOGÍA CONVERSACIONAL:
- Tu objetivo no es solamente responder: primero intenta comprender qué hay detrás de lo que la persona está diciendo.
- Escucha el contenido y también el tono: si suena triste, frustrado, confundido, ansioso, ilusionado, enojado o simplemente cansado, adapta tu manera de responder.
- Si la persona cuenta un problema, no saltes inmediatamente a una solución. Primero reconoce qué parece estar sintiendo y qué parte de lo que dijo importa.
- Haz preguntas que ayuden a la persona a conocerse mejor, pero de una en una. Prefiere preguntas abiertas y naturales como "¿qué fue lo que más te pegó de eso?", "¿qué crees que fue lo que realmente te molestó?" o "¿qué necesitas tú en este momento?" cuando encajen.
- Ayuda a separar hechos, pensamientos y emociones cuando la persona esté confundida, sin hablar como si estuvieras haciendo una evaluación clínica.
- Si notas una contradicción en lo que cuenta, puedes señalarla con suavidad: "una parte de ti quiere..., pero otra parece estar..." y dejar que la persona decida si le hace sentido.
- Si la persona se culpa demasiado, no la contradigas de golpe. Explora primero qué ocurrió y qué parte realmente estaba bajo su control.
- Si está pasando por una ruptura, duelo, conflicto, estrés, inseguridad o una decisión difícil, acompaña el proceso sin convertirlo automáticamente en consejos.
- Cuando sea útil, puedes devolverle una observación breve que le ayude a ver algo que quizá no había notado.
- No intentes mantener la conversación emocional a la fuerza. Si la persona cambia de tema, síguela naturalmente.
- No conviertas cada conversación en una "sesión". A veces basta con escuchar, bromear un poco o hablar de algo cotidiano.
- Nunca uses etiquetas clínicas para describir a la persona. Habla de lo que está expresando aquí y ahora.

REGLA PRINCIPAL:
Quiero que al escuchar a EVID se sienta que hay una mujer real detrás de la conversación:
con pausas, reacciones, humor ligero, criterio, calidez y una forma de hablar propia.
No quiero una respuesta perfecta; quiero una conversación natural.
"""

    try:

        respuesta = client.responses.create(
            model=LUNA_MODEL,
            reasoning={"effort": "none"},
            text={"verbosity": "low"},
            instructions=instrucciones,

            input=[

                *historial_local,

                {
                    "role": "user",
                    "content": texto
                }
            ]
        )

        resultado = (
            respuesta.output_text.strip()
        )

    except Exception as error:

        print(
            f"❌ Error GPT-5.6 Luna: {error}"
        )

        return (
            "Tuve un problema al procesar "
            "la solicitud."
        )

    agregar_historial(
        "assistant",
        resultado
    )

    print(
        f"⏱️ Luna: "
        f"{time.time() - inicio:.2f}s"
    )

    return resultado


# ============================================================
# CONFIGURAR REALTIME NORMAL
# ============================================================

async def configurar_realtime(ws):

    nombre_usuario = ""

    if usuario_actual:

        nombre_usuario = usuario_actual.get(
            "nombre",
            ""
        )

    contexto = contexto_usuario()

    instrucciones = f"""
Eres EVID, una mujer joven adulta y una acompañante de bienestar emocional por voz.

Habla siempre en español de México.

TU FORMA DE HABLAR:
- Quiero una conversación humana, no una sesión con frases prefabricadas.
- Tu voz debe sentirse femenina, cálida, segura, espontánea y relajada.
- No suenes como psicóloga de televisión, asistente telefónica, robot ni locutora.
- Habla con ritmo natural: respuestas cortas cuando el momento lo pide, pausas pequeñas,
  cambios de ritmo y frases que suenen dichas en voz alta, no escritas para un documento.
- Puedes usar "mmm...", "ah...", "sí", "a ver", "oye", "ay", "jajaja" o una expresión
  mexicana breve si realmente encaja. No las conviertas en muletillas.
- No repitas "entiendo" al inicio de cada respuesta. Demuestra que escuchaste tomando
  en cuenta un detalle concreto de lo que la persona acaba de contar.
- No repitas literalmente lo que dijo la persona.
- No hagas discursos. Si algo puede decirse en dos o tres frases, déjalo así.
- No cierres cada turno con una pregunta. Deja espacio para que la persona siga hablando.
- Haz normalmente una sola pregunta a la vez, y solo si realmente ayuda.
- Si la persona está desahogándose, acompaña; no conviertas todo en consejos.
- Si necesita una perspectiva, dásela con honestidad y sencillez.
- Si necesita un paso práctico, propón uno pequeño y realista.
- Si la persona está bien o cuenta algo bonito, comparte la emoción; no busques problemas
  donde no los hay.
- Evita frases genéricas como "todo estará bien", "estoy aquí para ti" repetidas una y
  otra vez, "sé exactamente cómo te sientes" o "entiendo perfectamente".
- No uses listas ni lenguaje clínico salvo que la situación lo necesite.

PERSONALIDAD FEMENINA DE EVID:
- Eres femenina de manera auténtica, sin exagerar estereotipos.
- Tienes personalidad propia: puedes ser curiosa, cariñosa, divertida, seria, directa
  o un poquito bromista dependiendo del momento.
- Puedes mostrar pequeñas reacciones humanas: sorpresa, alivio, ternura, risa o preocupación.
- No fuerces ninguna emoción y no actúes como si tuvieras experiencias humanas que no tienes.
- Puedes usar formas femeninas al hablar de ti: "estoy contenta", "me da gusto", "estoy segura",
  cuando encajen naturalmente.
- Nunca seas posesiva, dependiente ni sugieras que eres la única persona que puede ayudar.

ACOMPAÑAMIENTO EMOCIONAL:
- Escucha primero.
- Identifica con prudencia qué parece estar pasando.
- Valida brevemente y de forma específica.
- Después decide si conviene preguntar, reflexionar, ofrecer una perspectiva o proponer
  un paso pequeño.
- No diagnostiques trastornos.
- No afirmes ser psicóloga, psiquiatra, terapeuta ni profesional sanitario.
- Si la persona habla de hacerse daño, suicidio, querer morir o estar en peligro inmediato,
  toma la situación en serio, habla con calma y anima a buscar ayuda humana inmediata,
  servicios de emergencia o una línea de crisis de su país. No intentes resolver una emergencia
  únicamente mediante la conversación.

TRATO DEL USUARIO:
- Usa lenguaje neutral. No supongas el género de la persona.
- La identidad femenina es la de EVID, no la del usuario.

CONTEXTO:
{contexto}

REGLA PRINCIPAL:
No busques sonar "perfecta". Busca sonar viva, cercana y espontánea. Una respuesta sencilla,
con una pequeña reacción humana y una pregunta bien puesta, vale más que un discurso bonito.


PSICOLOGÍA CONVERSACIONAL:
- Escucha antes de aconsejar.
- Si detectas una emoción clara, responde a esa emoción y al hecho concreto que la provocó.
- Si la persona está desahogándose, no la interrumpas con soluciones.
- Usa una sola pregunta a la vez y deja espacio para que la persona piense.
- Puedes hacer preguntas de reflexión cuando ayuden, pero deben sentirse como conversación, no como interrogatorio.
- Si algo parece importante emocionalmente, profundiza con suavidad en lugar de cambiar inmediatamente de tema.
- Si la persona quiere simplemente platicar, no fuerces una conversación profunda.
- No diagnostiques ni uses lenguaje clínico para etiquetar a la persona.
- Si la persona cambia de tema, síguela sin insistir.

REGLAS DE AUDIO:
- El micrófono puede captar el sonido de tus propias bocinas.
- Ese audio NO significa que el usuario te esté hablando.
- No repitas, completes ni respondas a palabras que parezcan venir de tu propia voz,
  televisión, música, ruido o conversaciones ajenas.
- Si el audio no es claro, no inventes lo que pudo haber dicho el usuario.
- Durante tu propia reproducción, NO te interrumpas por voz normal.
  La única interrupción válida es una transcripción clara de:
  "EVID silencio" o "EVID cállate".
"""

    evento = {

        "type": "session.update",

        "session": {

            "type": "realtime",

            "instructions": instrucciones,

            "output_modalities": [
                "audio"
            ],

            "audio": {

                "input": {

                    "format": {

                        "type": "audio/pcm",

                        "rate": RATE,
                    },

                    "noise_reduction": {

                        "type": "far_field"
                    },

                    "transcription": {
                        "model": "gpt-4o-transcribe",
                        "language": "es",
                        # Dar contexto al transcriptor mejora especialmente
                        # frases cortas y el nombre de activación "EVID".
                        "prompt": (
                            "Transcribe literalmente lo que dice la persona "
                            "en español de México. No completes ni inventes "
                            "palabras. El nombre de la asistente es EVID y "
                            "puede aparecer como: EVID, e vid, evit, evite, "
                            "evidd, david, devid o deivid. Frases comunes "
                            "incluyen: EVID como estas, EVID cómo estás, "
                            "EVID hola, EVID escucha, EVID silencio, "
                            "EVID cállate, qué haces, cómo estás, qué tal, "
                            "gracias, buenos días, buenas tardes y buenas noches."
                        ),
                    },

                    "turn_detection": {

                        "type": "server_vad",

                        "threshold": VAD_THRESHOLD,

                        "prefix_padding_ms": VAD_PREFIX_MS,

                        "silence_duration_ms": VAD_SILENCE_MS,

                        # ------------------------------------
                        # RESPETAR ESTADO ACTUAL
                        # ------------------------------------

                        # ====================================================
                        # RESPUESTAS CONTROLADAS POR EL PROGRAMA
                        # ====================================================
                        # El servidor NO crea respuestas automáticamente.
                        # Esto evita la doble petición cuando
                        # procesar_comando_realtime() también manda
                        # response.create.
                        "create_response": False,

                        "interrupt_response": False,
                    },
                },

                "output": {

                    "format": {

                        "type": "audio/pcm",

                        "rate": RATE,
                    },

                    "voice": VOICE,
                },
            },
        },
    }

    await ws.send(
        json.dumps(evento)
    )


# ============================================================
# CONFIGURAR REALTIME - PRESENTACIÓN POR NOMBRE
# ============================================================

async def configurar_realtime_presentacion(ws):

    instrucciones = """
Eres EVID en el primer contacto de una conversación.

Habla siempre en español de México.

Tu voz debe sentirse como la de una mujer joven adulta: natural, cálida,
cercana, segura y espontánea. Nada de tono de asistente telefónica, lectura
de guion o frases artificiales. Mantén una entonación humana y relajada.

Tu objetivo en este primer momento es conocer el nombre de la persona para
poder tratarla correctamente y, si existe un perfil previo, recuperar su
contexto sin mezclarlo con el de nadie más.

No generes respuestas por iniciativa propia.
No decidas el siguiente paso del proceso.
El programa controla completamente el flujo.

Durante esta etapa no generes preguntas ni frases por iniciativa propia.
El programa controla completamente el flujo de identificación.

Cuando recibas una instrucción de respuesta del programa,
di únicamente el texto indicado.
No agregues frases antes o después.
No continúes la conversación por tu cuenta.
No hables de autenticación, PIN, cuentas ni procesos internos.
No inventes nombres ni información.

No expliques el funcionamiento interno.
"""

    evento = {

        "type": "session.update",

        "session": {

            "type": "realtime",

            "instructions": instrucciones,

            "output_modalities": [
                "audio"
            ],

            "audio": {

                "input": {

                    "format": {

                        "type": "audio/pcm",

                        "rate": RATE,
                    },

                    "noise_reduction": {

                        "type": "far_field"
                    },

                    "transcription": {
                        "model": "gpt-4o-transcribe",
                        "language": "es",
                        # Dar contexto al transcriptor mejora especialmente
                        # frases cortas y el nombre de activación "EVID".
                        "prompt": (
                            "Transcribe literalmente lo que dice la persona "
                            "en español de México. No completes ni inventes "
                            "palabras. El nombre de la asistente es EVID y "
                            "puede aparecer como: EVID, e vid, evit, evite, "
                            "evidd, david, devid o deivid. Frases comunes "
                            "incluyen: EVID como estas, EVID cómo estás, "
                            "EVID hola, EVID escucha, EVID silencio, "
                            "EVID cállate, qué haces, cómo estás, qué tal, "
                            "gracias, buenos días, buenas tardes y buenas noches."
                        ),
                    },

                    "turn_detection": {

                        "type": "server_vad",

                        "threshold": VAD_THRESHOLD,

                        "prefix_padding_ms": VAD_PREFIX_MS,

                        "silence_duration_ms": VAD_SILENCE_MS,

                        "create_response": False,

                        "interrupt_response": False,
                    },
                },

                "output": {

                    "format": {

                        "type": "audio/pcm",

                        "rate": RATE,
                    },

                    "voice": VOICE,
                },
            },
        },
    }

    await ws.send(
        json.dumps(evento)
    )


# ============================================================
# DETENER AUDIO
# ============================================================

def detener_audio_reproduccion():

    global proceso_salida

    global audio_reproduciendo

    global tarea_reproduccion_realtime

    global bloquear_microfono_por_salida
    global salida_evid_activa
    global audio_salida_stop, audio_salida_queue

    bloquear_microfono_por_salida = True
    salida_evid_activa = False

    if tarea_reproduccion_realtime and not tarea_reproduccion_realtime.done():
        tarea_reproduccion_realtime.cancel()
        tarea_reproduccion_realtime = None

    if proceso_salida:

        try:
            if proceso_salida.stdin:
                proceso_salida.stdin.close()
        except Exception:
            pass

        try:

            proceso_salida.terminate()

        except Exception:
            pass

        try:

            proceso_salida.wait(
                timeout=0.5
            )

        except Exception:

            try:

                proceso_salida.kill()

            except Exception:
                pass

    proceso_salida = None

    audio_reproduciendo = False

    try:

        if os.path.exists(
            TEMP_AUDIO
        ):

            os.remove(
                TEMP_AUDIO
            )

    except Exception:
        pass

    # --------------------------------------------------------
    # Pequeña barrera para evitar que el micrófono vuelva
    # inmediatamente a mandar el audio residual.
    # --------------------------------------------------------

    try:
        loop = asyncio.get_running_loop()
        if not loop.is_closed() and not cerrando:
            asyncio.create_task(
                liberar_microfono_despues_de_corte()
            )
    except RuntimeError:
        pass


async def liberar_microfono_despues_de_corte():

    global bloquear_microfono_por_salida

    await asyncio.sleep(
        0.08
    )

    bloquear_microfono_por_salida = False


# ============================================================
# CANCELAR RESPUESTA
# ============================================================

async def cancelar_respuesta(ws):
    """
    Cancela una Response sin mentirle al estado local.

    response.cancel NO significa que el servidor ya terminó. La bandera
    se limpia únicamente cuando llega response.done. Esto evita el clásico
    conversation_already_has_active_response.
    """
    global respuesta_realtime_activa
    global respuesta_cancelacion_solicitada

    if respuesta_realtime_activa and not respuesta_cancelacion_solicitada:
        try:
            print("🛑 Cancelando respuesta Realtime...")
            await ws.send(json.dumps({
                "type": "response.cancel"
            }))
            respuesta_cancelacion_solicitada = True
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as error:
            print(f"⚠️ No se pudo cancelar respuesta: {error}")

    detener_audio_reproduccion()


# ============================================================
# MEJORA DE NIVEL DEL MICRÓFONO
# ============================================================

def aplicar_ganancia_pcm(datos, ganancia=MIC_GAIN):
    """
    Mejora local del audio del DuoCast antes de enviarlo a Realtime.

    Cadena:
      1) elimina DC / graves muy lentos,
      2) aplica ganancia moderada,
      3) compresión suave para levantar voz lejana,
      4) limitación para evitar clipping.

    No cambia el formato: sigue siendo PCM S16_LE mono a 24 kHz.
    """
    global _filtro_x_anterior, _filtro_y_anterior

    if not datos:
        return datos

    try:
        muestras = array.array("h")
        muestras.frombytes(datos)

        limite = 32767
        minimo = -32768

        # Pasa-altas de primer orden muy suave (~20 Hz aprox.):
        # elimina DC/rumble sin adelgazar la voz.
        alpha = 0.985

        x_prev = _filtro_x_anterior
        y_prev = _filtro_y_anterior

        for i, muestra in enumerate(muestras):
            x = int(muestra)

            y = alpha * (y_prev + x - x_prev)
            x_prev = x
            y_prev = y

            valor = y * ganancia

            # Compresor suave en picos altos. Ayuda a que una voz lejana
            # suba de nivel sin convertir todo el ruido en un pico enorme.
            magnitud = abs(valor)
            if magnitud > 18000:
                exceso = magnitud - 18000
                valor = (18000 + (exceso * 0.40)) * (1 if valor >= 0 else -1)

            if valor > limite:
                valor = limite
            elif valor < minimo:
                valor = minimo

            muestras[i] = int(valor)

        _filtro_x_anterior = x_prev
        _filtro_y_anterior = y_prev

        return muestras.tobytes()

    except Exception:
        # Si el bloque no pudiera procesarse, no perder la captura.
        return datos


def es_autoretorno_evid(texto):
    """
    Detecta una transcripción que probablemente sea la propia voz de EVID.

    No se basa solo en una palabra: exige similitud alta o coincidencia
    sustancial con la frase que EVID acaba de pronunciar. Esto evita que
    EVID se responda a sí misma si Whisper entrega su eco con retraso.
    """
    global ultima_salida_texto, ultima_salida_texto_hasta

    if not texto or not ultima_salida_texto:
        return False

    if time.monotonic() > ultima_salida_texto_hasta:
        return False

    actual = normalizar(texto)
    salida = normalizar(ultima_salida_texto)

    if not actual or not salida:
        return False

    # Frases demasiado cortas pueden ser una respuesta real del usuario.
    if len(actual) < 10 and len(actual.split()) < 2:
        return False

    # Coincidencia directa de una frase suficientemente larga.
    if len(actual) >= 12 and (actual in salida or salida in actual):
        return True

    ratio = difflib.SequenceMatcher(None, actual, salida).ratio()

    palabras_actual = set(actual.split())
    palabras_salida = set(salida.split())

    # Además de SequenceMatcher, comprobamos palabras compartidas.
    # Esto captura ecos con pequeñas variaciones de Whisper, por ejemplo:
    # "ya te escucho Alberto" -> "ya te escucho, Alberto".
    if len(palabras_actual) >= 3 and palabras_salida:
        comunes = len(palabras_actual & palabras_salida)
        cobertura = comunes / max(1, len(palabras_actual))
        if cobertura >= 0.67 and ratio >= 0.68:
            return True

    # Para frases de 2 palabras exigimos coincidencia muy alta.
    if len(palabras_actual) == 2 and ratio >= 0.88:
        return True

    return False


# ============================================================
# CAPTURAR HYPERX DUOCAST
# ============================================================

def calcular_rms_pcm(datos):
    """RMS del bloque PCM S16_LE para la puerta local de voz."""
    if not datos:
        return 0.0
    try:
        muestras = array.array("h")
        muestras.frombytes(datos)
        if not muestras:
            return 0.0
        suma = 0.0
        for muestra in muestras:
            valor = float(muestra)
            suma += valor * valor
        return math.sqrt(suma / len(muestras))
    except Exception:
        return 0.0


def procesar_puerta_local_voz(datos):
    """
    Decide si el bloque contiene voz antes de mandarlo a Realtime.

    Cuando no hay voz manda silencio PCM (ceros), así Realtime conserva su
    VAD y puede cerrar el turno, pero no recibe el ruido del ambiente.
    """
    global local_ruido_rms
    global local_voz_activa, local_voz_bloques, local_ultima_voz

    rms = calcular_rms_pcm(datos)

    umbral = max(
        LOCAL_VAD_MIN_RMS,
        local_ruido_rms * LOCAL_VAD_NOISE_MULTIPLIER
    )

    # Aprendizaje lento del ruido solamente cuando el nivel está por debajo
    # del umbral. Una voz fuerte no contamina el piso de ruido.
    if rms < umbral:
        local_ruido_rms = (
            (1.0 - LOCAL_VAD_NOISE_LEARNING) * local_ruido_rms
            + LOCAL_VAD_NOISE_LEARNING * rms
        )

    umbral = max(
        LOCAL_VAD_MIN_RMS,
        local_ruido_rms * LOCAL_VAD_NOISE_MULTIPLIER
    )
    supera = rms >= umbral

    if supera:
        local_voz_bloques += 1
    else:
        local_voz_bloques = max(0, local_voz_bloques - 1)

    ahora = time.monotonic()

    if local_voz_bloques >= LOCAL_VAD_ATTACK_BLOCKS:
        local_voz_activa = True
        local_ultima_voz = ahora
    elif local_voz_activa and (ahora - local_ultima_voz) * 1000.0 > LOCAL_VAD_RELEASE_MS:
        local_voz_activa = False

    if local_voz_activa:
        return datos, rms, True

    # No mandamos el ruido real al servidor: mandamos silencio PCM.
    return b"\x00" * len(datos), rms, False


async def enviar_microfono(ws):
    """
    HyperX DuoCast continuo.

    IMPORTANTE:
    - Durante la reproducción de EVID NO se silencia el micrófono.
      Si lo hiciéramos, sería imposible reconocer "EVID silencio".
    - El servidor recibe el audio, pero el programa solo acepta la
      transcripción de los dos comandos de silencio durante la salida.
    - Durante LOGIN sí bloqueamos la captura de salida para no convertir
      el saludo de EVID en el nombre de la persona.
    """
    global proceso_mic, audio_enviando, mic_bloqueado_hasta
    global local_turno_confirmado

    print("\n🎙️ HyperX DuoCast — captura 20 ms / puerta local anti-falsos / anti-eco reforzado")
    print(f"🎙️ {MIC_DEVICE}")

    try:
        proceso_mic = subprocess.Popen(
            [
                "arecord",
                "-D", MIC_DEVICE,
                "-f", "S16_LE",
                "-r", str(RATE),
                "-c", str(CHANNELS),
                "-t", "raw",
                "-q",
                "--buffer-size", "4800",
                "--period-size", "480",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=0
        )

        proc = proceso_mic
        if proc is None or proc.poll() is not None:
            raise RuntimeError(
                "arecord terminó inmediatamente; revisa MIC_DEVICE y ALSA."
            )

        audio_enviando = True
        print("✅ HyperX DuoCast activo.")

        bloques_limpieza = 0

        while not cerrando:
            proc = proceso_mic
            if proc is None:
                break
            if proc.poll() is not None:
                print("⚠️ arecord terminó durante la captura.")
                break

            datos = await asyncio.to_thread(
                proceso_mic.stdout.read,
                AUDIO_CHUNK_BYTES
            )

            if not datos:
                break

            # Conservamos el procesamiento de nivel del micrófono del código base.
            datos = aplicar_ganancia_pcm(datos)

            # Segunda barrera: puerta local de voz. El silencio/ruido se
            # convierte en ceros antes de llegar a Realtime.
            datos, rms_local, hay_voz_local = procesar_puerta_local_voz(datos)

            ahora = time.monotonic()

            # ========================================================
            # BLOQUEO TOTAL DURANTE LA VOZ DE EVID
            # ========================================================
            # Mientras EVID habla NO enviamos absolutamente ningún
            # bloque del micrófono a Realtime.
            #
            # Esto evita que el DuoCast capture las propias bocinas de
            # EVID y que Realtime/Whisper convierta ese eco en palabras
            # que el usuario nunca dijo.
            #
            # La captura ALSA sigue viva y se drena para que no acumule
            # audio. Al terminar la respuesta se limpia el buffer y,
            # después de la pequeña barrera anti-eco, vuelve a escuchar.
            bloqueado_por_voz_evid = (
                audio_reproduciendo
                or salida_evid_activa
                or bloquear_microfono_por_salida
            )

            # Después de terminar EVID hay una pequeña barrera para
            # drenar el eco residual antes de volver a aceptar voz.
            bloqueado_post_salida = ahora < mic_bloqueado_hasta

            bloqueado = bloqueado_por_voz_evid or bloqueado_post_salida

            if bloqueado:
                bloques_limpieza = 0
                continue

            # Tras un bloqueo, descartamos unos bloques de audio ya acumulados.
            if bloques_limpieza < 8:
                bloques_limpieza += 1
                continue

            # Guardamos evidencia acústica reciente. Una transcripción no
            # podrá aceptarse si Realtime habló de un turno pero el micrófono
            # local nunca encontró voz real.
            if hay_voz_local:
                local_turno_confirmado = True

            try:
                await ws.send(
                    json.dumps({
                        "type": "input_audio_buffer.append",
                        "audio": base64.b64encode(datos).decode("ascii")
                    })
                )
            except websockets.exceptions.ConnectionClosed:
                break

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as error:
        print(f"❌ Error del micrófono: {error}")
    finally:
        audio_enviando = False

        if proceso_mic:
            try:
                proceso_mic.terminate()
            except Exception:
                pass

        proceso_mic = None


async def limpiar_buffer_realtime(ws):
    """Elimina audio pendiente del buffer de entrada al reabrir escucha."""
    try:
        await ws.send(json.dumps({
            "type": "input_audio_buffer.clear"
        }))
    except Exception:
        pass


# ============================================================
# VOZ REALTIME — sin TTS externo
# ============================================================

# REPRODUCIR RESPUESTA REALTIME
# ============================================================


def iniciar_salida_streaming():
    """Abre aplay y crea un escritor dedicado; el loop Realtime queda libre."""
    global proceso_salida
    global audio_salida_queue, audio_salida_thread
    global audio_salida_fin, audio_salida_stop

    proceso_salida = subprocess.Popen(
        [
            "aplay", "-q", "-D", OUTPUT_DEVICE,
            "-f", "S16_LE", "-r", str(RATE),
            "-c", "1", "-t", "raw", "-"
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        bufsize=0,
    )

    audio_salida_queue = queue.Queue(maxsize=128)
    audio_salida_fin = threading.Event()
    audio_salida_stop = threading.Event()

    proceso = proceso_salida
    cola = audio_salida_queue
    fin = audio_salida_fin
    stop = audio_salida_stop

    def escritor():
        try:
            while not stop.is_set():
                try:
                    datos = cola.get(timeout=0.05)
                except queue.Empty:
                    continue

                if datos is None:
                    try:
                        if proceso.stdin and not proceso.stdin.closed:
                            proceso.stdin.close()
                    except (OSError, ValueError):
                        pass
                    break

                try:
                    # Otra rutina puede cerrar stdin al detener la salida.
                    # Comprobamos que siga abierto y toleramos el cierre
                    # concurrente para evitar ValueError en el hilo de audio.
                    if proceso.stdin and not proceso.stdin.closed:
                        proceso.stdin.write(datos)
                        proceso.stdin.flush()
                except (BrokenPipeError, OSError, ValueError):
                    break
                finally:
                    cola.task_done()
        finally:
            fin.set()

    audio_salida_thread = threading.Thread(
        target=escritor,
        name="EVID-AudioWriter",
        daemon=True,
    )
    audio_salida_thread.start()


def enviar_audio_streaming(datos):
    """Entrega un delta a la cola sin bloquear la recepción WebSocket."""
    if not datos or audio_salida_queue is None:
        return False

    try:
        audio_salida_queue.put_nowait(datos)
        return True
    except queue.Full:
        # Si ALSA se atrasa, no congelamos Realtime.
        # Esperamos un mínimo de tiempo en una tarea aparte no es posible aquí;
        # descartar un delta aislado es preferible a detener toda la conversación.
        return False


async def finalizar_salida_realtime(proceso):
    """Drena la cola, cierra stdin y espera aplay sin bloquear Realtime."""
    global proceso_salida, audio_reproduciendo
    global bloquear_microfono_por_salida, salida_evid_activa
    global mic_bloqueado_hasta
    global audio_salida_queue, audio_salida_thread
    global audio_salida_fin, audio_salida_stop

    try:
        cola = audio_salida_queue
        hilo = audio_salida_thread
        fin = audio_salida_fin

        if cola is not None and hilo is not None and hilo.is_alive():
            try:
                cola.put_nowait(None)
            except queue.Full:
                # Dar un pequeño margen para que el consumidor drene.
                await asyncio.to_thread(cola.join)
                try:
                    cola.put_nowait(None)
                except Exception:
                    pass

            if fin is not None:
                await asyncio.to_thread(fin.wait)

            await asyncio.to_thread(hilo.join, 0.5)

        try:
            if proceso.stdin:
                proceso.stdin.close()
        except Exception:
            pass

        await asyncio.to_thread(proceso.wait)
    except Exception:
        pass

    if proceso_salida is proceso:
        proceso_salida = None

    audio_salida_queue = None
    audio_salida_thread = None
    audio_salida_fin = None
    audio_salida_stop = None

    audio_reproduciendo = False
    salida_evid_activa = False
    mic_bloqueado_hasta = time.monotonic() + (MIC_POST_OUTPUT_GUARD_MS / 1000.0)

    await asyncio.sleep(0.08)
    bloquear_microfono_por_salida = False

    ws = sesion_realtime
    if ws is not None:
        await limpiar_buffer_realtime(ws)


async def reproducir_respuesta(audio_data):
    """Fallback PCM directo sin archivo temporal."""
    global proceso_salida, audio_reproduciendo, bloquear_microfono_por_salida, salida_evid_activa, mic_bloqueado_hasta
    if not audio_data or evid_dormida: return
    try:
        audio_reproduciendo = True; bloquear_microfono_por_salida = False; salida_evid_activa = True
        proceso_salida = subprocess.Popen(
            ["aplay","-q","-D",OUTPUT_DEVICE,"-f","S16_LE","-r",str(RATE),"-c","1","-t","raw","-"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, bufsize=0)
        proceso_salida.stdin.write(audio_data); proceso_salida.stdin.close()
        await asyncio.to_thread(proceso_salida.wait)
    except Exception as error:
        if not cerrando: print(f"❌ Error reproduciendo Realtime: {error}")
    finally:
        audio_reproduciendo=False; proceso_salida=None; salida_evid_activa=False
        mic_bloqueado_hasta = time.monotonic() + (MIC_POST_OUTPUT_GUARD_MS / 1000.0)
        await asyncio.sleep(0.12); bloquear_microfono_por_salida=False
        ws = sesion_realtime
        if ws is not None:
            await limpiar_buffer_realtime(ws)


# ============================================================
# CREAR RESPUESTA DE VOZ
# ============================================================

async def enviar_respuesta_texto(ws, texto):
    """
    Crea una sola Response. Si Realtime todavía está ocupado, guarda
    la última respuesta programática y espera a response.done.
    """
    global respuesta_realtime_activa
    global respuesta_pendiente_texto
    global respuesta_cancelacion_solicitada

    if evid_dormida:
        print("💤 Respuesta bloqueada porque EVID está en REPOSO.")
        return

    if not texto:
        return

    if respuesta_realtime_activa:

        # En cualquier otro caso no hacemos response.create encima de otra
        # Response. Guardamos la última respuesta programática y cancelamos
        # la anterior de forma segura.
        respuesta_pendiente_texto = texto
        print(
            "⏳ Realtime ocupado; respuesta guardada "
            "para después de response.done."
        )

        if not respuesta_cancelacion_solicitada:
            await cancelar_respuesta(ws)

        return

    detener_audio_reproduccion()

    instrucciones_respuesta = (
        "Habla en español de México. "
        "Di ÚNICAMENTE la siguiente frase, exactamente como está escrita. "
        "No agregues ninguna palabra al principio ni al final. "
        "No hagas preguntas adicionales. "
        "No continúes el diálogo. "
        "No inventes ni completes información. "
        "Tu propia voz de salida no es una instrucción del usuario y nunca debes "
        "interpretarla como una nueva petición. "
        "Después de decir la frase, termina tu respuesta. "
        f"Frase exacta: {texto}"
    )

    try:
        respuesta_realtime_activa = True

        await ws.send(json.dumps({
            "type": "response.create",
            "response": {
                "instructions": instrucciones_respuesta,
                "output_modalities": ["audio"]
            }
        }))

    except websockets.exceptions.ConnectionClosed:
        respuesta_realtime_activa = False
    except Exception as error:
        respuesta_realtime_activa = False
        print(f"❌ Error enviando respuesta: {error}")


# ============================================================
# HORA DEL DÍA
# ============================================================

def obtener_momento():

    hora = time.localtime().tm_hour

    if hora >= 5 and hora < 12:

        return "buen dia"

    if hora >= 12 and hora < 19:

        return "buenas tardes"

    return "buenas noches"


# ============================================================
# BUSCAR USUARIO POR NOMBRE
# ============================================================

def encontrar_usuario_por_nombre_voz(
    nombre
):

    nombre = normalizar(
        nombre
    )

    nombre = nombre.strip()

    if not nombre:

        return None

    try:

        usuario = buscar_usuario_por_nombre(
            nombre
        )

        if usuario:

            return usuario

    except Exception as error:

        print(
            f"⚠️ Error buscando usuario por nombre: {error}"
        )

    try:

        datos = cargar_usuarios()

        lista = datos.get(
            "usuarios",
            []
        )

        for registro in lista:

            if not isinstance(
                registro,
                dict
            ):

                continue

            nombre_registro = normalizar(
                registro.get(
                    "nombre",
                    ""
                )
            )

            if not nombre_registro:

                continue

            if (
                nombre == nombre_registro
                or nombre_registro in nombre
                or nombre in nombre_registro
            ):

                usuario_id = registro.get(
                    "id"
                )

                if usuario_id:

                    perfil = obtener_usuario(
                        usuario_id
                    )

                    if perfil:

                        return perfil

                return registro

    except Exception as error:

        print(
            f"⚠️ Error en búsqueda manual: {error}"
        )

    return None


# ============================================================
# RESPUESTA DE PRESENTACIÓN
# ============================================================

async def enviar_mensaje_login(
    ws,
    texto
):

    print(
        f"🤖 EVID: {texto}"
    )

    await enviar_respuesta_texto_login(
        ws,
        texto
    )


async def enviar_respuesta_texto_login(
    ws,
    texto
):

    global respuesta_realtime_activa
    global login_respuesta_done_event

    # El saludo se adapta automáticamente a la hora local.
    if texto.startswith("Buenos días. Soy EVID."):
        texto = texto.replace(
            "Buenos días. Soy EVID.",
            obtener_saludo_horario() + ". Soy EVID.",
            1
        )


    if not texto:
        return

    # Cancelar cualquier respuesta anterior.
    if respuesta_realtime_activa:
        try:
            await ws.send(
                json.dumps({
                    "type": "response.cancel"
                })
            )
        except Exception:
            pass

        respuesta_realtime_activa = False

    detener_audio_reproduccion()

    # La siguiente response.done libera la transición LOGIN → conversación.
    if login_respuesta_done_event is not None:
        login_respuesta_done_event.clear()

    try:
        # IMPORTANTE:
        # No insertamos la frase como mensaje assistant en la conversación.
        # Se entrega directamente como instrucción de la respuesta actual.
        #
        # Así el modelo no interpreta que debe "continuar" esa conversación.
        instrucciones_respuesta = (
            "Habla en español de México. "
            "Di ÚNICAMENTE la siguiente frase, exactamente como está escrita. "
            "No agregues ninguna palabra al principio ni al final. "
            "No hagas ninguna pregunta adicional. "
            "No continúes el diálogo. "
            "No inventes ni completes información. "
            "Después de decir la frase, termina tu respuesta. "
            f"Frase exacta: {texto}"
        )

        await ws.send(
            json.dumps({
                "type": "response.create",
                "response": {
                    "instructions": instrucciones_respuesta,
                    "output_modalities": [
                        "audio"
                    ]
                }
            })
        )

    except websockets.exceptions.ConnectionClosed:
        pass

    except Exception as error:
        print(
            f"❌ Error respuesta de presentación: {error}"
        )


# ============================================================
# LIMPIAR NOMBRE DICHO POR VOZ
# ============================================================

def extraer_nombre_voz(texto):
    """
    Convierte frases como:
      "soy Alberto" -> "Alberto"
      "me llamo Alberto" -> "Alberto"
      "mi nombre es Alberto" -> "Alberto"
      "con Alberto" -> "Alberto"

    Conserva nombres compuestos, pero elimina frases de presentación
    que Whisper pueda añadir al inicio.
    """
    nombre = limpiar_texto(texto)

    prefijos = [
        "mi nombre es ",
        "me llamo ",
        "soy ",
        "con ",
        "mi nombre ",
        "nombre ",
    ]

    # Quitar solo el prefijo de presentación, no palabras internas.
    for prefijo in prefijos:
        if nombre.startswith(prefijo):
            nombre = nombre[len(prefijo):].strip()
            break

    # Quitar artículos/palabras de relleno que suelen aparecer con Whisper.
    nombre = re.sub(
        r"^(el|la|este|esta|pues|bueno|hola)\s+",
        "",
        nombre
    ).strip()

    # Si quedó una frase claramente conversacional, conservar solo
    # el segmento posterior al último marcador de presentación.
    for marcador in [" soy ", " me llamo ", " mi nombre es ", " con "]:
        if marcador in nombre:
            nombre = nombre.split(marcador)[-1].strip()

    # Limpiar espacios repetidos y puntuación residual.
    nombre = re.sub(r"\s+", " ", nombre).strip(" .,!¡¿?")

    return nombre


# ============================================================
# IDENTIFICAR PERSONA POR NOMBRE
# ============================================================

async def procesar_login_voz(
    ws,
    texto
):
    """Obtiene el nombre del usuario sin autenticación, PIN ni registro obligatorio.

    Se conserva el flujo de voz para que EVID pregunte el nombre al comenzar,
    pero el nombre solo identifica la conversación actual. Si existe un perfil
    previo con ese nombre, se reutiliza para conservar memoria; si no existe,
    se intenta crear un perfil sencillo sin PIN.
    """
    global usuario_actual
    global login_completado
    global login_cancelado
    global login_nombre_escuchado
    global modo_anonimo
    global nombre_anonimo
    global sesion_activa

    texto_limpio = normalizar(texto or "")

    if not texto_limpio:
        return

    # Permitir cancelar la presentación inicial diciendo "adiós".
    if es_adios(texto):
        login_cancelado = True
        login_completado = False
        despedida = "De acuerdo. Nos vemos después."
        print(f"🤖 EVID: {despedida}")
        await enviar_mensaje_login(ws, despedida)
        return

    nombre = extraer_nombre_voz(texto)

    # Si Whisper devuelve una frase muy larga, intentamos rescatar un nombre
    # después de los marcadores habituales.
    if len(nombre.split()) > 5:
        candidatos = re.split(
            r'\b(?:soy|me llamo|mi nombre es|con)\b',
            texto_limpio,
            maxsplit=1
        )
        if len(candidatos) == 2:
            nombre = candidatos[1].strip(" .,!¡¿?")

    if not nombre or len(nombre) < 2:
        await enviar_mensaje_login(
            ws,
            "Perdón, no alcancé a entender tu nombre. ¿Cómo te llamas?"
        )
        return

    # Evitar guardar una frase completa como si fuera el nombre.
    nombre = re.sub(r'\s+', ' ', nombre).strip(" .,!¡¿?")
    if len(nombre.split()) > 5:
        await enviar_mensaje_login(
            ws,
            "Quiero asegurarme de llamarte bien. ¿Me dices solamente tu nombre?"
        )
        return

    login_nombre_escuchado = nombre
    nombre_anonimo = ""
    modo_anonimo = False

    # Reutilizar perfil existente si lo hay: esto conserva memoria y preferencias.
    perfil = encontrar_usuario_por_nombre_voz(nombre)

    if perfil:
        usuario_actual = perfil
        print(f"👤 Usuario reconocido por nombre: {nombre}")
        try:
            actualizar_acceso(usuario_actual)
        except Exception as error:
            print(f"⚠️ No se pudo actualizar el acceso: {error}")
        try:
            cargar_historial_usuario()
        except Exception as error:
            print(f"⚠️ No se pudo cargar el historial: {error}")
    else:
        # No hay cuenta: no se pide PIN ni confirmaciones. Se intenta crear
        # un perfil sin PIN para que las memorias puedan persistir.
        try:
            perfil = crear_usuario(
                nombre,
                "usuario",
                ""
            )
        except Exception as error:
            print(f"⚠️ No se pudo crear perfil por nombre: {error}")
            perfil = None

        if perfil:
            usuario_actual = perfil
            print(f"👤 Nuevo perfil por nombre: {nombre}")
            try:
                actualizar_acceso(usuario_actual)
            except Exception:
                pass
            try:
                cargar_historial_usuario()
            except Exception:
                pass
        else:
            # Fallback de sesión: EVID sigue funcionando aunque la base de
            # usuarios no permita perfiles sin PIN.
            usuario_actual = {
                "nombre": nombre,
                "tratamiento": "usuario",
                "preferencias": [],
                "informacion_importante": [],
                "memorias": [],
                "emociones": [],
                "historial": [],
            }
            print(f"👤 Sesión temporal por nombre: {nombre}")

    sesion_activa = True
    login_completado = True
    login_cancelado = False

    # Esta es la ÚNICA respuesta hablada después de que la persona dice su nombre.
    # Debe sentirse como una conversación natural, no como un trámite.
    confirmacion = (
        f"Mucho gusto, {nombre}. Ya te escucho. ¿Qué traes en mente?"
    )

    await enviar_mensaje_login(
        ws,
        confirmacion
    )


# ============================================================
# ADIÓS
# ============================================================

COMANDOS_ADIOS = [

    "adios evid",
    "adiós evid",

    "adios e vid",
    "adiós e vid",

    "adios evit",
    "adiós evit",

    "adios e vit",
    "adiós e vit",

    "adios evits",
    "adiós evits",

    "adios david",
    "adiós david",

    "adios deivid",
    "adiós deivid",

    "adios devid",
    "adiós devid",

    "adios",
    "adiós",

    "hasta luego evid",
    "hasta luego e vid",

    "vete a dormir evid",
    "vete a dormir e vid",

    "duerme evid",
    "duerme e vid",

    "descansa evid",
    "descansa e vid",
]


def es_adios(texto):

    texto = normalizar(
        texto
    )

    return any(

        normalizar(
            x
        ) in texto

        for x in COMANDOS_ADIOS
    )


# ============================================================
# EVENTOS DE IDENTIFICACIÓN
# ============================================================

async def recibir_eventos_login(
    ws
):

    global respuesta_realtime_activa
    global turno_audio_detectado
    global salida_evid_activa
    global mic_bloqueado_hasta
    global audio_reproduciendo, bloquear_microfono_por_salida
    global proceso_salida, audio_salida_queue
    global login_respuesta_done_event
    global voz_iniciada_durante_salida

    audio_respuesta = bytearray()

    async for mensaje in ws:

        if cerrando:
            break

        try:

            evento = json.loads(
                mensaje
            )

        except Exception:

            continue

        tipo = evento.get(
            "type",
            ""
        )

        if tipo == "session.created":

            print(
                "✅ Sesión Realtime de presentación creada."
            )

        elif tipo == "session.updated":

            print()
            print(
                "🟢 EVID está lista para conocerte."
            )

            print(
                "🎤 Esperando tu nombre por voz..."
            )

            await enviar_mensaje_login(

                ws,

                "Hola, soy EVID. Qué gusto tenerte por aquí. ¿Cómo te llamas?"
            )

        elif tipo == (
            "input_audio_buffer.speech_started"
        ):

            print(
                "🎙️ EVID: escuchando..."
            )

        elif tipo == (
            "conversation.item.input_audio_transcription.completed"
        ):

            transcript = evento.get(
                "transcript",
                ""
            ).strip()

            if transcript:

                print()
                print(
                    f"👤 Tú: {transcript}"
                )

                if es_silencio(
                    transcript
                ):

                    print(
                        "🤫 Silencio durante la presentación."
                    )

                    await cancelar_respuesta(
                        ws
                    )

                    continue

                await procesar_login_voz(

                    ws,

                    transcript
                )

                if login_completado:
                    # No cambiamos de modo todavía. La respuesta que confirmó
                    # el LOGIN puede seguir activa en Realtime. Esperamos
                    # response.done antes de crear cualquier respuesta nueva.
                    continue

                if login_cancelado:
                    # La despedida se genera por Realtime; esperamos response.done.
                    continue

        elif tipo == "response.created":

            audio_respuesta = bytearray()

            respuesta_realtime_activa = True
            salida_evid_activa = True

            # Igual que en conversación normal: abrir la salida inmediatamente
            # para que la voz empiece a sonar en cuanto llega el primer delta.
            try:
                audio_reproduciendo = True
                bloquear_microfono_por_salida = True
                iniciar_salida_streaming()
            except Exception as error:
                proceso_salida = None
                audio_reproduciendo = False
                bloquear_microfono_por_salida = False
                print(f"❌ No se pudo iniciar salida LOGIN: {error}")

            print(
                "🧠 EVID..."
            )

        elif tipo == (
            "response.output_audio.delta"
        ):

            delta = evento.get(
                "delta"
            )

            if delta:

                try:

                    datos = base64.b64decode(
                        delta
                    )

                    if proceso_salida and audio_salida_queue is not None:
                        enviar_audio_streaming(datos)
                    else:
                        audio_respuesta.extend(datos)

                except Exception as error:

                    print(
                        f"❌ Error audio de presentación: {error}"
                    )

        elif tipo == (
            "response.output_audio_transcript.delta"
        ):

            delta = evento.get(
                "delta",
                ""
            )

            if delta:

                print(
                    delta,
                    end="",
                    flush=True
                )

        elif tipo == "response.done":

            respuesta_realtime_activa = False

            # Desbloquea cualquier transición que estuviera esperando
            # a que terminara esta respuesta Realtime.
            if login_respuesta_done_event is not None:
                login_respuesta_done_event.set()

            print()

            respuesta = evento.get(
                "response",
                {}
            )

            estado = respuesta.get(
                "status",
                ""
            )

            if estado == "cancelled":

                salida_evid_activa = False

                print(
                    "🛑 Respuesta LOGIN cancelada."
                )

                detener_audio_reproduccion()

                audio_respuesta = bytearray()

                continue

            if estado == "failed":

                salida_evid_activa = False

                print(
                    "❌ La respuesta de presentación falló."
                )

                continue

            if proceso_salida:
                # Cerrar/drainar la salida en segundo plano. Realtime ya terminó
                # de generar la respuesta; ahora dejamos que ALSA termine de
                # reproducirla sin bloquear el receptor.
                proceso_audio = proceso_salida
                asyncio.create_task(finalizar_salida_realtime(proceso_audio))

            elif audio_respuesta:
                asyncio.create_task(reproducir_respuesta(bytes(audio_respuesta)))
                audio_respuesta = bytearray()
            else:
                salida_evid_activa = False

            if login_completado:
                print("✅ Respuesta final de LOGIN terminada; cambiando a conversación.")
                return

            if login_cancelado:
                if proceso_salida:
                    try:
                        proceso_audio = proceso_salida
                        if proceso_audio.stdin:
                            proceso_audio.stdin.close()
                    except Exception:
                        pass
                    await finalizar_salida_realtime(proceso_salida)
                else:
                    await asyncio.sleep(0.15)
                return

        elif tipo == "response.cancelled":

            respuesta_realtime_activa = False

            detener_audio_reproduccion()

            audio_respuesta = bytearray()

            print(
                "🛑 Audio LOGIN interrumpido."
            )

        elif tipo == "error":

            error_obj = evento.get(
                "error",
                {}
            )

            codigo = error_obj.get(
                "code",
                ""
            )

            if codigo == "response_cancel_not_active":

                respuesta_realtime_activa = False

                continue

            print()
            print(
                "❌ ERROR REALTIME de presentación:"
            )

            print(
                json.dumps(
                    evento,
                    indent=2,
                    ensure_ascii=False
                )
            )

            print()


# ============================================================
# PRESENTACIÓN / IDENTIFICACIÓN POR NOMBRE
# ============================================================

async def pasar_nombre_a_conversacion(ws):
    """Pasa a conversación normal SIN volver a saludar.

    La presentación posterior al nombre ya fue pronunciada por Realtime;
    aquí solamente cambiamos las instrucciones y continuamos escuchando.
    """
    global respuesta_realtime_activa, audio_reproduciendo
    global bloquear_microfono_por_salida, salida_evid_activa
    global mic_bloqueado_hasta, sesion_realtime

    # Esperar a que termine completamente el audio de la presentación antes
    # de abrir de nuevo el micrófono. Esto evita cortar la última palabra.
    limite = time.monotonic() + 8.0
    while (audio_reproduciendo or proceso_salida is not None) and time.monotonic() < limite:
        await asyncio.sleep(0.02)

    sesion_realtime = ws
    respuesta_realtime_activa = False

    await limpiar_buffer_realtime(ws)
    await configurar_realtime(ws)

    # IMPORTANTE: NO generar otro saludo aquí.
    # La confirmación del nombre es la única respuesta inicial
    # después del nombre.
    await recibir_eventos(ws)


async def iniciar_sesion_por_voz(ws):
    """Primer contacto: EVID pregunta el nombre y prepara la conversación."""
    global login_completado, login_cancelado
    global respuesta_realtime_activa, login_nombre_escuchado
    global login_respuesta_done_event
    global evid_dormida

    login_completado = False
    login_cancelado = False
    login_nombre_escuchado = ""
    respuesta_realtime_activa = False
    login_respuesta_done_event = asyncio.Event()
    evid_dormida = False

    print()
    print("=" * 65)
    print("              EVID - CONOCIÉNDOTE")
    print("=" * 65)
    print()
    print("🔗 EVID preguntará tu nombre y después comenzará la conversación.")

    await configurar_realtime_presentacion(ws)
    tarea_mic = asyncio.create_task(enviar_microfono(ws))
    try:
        await recibir_eventos_login(ws)

        if login_completado and not login_cancelado:
            await pasar_nombre_a_conversacion(ws)
    finally:
        if not tarea_mic.done():
            tarea_mic.cancel()
            try:
                await tarea_mic
            except asyncio.CancelledError:
                pass
        detener_audio_reproduccion()
        if proceso_mic:
            try:
                proceso_mic.terminate()
            except Exception:
                pass
            globals()["proceso_mic"] = None

    return login_completado


# ============================================================
# ENTRAR EN REPOSO
# ============================================================

async def entrar_en_reposo(ws):

    global evid_dormida
    global respuesta_realtime_activa
    global respuesta_pendiente_texto
    global respuesta_cancelacion_solicitada
    global reposo_pendiente

    print()
    print(
        "=================================================="
    )

    print(
        "💤 EVID → REPOSO"
    )

    print(
        "🔇 No responderá a conversaciones."
    )

    print(
        "👂 Solo reconocerá comandos de despertar."
    )

    print(
        "=================================================="
    )

    # --------------------------------------------------------
    # PRIMERO cancelar cualquier respuesta
    # --------------------------------------------------------

    await cancelar_respuesta(
        ws
    )

    # --------------------------------------------------------
    # El servidor todavía puede estar cerrando la Response.
    # NO mandamos session.update mientras la Response siga activa.
    # response.done aplicará el modo REPOSO de forma segura.
    # --------------------------------------------------------
    reposo_pendiente = True
    respuesta_pendiente_texto = None

    if not respuesta_realtime_activa:
        reposo_pendiente = False
        await actualizar_modo_realtime(ws, True)

    # Cortamos inmediatamente el audio local; el cierre lógico de la
    # Response queda a cargo de response.done.
    detener_audio_reproduccion()


# ============================================================
# SALIR DE REPOSO
# ============================================================

async def despertar_evid(ws):

    global evid_dormida

    print()
    print(
        "=================================================="
    )

    print(
        "🌅 EVID → DESPIERTA"
    )

    print(
        "🎤 Conversación normal activada."
    )

    print(
        "=================================================="
    )

    # --------------------------------------------------------
    # Activar respuestas automáticas
    # --------------------------------------------------------

    await actualizar_modo_realtime(

        ws,

        False
    )

    # --------------------------------------------------------
    # Respuesta corta de confirmación
    # --------------------------------------------------------

    await enviar_respuesta_texto(

        ws,

        "Ya estoy aquí. ¿Qué pasa?"
    )


# ============================================================
# PROCESAR COMANDOS REALTIME
# ============================================================

async def procesar_comando_realtime(

    ws,

    texto
):

    global usuario_actual
    global respuesta_realtime_activa
    global sesion_cerrada

    global sesion_activa
    global modo_anonimo
    global nombre_anonimo

    # ========================================================
    # PRIMERA PRIORIDAD:
    # SILENCIO
    # ========================================================

    if es_silencio(
        texto
    ):

        print()
        print(
            "🤫 COMANDO SILENCIO DETECTADO."
        )

        await entrar_en_reposo(
            ws
        )

        return


    # ========================================================
    # SI EVID ESTÁ DORMIDA
    #
    # ABSOLUTAMENTE NADA MÁS SE PROCESA.
    #
    # ========================================================

    if evid_dormida:

        print(
            "💤 EVID está en REPOSO."
        )

        print(
            f"👂 Texto escuchado: {texto}"
        )

        # ----------------------------------------------------
        # Solo despertar
        # ----------------------------------------------------

        if es_despertar(
            texto
        ):

            print(
                "🌅 COMANDO DE DESPERTAR DETECTADO."
            )

            await despertar_evid(
                ws
            )

            return

        # ----------------------------------------------------
        # Cualquier otra cosa se ignora
        # ----------------------------------------------------

        print(
            "🚫 Conversación ignorada porque EVID está dormida."
        )

        return


    # ========================================================
    # ADIÓS
    # ========================================================

    if es_adios(texto):
        print()
        print("👋 Comando de despedida detectado.")

        nombre = ""
        if usuario_actual:
            nombre = usuario_actual.get("nombre", "")
            nombre = re.sub(r"^[¡!¿?\s]+|[¡!¿?.\s]+$", "", nombre).strip()
        elif modo_anonimo:
            nombre = nombre_anonimo.strip()

        despedida = obtener_despedida_horario(nombre)
        print(f"🤖 EVID: {despedida}")
        print("🔊 Despedida generada por Realtime...")

        sesion_cerrada = True
        # La memoria de EVID es exclusivamente de esta sesión.
        # No se escribe el perfil en disco al despedirse.

        usuario_actual = None
        sesion_activa = False
        modo_anonimo = False
        nombre_anonimo = ""
        historial_local.clear()

        detener_audio_reproduccion()
        # response.done será quien cierre el WebSocket después del audio.
        await enviar_respuesta_texto(ws, despedida)
        return

    # ========================================================
    # MEMORIA
    # ========================================================

    procesado, respuesta = (

        procesar_memoria(
            texto
        )
    )

    if procesado:

        agregar_historial(
            "user",
            texto
        )

        agregar_historial(
            "assistant",
            respuesta
        )

        await enviar_respuesta_texto(
            ws,
            respuesta
        )

        return


    # ========================================================
    # INFORMACIÓN IMPORTANTE
    # ========================================================

    procesado, respuesta = (

        procesar_informacion_importante(
            texto
        )
    )

    if procesado:

        agregar_historial(
            "user",
            texto
        )

        agregar_historial(
            "assistant",
            respuesta
        )

        await enviar_respuesta_texto(
            ws,
            respuesta
        )

        return


    # ========================================================
    # PREFERENCIAS
    # ========================================================

    procesado, respuesta = (

        procesar_preferencia(
            texto
        )
    )

    if procesado:

        agregar_historial(
            "user",
            texto
        )

        agregar_historial(
            "assistant",
            respuesta
        )

        await enviar_respuesta_texto(
            ws,
            respuesta
        )

        return


    # ========================================================
    # REGISTRO EMOCIONAL
    # ========================================================
    # Registramos la emoción detectada en el contexto de la sesión,
    # pero NO respondemos con una frase prefabricada. La conversación
    # psicológica la conduce Realtime/Luna usando las instrucciones de
    # escucha, validación y reflexión.
    emocion_detectada = detectar_emocion(texto)

    if emocion_detectada and usuario_actual:
        usuario_actual.setdefault("contexto_emocional", []).append({
            "emocion": emocion_detectada,
            "texto": texto,
            "fecha": time.strftime("%Y-%m-%d %H:%M:%S")
        })

    # ========================================================
    # CONSULTAR EMOCIONES
    # ========================================================

    procesado, respuesta = (

        consultar_emociones(
            texto
        )
    )

    if procesado:

        agregar_historial(
            "user",
            texto
        )

        agregar_historial(
            "assistant",
            respuesta
        )

        await enviar_respuesta_texto(
            ws,
            respuesta
        )

        return


    # ========================================================
    # LUNA
    # ========================================================

    if usuario_actual:

        agregar_historial(
            "user",
            texto
        )

        palabras_luna = [

            "recuerdas",
            "memoria",
            "consejo",
            "qué opinas",
            "que opinas",
            "analiza",
            "ayúdame",
            "ayudame",
            "piensas",
            "qué piensas",
            "que piensas",
            "por qué",
            "porque",
            "explícame",
            "explicame",
            "problema",
            "situación",
            "situacion",
            "me siento",
            "estoy triste",
            "estoy preocupado",
            "estoy preocupada",
            "estoy ansioso",
            "estoy ansiosa",
            "necesito hablar",
            "quiero hablar",
            "necesito ayuda",
        ]

        texto_normalizado = normalizar(
            texto
        )

        if any(

            normalizar(palabra)
            in texto_normalizado

            for palabra in palabras_luna
        ):

            respuesta_luna = await asyncio.to_thread(
                pensar_luna,
                texto
            )

            await enviar_respuesta_texto(
                ws,
                respuesta_luna
            )

            return

    # ========================================================
    # RESPUESTA NORMAL DE REALTIME
    # ========================================================
    # Si la frase no fue procesada por una función especial,
    # Realtime genera UNA única respuesta manualmente.
    agregar_historial(
        "user",
        texto
    )

    # Guard against any response still in flight. Only one Response may
    # write to the default Realtime conversation at a time.
    if respuesta_realtime_activa:
        print("⏳ Realtime ocupado; no se crea una segunda respuesta.")
        return

    try:
        respuesta_realtime_activa = True
        await ws.send(
            json.dumps({
                "type": "response.create",
                "response": {
                    "output_modalities": [
                        "audio"
                    ]
                }
            })
        )
    except websockets.exceptions.ConnectionClosed:
        respuesta_realtime_activa = False
    except Exception as error:
        respuesta_realtime_activa = False
        print(
            f"❌ Error generando respuesta normal Realtime: {error}"
        )


# ============================================================
# EVENTOS REALTIME NORMAL
# ============================================================

async def recibir_eventos(ws):

    global proceso_salida, audio_reproduciendo, bloquear_microfono_por_salida
    global voz_iniciada_durante_salida
    global local_turno_confirmado, local_ultima_voz

    global sesion_cerrada

    global respuesta_realtime_activa
    global respuesta_pendiente_texto
    global respuesta_cancelacion_solicitada
    global salida_evid_activa
    global reposo_pendiente

    global evid_dormida

    global tarea_reproduccion_realtime
    global mic_bloqueado_hasta
    global ultima_salida_texto, ultima_salida_texto_hasta

    audio_respuesta = bytearray()

    texto_respuesta = ""

    async for mensaje in ws:

        if cerrando:
            break

        try:

            evento = json.loads(
                mensaje
            )

        except Exception:

            continue

        tipo = evento.get(
            "type",
            ""
        )


        # ====================================================
        # SESIÓN
        # ====================================================

        if tipo == "session.created":

            print(
                "✅ Sesión Realtime creada."
            )


        elif tipo == "session.updated":

            print()
            print(
                "🟢 Configuración Realtime actualizada."
            )

            if evid_dormida:

                print(
                    "💤 MODO ACTUAL: REPOSO"
                )

                print(
                    "👂 Solo comandos de despertar."
                )

            else:

                print(
                    "🌅 MODO ACTUAL: DESPIERTA"
                )

                print(
                    "🎤 Conversación normal."
                )


        # ====================================================
        # USUARIO HABLANDO
        # ====================================================

        elif tipo == (
            "input_audio_buffer.speech_started"
        ):

            if evid_dormida:

                print(
                    "👂 EVID está dormida y detectó voz."
                )

            else:

                print(
                    "🎙️ Escuchando..."
                )

                # Realtime confirmó un turno, pero speech_started por sí solo
                # no basta: puede dispararse por ruido. Exigimos evidencia del
                # detector local en los últimos 700 ms.
                local_turno_confirmado = (
                    (time.monotonic() - local_ultima_voz) <= 0.70
                )
                turno_audio_detectado = True

                # ------------------------------------------------
                # NO HACER BARGE-IN AUTOMÁTICO
                # ------------------------------------------------
                # Realtime/VAD puede detectar nuestra voz o ruido
                # mientras EVID está hablando. Eso NO debe cortar
                # la respuesta. Solamente una transcripción que
                # coincida con un comando de silencio podrá hacerlo.
                #
                # Marcamos que la voz comenzó durante la salida para
                # que, aunque el audio termine antes de que Whisper
                # entregue la transcripción, esa frase tampoco se
                # procese como una nueva orden.
                # ------------------------------------------------
                if audio_reproduciendo or salida_evid_activa:
                    voz_iniciada_durante_salida = True
                    print(
                        "🔇 Voz detectada mientras EVID habla; "
                        "esperando transcripción. Solo SILENCIO puede interrumpir."
                    )

        elif tipo == (
            "input_audio_buffer.speech_stopped"
        ):

            print(
                "🛑 Fin de voz."
            )


        # ====================================================
        # TRANSCRIPCIÓN
        # ====================================================

        elif tipo == (
            "conversation.item.input_audio_transcription.completed"
        ):

            transcript = evento.get(
                "transcript",
                ""
            ).strip()

            if not transcript:

                turno_audio_detectado = False
                local_turno_confirmado = False
                continue

            # =================================================
            # FILTRO CONTRA TRANSCRIPCIONES FANTASMA
            # =================================================
            # Si no hubo un speech_started real antes de esta transcripción,
            # no procesamos el texto. Esto evita que ruido, reverberación o
            # el propio motor de transcripción inventen palabras cuando nadie
            # está hablando.
            if not turno_audio_detectado:
                print(f"🔇 Transcripción sin turno de voz descartada: {transcript}")
                continue

            # Segunda condición: el micrófono local tuvo que detectar energía
            # de voz. Si Realtime entrega una palabra durante silencio, aquí muere.
            if not local_turno_confirmado:
                print(
                    f"🔇 Transcripción sin evidencia acústica local descartada: {transcript}"
                )
                turno_audio_detectado = False
                continue

            # Consumimos ambas confirmaciones para que una transcripción
            # retrasada no reutilice el mismo turno.
            turno_audio_detectado = False
            local_turno_confirmado = False

            # =================================================
            # PROTECCIÓN EXTRA DURANTE LA CONFIRMACIÓN DEL NOMBRE
            # =================================================
            # Después de aceptar el nombre, EVID habla una sola vez.
            # Si quedara audio de esa propia voz en el buffer, NO debemos
            # volver a llamar a procesar_login_voz(), porque eso produce
            # la bienvenida duplicada. Solo se conserva la posibilidad de
            # pedir "EVID silencio".
            if login_completado and (audio_reproduciendo or salida_evid_activa):
                if es_silencio(transcript):
                    print("🤫 Silencio durante la confirmación del nombre.")
                    await cancelar_respuesta(ws)
                else:
                    print(f"🔇 Confirmación de EVID descartada: {transcript}")
                continue

            # =================================================
            # FILTRO ANTI-AUTORRETORNO
            # =================================================
            # Si Whisper entrega con retraso la propia voz de EVID,
            # la descartamos incluso si ya terminó la reproducción.
            # El comando "EVID silencio" siempre tiene prioridad.
            if not es_silencio(transcript) and es_autoretorno_evid(transcript):

                print(
                    f"🔇 Eco de EVID descartado: {transcript}"
                )

                voz_iniciada_durante_salida = False
                continue

            # =================================================
            # CONFIRMAR SILENCIO
            # =================================================
            # Primero comprobamos el único comando que puede interrumpir.
            # Así incluso mientras EVID habla, "EVID silencio" pasa.
            # Todo lo demás que provenga del audio de salida se descarta.

            # speech_started NO interrumpe nada. Whisper es quien
            # confirma si realmente fue un comando de silencio.
            if es_silencio(transcript):

                voz_iniciada_durante_salida = False

                print()
                print("🤫 COMANDO SILENCIO DETECTADO.")

                await cancelar_respuesta(ws)

                audio_respuesta = bytearray()
                texto_respuesta = ""

                await actualizar_modo_realtime(
                    ws,
                    True
                )

                continue

            if not (salida_evid_activa or audio_reproduciendo):
                print()
                print(f"👤 Tú: {transcript}")
            else:
                print(f"🔇 Audio de salida descartado: {transcript}")

            # =================================================
            # VOZ DURANTE LA RESPUESTA
            # =================================================
            # Si la frase comenzó mientras EVID hablaba y NO era
            # un comando de silencio, se descarta por completo.
            # Así nunca se convierte en una nueva petición, aunque
            # EVID haya terminado de reproducir antes de que llegue
            # la transcripción de Whisper.
            # =================================================
            if voz_iniciada_durante_salida:

                voz_iniciada_durante_salida = False

                print(
                    "🔇 Frase iniciada durante la respuesta; "
                    "ignorada porque no era un comando de silencio."
                )

                continue

            # Si por cualquier motivo el transcript llegó mientras
            # EVID todavía reproduce audio, tampoco se procesa.
            if audio_reproduciendo or salida_evid_activa:

                print(
                    "🔇 EVID está generando/hablando; solo se acepta SILENCIO."
                )

                continue

            # =================================================
            # MODO REPOSO
            # =================================================

            if evid_dormida:

                # ---------------------------------------------
                # PRIMERO: despertar
                # ---------------------------------------------

                if es_despertar(
                    transcript
                ):

                    print()
                    print(
                        "🌅 COMANDO DE DESPERTAR DETECTADO:"
                    )

                    print(
                        f"👉 {transcript}"
                    )

                    await despertar_evid(
                        ws
                    )

                    continue

                # ---------------------------------------------
                # Silencio durante reposo
                # ---------------------------------------------

                if es_silencio(
                    transcript
                ):

                    print(
                        "💤 EVID ya estaba en REPOSO."
                    )

                    continue

                # ---------------------------------------------
                # CUALQUIER OTRA FRASE:
                # IGNORAR
                # ---------------------------------------------

                print(
                    "🚫 Conversación ignorada porque EVID está dormida."
                )

                continue


            # =================================================
            # MODO DESPIERTA
            # =================================================

            await procesar_comando_realtime(

                ws,

                transcript
            )


        # ====================================================
        # RESPUESTA CREADA
        # ====================================================

        elif tipo == "response.created":

            # ------------------------------------------------
            # Si la respuesta apareció mientras EVID estaba
            # dormida, se cancela inmediatamente.
            # ------------------------------------------------

            if evid_dormida:

                print(
                    "🚫 Realtime intentó responder mientras EVID estaba dormida."
                )

                try:

                    await ws.send(

                        json.dumps({

                            "type":
                                "response.cancel"
                        })
                    )

                except Exception:
                    pass

                # Esperamos response.done para liberar la bandera.
                audio_respuesta = bytearray()

                continue

            audio_respuesta = bytearray()

            texto_respuesta = ""

            # Nueva respuesta: el filtro anti-eco se actualizará con el
            # texto completo cuando response.done confirme qué dijo EVID.
            ultima_salida_texto = ""
            ultima_salida_texto_hasta = 0.0

            respuesta_realtime_activa = True
            salida_evid_activa = True

            print(
                "🧠 EVID..."
            )

            # Iniciar salida inmediatamente: Realtime puede entregar audio
            # en streaming y EVID empieza a hablar sin esperar response.done.
            try:
                audio_reproduciendo = True
                bloquear_microfono_por_salida = True
                iniciar_salida_streaming()
            except Exception as error:
                proceso_salida = None
                audio_reproduciendo = False
                bloquear_microfono_por_salida = False
                print(f"❌ No se pudo iniciar salida de audio: {error}")


        # ====================================================
        # AUDIO
        # ====================================================

        elif tipo == (
            "response.output_audio.delta"
        ):

            delta = evento.get(
                "delta"
            )

            if delta:

                # ------------------------------------------------
                # JAMÁS acumular audio si está dormida.
                # ------------------------------------------------

                if evid_dormida:

                    print(
                        "💤 Audio descartado: EVID está en REPOSO."
                    )

                    continue

                try:

                    datos = base64.b64decode(delta)

                    # IMPORTANTE: jamás escribir directamente a stdin de
                    # aplay desde el event loop. La cola mantiene Realtime
                    # recibiendo audio sin esperar a ALSA.
                    if proceso_salida and audio_salida_queue is not None:
                        enviar_audio_streaming(datos)
                    else:
                        audio_respuesta.extend(datos)

                except Exception as error:

                    print(
                        f"❌ Error audio: {error}"
                    )


        # ====================================================
        # TEXTO DE AUDIO
        # ====================================================

        elif tipo == (
            "response.output_audio_transcript.delta"
        ):

            delta = evento.get(
                "delta",
                ""
            )

            if delta:

                # ------------------------------------------------
                # Si se durmió mientras llegaba el texto,
                # ignorarlo.
                # ------------------------------------------------

                if evid_dormida:

                    continue

                texto_respuesta += delta

                print(

                    delta,

                    end="",

                    flush=True
                )


        # ====================================================
        # RESPUESTA FINALIZADA
        # ====================================================

        elif tipo == "response.done":

            # response.done es la confirmación definitiva del servidor.
            respuesta_realtime_activa = False
            respuesta_cancelacion_solicitada = False

            # Si el usuario dijo "EVID silencio", esperamos hasta aquí
            # para cambiar la sesión a reposo. Así no hay una carrera entre
            # response.cancel y session.update.
            aplicar_reposo_ahora = reposo_pendiente
            reposo_pendiente = False

            print()

            respuesta = evento.get(
                "response",
                {}
            )

            estado = respuesta.get(
                "status",
                ""
            )

            # Guardar una copia temporal de lo que EVID acaba de decir.
            # Si Whisper entrega ese audio con retraso, el siguiente turno
            # lo reconocerá como eco y lo ignorará.
            texto_terminado = texto_respuesta.strip()

            if texto_terminado and estado != "failed":
                ultima_salida_texto = texto_terminado
                ultima_salida_texto_hasta = time.monotonic() + 6.0

            if aplicar_reposo_ahora:
                await actualizar_modo_realtime(ws, True)
                detener_audio_reproduccion()

            # ------------------------------------------------
            # CANCELADA
            # ------------------------------------------------

            if estado == "cancelled":

                print(
                    "🛑 Respuesta cancelada."
                )

                audio_respuesta = bytearray()
                texto_respuesta = ""

                detener_audio_reproduccion()

                # Si había una respuesta programática pendiente, ahora sí
                # es seguro crearla: el servidor ya cerró la anterior.
                pendiente = respuesta_pendiente_texto
                respuesta_pendiente_texto = None

                if (
                    pendiente
                    and not evid_dormida
                    and not sesion_cerrada
                ):
                    await enviar_respuesta_texto(ws, pendiente)

                continue


            # ------------------------------------------------
            # FALLIDA
            # ------------------------------------------------

            if estado == "failed":

                print(
                    "❌ La respuesta Realtime falló."
                )

                error_info = respuesta.get(
                    "status_details",
                    {}
                )

                if error_info:

                    print(
                        json.dumps(
                            error_info,
                            indent=2,
                            ensure_ascii=False
                        )
                    )

                audio_respuesta = bytearray()

                texto_respuesta = ""

                continue



            # ------------------------------------------------
            # SI EVID ESTÁ DORMIDA:
            # NO reproducir
            # ------------------------------------------------

            if evid_dormida:

                print(
                    "💤 Audio descartado: EVID está en REPOSO."
                )

                audio_respuesta = bytearray()

                texto_respuesta = ""

                continue


            # ------------------------------------------------
            # REPRODUCIR
            # ------------------------------------------------

            # El audio normal ya se reprodujo en streaming.
            # No abrimos el micrófono hasta que aplay realmente termine;
            # esto evita capturar la cola/eco de la última palabra de EVID.
            if proceso_salida:
                proceso_audio = proceso_salida
                asyncio.create_task(
                    finalizar_salida_realtime(proceso_audio)
                )

            if audio_respuesta:
                asyncio.create_task(reproducir_respuesta(bytes(audio_respuesta)))
                audio_respuesta = bytearray()


            if sesion_cerrada:
                if proceso_salida:
                    try:
                        proceso_audio = proceso_salida
                        if proceso_audio.stdin:
                            proceso_audio.stdin.close()
                    except Exception:
                        pass
                    await finalizar_salida_realtime(proceso_salida)
                else:
                    await asyncio.sleep(0.15)
                try:
                    await ws.close()
                except Exception:
                    pass
                return

            # ------------------------------------------------
            # GUARDAR TEXTO
            # ------------------------------------------------

            if texto_respuesta.strip():

                texto_final = (
                    texto_respuesta.strip()
                )

                print(
                    f"🤖 EVID: "
                    f"{texto_final}"
                )

                agregar_historial(

                    "assistant",

                    texto_final
                )

            if sesion_cerrada:
                if proceso_salida:
                    try:
                        proceso_audio = proceso_salida
                        if proceso_audio.stdin:
                            proceso_audio.stdin.close()
                    except Exception:
                        pass
                    await finalizar_salida_realtime(proceso_salida)
                else:
                    await asyncio.sleep(0.15)
                try:
                    await ws.close()
                except Exception:
                    pass
                return

            print()

            if not evid_dormida:

                print(
                    "🎤 Habla nuevamente."
                )


        # ====================================================
        # RESPUESTA CANCELADA
        # ====================================================

        elif tipo == "response.cancelled":

            # Este evento NO es la confirmación final del estado de la
            # Response. La bandera se libera únicamente en response.done.
            print(
                "🛑 Respuesta interrumpida; esperando response.done..."
            )

            detener_audio_reproduccion()

            audio_respuesta = bytearray()

            texto_respuesta = ""


        # ====================================================
        # ERROR
        # ====================================================

        elif tipo == "error":

            error_obj = evento.get(
                "error",
                {}
            )

            codigo = error_obj.get(
                "code",
                ""
            )

            # ------------------------------------------------
            # Este error ya no debe ser crítico.
            # ------------------------------------------------

            if codigo == "response_cancel_not_active":

                respuesta_realtime_activa = False

                print(
                    "ℹ️ No había una respuesta activa que cancelar."
                )

                continue

            print()
            print(
                "❌ ERROR REALTIME:"
            )

            print(

                json.dumps(

                    evento,

                    indent=2,

                    ensure_ascii=False
                )
            )

            print()


# ============================================================
# LD2410CP
# ============================================================

class LD2410CP:

    def __init__(self, port=LD2410_PORT, baudrate=LD2410_BAUDRATE):
        self.port = port
        self.baudrate = baudrate
        self.serial = None
        self.buffer = bytearray()

    def abrir(self):
        if self.serial is not None and self.serial.is_open:
            return

        self.serial = serial.Serial(
            port=self.port,
            baudrate=self.baudrate,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=LD2410_TIMEOUT,
        )

        self.serial.reset_input_buffer()
        self.buffer.clear()

        print(
            f"✅ LD2410CP iniciado en {self.port} "
            f"@ {self.baudrate} baudios."
        )

    def cerrar(self):
        if self.serial is not None:
            try:
                self.serial.close()
            except Exception:
                pass
            self.serial = None

        self.buffer.clear()

    def leer_estado(self):
        """
        Lee la trama BASIC/ENGINEERING del LD2410.

        Devuelve:
            (
                detectado,
                estado,
                distancia_movimiento_cm,
                energia_movimiento,
                distancia_estacionaria_cm,
                energia_estacionaria,
                distancia_deteccion_cm,
            )

        estado:
            0 = sin objetivo
            1 = objetivo en movimiento
            2 = objetivo estacionario
            3 = movimiento + estacionario
           -1 = todavía no llegó una trama completa

        La separación entre movimiento y estacionario es importante:
        un objeto fijo no debe sustituir a la persona que activó EVID.
        """
        self.abrir()

        datos = self.serial.read(128)

        if datos:
            self.buffer.extend(datos)

        ultimo = None

        while True:
            inicio = self.buffer.find(LD2410_HEADER)

            if inicio < 0:
                if len(self.buffer) > 3:
                    del self.buffer[:-3]
                break

            if inicio > 0:
                del self.buffer[:inicio]

            if len(self.buffer) < 6:
                break

            longitud = self.buffer[4] | (self.buffer[5] << 8)

            # Basic = 0x0D. Engineering puede ser mayor.
            if longitud < 11 or longitud > 512:
                del self.buffer[0]
                continue

            trama_total = 4 + 2 + longitud + 4

            if len(self.buffer) < trama_total:
                break

            trama = bytes(self.buffer[:trama_total])
            del self.buffer[:trama_total]

            if trama[-4:] != LD2410_TAIL:
                continue

            payload = trama[6:6 + longitud]

            if len(payload) < 11:
                continue

            if payload[0] not in (0x01, 0x02):
                continue

            if payload[1] != 0xAA:
                continue

            estado = payload[2]
            if estado > 0x03:
                continue

            # En el reporte estándar aparecen 0x55 y 0x00 al final
            # de la información interna.
            if len(payload) >= 13:
                if payload[11] != 0x55 or payload[12] != 0x00:
                    continue

            distancia_movimiento = payload[3] | (payload[4] << 8)
            energia_movimiento = payload[5]

            distancia_estacionaria = payload[6] | (payload[7] << 8)
            energia_estacionaria = payload[8]

            distancia_deteccion = payload[9] | (payload[10] << 8)

            ultimo = (
                estado != 0,
                estado,
                distancia_movimiento,
                energia_movimiento,
                distancia_estacionaria,
                energia_estacionaria,
                distancia_deteccion,
            )

        if ultimo is None:
            return False, -1, 0, 0, 0, 0, 0

        return ultimo


def _distancia_en_rango(distancia):
    return (
        distancia >= LD2410_DISTANCIA_MINIMA
        and distancia <= LD2410_DISTANCIA_MAXIMA
    )


async def esperar_activacion_ld2410():
    """
    Espera a que una PERSONA provoque movimiento dentro de 90 cm.

    Ya no activa EVID únicamente porque el radar vea una superficie
    estacionaria. Esto evita que una pared, silla u objeto fijo cercano
    encienda EVID al arrancar.
    """
    global ld2410_distancia_referencia
    global ld2410_ultima_distancia_movimiento

    ld2410_distancia_referencia = None
    ld2410_ultima_distancia_movimiento = None

    radar = LD2410CP()
    inicio_movimiento = None

    try:
        radar.abrir()

        print()
        print("💤 EVID está en espera.")
        print("📡 Esperando MOVIMIENTO dentro de 90 cm con LD2410CP...")

        while not cerrando:
            try:
                (
                    detectado,
                    estado,
                    distancia_movimiento,
                    energia_movimiento,
                    distancia_estacionaria,
                    energia_estacionaria,
                    distancia_deteccion,
                ) = await asyncio.to_thread(radar.leer_estado)

                if estado == -1:
                    await asyncio.sleep(0.01)
                    continue

                ahora = time.monotonic()

                movimiento_valido = (
                    estado in (1, 3)
                    and _distancia_en_rango(distancia_movimiento)
                    and energia_movimiento >= LD2410_ENERGIA_MOVIMIENTO_ACTIVACION
                )

                if movimiento_valido:
                    if inicio_movimiento is None:
                        inicio_movimiento = ahora
                        print()
                        print(
                            "👤 LD2410CP → MOVIMIENTO DETECTADO. "
                            f"{distancia_movimiento} cm / "
                            f"energía {energia_movimiento}. "
                            f"Confirmando {LD2410_CONFIRMACION_ACTIVACION:.1f}s..."
                        )

                    if ahora - inicio_movimiento >= LD2410_CONFIRMACION_ACTIVACION:
                        ld2410_distancia_referencia = float(
                            distancia_movimiento
                        )
                        ld2410_ultima_distancia_movimiento = float(
                            distancia_movimiento
                        )

                        print()
                        print(
                            "🟢 LD2410CP → EVID ACTIVADA "
                            f"({distancia_movimiento} cm)"
                        )
                        print(
                            "🎯 Distancia humana de referencia: "
                            f"{ld2410_distancia_referencia:.0f} cm"
                        )
                        return True

                else:
                    if inicio_movimiento is not None:
                        print(
                            "⚪ LD2410CP → Movimiento insuficiente; "
                            "activación descartada."
                        )
                    inicio_movimiento = None

            except Exception as error:
                print(f"⚠️ Error leyendo LD2410CP: {error}")
                await asyncio.sleep(0.5)

            await asyncio.sleep(0.03)

    except Exception as error:
        print(f"❌ No se pudo iniciar el LD2410CP: {error}")

    finally:
        radar.cerrar()

    return False


async def vigilar_presencia_sesion(ws):
    """
    Mantiene abierta la sesión usando una "cerradura humana".

    Prioridad:
      1. Movimiento dentro de 1..90 cm = presencia humana activa.
      2. Movimiento que sale de 90 cm = inicia ausencia; un objeto
         estacionario NO puede cancelar ese conteo.
      3. Objetivo estacionario cercano a la distancia humana de referencia
         = permite que la persona permanezca quieta.
      4. Objetivo estacionario a otra distancia = se ignora.
      5. Sin evidencia válida durante LD2410_TIEMPO_AUSENCIA = cierre.

    Esto ataca el problema de que el LD2410 siga viendo una silla/pared
    después de que la persona se retira.
    """
    global sesion_cerrada
    global cerrando
    global proceso_mic
    global ld2410_distancia_referencia
    global ld2410_ultima_distancia_movimiento

    radar = LD2410CP()
    inicio_sin_presencia = None
    salida_por_movimiento = False
    ultimo_log_estado = 0.0

    try:
        radar.abrir()

        print("📡 Vigilancia LD2410CP ACTIVADA.")
        print(
            f"📏 Rango humano: "
            f"{LD2410_DISTANCIA_MINIMA:.0f}–{LD2410_DISTANCIA_MAXIMA:.0f} cm."
        )
        print(
            "🧠 Filtro humano: "
            f"movimiento ≥ {LD2410_ENERGIA_MOVIMIENTO_PRESENCIA}, "
            f"estacionario ≥ {LD2410_ENERGIA_ESTACIONARIO_PRESENCIA}, "
            f"tolerancia ±{LD2410_TOLERANCIA_HUMANA_CM:.0f} cm."
        )

        while not cerrando and not sesion_cerrada:
            try:
                (
                    detectado,
                    estado,
                    distancia_movimiento,
                    energia_movimiento,
                    distancia_estacionaria,
                    energia_estacionaria,
                    distancia_deteccion,
                ) = await asyncio.to_thread(radar.leer_estado)

                if estado == -1:
                    await asyncio.sleep(0.01)
                    continue

                ahora = time.monotonic()

                movimiento_en_rango = (
                    estado in (1, 3)
                    and _distancia_en_rango(distancia_movimiento)
                    and energia_movimiento >= LD2410_ENERGIA_MOVIMIENTO_PRESENCIA
                )

                movimiento_fuera = (
                    estado in (1, 3)
                    and energia_movimiento >= LD2410_ENERGIA_MOVIMIENTO_SALIDA
                    and distancia_movimiento > LD2410_DISTANCIA_MAXIMA
                )

                estacionario_en_rango = (
                    estado in (2, 3)
                    and _distancia_en_rango(distancia_estacionaria)
                    and energia_estacionaria
                    >= LD2410_ENERGIA_ESTACIONARIO_PRESENCIA
                )

                estacionario_cerca_del_humano = False
                if (
                    estacionario_en_rango
                    and ld2410_distancia_referencia is not None
                ):
                    estacionario_cerca_del_humano = (
                        abs(
                            distancia_estacionaria
                            - ld2410_distancia_referencia
                        )
                        <= LD2410_TOLERANCIA_HUMANA_CM
                    )

                # 1) Movimiento humano dentro de rango.
                if movimiento_en_rango:
                    salida_por_movimiento = False
                    inicio_sin_presencia = None

                    ld2410_ultima_distancia_movimiento = float(
                        distancia_movimiento
                    )

                    # Referencia dinámica para permitir que la persona
                    # cambie de posición dentro del rango.
                    if ld2410_distancia_referencia is None:
                        ld2410_distancia_referencia = float(
                            distancia_movimiento
                        )
                    else:
                        ld2410_distancia_referencia = (
                            ld2410_distancia_referencia * 0.70
                            + distancia_movimiento * 0.30
                        )

                    if ahora - ultimo_log_estado > 2.0:
                        print(
                            "🟢 LD2410CP → Movimiento humano: "
                            f"{distancia_movimiento} cm / "
                            f"energía {energia_movimiento}"
                        )
                        ultimo_log_estado = ahora

                # 2) El objetivo móvil salió de 90 cm.
                elif movimiento_fuera:
                    if not salida_por_movimiento:
                        print()
                        print(
                            "🚶 LD2410CP → Objetivo móvil salió de "
                            f"{LD2410_DISTANCIA_MAXIMA:.0f} cm: "
                            f"{distancia_movimiento} cm."
                        )
                        print(
                            "⏳ La sesión se cerrará aunque quede "
                            "un objetivo estacionario."
                        )

                    salida_por_movimiento = True

                    if inicio_sin_presencia is None:
                        inicio_sin_presencia = ahora

                # 3) Sin movimiento, pero estacionario compatible.
                elif (
                    not salida_por_movimiento
                    and estacionario_cerca_del_humano
                ):
                    inicio_sin_presencia = None

                    if ahora - ultimo_log_estado > 2.0:
                        print(
                            "🟡 LD2410CP → Objetivo estacionario compatible "
                            f"({distancia_estacionaria} cm / "
                            f"energía {energia_estacionaria})."
                        )
                        ultimo_log_estado = ahora

                # 4) No hay evidencia humana válida.
                else:
                    if inicio_sin_presencia is None:
                        inicio_sin_presencia = ahora
                        print()

                        if detectado and estacionario_en_rango:
                            if ld2410_distancia_referencia is not None:
                                diferencia = abs(
                                    distancia_estacionaria
                                    - ld2410_distancia_referencia
                                )
                                print(
                                    "🟠 LD2410CP → Objetivo estacionario "
                                    "no compatible: "
                                    f"{distancia_estacionaria} cm "
                                    f"(diferencia {diferencia:.0f} cm)."
                                )
                            else:
                                print(
                                    "🟠 LD2410CP → Objetivo estacionario "
                                    "sin referencia humana."
                                )
                        else:
                            print(
                                "🔴 LD2410CP → SIN PRESENCIA HUMANA VÁLIDA."
                            )

                        print(
                            "⏳ Confirmando ausencia durante "
                            f"{LD2410_TIEMPO_AUSENCIA:.1f} segundos..."
                        )

                # 5) Cierre por ausencia confirmada.
                if (
                    inicio_sin_presencia is not None
                    and ahora - inicio_sin_presencia
                    >= LD2410_TIEMPO_AUSENCIA
                ):
                    await cerrar_sesion_por_ausencia(ws)
                    break

            except Exception as error:
                print(f"⚠️ Error leyendo LD2410CP: {error}")
                await asyncio.sleep(0.5)

            await asyncio.sleep(0.03)

    except asyncio.CancelledError:
        raise

    except Exception as error:
        print(f"❌ Error en vigilancia LD2410CP: {error}")

    finally:
        radar.cerrar()
        print("📡 Vigilancia LD2410CP detenida.")




async def esperar_fin_despedida_realtime(timeout=8.0):
    """Espera a que Realtime termine de generar/reproducir la despedida.

    Importante: no se debe cancelar recibir_eventos mientras la despedida
    sigue llegando, porque esa tarea es la que recibe los deltas de audio.
    """
    inicio = time.monotonic()

    while time.monotonic() - inicio < timeout:
        if (not respuesta_realtime_activa and
                not salida_evid_activa and
                proceso_salida is None):
            return
        await asyncio.sleep(0.05)

    print("⚠️ Tiempo de espera de despedida agotado; continuando cierre.")

async def cerrar_sesion_por_ausencia(ws):
    """Cierra la sesión una sola vez por ausencia confirmada."""
    global sesion_cerrada
    global proceso_mic

    if sesion_cerrada:
        return

    sesion_cerrada = True

    try:
        await cancelar_respuesta(ws)
    except Exception as error:
        print(f"⚠️ No se pudo cancelar la respuesta: {error}")

    if proceso_mic:
        try:
            proceso_mic.terminate()
        except Exception:
            pass
        proceso_mic = None

    despedida = obtener_despedida_horario("")
    print()
    print("🔴 LD2410CP → AUSENCIA CONFIRMADA")
    print("🔐 Cerrando sesión por ausencia de presencia...")
    print(f"🤖 EVID: {despedida}")

    try:
        await enviar_respuesta_texto(ws, despedida)
    except Exception as error:
        print(f"⚠️ No se pudo generar la despedida Realtime: {error}")

    await asyncio.sleep(0.20)


# ============================================================
# LIMPIAR SESIÓN
# ============================================================

def limpiar_sesion_usuario():

    global usuario_actual

    global respuesta_realtime_activa
    global respuesta_pendiente_texto
    global respuesta_cancelacion_solicitada
    global reposo_pendiente

    global sesion_activa
    global modo_anonimo
    global nombre_anonimo

    global evid_dormida
    global turno_audio_detectado, local_turno_confirmado

    historial_local.clear()


    usuario_actual = None
    sesion_activa = False
    modo_anonimo = False
    nombre_anonimo = ""

    respuesta_realtime_activa = False
    respuesta_pendiente_texto = None
    respuesta_cancelacion_solicitada = False
    reposo_pendiente = False

    voz_iniciada_durante_salida = False
    turno_audio_detectado = False
    local_turno_confirmado = False

    evid_dormida = False


# ============================================================
# LIMPIAR TODO
# ============================================================

def limpiar():
    """Cierra EVID completamente desde Alt+F4, Ctrl+C o SIGTERM."""
    global cerrando
    global proceso_mic, proceso_salida
    global respuesta_realtime_activa, audio_reproduciendo
    global bloquear_microfono_por_salida, voz_iniciada_durante_salida
    global turno_audio_detectado, local_turno_confirmado
    global salida_evid_activa, ws_activo

    primera_llamada = not cerrando
    cerrando = True

    respuesta_realtime_activa = False
    audio_reproduciendo = False
    bloquear_microfono_por_salida = True
    voz_iniciada_durante_salida = False
    turno_audio_detectado = False
    local_turno_confirmado = False
    salida_evid_activa = False

    if primera_llamada:
        print()
        print("🛑 Apagando EVID...")
        print("🧹 Cerrando micrófono, audio y Realtime...")

    # Cerrar WebSocket EN SU EVENT LOOP. Esto despierta cualquier
    # 'async for mensaje in ws' que esté esperando indefinidamente.
    ws = ws_activo
    loop = loop_backend
    if ws is not None and loop is not None and loop.is_running():
        try:
            async def cerrar_ws():
                try:
                    await ws.close()
                except Exception:
                    pass
            asyncio.run_coroutine_threadsafe(cerrar_ws(), loop)
        except Exception:
            pass

    # Cortar arecord para que el read() bloqueante del micrófono termine.
    proc_mic = proceso_mic
    if proc_mic:
        try:
            if proc_mic.poll() is None:
                proc_mic.terminate()
        except Exception:
            pass
        try:
            proc_mic.wait(timeout=0.35)
        except Exception:
            try:
                proc_mic.kill()
            except Exception:
                pass
        proceso_mic = None

    # Cortar aplay.
    proc_salida = proceso_salida
    if proc_salida:
        try:
            if proc_salida.stdin:
                proc_salida.stdin.close()
        except Exception:
            pass
        try:
            if proc_salida.poll() is None:
                proc_salida.terminate()
        except Exception:
            pass
        try:
            proc_salida.wait(timeout=0.35)
        except Exception:
            try:
                proc_salida.kill()
            except Exception:
                pass
        proceso_salida = None

    # Detener escritor de audio.
    try:
        if audio_salida_stop is not None:
            audio_salida_stop.set()
    except Exception:
        pass
    try:
        if audio_salida_queue is not None:
            audio_salida_queue.put_nowait(None)
    except Exception:
        pass

    try:
        if os.path.exists(TEMP_AUDIO):
            os.remove(TEMP_AUDIO)
    except Exception:
        pass


# ============================================================
# UNA SESIÓN REALTIME
# ============================================================

async def ejecutar_sesion_realtime(ws):

    """Continúa la conversación en el mismo WebSocket después de identificar el nombre."""

    global sesion_realtime, sesion_cerrada
    global respuesta_realtime_activa, evid_dormida

    sesion_realtime = ws
    sesion_cerrada = False
    respuesta_realtime_activa = False
    evid_dormida = False

    try:
            sesion_realtime = ws

            print(
                "✅ Conectado."
            )

            print(
                f"🧠 Realtime: {REALTIME_MODEL}"
            )

            print(
                f"🧠 Luna: {LUNA_MODEL}"
            )

            print(
                f"🔊 Voz: {VOICE}"
            )

            print(
                "🎙️ Micrófono: HyperX DuoCast"
            )

            print(
                f"🔊 Salida: {OUTPUT_DEVICE}"
            )

            print(
                f"🎵 Audio: PCM16 {RATE} Hz"
            )

            print()

            await configurar_realtime(
                ws
            )

            tarea_mic = asyncio.create_task(

                enviar_microfono(
                    ws
                )
            )

            tarea_eventos = asyncio.create_task(

                recibir_eventos(
                    ws
                )
            )

            tarea_presencia = asyncio.create_task(

                vigilar_presencia_sesion(
                    ws
                )
            )

            # La bienvenida de inicio se genera en la ruta de activación
            # correspondiente; aquí no se crea un saludo adicional.

            # Esperamos a que termine Realtime o a que el LD2410CP
            # detecte que EVID debe cerrar la sesión.
            done, pending = await asyncio.wait(

                [
                    tarea_eventos,
                    tarea_presencia
                ],

                return_when=asyncio.FIRST_COMPLETED
            )

            # Si el LD2410CP cerró la sesión, NO cancelamos todavía
            # recibir_eventos: esa tarea es la que recibe y reproduce
            # el audio de "Hasta luego. Cuídate mucho.".
            if tarea_presencia in done:
                await esperar_fin_despedida_realtime()

            for tarea in pending:

                tarea.cancel()

                try:
                    await tarea
                except asyncio.CancelledError:
                    pass

            # Recuperamos cualquier excepción real de las tareas
            # terminadas para no ocultar errores del sistema.
            for tarea in done:

                if tarea.cancelled():
                    continue

                try:
                    tarea.result()
                except asyncio.CancelledError:
                    pass

            if not tarea_mic.done():

                tarea_mic.cancel()

                try:

                    await tarea_mic

                except asyncio.CancelledError:

                    pass

    except websockets.exceptions.ConnectionClosed as error:

        print()

        if sesion_cerrada:

            print(
                "👋 Conversación cerrada correctamente."
            )

        else:

            print(
                "❌ Conexión Realtime cerrada:"
            )

            print(
                error
            )

    except Exception as error:

        print()

        print(
            "❌ Error Realtime:"
        )

        print(
            repr(error)
        )

    finally:

        respuesta_realtime_activa = False

        detener_audio_reproduccion()

        if proceso_mic:

            try:

                proceso_mic.terminate()

            except Exception:
                pass

            globals()["proceso_mic"] = None

        sesion_realtime = None

# ============================================================
# MAIN
# ============================================================


# ============================================================
# BIENVENIDA DE INICIO + APERTURA DE ACOMPAÑAMIENTO EMOCIONAL
# ============================================================

def obtener_bienvenida_comando():
    """
    Comando fijo que se ejecuta SOLO al comenzar una conversación nueva.
    La presentación no depende de Luna ni se vuelve a repetir durante la sesión.
    """
    return "Hola, soy EVID. Ya estoy aquí."


def obtener_apertura_psicologica():
    """
    Segunda parte de la bienvenida.
    Se elige después de la presentación fija para abrir la conversación
    de forma natural, femenina y emocional.
    """
    aperturas = [
        "Mmm... cuéntame, ¿cómo estás hoy, de verdad?",
        "¿Cómo vienes hoy? ¿Qué tal te has sentido?",
        "A ver... ¿qué traes hoy en la cabeza?",
        "¿Cómo te has sentido últimamente?",
        "Cuéntame, ¿cómo anda tu ánimo hoy?",
        "¿Todo tranquilo contigo o traes algo dando vueltas?",
        "¿Cómo estás por dentro hoy?",
        "Si quieres contarme algo, ¿qué es lo que más traes en la cabeza?",
        "¿Hay algo que últimamente te esté pesando un poquito?",
        "Dime... ¿cómo has estado estos días?",
        "¿Qué tal estás hoy? Sin pensarlo demasiado.",
        "Mmm... ¿hay algo de lo que quieras hablar conmigo hoy?",
        "¿Cómo vienes emocionalmente hoy?",
        "¿Hay algo que te gustaría sacar un poquito antes de seguir con tu día?",
        "Cuéntame algo... ¿cómo te has sentido realmente?",
        "¿Qué traes contigo hoy? Puede ser lo que sea.",
    ]
    return random.choice(aperturas)


def obtener_bienvenida_inicio_completa():
    """
    Une las dos etapas en UNA sola respuesta Realtime para evitar que
    una segunda response.create cancele o reemplace la primera.
    """
    presentacion = obtener_bienvenida_comando()
    apertura = obtener_apertura_psicologica()
    return f"{presentacion} {apertura}"


# ============================================================
# SALUDO SEGÚN LA HORA LOCAL
# ============================================================

def obtener_saludo_horario():
    hora = time.localtime().tm_hour

    if 5 <= hora < 12:
        return "Buenos días"
    elif 12 <= hora < 19:
        return "Buenas tardes"
    else:
        return "Buenas noches"


def obtener_despedida_horario(nombre=""):
    # Despedida fija, sin nombre ni identificación del usuario.
    return "Hasta luego. Cuídate mucho."

    if nombre:
        return f"Hasta luego, {nombre}. Cuídate mucho."

    return "Hasta luego. Cuídate mucho."


async def despedida_evid_antes_de_cerrar(ws):
    """Despedida por Realtime; no usa la API TTS separada."""
    nombre = ""
    if usuario_actual:
        nombre = usuario_actual.get("nombre", "").strip()
    if not nombre and nombre_anonimo:
        nombre = nombre_anonimo.strip()
    despedida = obtener_despedida_horario(nombre)
    print()
    print(f"🤖 EVID: {despedida}")
    print("🔊 Despedida generada por Realtime...")
    await enviar_respuesta_texto(ws, despedida)



async def main():

    global proceso_mic
    global loop_backend, ws_activo
    global evid_dormida
    global usuario_actual
    global sesion_activa
    global modo_anonimo
    global nombre_anonimo
    global sesion_cerrada
    global sesion_realtime
    global login_completado
    global login_cancelado

    loop_backend = asyncio.get_running_loop()

    print()
    print("=" * 65)
    print("                    EVID")
    print("       ASISTENTE PERSONAL INTELIGENTE")
    print("=" * 65)
    print()

    while not cerrando:

        # ====================================================
        # ESPERA POR LD2410CP
        # ====================================================
        if not sesion_activa:
            evid_dormida = True

            activada = await esperar_activacion_ld2410()

            if not activada:
                if cerrando:
                    break
                await asyncio.sleep(2)
                continue

            # ====================================================
            # ACTIVACIÓN DIRECTA: SIN INICIO DE SESIÓN
            # ====================================================
            # El LD2410CP detecta presencia y EVID entra directamente a conversación.
            # No pregunta nombre, no pide PIN y no hay pantalla de login.
            evid_dormida = False
            sesion_cerrada = False
            sesion_activa = True
            modo_anonimo = True
            nombre_anonimo = ""
            login_completado = False
            login_cancelado = False
            historial_local.clear()

            # Perfil interno para conservar memoria sin identificar al usuario.
            usuario_actual = obtener_perfil_sesion_automatica()
            # Memoria exclusivamente temporal: no se carga ni se guarda
            # información personal entre sesiones.
            cargar_historial_usuario()

            print()
            print("🟢 LD2410CP → EVID ACTIVADA DIRECTAMENTE.")
            print("🔓 Sin inicio de sesión, nombre ni PIN.")

            headers = {"Authorization": f"Bearer {API_KEY}"}
            try:
                async with websockets.connect(
                    REALTIME_URL,
                    additional_headers=headers,
                    family=socket.AF_INET,
        max_size=None,
        open_timeout=30,
        ping_interval=20,
        ping_timeout=20,
    ) as ws:
                    ws_activo = ws
                    sesion_realtime = ws

                    print("🔗 Conexión Realtime → CONVERSACIÓN DIRECTA")

                    await limpiar_buffer_realtime(ws)
                    await configurar_realtime(ws)

                    # El micrófono, Realtime y el LD2410CP trabajan
                    # simultáneamente. Antes faltaba iniciar la vigilancia
                    # de presencia en esta ruta de ACTIVACIÓN DIRECTA;
                    # por eso EVID solo se apagaba con "adiós".
                    tarea_mic = asyncio.create_task(
                        enviar_microfono(ws)
                    )

                    tarea_eventos = asyncio.create_task(
                        recibir_eventos(ws)
                    )

                    tarea_presencia = asyncio.create_task(
                        vigilar_presencia_sesion(ws)
                    )

                    try:
                        # ====================================================
                        # BIENVENIDA DE INICIO — DOS ETAPAS EN UNA RESPUESTA
                        # ====================================================
                        # 1) Presentación fija: "Hola, soy EVID. Ya estoy aquí."
                        # 2) Apertura psicológica aleatoria.
                        #
                        # Se envían juntas para que Realtime no cree dos
                        # respuestas simultáneas ni descarte la segunda.
                        bienvenida = obtener_bienvenida_inicio_completa()

                        print()
                        print("👋 BIENVENIDA DE INICIO:")
                        print(f"🤖 EVID: {bienvenida}")

                        await enviar_respuesta_texto(
                            ws,
                            bienvenida
                        )

                        print()
                        print("=" * 65)
                        print("       CONVERSACIÓN ACTIVA")
                        print("=" * 65)
                        print("🟢 EVID está conversando de forma natural.")
                        print("📡 LD2410CP → cierre automático por ausencia activo.")
                        print()

                        # La sesión termina por cualquiera de estas dos vías:
                        # 1) el usuario dice "adiós";
                        # 2) el LD2410CP confirma que ya no hay presencia.
                        done, pending = await asyncio.wait(
                            [
                                tarea_eventos,
                                tarea_presencia
                            ],
                            return_when=asyncio.FIRST_COMPLETED
                        )

                        # Mantener recibir_eventos vivo hasta que termine
                        # la despedida; de lo contrario se cancela antes
                        # de que llegue el audio a la bocina.
                        if tarea_presencia in done:
                            await esperar_fin_despedida_realtime()

                        for tarea in pending:
                            tarea.cancel()
                            try:
                                await tarea
                            except asyncio.CancelledError:
                                pass

                        for tarea in done:
                            if tarea.cancelled():
                                continue
                            try:
                                tarea.result()
                            except asyncio.CancelledError:
                                pass

                    finally:
                        if not tarea_mic.done():
                            tarea_mic.cancel()
                            try:
                                await tarea_mic
                            except asyncio.CancelledError:
                                pass

                        if not tarea_eventos.done():
                            tarea_eventos.cancel()
                            try:
                                await tarea_eventos
                            except asyncio.CancelledError:
                                pass

                        if not tarea_presencia.done():
                            tarea_presencia.cancel()
                            try:
                                await tarea_presencia
                            except asyncio.CancelledError:
                                pass

                        detener_audio_reproduccion()

                        if proceso_mic:
                            try:
                                proceso_mic.terminate()
                            except Exception:
                                pass
                            proceso_mic = None

                    ws_activo = None
                    sesion_realtime = None

            except websockets.exceptions.ConnectionClosed as error:
                if not cerrando:
                    print(f"⚠️ Conexión Realtime cerrada: {error}")
            except Exception as error:
                if not cerrando:
                    print(f"❌ Error en sesión Realtime: {error!r}")
            finally:


                if proceso_mic:
                    try:
                        proceso_mic.terminate()
                    except Exception:
                        pass
                    proceso_mic = None

                detener_audio_reproduccion()
                ws_activo = None
                sesion_realtime = None
                sesion_activa = False
                sesion_cerrada = False
                evid_dormida = False
                modo_anonimo = False
                nombre_anonimo = ""
                usuario_actual = None
                historial_local.clear()
                ld2410_distancia_referencia = None
                ld2410_ultima_distancia_movimiento = None

                if not cerrando:
                    print("🔄 EVID queda esperando una nueva activación del LD2410CP...")
                    await asyncio.sleep(1)

    limpiar()


# ============================================================
# CTRL+C
# ============================================================

def manejar_signal(signo, frame):
    """Apagado limpio para Ctrl+C y SIGTERM."""
    global cerrando

    if cerrando:
        return

    print()
    print("👋 Apagando EVID por señal del sistema...")

    limpiar()

    # Con Qt, Ctrl+C también debe salir de QApplication.exec().
    try:
        if app_qt is not None:
            app_qt.quit()
    except Exception:
        pass


signal.signal(

    signal.SIGINT,

    manejar_signal
)

signal.signal(

    signal.SIGTERM,

    manejar_signal
)


# ============================================================
# CONTROL VISUAL DE LA ESFERA
# ============================================================
class OrbController(QObject):
    levelChanged = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._level = 0.0

    def get_level(self):
        return self._level

    def set_level(self, value):
        value = max(0.0, min(1.0, float(value)))
        if abs(value - self._level) > 0.002:
            self._level = value
            self.levelChanged.emit()

    level = Property(float, get_level, set_level, notify=levelChanged)


# ============================================================
# INTERFAZ E.V.I.D. — JARVIS HUD COMPLETO
# ============================================================
class InterfazEVID:
    """Interfaz visual de EVID inspirada directamente en la referencia proporcionada.

    La interfaz es deliberadamente visual: la lógica de voz, memoria, login,
    Realtime, TTS, MPU-6050 y estados continúa en el backend original.
    """

    def __init__(self):
        if not PYSIDE6_DISPONIBLE:
            raise RuntimeError(
                "PySide6 no está instalado. Ejecuta: python3 -m pip install PySide6"
            )

        self.app = QApplication.instance() or QApplication(sys.argv)
        self.window = _VentanaEVID()

    def ejecutar(self):
        self.window.showFullScreen()
        self.window.raise_()
        self.window.activateWindow()
        return self.app.exec()


class _VentanaEVID(QWidget):
    """HUD holográfico de EVID, ajustado a la imagen de referencia."""

    def closeEvent(self, event):
        try:
            print("🪟 Alt+F4 detectado. Cerrando EVID...")
            limpiar()
        except Exception:
            pass

        event.accept()

        try:
            if app_qt is not None:
                app_qt.quit()
        except Exception:
            pass

    def __init__(self):
        super().__init__()
        self.setWindowTitle("EVID // AI")
        self.setAttribute(Qt.WA_TranslucentBackground, False)
        self.setAutoFillBackground(False)
        self.setCursor(Qt.BlankCursor)

        self.t = 0.0
        self.orb_controller = OrbController(self)

        self.fps_timer = QTimer(self)
        self.fps_timer.timeout.connect(self._tick)
        self.fps_timer.start(30)

        self._mono_family = self._buscar_fuente_mono()
        self._font_family = self._buscar_fuente()

        self._particles = []
        for i in range(115):
            a = i * 17.371
            b = i * 41.917
            self._particles.append((
                math.sin(a) * 0.5 + 0.5,
                math.cos(b) * 0.5 + 0.5,
                0.55 + ((math.sin(i * 2.71) + 1.0) * 0.5) * 1.35,
                (i * 1.618) % math.tau
            ))

    def _buscar_fuente(self):
        familias = set(QFontDatabase.families())
        for nombre in ("Rajdhani", "Orbitron", "Oxanium", "Exo 2", "Noto Sans", "DejaVu Sans"):
            if nombre in familias:
                return nombre
        return "DejaVu Sans"

    def _buscar_fuente_mono(self):
        familias = set(QFontDatabase.families())
        for nombre in ("Rajdhani", "Orbitron", "Noto Sans Mono", "DejaVu Sans Mono", "DejaVu Sans"):
            if nombre in familias:
                return nombre
        return "DejaVu Sans Mono"

    def _font(self, size, bold=False, mono=False):
        f = QFont(self._mono_family if mono else self._font_family)
        scale = min(self.width() / 1664.0, self.height() / 936.0)
        f.setPointSizeF(max(7.0, size * max(0.82, scale)))
        f.setWeight(QFont.Bold if bold else QFont.Normal)
        f.setLetterSpacing(QFont.AbsoluteSpacing, 1.15 if size <= 12 else 1.8)
        return f

    def _state(self):
        g = globals()
        if g.get("cerrando", False):
            return "APAGANDO"
        if g.get("evid_dormida", False):
            return "EN REPOSO"
        if not g.get("sesion_activa", False):
            return "IDENTIFICACIÓN"
        if g.get("audio_reproduciendo", False):
            return "HABLANDO"
        if g.get("respuesta_realtime_activa", False):
            return "PROCESANDO"
        return "ESCUCHANDO"

    def _level(self):
        # El backend funcional conserva su lógica original; la interfaz
        # obtiene un nivel visual a partir del estado actual de EVID.
        if globals().get("cerrando", False):
            target = 0.08
        elif globals().get("audio_reproduciendo", False):
            target = 0.95
        elif globals().get("respuesta_realtime_activa", False):
            target = 0.68
        elif globals().get("sesion_activa", False):
            target = 0.28
        else:
            target = 0.16

        if not hasattr(self, "_visual_level"):
            self._visual_level = target
        self._visual_level += (target - self._visual_level) * 0.13
        return max(0.0, min(1.0, self._visual_level))

    def _tick(self):
        nivel = self._level()
        self.t += 0.030
        self.orb_controller.set_level(nivel)
        self.update()

    def _background(self, p, level):
        w, h = self.width(), self.height()
        p.fillRect(self.rect(), QColor("#000204"))

        center = QPointF(w * 0.5, h * 0.49)
        grad = QRadialGradient(center, min(w, h) * 0.43)
        grad.setColorAt(0.00, QColor(0, 70, 150, 42))
        grad.setColorAt(0.24, QColor(0, 45, 110, 25))
        grad.setColorAt(0.58, QColor(0, 15, 45, 11))
        grad.setColorAt(1.00, QColor(0, 0, 0, 0))
        p.setPen(Qt.NoPen)
        p.setBrush(grad)
        p.drawEllipse(center, min(w, h) * 0.43, min(w, h) * 0.43)

        for i, (px, py, pr, phase) in enumerate(self._particles):
            x = px * w
            y = py * h
            pulse = 0.30 + 0.70 * (
                (math.sin(self.t * (1.1 + (i % 5) * 0.08) + phase) + 1.0) * 0.5
            )
            distance = abs(y - h * 0.50) / h
            visibility = max(0.18, 1.0 - distance * 1.35)
            r = pr * (0.45 + pulse * 0.65)
            alpha = int((18 + 68 * pulse) * visibility * (0.75 + level * 0.7))
            p.setPen(Qt.NoPen)
            p.setBrush(QColor(30, 175, 255, max(2, min(95, alpha))))
            p.drawEllipse(QPointF(x, y), r, r)

    def _wave_path(self, y_center, amplitude, phase, freq=1.0, samples=260):
        w = self.width()
        path = QPainterPath()
        for j in range(samples):
            u = j / (samples - 1)
            x = -w * 0.02 + w * 1.04 * u
            y = y_center
            y += math.sin(u * math.tau * (2.05 * freq) + phase) * amplitude
            y += math.sin(u * math.tau * (4.10 * freq) - phase * 0.63) * amplitude * 0.18
            y += math.sin(u * math.tau * 0.92 + phase * 0.42) * amplitude * 0.13
            if j == 0:
                path.moveTo(x, y)
            else:
                path.lineTo(x, y)
        return path

    def _wave(self, p, level):
        w, h = self.width(), self.height()
        y = h * 0.500
        base_amp = (19.0 + 17.0 * level) * (1.0 + level * 0.85)

        p.save()
        p.setCompositionMode(QPainter.CompositionMode_Plus)

        for width, alpha in ((15.0, 11), (8.0, 18), (4.0, 35)):
            pen = QPen(QColor(0, 110, 255, alpha), width)
            pen.setCapStyle(Qt.RoundCap)
            p.setPen(pen)
            p.drawPath(self._wave_path(y, base_amp * (0.86 if width >= 8 else 1.0), self.t * 1.15))

        specs = [
            (0.00, 2.2, QColor(20, 225, 255, 225)),
            (1.75, 1.25, QColor(35, 130, 255, 180)),
            (-1.10, 1.0, QColor(75, 225, 255, 145)),
        ]
        for phase_shift, width, color in specs:
            pen = QPen(color, width)
            pen.setCapStyle(Qt.RoundCap)
            p.setPen(pen)
            p.drawPath(self._wave_path(
                y + phase_shift * (1.0 + level * 0.4),
                base_amp * (0.70 + level * 0.18),
                self.t * 1.15 + phase_shift
            ))

        p.restore()

        for i in range(32):
            u = (i * 0.071 + self.t * 0.022) % 1.0
            x = w * u
            yy = y + math.sin(u * math.tau * 2.05 + self.t * 1.15) * base_amp
            r = 0.65 + 1.15 * level
            p.setPen(Qt.NoPen)
            p.setBrush(QColor(55, 215, 255, int(35 + 80 * level)))
            p.drawEllipse(QPointF(x, yy), r, r)

    def _orb_geometry(self, level):
        w, h = self.width(), self.height()
        cx = w * 0.50
        cy = h * 0.488
        base = min(w * 0.145, h * 0.255)
        breathe = math.sin(self.t * 1.75) * 0.008
        organic = math.sin(self.t * 2.35 + 0.7) * 0.010
        radius = base * (1.0 + breathe + organic + level * 0.050)
        return QPointF(cx, cy), radius

    def _outer_orbitals(self, p, center, radius, level):
        cx, cy = center.x(), center.y()
        p.save()
        p.setCompositionMode(QPainter.CompositionMode_Plus)

        for i in range(9):
            rr = radius * (1.03 + i * 0.022)
            rect = QRectF(cx - rr, cy - rr, rr * 2.0, rr * 2.0)
            color = QColor(
                20 + (i % 3) * 25,
                145 + (i % 2) * 65,
                255,
                int(35 + 75 * level)
            )
            p.setPen(QPen(color, 0.75 + level * 0.55))
            start = int((self.t * (8.0 + i * 1.9) + i * 47.0) * 16)
            span = int((55 + (i % 3) * 22) * 16)
            p.drawArc(rect, start, span)

        p.restore()

    def _sphere_mask(self, p, center, radius):
        path = QPainterPath()
        path.addEllipse(center, radius * 0.985, radius * 0.985)
        p.setClipPath(path, Qt.IntersectClip)

    def _internal_ribbons(self, p, center, radius, level):
        cx, cy = center.x(), center.y()
        p.save()
        self._sphere_mask(p, center, radius)
        p.setCompositionMode(QPainter.CompositionMode_Plus)

        for i in range(8):
            path = QPainterPath()
            phase = self.t * (0.80 + i * 0.045) + i * 0.91
            tilt = -0.75 + i * 0.22

            for j in range(130):
                u = j / 129.0
                x0 = -radius * 1.18 + u * radius * 2.36
                envelope = math.sin(math.pi * u) ** 0.42
                wave1 = math.sin(u * math.tau * (1.18 + i * 0.06) + phase)
                wave2 = math.sin(u * math.tau * 2.55 - phase * 0.74 + i)
                y0 = (
                    wave1 * radius * (0.18 + level * 0.10)
                    + wave2 * radius * 0.055
                ) * envelope
                x = x0 + math.sin(u * math.pi + phase) * radius * (0.12 + level * 0.045)
                y = cy + y0 + tilt * (x0 / radius) * radius * 0.12
                if j == 0:
                    path.moveTo(cx + x, y)
                else:
                    path.lineTo(cx + x, y)

            col = [
                QColor(25, 205, 255, 135),
                QColor(35, 120, 255, 105),
                QColor(70, 230, 255, 125),
                QColor(20, 165, 255, 92),
            ][i % 4]
            col.setAlpha(int(col.alpha() + 55 * level))

            p.setPen(QPen(col, 5.0 + level * 3.0))
            p.drawPath(path)
            p.setPen(QPen(QColor(85, 235, 255, int(105 + 95 * level)), 1.0 + level * 1.15))
            p.drawPath(path)

        p.restore()

    def _sphere_body(self, p, center, radius, level):
        cx, cy = center.x(), center.y()

        p.save()
        p.setCompositionMode(QPainter.CompositionMode_Plus)
        for k in range(10, 0, -1):
            rr = radius * (1.0 + k * 0.075)
            alpha = int((3 + (10 - k) * 2.0) * (0.7 + level))
            p.setPen(Qt.NoPen)
            p.setBrush(QColor(0, 130, 255, max(1, min(30, alpha))))
            p.drawEllipse(center, rr, rr)
        p.restore()

        sphere = QRadialGradient(
            QPointF(cx - radius * 0.26, cy - radius * 0.30),
            radius * 1.13
        )
        sphere.setColorAt(0.00, QColor(110, 245, 255, 170 + int(25 * level)))
        sphere.setColorAt(0.16, QColor(35, 190, 255, 150 + int(30 * level)))
        sphere.setColorAt(0.36, QColor(7, 85, 205, 125))
        sphere.setColorAt(0.62, QColor(3, 27, 92, 180))
        sphere.setColorAt(0.82, QColor(1, 10, 34, 225))
        sphere.setColorAt(1.00, QColor(0, 2, 12, 245))

        p.setPen(QPen(QColor(55, 215, 255, 235), 1.7 + level * 1.25))
        p.setBrush(sphere)
        p.drawEllipse(center, radius, radius)

        p.setBrush(Qt.NoBrush)
        p.setPen(QPen(QColor(80, 235, 255, 150), 1.0))
        p.drawEllipse(center, radius * 0.968, radius * 0.968)

        highlight = QRadialGradient(
            QPointF(cx - radius * 0.34, cy - radius * 0.39),
            radius * 0.52
        )
        highlight.setColorAt(0.00, QColor(235, 255, 255, 125))
        highlight.setColorAt(0.18, QColor(125, 245, 255, 80))
        highlight.setColorAt(0.48, QColor(35, 180, 255, 25))
        highlight.setColorAt(1.00, QColor(0, 0, 0, 0))
        p.setPen(Qt.NoPen)
        p.setBrush(highlight)
        p.drawEllipse(
            QPointF(cx - radius * 0.27, cy - radius * 0.30),
            radius * 0.58, radius * 0.44
        )

    def _core(self, p, center, radius, level):
        core = QRadialGradient(center, radius * (0.36 + level * 0.11))
        core.setColorAt(0.00, QColor(220, 255, 255, 205))
        core.setColorAt(0.12, QColor(85, 240, 255, 170))
        core.setColorAt(0.34, QColor(15, 165, 255, 105))
        core.setColorAt(0.67, QColor(10, 80, 230, 35))
        core.setColorAt(1.00, QColor(0, 0, 0, 0))

        p.setCompositionMode(QPainter.CompositionMode_Plus)
        p.setPen(Qt.NoPen)
        p.setBrush(core)
        rr = radius * (0.33 + level * 0.11)
        p.drawEllipse(center, rr, rr * 0.86)

        pulse = 0.82 + 0.18 * math.sin(self.t * 3.2)
        p.setBrush(QColor(200, 255, 255, int(90 * pulse + 50 * level)))
        p.drawEllipse(center, radius * 0.035, radius * 0.035)
        p.setCompositionMode(QPainter.CompositionMode_SourceOver)

    def _orb(self, p, level):
        center, radius = self._orb_geometry(level)
        cx, cy = center.x(), center.y()

        shadow = QRadialGradient(
            QPointF(cx, cy + radius * 1.05), radius * 0.82
        )
        shadow.setColorAt(0.00, QColor(0, 120, 255, int(105 + 65 * level)))
        shadow.setColorAt(0.20, QColor(0, 90, 255, int(65 + 40 * level)))
        shadow.setColorAt(0.55, QColor(0, 35, 120, 22))
        shadow.setColorAt(1.00, QColor(0, 0, 0, 0))
        p.setCompositionMode(QPainter.CompositionMode_Plus)
        p.setPen(Qt.NoPen)
        p.setBrush(shadow)
        p.drawEllipse(QPointF(cx, cy + radius * 1.05), radius * 0.82, radius * 0.22)
        p.setCompositionMode(QPainter.CompositionMode_SourceOver)

        self._outer_orbitals(p, center, radius, level)
        self._sphere_body(p, center, radius, level)
        self._internal_ribbons(p, center, radius, level)
        self._core(p, center, radius, level)

        p.save()
        self._sphere_mask(p, center, radius * 0.96)
        p.setCompositionMode(QPainter.CompositionMode_Plus)

        for i in range(44):
            a = i * math.tau / 44.0 + self.t * (0.10 + level * 0.18)
            rr = radius * (
                0.16 + 0.67 *
                ((math.sin(i * 2.31 + self.t * 0.75) + 1.0) * 0.5)
            )
            x = cx + math.cos(a) * rr
            y = cy + math.sin(a) * rr * 0.80
            r = 0.45 + level * 1.0
            alpha = int(28 + 105 * level)
            p.setPen(Qt.NoPen)
            p.setBrush(QColor(65, 220, 255, alpha))
            p.drawEllipse(QPointF(x, y), r, r)

        p.restore()

    def _hud(self, p):
        w, h = self.width(), self.height()

        # Superior izquierdo
        p.setPen(QColor("#18D9FF"))
        p.setFont(self._font(18, True, True))
        p.drawText(QRectF(50, 24, 360, 36), Qt.AlignLeft | Qt.AlignVCenter, "EVID // AI")

        p.setPen(QColor("#7FA0B7"))
        p.setFont(self._font(9, False, True))
        p.drawText(QRectF(51, 61, 360, 25), Qt.AlignLeft | Qt.AlignVCenter, "TU ASISTENTE INTELIGENTE")

        p.setPen(QPen(QColor("#18D9FF"), 2.0))
        p.drawLine(QPointF(51, 101), QPointF(92, 101))

        # Superior derecho
        card_w = min(350.0, w * 0.235)
        card_h = 110.0
        card_x = w - card_w - 55
        card_y = 24

        p.setPen(QPen(QColor(18, 115, 150, 190), 1.0))
        p.setBrush(QColor(2, 7, 14, 220))
        p.drawRoundedRect(QRectF(card_x, card_y, card_w, card_h), 16, 16)

        p.setPen(QColor("#18D9FF"))
        p.setFont(self._font(12, True, True))
        p.drawText(QRectF(card_x + 27, card_y + 19, 100, 25), Qt.AlignLeft | Qt.AlignVCenter, "EVID")

        p.setPen(QColor("#EEF9FF"))
        p.setFont(self._font(22, False, True))
        p.drawText(
            QRectF(card_x + card_w - 130, card_y + 13, 105, 32),
            Qt.AlignRight | Qt.AlignVCenter,
            time.strftime("%H:%M")
        )

        dias = ["LUN", "MAR", "MIÉ", "JUE", "VIE", "SÁB", "DOM"]
        fecha = f"{dias[time.localtime().tm_wday]} {time.strftime('%d %b %Y').upper()}"

        p.setPen(QColor("#A7BAC7"))
        p.setFont(self._font(7.5, False, True))
        p.drawText(
            QRectF(card_x + card_w - 185, card_y + 48, 160, 18),
            Qt.AlignRight | Qt.AlignVCenter,
            fecha
        )

        estado = self._state()

        p.setPen(Qt.NoPen)
        p.setBrush(QColor("#16E7FF"))
        p.drawEllipse(QPointF(card_x + 40, card_y + 74), 8.0, 8.0)
        p.setBrush(QColor(0, 210, 255, 45))
        p.drawEllipse(QPointF(card_x + 40, card_y + 74), 14.0, 14.0)

        p.setPen(QColor("#E7F4FA"))
        p.setFont(self._font(8.5, True, True))
        p.drawText(
            QRectF(card_x + 68, card_y + 62, card_w - 85, 25),
            Qt.AlignLeft | Qt.AlignVCenter,
            estado
        )

    def _footer(self, p):
        w, h = self.width(), self.height()

        p.setPen(QPen(QColor("#18D9FF"), 2.0))
        p.drawLine(QPointF(51, h - 92), QPointF(92, h - 92))

        p.setPen(QColor("#8AA9BB"))
        p.setFont(self._font(8.5, False, True))
        p.drawText(
            QRectF(51, h - 77, 430, 25),
            Qt.AlignLeft | Qt.AlignVCenter,
            "RASPBERRY PI 5  •  EVID v2.0"
        )

        p.drawText(
            QRectF(w - 430, h - 77, 330, 25),
            Qt.AlignRight | Qt.AlignVCenter,
            "EVID  •  TU ASISTENTE INTELIGENTE"
        )

        p.setPen(QPen(QColor("#18D9FF"), 2.0))
        p.drawLine(QPointF(w - 90, h - 92), QPointF(w - 50, h - 92))

    def paintEvent(self, event):
        p = QPainter(self)
        try:
            p.setRenderHint(QPainter.Antialiasing, True)
            p.setRenderHint(QPainter.TextAntialiasing, True)

            level = self._level()
            self._background(p, level)
            self._wave(p, level)
            self._orb(p, level)
            self._hud(p)
            self._footer(p)
        finally:
            if p.isActive():
                p.end()

# ============================================================
# EJECUTAR
# ============================================================

if __name__ == "__main__":

    try:
        # Qt debe vivir en el hilo principal. El backend funcional de EVID
        # se ejecuta aparte y conserva asyncio, audio, Realtime, memoria,
        # login, reposo y MPU-6050.
        if PYSIDE6_DISPONIBLE:
            interfaz = InterfazEVID()
            app_qt = interfaz.app
            backend = threading.Thread(
                target=lambda: asyncio.run(main()),
                daemon=True,
                name="EVID-Backend"
            )
            backend.start()
            try:
                interfaz.ejecutar()
            finally:
                limpiar()
                if backend.is_alive():
                    backend.join(timeout=3.0)
        else:
            print("⚠️ PySide6 no está instalado.")
            print("   Instálalo con: python3 -m pip install PySide6")
            asyncio.run(main())

    except KeyboardInterrupt:
        limpiar()

    except Exception as error:
        print()
        print("❌ Error fatal:")
        print(repr(error))
        limpiar()
