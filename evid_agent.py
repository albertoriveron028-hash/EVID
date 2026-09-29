#!/usr/bin/env python3

import os
import subprocess
import tarfile
import shutil
from datetime import datetime

BASE_DIR = "/home/evidence/jarvis"
BACKUP_DIR = "/home/evidence/backups_evid/updates"
TEMP_DIR = "/tmp/evid_update_test"

REPO = "origin"
BRANCH = "main"

FILES_TO_CHECK = [
    "evid_mark72.py",
    "usuarios.py",
]


def run_command(command, cwd=BASE_DIR):
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=True
    )

    return (
        result.returncode,
        result.stdout.strip(),
        result.stderr.strip()
    )


def create_backup():
    os.makedirs(BACKUP_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(
        BACKUP_DIR,
        f"evid_backup_{timestamp}.tar.gz"
    )

    print("💾 Creando respaldo seguro...")
    print(backup_file)
    print()

    # Obtener únicamente archivos controlados por Git
    code, stdout, stderr = run_command(
        ["git", "ls-files"]
    )

    if code != 0:
        print("❌ No se pudieron obtener los archivos de Git.")
        print(stderr)
        return None

    tracked_files = [
        line.strip()
        for line in stdout.splitlines()
        if line.strip()
    ]

    try:
        with tarfile.open(backup_file, "w:gz") as tar:

            # Código controlado por Git
            for relative_path in tracked_files:
                full_path = os.path.join(BASE_DIR, relative_path)

                if os.path.exists(full_path):
                    tar.add(
                        full_path,
                        arcname=relative_path,
                        recursive=True
                    )

            # Archivo de configuración privado
            env_file = os.path.join(BASE_DIR, ".env")

            if os.path.exists(env_file):
                tar.add(
                    env_file,
                    arcname=".env",
                    recursive=False
                )

            # Datos privados de cada EVID
            for directory in ["usuarios", "memoria"]:
                local_dir = os.path.join(BASE_DIR, directory)

                if os.path.exists(local_dir):
                    tar.add(
                        local_dir,
                        arcname=directory,
                        recursive=True
                    )

    except Exception as e:
        print("❌ Error creando el respaldo:")
        print(e)

        if os.path.exists(backup_file):
            os.remove(backup_file)

        return None

    size_mb = os.path.getsize(backup_file) / (1024 * 1024)

    print(f"✅ Respaldo creado correctamente.")
    print(f"📦 Tamaño del respaldo: {size_mb:.2f} MB")
    print()

    return backup_file

def prepare_update(remote_commit):
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)

    print("📥 Descargando actualización en carpeta temporal...")
    print(TEMP_DIR)
    print()

    code, stdout, stderr = run_command(
        [
            "git",
            "clone",
            "--branch",
            BRANCH,
            "--single-branch",
            f"git@github.com:bzfr2mgcw6-glitch/EVID.git",
            TEMP_DIR
        ],
        cwd="/tmp"
    )

    if code != 0:
        print("❌ No se pudo descargar la actualización.")
        print(stderr)
        return False

    print("✅ Actualización descargada.")
    print()

    code, commit, stderr = run_command(
        ["git", "rev-parse", "HEAD"],
        cwd=TEMP_DIR
    )

    if code != 0:
        print("❌ No se pudo verificar el commit descargado.")
        return False

    print("☁️ Commit descargado:")
    print(commit)
    print()

    if commit != remote_commit:
        print("❌ El commit descargado no coincide con GitHub.")
        return False

    print("✅ Commit verificado.")
    print()

    print("🧪 Comprobando sintaxis Python...")

    for filename in FILES_TO_CHECK:
        filepath = os.path.join(TEMP_DIR, filename)

        if not os.path.exists(filepath):
            print(f"❌ Falta el archivo: {filename}")
            return False

        code, stdout, stderr = run_command(
            ["python", "-m", "py_compile", filename],
            cwd=TEMP_DIR
        )

        if code != 0:
            print(f"❌ Error de sintaxis en {filename}")
            print(stderr)
            return False

        print(f"   ✅ {filename}")

    print()
    print("✅ Todas las comprobaciones pasaron.")
    print()

    return True


def main():

    print("=" * 55)
    print("        EVID UPDATE AGENT - MODO SEGURO")
    print("=" * 55)
    print(
        f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print()

    print("📁 Directorio:")
    print(BASE_DIR)
    print()

    print("🔎 Comprobando Git...")

    code, stdout, stderr = run_command(
        ["git", "rev-parse", "--is-inside-work-tree"]
    )

    if code != 0:
        print("❌ Este directorio no es un repositorio Git.")
        print(stderr)
        return

    print("✅ Repositorio Git detectado.")
    print()

    print("🌐 Comprobando conexión con GitHub...")

    code, stdout, stderr = run_command(
        ["git", "fetch", REPO, BRANCH]
    )

    if code != 0:
        print("❌ No se pudo consultar GitHub.")
        print(stderr)
        return

    print("✅ GitHub respondió correctamente.")
    print()

    code, local, _ = run_command(
        ["git", "rev-parse", "HEAD"]
    )

    code, remote, _ = run_command(
        ["git", "rev-parse", f"{REPO}/{BRANCH}"]
    )

    print("📌 Versión local:")
    print(local)

    print()
    print("☁️ Versión en GitHub:")
    print(remote)
    print()

    if local == remote:
        print("✅ EVID está actualizado.")
        print()
        print("=" * 55)
        return

    print("🆕 Hay una actualización disponible.")
    print()

    backup_file = create_backup()

    if not backup_file or not os.path.exists(backup_file):
        print("❌ No se pudo crear el respaldo.")
        print("🛑 ACTUALIZACIÓN CANCELADA.")
        return

    success = prepare_update(remote)

    if not success:
        print()
        print("🛑 Las comprobaciones fallaron.")
        print("🛡️ Tu EVID actual NO fue modificado.")
        print("=" * 55)
        return

    print("======================================================")
    print("✅ ACTUALIZACIÓN PREPARADA CORRECTAMENTE")
    print("======================================================")
    print()
    print("🛡️ Respaldo:")
    print(backup_file)
    print()
    print("📦 Nueva versión:")
    print(remote)
    print()
    print("⚠️ MODO SEGURO:")
    print("La actualización todavía NO se instaló.")
    print("Tu EVID actual permanece intacto.")
    print()
    print("=" * 55)


if __name__ == "__main__":
    main()
# UPDATE TEST - 2026-09-29 14:41:05
# TEST REMOTO - nueva version disponible
