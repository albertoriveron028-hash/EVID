#!/usr/bin/env python3

import os
import subprocess
from datetime import datetime

BASE_DIR = "/home/evidence/jarvis"
REPO = "origin"
BRANCH = "main"


def run_command(command):
    result = subprocess.run(
        command,
        cwd=BASE_DIR,
        text=True,
        capture_output=True
    )

    return result.returncode, result.stdout.strip(), result.stderr.strip()


def main():
    print("=" * 50)
    print("        EVID UPDATE AGENT - PRUEBA")
    print("=" * 50)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
    else:
        print("🆕 Hay una actualización disponible.")
        print("⚠️ MODO PRUEBA: NO se instalará todavía.")

    print()
    print("=" * 50)


if __name__ == "__main__":
    main()
