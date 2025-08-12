#!/bin/bash
set -e

# Esperar a que PostgreSQL esté listo (solo para troubleshooting)
echo "Verificando conexión a PostgreSQL..."
until psql -Atx "$DATABASE_URL" -c 'SELECT 1' >/dev/null; do
  echo "PostgreSQL no está disponible aún - esperando..."
  sleep 2
done

echo "PostgreSQL está listo - iniciando aplicación..."
exec gunicorn app:app --workers 4 --bind 0.0.0.0:$PORT --timeout 120 --access-logfile -