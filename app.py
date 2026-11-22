# Servidor Flask en la nube para el sistema de riego
# Misma arquitectura de la guía del laboratorio (cliente-servidor, API REST, SQL),
# pero publicado en internet:
#
#   ESP32  --HTTPS POST JSON-->  Flask en Render  --SQL-->  PostgreSQL en Supabase
#   Navegador/celular  --GET /-->  Flask  (el panel pide /api/lecturas cada 5 s)
#
# Variables de entorno (se configuran en Render, no se escriben en el código):
#   DATABASE_URL  cadena de conexión de Supabase ("Session pooler", puerto 5432)
#   API_KEY       clave que el ESP32 envía en el encabezado X-API-Key
#
# Para probar en tu PC:
#   pip install -r requirements.txt
#   set DATABASE_URL=postgresql://...     (PowerShell: $env:DATABASE_URL="postgresql://...")
#   set API_KEY=riego2026
#   python app.py

import os
from zoneinfo import ZoneInfo

import psycopg
from flask import Flask, jsonify, render_template_string, request
from psycopg.rows import dict_row

DATABASE_URL = os.environ.get("DATABASE_URL", "")
API_KEY = os.environ.get("API_KEY", "riego2026")
ZONA = ZoneInfo("America/Bogota")

app = Flask(__name__)

ESQUEMA = """
CREATE TABLE IF NOT EXISTS lecturas (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  temperatura NUMERIC(5,1) NOT NULL,
  humedad     NUMERIC(5,1) NOT NULL,
  suelo       SMALLINT     NOT NULL CHECK (suelo BETWEEN 0 AND 100),
  bomba       BOOLEAN      NOT NULL DEFAULT FALSE,
  ventilador  BOOLEAN      NOT NULL DEFAULT FALSE,
  creado_en   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_lecturas_creado ON lecturas (creado_en);
"""


def get_db_connection():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=10)


def crear_tabla():
    """Crea la tabla si no existe. Se ejecuta una vez al arrancar el servidor."""
    if not DATABASE_URL:
        print("[AVISO] DATABASE_URL no está configurada")
        return
    try:
        with get_db_connection() as conn:
            conn.execute(ESQUEMA)
        print("[OK] Tabla 'lecturas' lista")
    except psycopg.Error as e:
        print(f"[ERROR] No se pudo crear la tabla: {e}")


crear_tabla()


# ---------- API: el ESP32 envía una lectura ----------
@app.route("/api/lecturas", methods=["POST"])
def guardar_lectura():
    if request.headers.get("X-API-Key") != API_KEY:
        return jsonify(error="no autorizado"), 401

    d = request.get_json(silent=True)
    if not d:
        return jsonify(error="se esperaba JSON"), 400
    try:
        valores = (
            float(d["temperatura"]),
            float(d["humedad"]),
            int(d["suelo"]),
            bool(d.get("bomba", False)),
            bool(d.get("ventilador", False)),
        )
    except (KeyError, ValueError, TypeError):
        return jsonify(error="faltan campos o tienen formato incorrecto"), 400
    if not 0 <= valores[2] <= 100:
        return jsonify(error="suelo debe estar entre 0 y 100"), 400

    try:
        with get_db_connection() as conn:
            fila = conn.execute(
                "INSERT INTO lecturas (temperatura, humedad, suelo, bomba, ventilador) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                valores,
            ).fetchone()
    except psycopg.Error as e:
        print(f"[ERROR] Base de datos: {e}")
        return jsonify(error="error de base de datos"), 500

    return jsonify(ok=True, id=fila["id"]), 201


# ---------- API: el panel consulta las últimas lecturas ----------
@app.route("/api/lecturas", methods=["GET"])
def listar_lecturas():
    n = max(1, min(request.args.get("n", 100, type=int), 1000))
    try:
        with get_db_connection() as conn:
            filas = conn.execute(
                "SELECT id, temperatura, humedad, suelo, bomba, ventilador, creado_en "
                "FROM lecturas ORDER BY id DESC LIMIT %s",
                (n,),
            ).fetchall()
    except psycopg.Error as e:
        print(f"[ERROR] Base de datos: {e}")
        return jsonify(error="error de base de datos"), 500

    filas.reverse()  # de la más antigua a la más reciente, para graficar
    for f in filas:
        f["temperatura"] = float(f["temperatura"])
        f["humedad"] = float(f["humedad"])
        f["creado_en"] = f["creado_en"].astimezone(ZONA).strftime("%Y-%m-%d %H:%M:%S")
    return jsonify(filas)


@app.route("/api/status")
def status():
    try:
        with get_db_connection() as conn:
            conn.execute("SELECT 1")
        return jsonify(status="OK", db="conectada")
    except psycopg.Error:
        return jsonify(status="ERROR", db="desconectada"), 500


# ---------- Página del panel ----------
PANEL_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Monitoreo de riego</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
  <style>
    body { font-family: system-ui, sans-serif; margin: 0; background: #f4f6f4; color: #1d2b1f; }
    header { background: #2e6b3a; color: #fff; padding: 14px 20px; }
    header h1 { margin: 0; font-size: 1.3rem; }
    main { max-width: 1000px; margin: 0 auto; padding: 16px; }
    .tarjetas { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }
    .tarjeta { background: #fff; border-radius: 10px; padding: 14px; box-shadow: 0 1px 3px rgba(0,0,0,.1); }
    .tarjeta span { display: block; font-size: .85rem; color: #5b6b5d; }
    .tarjeta b { font-size: 1.6rem; }
    .on { color: #2e6b3a; } .off { color: #8a8f8a; }
    .grafica { background: #fff; border-radius: 10px; padding: 14px; margin-top: 16px; box-shadow: 0 1px 3px rgba(0,0,0,.1); }
    #estado { font-size: .85rem; color: #5b6b5d; margin-top: 10px; }
  </style>
</head>
<body>
<header><h1>Monitoreo de riego</h1></header>
<main>
  <div class="tarjetas">
    <div class="tarjeta"><span>Temperatura</span><b id="t">--</b></div>
    <div class="tarjeta"><span>Humedad del aire</span><b id="h">--</b></div>
    <div class="tarjeta"><span>Humedad del suelo</span><b id="s">--</b></div>
    <div class="tarjeta"><span>Bomba</span><b id="b">--</b></div>
    <div class="tarjeta"><span>Ventilador</span><b id="v">--</b></div>
  </div>
  <div class="grafica"><canvas id="grafica" height="120"></canvas></div>
  <div id="estado">Cargando...</div>
</main>
<script>
const grafica = new Chart(document.getElementById("grafica"), {
  type: "line",
  data: { labels: [], datasets: [
    { label: "Temperatura (°C)", data: [], borderColor: "#d9534f", tension: .3, pointRadius: 0 },
    { label: "Humedad aire (%)", data: [], borderColor: "#2a7fd4", tension: .3, pointRadius: 0 },
    { label: "Humedad suelo (%)", data: [], borderColor: "#3a9b4a", tension: .3, pointRadius: 0 }
  ]},
  options: { animation: false, scales: { y: { suggestedMin: 0, suggestedMax: 100 } } }
});

function estadoRele(id, on, texto) {
  const el = document.getElementById(id);
  el.textContent = on ? texto[0] : texto[1];
  el.className = on ? "on" : "off";
}

async function actualizar() {
  try {
    const r = await fetch("/api/lecturas?n=100");
    const datos = await r.json();
    if (!r.ok) throw new Error(datos.error || r.status);
    if (datos.length === 0) { document.getElementById("estado").textContent = "Sin datos todavía"; return; }

    const u = datos[datos.length - 1];
    document.getElementById("t").textContent = u.temperatura.toFixed(1) + " °C";
    document.getElementById("h").textContent = u.humedad.toFixed(0) + " %";
    document.getElementById("s").textContent = u.suelo + " %";
    estadoRele("b", u.bomba, ["Encendida", "Apagada"]);
    estadoRele("v", u.ventilador, ["Encendido", "Apagado"]);

    grafica.data.labels = datos.map(d => d.creado_en.slice(11));
    grafica.data.datasets[0].data = datos.map(d => d.temperatura);
    grafica.data.datasets[1].data = datos.map(d => d.humedad);
    grafica.data.datasets[2].data = datos.map(d => d.suelo);
    grafica.update();
    document.getElementById("estado").textContent = "Última lectura: " + u.creado_en;
  } catch (e) {
    document.getElementById("estado").textContent = "Error al leer el servidor: " + e.message;
  }
}
actualizar();
setInterval(actualizar, 5000);
</script>
</body>
</html>
"""


@app.route("/")
def panel():
    return render_template_string(PANEL_HTML)


if __name__ == "__main__":
    # Solo para pruebas en el PC. En Render se arranca con gunicorn (ver render.yaml)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
