# Servidor en la nube para el carro de inspección de estantería
#
#   ESP32 (carro) --HTTPS cada 1 s--> /api/dispositivo/sync
#        envía: su estado + eventos nuevos (falta producto, repuesto, inicio, paro...)
#        recibe: las órdenes pendientes (iniciar, detener, borrar alarmas, cambiar pausa)
#
#   Navegador / celular --> panel con contraseña (/), que lee /api/estado y /api/eventos
#        y envía órdenes a /api/comando
#
# Variables de entorno (se configuran en Render):
#   DATABASE_URL  cadena de conexión de Supabase ("Session pooler", puerto 5432)
#   API_KEY       clave que el ESP32 envía en el encabezado X-API-Key
#   PANEL_CLAVE   contraseña para entrar al panel
#   SECRET_KEY    texto largo al azar para firmar la sesión del panel
#   DEV=1         solo para pruebas en el PC por http (no la pongas en Render)

import hmac
import os
import time
from collections import deque
from datetime import timedelta
from functools import wraps
from zoneinfo import ZoneInfo

import psycopg
from flask import Flask, jsonify, redirect, render_template_string, request, session, url_for
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get("DATABASE_URL", "")
API_KEY = os.environ.get("API_KEY", "")
PANEL_CLAVE = os.environ.get("PANEL_CLAVE", "")
ZONA = ZoneInfo("America/Bogota")

LIMITE_EN_LINEA = 8      # segundos sin noticias del carro para considerarlo desconectado
VIGENCIA_COMANDO = 30    # una orden que el carro no recoge en 30 s se descarta (seguridad)
COMANDOS = {"on", "off", "reset", "espera"}

# Historial del voltaje del potenciómetro (posición del carro): 1 punto por reporte, últimos 30 min.
# Vive en la memoria del servidor (gunicorn con 1 worker); se reinicia si Render reinicia el servicio.
HISTORIAL_MV = deque(maxlen=1800)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(32)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=not os.environ.get("DEV"),
    PERMANENT_SESSION_LIFETIME=timedelta(days=7),
)

ESQUEMA = """
CREATE TABLE IF NOT EXISTS est_estado (
  id          SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  datos       JSONB       NOT NULL,
  actualizado TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS est_comandos (
  id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  comando   TEXT        NOT NULL,
  valor     INT         NOT NULL DEFAULT 0,
  estado    TEXT        NOT NULL DEFAULT 'pendiente',   -- pendiente | entregado | vencido
  creado    TIMESTAMPTZ NOT NULL DEFAULT now(),
  entregado TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS est_eventos (
  id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tipo     TEXT        NOT NULL,
  posicion SMALLINT,
  mensaje  TEXT        NOT NULL,
  creado   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_est_eventos_creado ON est_eventos (creado DESC);
CREATE INDEX IF NOT EXISTS idx_est_comandos_pend ON est_comandos (estado) WHERE estado = 'pendiente';
"""

pool = None
if DATABASE_URL:
    pool = ConnectionPool(
        DATABASE_URL,
        min_size=1,
        max_size=4,
        max_idle=120,
        check=ConnectionPool.check_connection,
        kwargs={"row_factory": dict_row, "autocommit": True, "connect_timeout": 10},
        open=True,
    )
    try:
        with pool.connection() as conn:
            conn.execute(ESQUEMA)
        print("[OK] Tablas de la estantería listas")
    except psycopg.Error as e:
        print(f"[ERROR] No se pudieron crear las tablas: {e}")
else:
    print("[AVISO] DATABASE_URL no está configurada")


def error_bd(e):
    print(f"[ERROR] Base de datos: {e}")
    return jsonify(error="error de base de datos"), 500


# ================= API del carro (ESP32) =================
@app.route("/api/dispositivo/sync", methods=["POST"])
def sync():
    if not API_KEY or not hmac.compare_digest(request.headers.get("X-API-Key", ""), API_KEY):
        return "no autorizado", 401
    d = request.get_json(silent=True)
    if not isinstance(d, dict) or not isinstance(d.get("estado"), dict):
        return "se esperaba {estado, eventos}", 400

    mv = d["estado"].get("mv")
    if isinstance(mv, (int, float)) and 0 <= mv <= 3300:
        HISTORIAL_MV.append((time.time(), int(mv), str(d["estado"].get("estado", ""))[:12]))

    eventos = []
    for ev in (d.get("eventos") or [])[:20]:
        if not isinstance(ev, dict):
            continue
        tipo = str(ev.get("tipo", ""))[:20]
        msg = str(ev.get("msg", ""))[:200]
        pos = ev.get("pos")
        pos = pos if isinstance(pos, int) and 0 <= pos < 20 else None
        if tipo and msg:
            eventos.append((tipo, pos, msg))

    try:
        with pool.connection() as conn, conn.transaction():
            conn.execute(
                "INSERT INTO est_estado (id, datos, actualizado) VALUES (1, %s, now()) "
                "ON CONFLICT (id) DO UPDATE SET datos = EXCLUDED.datos, actualizado = now()",
                (Jsonb(d["estado"]),),
            )
            for ev in eventos:
                conn.execute("INSERT INTO est_eventos (tipo, posicion, mensaje) VALUES (%s, %s, %s)", ev)
            conn.execute(
                "UPDATE est_comandos SET estado = 'vencido' "
                "WHERE estado = 'pendiente' AND creado <= now() - make_interval(secs => %s)",
                (VIGENCIA_COMANDO,),
            )
            ordenes = conn.execute(
                "UPDATE est_comandos SET estado = 'entregado', entregado = now() "
                "WHERE estado = 'pendiente' RETURNING id, comando, valor"
            ).fetchall()
    except psycopg.Error as e:
        print(f"[ERROR] Base de datos: {e}")
        return "error de base de datos", 500

    # Respuesta en texto simple para que el ESP32 la lea sin librerías: "id comando valor" por línea
    ordenes.sort(key=lambda o: o["id"])
    cuerpo = "".join(f"{o['id']} {o['comando']} {o['valor']}\n" for o in ordenes)
    return cuerpo, 200, {"Content-Type": "text/plain; charset=utf-8"}


# ================= Sesión del panel =================
def requiere_sesion(f):
    @wraps(f)
    def envoltura(*a, **k):
        if not session.get("ok"):
            if request.path.startswith("/api/"):
                return jsonify(error="sesión vencida"), 401
            return redirect(url_for("login"))
        return f(*a, **k)
    return envoltura


@app.route("/login", methods=["GET", "POST"])
def login():
    error = ""
    if request.method == "POST":
        clave = request.form.get("clave", "")
        if PANEL_CLAVE and hmac.compare_digest(clave, PANEL_CLAVE):
            session.permanent = True
            session["ok"] = True
            return redirect(url_for("panel"))
        time.sleep(1)  # frena los intentos repetidos
        error = "Contraseña incorrecta." if PANEL_CLAVE else "Falta configurar PANEL_CLAVE en el servidor."
    return render_template_string(LOGIN_HTML, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ================= API del panel =================
@app.route("/api/estado")
@requiere_sesion
def api_estado():
    try:
        with pool.connection() as conn:
            fila = conn.execute(
                "SELECT datos, actualizado, EXTRACT(EPOCH FROM now() - actualizado) AS edad FROM est_estado WHERE id = 1"
            ).fetchone()
            pend = conn.execute("SELECT count(*) AS n FROM est_comandos WHERE estado = 'pendiente'").fetchone()["n"]
    except psycopg.Error as e:
        return error_bd(e)
    if not fila:
        return jsonify(sinDatos=True, enLinea=False, pendientes=pend)
    datos = dict(fila["datos"])
    edad = float(fila["edad"])
    datos.update(
        enLinea=edad <= LIMITE_EN_LINEA,
        edad=round(edad),
        actualizado=fila["actualizado"].astimezone(ZONA).strftime("%d/%m %H:%M:%S"),
        pendientes=pend,
    )
    return jsonify(datos)


@app.route("/api/comando", methods=["POST"])
@requiere_sesion
def api_comando():
    d = request.get_json(silent=True) or {}
    comando = d.get("comando")
    if comando not in COMANDOS:
        return jsonify(error="orden no válida"), 400
    valor = 0
    if comando == "espera":
        try:
            valor = int(d.get("valor"))
        except (TypeError, ValueError):
            return jsonify(error="la pausa debe ser un número"), 400
        if not 1 <= valor <= 600:
            return jsonify(error="la pausa debe estar entre 1 y 600 s"), 400
    try:
        with pool.connection() as conn:
            if comando == "espera":
                # solo vale la última pausa pedida
                conn.execute("UPDATE est_comandos SET estado = 'vencido' WHERE estado = 'pendiente' AND comando = 'espera'")
            fila = conn.execute(
                "INSERT INTO est_comandos (comando, valor) VALUES (%s, %s) RETURNING id", (comando, valor)
            ).fetchone()
    except psycopg.Error as e:
        return error_bd(e)
    return jsonify(ok=True, id=fila["id"]), 201


@app.route("/api/eventos")
@requiere_sesion
def api_eventos():
    n = max(1, min(request.args.get("n", 40, type=int), 200))
    try:
        with pool.connection() as conn:
            filas = conn.execute(
                "SELECT id, tipo, posicion, mensaje, creado FROM est_eventos ORDER BY id DESC LIMIT %s", (n,)
            ).fetchall()
    except psycopg.Error as e:
        return error_bd(e)
    for f in filas:
        f["hora"] = f.pop("creado").astimezone(ZONA).strftime("%d/%m %H:%M:%S")
    return jsonify(filas)


@app.route("/api/voltaje")
@requiere_sesion
def api_voltaje():
    seg = max(30, min(request.args.get("seg", 300, type=int), 1800))
    desde = time.time() - seg
    puntos = [[round(t * 1000), mv, e] for t, mv, e in list(HISTORIAL_MV) if t >= desde]
    return jsonify(ahora=round(time.time() * 1000), seg=seg, puntos=puntos)


@app.route("/api/status")
def status():
    try:
        with pool.connection() as conn:
            conn.execute("SELECT 1")
        return jsonify(status="OK", db="conectada")
    except Exception:
        return jsonify(status="ERROR", db="desconectada"), 500


@app.route("/")
@requiere_sesion
def panel():
    return render_template_string(PANEL_HTML)


# ================= Páginas =================
LOGIN_HTML = """<!doctype html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#1C2733">
<title>Entrar · Estantería</title>
<style>
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:#DCE1E5;color:#1C2733;font:15px/1.45 Roboto,system-ui,-apple-system,"Segoe UI",sans-serif}
form{width:min(360px,92vw);background:#EDF0F2;border:1px solid #B8C1C9;border-radius:6px;padding:24px}
h1{margin:0 0 4px;font:600 22px "Roboto Condensed","Arial Narrow",sans-serif}
p{margin:0 0 18px;color:#5A6977}
label{display:block;font-size:14px;margin-bottom:6px}
input{width:100%;min-height:48px;border:1px solid #B8C1C9;border-radius:4px;padding:0 12px;font:inherit;background:#fff}
input:focus-visible,button:focus-visible{outline:3px solid #1C2733;outline-offset:2px}
button{width:100%;min-height:52px;margin-top:14px;border:0;border-radius:6px;background:#1C2733;color:#fff;font:600 17px "Roboto Condensed","Arial Narrow",sans-serif;cursor:pointer}
.err{color:#C8233A;margin:10px 0 0;font-size:14px}
</style></head><body>
<form method="post">
<h1>Estantería de inspección</h1>
<p>Panel de control en la nube</p>
<label for="clave">Contraseña</label>
<input id="clave" name="clave" type="password" autocomplete="current-password" required autofocus>
{% if error %}<p class="err" role="alert">{{ error }}</p>{% endif %}
<button type="submit">Entrar</button>
</form>
</body></html>"""

PANEL_HTML = r"""<!DOCTYPE html>
<html lang="es" data-tema="oscuro">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#111922">
<script>try{var t=localStorage.getItem("temaPanel");if(t)document.documentElement.setAttribute("data-tema",t);}catch(e){}</script>
<title>Estación de inspección · Estantería</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans+Condensed:wght@500;600;700&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{
  --fondo:#E3E7EB; --panel:#F6F7F9; --panel-2:#ECEFF2; --linea:#C9D0D7; --linea-2:#DCE1E6;
  --tinta:#16202B; --tenue:#56646F; --suave:#7A8794; --titulo:#2B3946; --campo:#FFFFFF;
  --top:#111922; --top-2:#1B2631; --top-tx:#E8EDF2; --top-tenue:#93A3B3; --top-borde:#33424F; --logo-bg:#2A3A4A; --logo-tx:#9CC4E8;
  --dato:#1F5F99; --dato-area:rgba(31,95,153,.10); --dato-tx:#FFFFFF;
  --alarma:#C3213A; --alarma-suave:#F7DCE0; --alarma-borde:#E7AEB7; --alarma-tx:#6E0F1F;
  --ok:#2B6E4C; --ok-suave:#DDEEE4; --ok-borde:#B5D8C4; --ok-tx:#1F5539;
  --caja-ok:#44535F; --riel:#AEB8C2; --ref:#8996A2; --rejilla:#DCE1E6; --rejilla-2:#ECEFF2; --eje:#56646F; --etq-tx:#3B4954;
  --prim-bg:#16202B; --prim-tx:#FFFFFF;
  --cond:"IBM Plex Sans Condensed","Roboto Condensed","Arial Narrow",sans-serif;
  --base:"IBM Plex Sans",Roboto,system-ui,-apple-system,"Segoe UI",sans-serif;
  --mono:"IBM Plex Mono",ui-monospace,Consolas,monospace;
}
[data-tema="oscuro"]{
  --fondo:#0E141B; --panel:#151D26; --panel-2:#1B2531; --linea:#2A3644; --linea-2:#222D39;
  --tinta:#E3E9EF; --tenue:#9AA8B6; --suave:#7D8B99; --titulo:#C3CFDA; --campo:#0F161E;
  --top:#0A0F15; --top-2:#16202A; --top-tx:#E3E9EF; --top-tenue:#8D9CAB; --top-borde:#2A3644; --logo-bg:#1E2B38; --logo-tx:#7FC8F0;
  --dato:#4FB3E8; --dato-area:rgba(79,179,232,.14); --dato-tx:#06202E;
  --alarma:#F0505F; --alarma-suave:#3A1C22; --alarma-borde:#6B2A33; --alarma-tx:#FFC2C9;
  --ok:#3DBE7E; --ok-suave:#15301F; --ok-borde:#24563A; --ok-tx:#9BE6BD;
  --caja-ok:#3A4A5A; --riel:#3A4757; --ref:#6B7B8B; --rejilla:#222D39; --rejilla-2:#1A232D; --eje:#8A98A6; --etq-tx:#C3CFDA;
  --prim-bg:#E3E9EF; --prim-tx:#0E141B;
}
[data-tema="azul"]{
  --fondo:#EEF2F7; --panel:#FFFFFF; --panel-2:#F2F5F9; --linea:#D5DDE7; --linea-2:#E6EBF1;
  --tinta:#0F2440; --tenue:#50627A; --suave:#7A8BA0; --titulo:#0F2440; --campo:#FFFFFF;
  --top:#0B2A55; --top-2:#143A6E; --top-tx:#EAF1FA; --top-tenue:#A9BED8; --top-borde:#2B5590; --logo-bg:#1B4F91; --logo-tx:#D6E6FA;
  --dato:#1565C0; --dato-area:rgba(21,101,192,.10);
  --alarma:#C62828; --alarma-suave:#FBE3E3; --alarma-borde:#EFB0B0; --alarma-tx:#7A1414;
  --ok:#2E7D32; --ok-suave:#E3F2E4; --ok-borde:#B7DDB9; --ok-tx:#1B5E20;
  --caja-ok:#1E3A5F; --riel:#B9C6D6; --ref:#8FA1B6; --rejilla:#E3E9F0; --rejilla-2:#F0F3F7; --eje:#50627A; --etq-tx:#334A66;
  --prim-bg:#0B2A55; --prim-tx:#FFFFFF;
}
[data-tema="verde"]{
  --fondo:#E4EBE8; --panel:#F5F8F7; --panel-2:#EAF0ED; --linea:#C3D0CA; --linea-2:#D9E2DE;
  --tinta:#13241E; --tenue:#4F635B; --suave:#74867E; --titulo:#1E3A31; --campo:#FFFFFF;
  --top:#0F2A24; --top-2:#183B33; --top-tx:#E6F2EE; --top-tenue:#94B3A9; --top-borde:#2C5248; --logo-bg:#1F4A40; --logo-tx:#9FE0CC;
  --dato:#0E7C66; --dato-area:rgba(14,124,102,.12);
  --alarma:#C3213A; --alarma-suave:#F7DCE0; --alarma-borde:#E7AEB7; --alarma-tx:#6E0F1F;
  --ok:#2B6E4C; --ok-suave:#DDEEE4; --ok-borde:#B5D8C4; --ok-tx:#1F5539;
  --caja-ok:#2F4A42; --riel:#A9BBB3; --ref:#7F948B; --rejilla:#D9E2DE; --rejilla-2:#EAF0ED; --eje:#4F635B; --etq-tx:#34483F;
  --prim-bg:#13241E; --prim-tx:#FFFFFF;
}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;background:var(--fondo);color:var(--tinta);font:14px/1.45 var(--base);font-variant-numeric:tabular-nums}
button{font:inherit;color:inherit;cursor:pointer}
button:focus-visible,a:focus-visible,select:focus-visible{outline:3px solid var(--dato);outline-offset:2px}
.tema{min-height:34px;padding:0 8px;border-radius:4px;border:1px solid var(--top-borde);background:var(--top-2);color:var(--top-tx);font:500 13px var(--cond)}
[hidden]{display:none!important}

/* ---------- Barra superior ---------- */
.top{background:var(--top);color:var(--top-tx);display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:10px 16px;padding:10px 20px}
.marca{display:flex;align-items:center;gap:12px;min-width:0}
.logo{width:36px;height:36px;border-radius:4px;background:var(--logo-bg);display:grid;place-items:center;font:700 14px var(--mono);color:var(--logo-tx);flex:none}
.marca h1{margin:0;font:600 18px/1.15 var(--cond);letter-spacing:.02em}
.marca .sub{display:block;font:400 12px/1.2 var(--mono);color:var(--top-tenue)}
.top-der{display:flex;flex-wrap:wrap;align-items:center;gap:8px}
.pill{display:inline-flex;align-items:center;gap:8px;padding:6px 11px;border-radius:999px;background:var(--top-2);font:500 13px/1 var(--cond);white-space:nowrap}
.pill i{width:8px;height:8px;border-radius:50%;background:#5FBF8C}
.pill.caido i{background:#FF6B7D}
.reloj{font:400 12px var(--mono);color:var(--top-tenue);padding:6px 4px}
.salir{color:var(--top-tx);font:500 13px var(--cond);text-decoration:none;border:1px solid var(--top-borde);border-radius:4px;padding:7px 12px}
.salir:hover{background:var(--top-2)}

/* ---------- Bandas de aviso ---------- */
.banda{display:flex;flex-wrap:wrap;align-items:center;gap:10px 14px;padding:10px 20px;border-bottom:1px solid}
.banda .ico{width:24px;height:24px;display:grid;place-items:center;font:700 15px var(--mono);flex:none}
.banda .txt{flex:1 1 260px;min-width:0}
.banda b{font:600 15px var(--cond);margin-right:8px}
.banda button{min-height:36px;padding:0 14px;border-radius:4px;background:transparent;font:600 13px var(--cond)}
.banda-alarma{background:var(--alarma-suave);border-color:var(--alarma-borde);color:var(--alarma-tx)}
.banda-alarma .ico{background:var(--alarma);color:#fff;border-radius:3px}
.banda-alarma button{border:1px solid var(--alarma);color:var(--alarma)}
.banda-desc{background:#2A1A1D;border-color:#4A2A30;color:#F3D3D8}
.banda-desc .ico{border:2px solid #FF6B7D;color:#FF6B7D;border-radius:50%}

/* ---------- Estructura ---------- */
.contenido{max-width:1320px;margin:0 auto;padding:16px 20px 28px;display:flex;flex-direction:column;gap:14px}
.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
.kpi{background:var(--panel);border:1px solid var(--linea);border-radius:4px;padding:10px 14px;min-width:0}
.kpi dt{font:500 12px var(--cond);color:var(--tenue);letter-spacing:.04em;text-transform:uppercase}
.kpi dd{margin:2px 0 0;font:600 22px/1.2 var(--mono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.kpi dd small{font:500 13px var(--base);color:var(--tenue)}
.kpi.alerta dd{color:var(--alarma)}
.fila{display:grid;grid-template-columns:minmax(0,1.65fr) minmax(0,1fr);gap:14px}
.card{background:var(--panel);border:1px solid var(--linea);border-radius:4px;min-width:0}
.card-cab{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:8px 12px;padding:10px 14px;border-bottom:1px solid var(--linea-2)}
.card-cab h2{margin:0;font:600 15px/1.2 var(--cond);letter-spacing:.03em;text-transform:uppercase;color:var(--titulo)}
.card-cab .meta{font:400 12px var(--mono);color:var(--suave)}
.card-cuerpo{padding:14px}

/* ---------- Vista del proceso ---------- */
.proceso{position:relative;height:208px;margin:6px 40px 0}
.estacion{position:absolute;top:0;width:110px;margin-left:-55px;display:flex;flex-direction:column;align-items:center;gap:6px}
.estacion .tag{font:600 11px var(--mono);color:var(--tenue);letter-spacing:.06em}
.caja{width:64px;height:54px;border:2px dashed var(--linea);border-radius:3px;display:grid;place-items:center;background:transparent;color:var(--suave)}
.caja svg{width:24px;height:24px}
.estacion[data-c="OK"] .caja{border:2px solid var(--caja-ok);background:var(--caja-ok);color:#fff}
.estacion[data-c="FALTA"] .caja{border:2px solid var(--alarma);background:var(--alarma-suave);color:var(--alarma);animation:parpadeo 1s steps(2,start) infinite}
@keyframes parpadeo{to{background:transparent}}
.estacion .nombre{font:600 14px/1.1 var(--cond);text-align:center}
.estacion .est{font:500 12px var(--base);color:var(--tenue)}
.estacion[data-c="FALTA"] .est{color:var(--alarma);font-weight:600}
.riel{position:absolute;left:-40px;right:-40px;top:146px;height:8px;background:repeating-linear-gradient(90deg,var(--riel) 0 2px,transparent 2px 12px),var(--panel-2);border:1px solid var(--linea);border-radius:2px}
.parada{position:absolute;top:138px;width:2px;height:24px;margin-left:-1px;background:var(--ref)}
.carro{position:absolute;top:130px;left:0;width:52px;margin-left:-26px;transition:left 1s linear}
.carro-cuerpo{height:26px;border-radius:3px;background:var(--dato);color:var(--dato-tx);display:grid;place-items:center;font:700 13px var(--mono)}
.carro-ruedas{display:flex;justify-content:space-between;padding:0 6px;margin-top:-3px}
.carro-ruedas span{width:9px;height:9px;border-radius:50%;background:var(--tinta);border:2px solid var(--panel)}
.escala{position:absolute;left:0;right:0;top:166px;height:16px}
.escala span{position:absolute;transform:translateX(-50%);font:500 11px var(--mono);color:var(--tenue);white-space:nowrap}
.escala .actual{top:22px;color:var(--dato);font-weight:600}

/* ---------- Control ---------- */
.estado-grande{display:flex;align-items:stretch;gap:12px;border:1px solid var(--linea);border-radius:4px;background:var(--campo);overflow:hidden}
.estado-grande .barra{width:6px;background:var(--ref);flex:none}
.estado-grande.marcha .barra{background:var(--dato)}
.estado-grande.desc .barra{background:var(--alarma)}
.estado-grande .info{padding:10px 12px 10px 0}
.estado-grande small{display:block;font:500 11px var(--cond);color:var(--tenue);letter-spacing:.06em}
.estado-grande strong{display:block;font:600 22px/1.15 var(--cond);letter-spacing:.02em}
.estado-grande span{font-size:13px;color:var(--tenue)}
.mandos{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}
.btn{min-height:56px;border:0;border-radius:4px;font:600 17px/1 var(--cond);letter-spacing:.03em;display:inline-flex;align-items:center;justify-content:center;gap:10px;transition:transform .08s,opacity .2s}
.btn:active{transform:scale(.98)}
.btn svg{width:16px;height:16px}
.btn-iniciar{background:var(--prim-bg);color:var(--prim-tx)}
.btn-detener{background:var(--alarma);color:#fff}
.btn[disabled]{opacity:.4;cursor:default}
.ajuste{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:14px;padding-top:12px;border-top:1px solid var(--linea-2)}
.ajuste label{font:500 14px var(--base)}
.paso{display:flex;align-items:center;gap:6px}
.paso button{width:40px;height:40px;border:1px solid var(--linea);border-radius:4px;background:var(--campo);font:600 18px var(--mono)}
.paso output{min-width:58px;text-align:center;font:600 18px var(--mono)}
.nota{min-height:20px;margin:10px 0 0;font-size:13px;color:var(--tenue)}

/* ---------- Tendencia ---------- */
.lectura{display:flex;flex-wrap:wrap;align-items:flex-end;gap:10px 28px;margin-bottom:10px}
.lectura .valor{font:600 34px/1 var(--mono);color:var(--dato)}
.lectura .valor small{font:500 16px var(--base);color:var(--tenue);margin-left:4px}
.lectura dl{display:flex;gap:22px;margin:0}
.lectura dt{font:500 11px var(--cond);color:var(--tenue);letter-spacing:.05em;text-transform:uppercase}
.lectura dd{margin:2px 0 0;font:500 15px var(--mono)}
.rangos{display:flex;border:1px solid var(--linea);border-radius:4px;overflow:hidden}
.rangos button{min-height:32px;padding:0 12px;border:0;border-left:1px solid var(--linea);background:var(--campo);font:500 13px var(--mono);color:var(--tenue)}
.rangos button:first-child{border-left:0}
.rangos button[aria-pressed="true"]{background:var(--prim-bg);color:var(--prim-tx)}
.grafica{position:relative;width:100%;height:270px}
.grafica svg{display:block;width:100%;height:100%}
.grafica .vacio{position:absolute;inset:0;display:grid;place-items:center;color:var(--suave);font-size:14px}

/* ---------- Tablas ---------- */
.tabla-caja{overflow-x:auto}
table{width:100%;border-collapse:collapse}
th{text-align:left;font:500 12px var(--cond);letter-spacing:.05em;text-transform:uppercase;color:var(--tenue);padding:8px 14px;border-bottom:1px solid var(--linea-2);white-space:nowrap}
td{padding:9px 14px;border-bottom:1px solid var(--linea-2);vertical-align:middle}
tr:last-child td{border-bottom:0}
td.mono{font:400 13px var(--mono);color:var(--tenue);white-space:nowrap}
.etq{display:inline-block;padding:2px 8px;border-radius:3px;font:600 11px var(--mono);letter-spacing:.04em;white-space:nowrap;background:var(--panel-2);color:var(--etq-tx);border:1px solid var(--linea)}
.etq.rojo{background:var(--alarma-suave);color:var(--alarma-tx);border-color:var(--alarma-borde)}
.etq.verde{background:var(--ok-suave);color:var(--ok-tx);border-color:var(--ok-borde)}
.registro{max-height:320px;overflow:auto}
.vacio-txt{padding:14px;color:var(--suave);margin:0}

.toast{position:fixed;left:50%;bottom:24px;transform:translate(-50%,20px);background:var(--prim-bg);color:var(--prim-tx);padding:10px 16px;border-radius:4px;font-size:14px;opacity:0;pointer-events:none;transition:opacity .2s,transform .2s;max-width:90vw}
.toast.ver{opacity:1;transform:translate(-50%,0)}

@media (max-width:960px){
  .fila{grid-template-columns:minmax(0,1fr)}
  .kpis{grid-template-columns:repeat(2,minmax(0,1fr))}
}
@media (max-width:520px){
  .contenido{padding:12px 12px 24px}
  .top{padding:10px 12px}
  .banda{padding:10px 12px}
  .proceso{margin:6px 30px 0}
  .estacion{width:90px;margin-left:-45px}
  .lectura .valor{font-size:28px}
  .grafica{height:230px}
}
@media (prefers-reduced-motion:reduce){.carro,.btn,.toast{transition:none}.estacion[data-c="FALTA"] .caja{animation:none}}
</style>
</head>
<body>

<header class="top">
  <div class="marca">
    <div class="logo" aria-hidden="true">EI</div>
    <div><h1>Estación de inspección</h1><span class="sub">Estantería A · carro lineal · ESP32</span></div>
  </div>
  <div class="top-der">
    <span class="pill caido" id="enlace"><i></i><span id="enlaceTxt">Conectando</span></span>
    <span class="reloj" id="reloj">Último dato —</span>
    <select class="tema" id="tema" aria-label="Colores del panel">
      <option value="oscuro">Oscuro SCADA</option>
      <option value="gris">Gris industrial</option>
      <option value="azul">Azul corporativo</option>
      <option value="verde">Verde planta</option>
    </select>
    <a class="salir" href="/logout">Salir</a>
  </div>
</header>

<div class="banda banda-desc" id="bandaDesc" role="alert" hidden>
  <span class="ico" aria-hidden="true">!</span>
  <div class="txt"><b>Sin comunicación con el carro</b><span id="bandaDescTxt"></span></div>
</div>
<div class="banda banda-alarma" id="bandaAlarma" role="alert" hidden>
  <span class="ico" aria-hidden="true">!</span>
  <div class="txt"><b id="bandaAlarmaTit">Alarma activa</b><span id="bandaAlarmaTxt"></span></div>
  <button id="btnBorrar">Borrar alarmas</button>
</div>

<main class="contenido">
  <dl class="kpis">
    <div class="kpi"><dt>Estado</dt><dd id="kEstado">—</dd></div>
    <div class="kpi" id="kFaltaCaja"><dt>Faltantes activos</dt><dd id="kFalta">—</dd></div>
    <div class="kpi"><dt>Recorridos</dt><dd id="kVueltas">—</dd></div>
    <div class="kpi"><dt>Señal WiFi</dt><dd id="kSenal">—</dd></div>
  </dl>

  <div class="fila">
    <section class="card" aria-labelledby="tProceso">
      <div class="card-cab"><h2 id="tProceso">Vista del proceso</h2><span class="meta" id="posTxt">—</span></div>
      <div class="card-cuerpo">
        <div class="proceso">
          <div class="riel"></div>
          <div class="parada" id="p0"></div><div class="parada" id="p1"></div><div class="parada" id="p2"></div>
          <div class="estacion" id="b0" data-c="PEND"><span class="tag">P1</span><div class="caja"></div><span class="nombre">Material 1</span><span class="est">Sin revisar</span></div>
          <div class="estacion" id="b1" data-c="PEND"><span class="tag">P2</span><div class="caja"></div><span class="nombre">Material 2</span><span class="est">Sin revisar</span></div>
          <div class="estacion" id="b2" data-c="PEND"><span class="tag">P3</span><div class="caja"></div><span class="nombre">Material 3</span><span class="est">Sin revisar</span></div>
          <div class="carro" id="carro" aria-hidden="true">
            <div class="carro-cuerpo" id="flecha"></div>
            <div class="carro-ruedas"><span></span><span></span></div>
          </div>
          <div class="escala" id="escala"></div>
        </div>
      </div>
    </section>

    <section class="card" aria-labelledby="tControl">
      <div class="card-cab"><h2 id="tControl">Control</h2><span class="meta" id="pendTxt"></span></div>
      <div class="card-cuerpo">
        <div class="estado-grande" id="estadoGrande">
          <div class="barra"></div>
          <div class="info"><small>ESTADO DEL CARRO</small><strong id="estadoTxt">—</strong><span id="estadoSub">Esperando datos…</span></div>
        </div>
        <div class="mandos">
          <button class="btn btn-iniciar" id="btnIniciar"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M7 4.5v15l13-7.5z"/></svg>INICIAR</button>
          <button class="btn btn-detener" id="btnDetener"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><rect x="5" y="5" width="14" height="14" rx="1.5"/></svg>DETENER</button>
        </div>
        <div class="ajuste">
          <label for="pausa">Pausa en cada parada</label>
          <div class="paso"><button id="menos" aria-label="Menos pausa">−</button><output id="pausa">—</output><button id="mas" aria-label="Más pausa">+</button></div>
        </div>
        <p class="nota" id="nota" role="status"></p>
      </div>
    </section>
  </div>

  <section class="card" aria-labelledby="tTend">
    <div class="card-cab">
      <h2 id="tTend">Sensor de posición · tendencia del voltaje</h2>
      <div class="rangos" role="group" aria-label="Periodo de la gráfica">
        <button data-seg="60" aria-pressed="false">1 min</button>
        <button data-seg="300" aria-pressed="true">5 min</button>
        <button data-seg="900" aria-pressed="false">15 min</button>
        <button data-seg="1800" aria-pressed="false">30 min</button>
      </div>
    </div>
    <div class="card-cuerpo">
      <div class="lectura">
        <div class="valor" id="vAhora">—<small>V</small></div>
        <dl>
          <div><dt>Recorrido</dt><dd id="vPct">—</dd></div>
          <div><dt>Mínimo</dt><dd id="vMin">—</dd></div>
          <div><dt>Máximo</dt><dd id="vMax">—</dd></div>
          <div><dt>Puntos</dt><dd id="vN">—</dd></div>
        </dl>
      </div>
      <div class="grafica" id="grafica"><div class="vacio" id="graficaVacia">Sin datos en este periodo</div></div>
    </div>
  </section>

  <div class="fila">
    <section class="card" aria-labelledby="tReg">
      <div class="card-cab"><h2 id="tReg">Registro de eventos</h2><span class="meta" id="regTxt"></span></div>
      <div class="tabla-caja registro">
        <table><thead><tr><th>Fecha y hora</th><th>Tipo</th><th>Descripción</th></tr></thead><tbody id="registro"></tbody></table>
        <p class="vacio-txt" id="sinRegistro">Aquí aparecerán las alarmas, reposiciones, inicios y paros.</p>
      </div>
    </section>

    <section class="card" aria-labelledby="tPos">
      <div class="card-cab"><h2 id="tPos">Posiciones</h2></div>
      <div class="tabla-caja">
        <table><thead><tr><th>Pos.</th><th>Material</th><th>Estado</th><th>Parada</th></tr></thead><tbody id="tablaPos"></tbody></table>
      </div>
    </section>
  </div>
</main>
<div class="toast" id="toast" role="status"></div>

<script>
(function () {
  const $ = id => document.getElementById(id);
  const enApp = !!(window.AppInventor && window.AppInventor.setWebViewString);
  const ICONO = {
    OK: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12.5l4.5 4.5L19 7.5"/></svg>',
    FALTA: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round"><path d="M6 6l12 12M18 6L6 18"/></svg>',
    PEND: ''
  };
  const TEXTO = { OK: 'En su lugar', FALTA: 'Falta', PEND: 'Sin revisar' };
  const ETQ = { FALTA: ['FALTA', 'rojo'], REPUESTO: ['REPUESTO', 'verde'], INICIO: ['INICIO', ''], PARO: ['PARO', ''], RESET: ['RESET', ''], ENCENDIDO: ['ENCENDIDO', ''] };
  const fmtV = mv => (mv / 1000).toFixed(2);
  const fmtHora = ms => new Date(ms).toLocaleTimeString('es-CO', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false, timeZone: 'America/Bogota' });

  let ultimaAlerta = -1, pausaActual = 1, pausaEditada = 0, fallos = 0, audio = null, ultimoEvento = -1;
  let enLinea = false, ultimoEstado = null, rango = 300, puntos = [];

  function toast(msg) { const t = $('toast'); t.textContent = msg; t.classList.add('ver'); clearTimeout(t._t); t._t = setTimeout(() => t.classList.remove('ver'), 2200); }
  function esc(s) { return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])); }

  function desbloquearAudio() {
    if (!audio) { try { audio = new (window.AudioContext || window.webkitAudioContext)(); } catch (e) {} }
    if (audio && audio.state === 'suspended') audio.resume();
  }
  document.addEventListener('pointerdown', desbloquearAudio);
  function pitar() {
    if (!audio) return;
    const t0 = audio.currentTime;
    for (let i = 0; i < 3; i++) {
      const o = audio.createOscillator(), g = audio.createGain();
      o.frequency.value = 880;
      g.gain.setValueAtTime(0.0001, t0 + i * 0.28);
      g.gain.exponentialRampToValueAtTime(0.35, t0 + i * 0.28 + 0.02);
      g.gain.exponentialRampToValueAtTime(0.0001, t0 + i * 0.28 + 0.18);
      o.connect(g).connect(audio.destination); o.start(t0 + i * 0.28); o.stop(t0 + i * 0.28 + 0.2);
    }
  }
  function avisarAlerta(id, msg) {
    if (enApp) window.AppInventor.setWebViewString('ALERTA|' + id + '|' + msg);
    else { pitar(); if (navigator.vibrate) navigator.vibrate([300, 120, 300]); }
  }
  function sesion(r) { if (r.status === 401) { location.href = '/login'; throw new Error('sesion'); } return r; }

  // ---------- Órdenes ----------
  function comando(c, valor, msg) {
    desbloquearAudio();
    return fetch('/api/comando', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ comando: c, valor: valor }) })
      .then(sesion).then(r => r.json().then(j => {
        if (!r.ok) throw new Error(j.error || 'error');
        $('nota').textContent = enLinea ? 'Orden enviada. El carro la recibe en 1 a 2 segundos.'
          : 'Orden guardada, pero el carro no está conectado. Si no se conecta en 30 s, se descarta.';
        if (msg) toast(msg);
      })).catch(e => { if (e.message !== 'sesion') toast('No se pudo enviar la orden'); });
  }
  $('btnIniciar').onclick = () => comando('on', 0, 'Iniciando ronda…');
  $('btnDetener').onclick = () => comando('off', 0, 'Deteniendo carro…');
  $('btnBorrar').onclick = () => comando('reset', 0, 'Borrando alarmas…');
  let tPausa = null;
  function cambiarPausa(d) {
    pausaEditada = Date.now();
    pausaActual = Math.min(600, Math.max(1, pausaActual + d));
    $('pausa').textContent = pausaActual + ' s';
    clearTimeout(tPausa); tPausa = setTimeout(() => comando('espera', pausaActual), 600);
  }
  $('menos').onclick = () => cambiarPausa(-1);
  $('mas').onclick = () => cambiarPausa(1);

  // ---------- Estado ----------
  function textoEstado(e) {
    if (!e.enLinea) return ['SIN CONEXIÓN', 'Último dato: ' + (e.actualizado || '—'), 'desc'];
    switch (e.estado) {
      case 'DERECHA':   return ['EN RONDA', 'Avanzando hacia la derecha', 'marcha'];
      case 'IZQUIERDA': return ['EN RONDA', 'Avanzando hacia la izquierda', 'marcha'];
      case 'ESPERANDO': return ['INSPECCIONANDO', 'Revisando la posición actual', 'marcha'];
      default:          return ['DETENIDO', 'Listo para iniciar una ronda', ''];
    }
  }
  function lugar(e) {
    if (Math.abs(e.mv - e.vMed) < 120) return 'en P2';
    if (Math.abs(e.mv - e.vIzq) < 120) return 'en P1';
    if (Math.abs(e.mv - e.vDer) < 120) return 'en P3';
    return e.mv < e.vMed ? 'entre P1 y P2' : 'entre P2 y P3';
  }

  function pintar(e) {
    ultimoEstado = e;
    enLinea = !!e.enLinea;
    $('enlace').classList.toggle('caido', !enLinea);
    $('enlaceTxt').textContent = enLinea ? 'Carro en línea' : 'Carro sin conexión';
    $('reloj').textContent = 'Último dato ' + (e.actualizado || '—');
    $('bandaDesc').hidden = enLinea;
    $('bandaDescTxt').textContent = e.sinDatos ? 'El carro todavía no ha enviado datos. Revisa que esté encendido y con internet.'
      : 'Sin reportes desde hace ' + e.edad + ' s. Revisa que el ESP32 esté encendido y con internet.';
    const [tit, sub, cls] = textoEstado(e);
    $('estadoTxt').textContent = tit; $('estadoSub').textContent = sub;
    $('estadoGrande').className = 'estado-grande ' + cls;
    $('kEstado').textContent = tit;
    $('pendTxt').textContent = e.pendientes ? e.pendientes + ' orden(es) en camino' : '';
    if (e.sinDatos) return;

    const rangoV = e.vDer - e.vIzq;
    const pct = v => Math.min(100, Math.max(0, (v - e.vIzq) / rangoV * 100));
    [e.vIzq, e.vMed, e.vDer].forEach((v, i) => { $('b' + i).style.left = pct(v) + '%'; $('p' + i).style.left = pct(v) + '%'; });
    $('carro').style.left = pct(e.mv) + '%';
    $('flecha').textContent = e.estado === 'DERECHA' ? '▶' : e.estado === 'IZQUIERDA' ? '◀' : e.estado === 'ESPERANDO' ? '●' : '■';
    $('escala').innerHTML =
      [e.vIzq, e.vMed, e.vDer].map(v => '<span style="left:' + pct(v) + '%">' + fmtV(v) + ' V</span>').join('') +
      '<span class="actual" style="left:' + pct(e.mv) + '%">' + fmtV(e.mv) + ' V</span>';
    $('posTxt').textContent = 'Carro ' + lugar(e);

    let faltan = 0, filas = '';
    e.productos.forEach((p, i) => {
      const b = $('b' + i);
      if (b.dataset.c !== p.codigo) { b.dataset.c = p.codigo; b.querySelector('.caja').innerHTML = ICONO[p.codigo] || ''; }
      b.querySelector('.nombre').textContent = p.nombre;
      b.querySelector('.est').textContent = TEXTO[p.codigo] || p.codigo;
      if (p.codigo === 'FALTA') faltan++;
      const cls = p.codigo === 'FALTA' ? 'rojo' : p.codigo === 'OK' ? 'verde' : '';
      const parada = [e.vIzq, e.vMed, e.vDer][i];
      filas += '<tr><td class="mono">P' + (i + 1) + '</td><td>' + esc(p.nombre) + '</td><td><span class="etq ' + cls + '">' +
        esc((TEXTO[p.codigo] || p.codigo).toUpperCase()) + '</span></td><td class="mono">' + fmtV(parada) + ' V</td></tr>';
    });
    $('tablaPos').innerHTML = filas;

    $('kFalta').textContent = faltan;
    $('kFaltaCaja').classList.toggle('alerta', faltan > 0);
    $('kVueltas').textContent = e.vueltas;
    const r = e.rssi;
    $('kSenal').innerHTML = r + ' <small>dBm · ' + (r >= -60 ? 'buena' : r >= -75 ? 'regular' : 'débil') + '</small>';

    $('bandaAlarma').hidden = faltan === 0;
    if (faltan) {
      const nombres = e.productos.filter(p => p.codigo === 'FALTA').map(p => p.nombre).join(', ');
      $('bandaAlarmaTit').textContent = faltan === 1 ? 'Alarma activa' : faltan + ' alarmas activas';
      $('bandaAlarmaTxt').textContent = 'Falta producto: ' + nombres + '. LED rojo encendido en la estantería.';
    }

    if (Date.now() - pausaEditada > 4000) { pausaActual = e.pausa; $('pausa').textContent = e.pausa + ' s'; }
    const corriendo = enLinea && e.estado !== 'PARADO';
    $('btnIniciar').disabled = corriendo;
    if (enLinea && e.pendientes === 0) $('nota').textContent = '';

    $('vAhora').innerHTML = fmtV(e.mv) + '<small>V</small>';
    $('vPct').textContent = Math.round(pct(e.mv)) + ' %';

    if (ultimaAlerta < 0) ultimaAlerta = e.alertaId;
    else if (e.alertaId > ultimaAlerta) { ultimaAlerta = e.alertaId; avisarAlerta(e.alertaId, e.alerta); }
    else if (e.alertaId < ultimaAlerta) ultimaAlerta = e.alertaId;
  }

  // ---------- Gráfica del voltaje ----------
  function dibujar(ahora) {
    const caja = $('grafica');
    const W = caja.clientWidth, H = caja.clientHeight;
    const viejo = caja.querySelector('svg'); if (viejo) viejo.remove();
    $('graficaVacia').hidden = puntos.length > 1;
    $('vN').textContent = puntos.length;
    if (puntos.length) {
      const vals = puntos.map(p => p[1]);
      $('vMin').textContent = fmtV(Math.min.apply(null, vals)) + ' V';
      $('vMax').textContent = fmtV(Math.max.apply(null, vals)) + ' V';
    } else { $('vMin').textContent = '—'; $('vMax').textContent = '—'; }
    if (puntos.length < 2 || !W) return;

    const cs = getComputedStyle(document.documentElement), col = n => cs.getPropertyValue(n).trim();
    const C = { rej: col('--rejilla'), rej2: col('--rejilla-2'), eje: col('--eje'), ref: col('--ref'), tx: col('--etq-tx'), dato: col('--dato'), area: col('--dato-area'), fondo: col('--panel') };
    const m = { l: 46, r: 66, t: 10, b: 26 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    const t1 = ahora, t0 = ahora - rango * 1000;
    const X = t => m.l + (t - t0) / (t1 - t0) * iw;
    const Y = mv => m.t + (1 - mv / 2500) * ih;
    let s = '<svg viewBox="0 0 ' + W + ' ' + H + '" role="img" aria-label="Voltaje del sensor de posición en el tiempo">';
    for (let v = 0; v <= 2500; v += 500) {
      s += '<line x1="' + m.l + '" x2="' + (W - m.r) + '" y1="' + Y(v) + '" y2="' + Y(v) + '" stroke="' + C.rej + '" stroke-width="1"/>';
      s += '<text x="' + (m.l - 8) + '" y="' + (Y(v) + 4) + '" text-anchor="end" font-family="IBM Plex Mono,monospace" font-size="11" fill="' + C.eje + '">' + (v / 1000).toFixed(1) + '</text>';
    }
    const nx = W < 560 ? 2 : 4;   // menos marcas de tiempo en pantallas angostas
    for (let i = 0; i <= nx; i++) {
      const t = t0 + (t1 - t0) * i / nx, x = X(t);
      s += '<line x1="' + x + '" x2="' + x + '" y1="' + m.t + '" y2="' + (H - m.b) + '" stroke="' + C.rej2 + '" stroke-width="1"/>';
      s += '<text x="' + x + '" y="' + (H - 8) + '" text-anchor="' + (i === 0 ? 'start' : i === nx ? 'end' : 'middle') + '" font-family="IBM Plex Mono,monospace" font-size="11" fill="' + C.eje + '">' + fmtHora(t) + '</text>';
    }
    const e = ultimoEstado;
    if (e && !e.sinDatos) {
      [[e.vIzq, 'P1'], [e.vMed, 'P2'], [e.vDer, 'P3']].forEach(([v, n]) => {
        s += '<line x1="' + m.l + '" x2="' + (W - m.r) + '" y1="' + Y(v) + '" y2="' + Y(v) + '" stroke="' + C.ref + '" stroke-width="1" stroke-dasharray="5 4"/>';
        s += '<text x="' + (W - m.r + 6) + '" y="' + (Y(v) + 4) + '" font-family="IBM Plex Mono,monospace" font-size="11" fill="' + C.tx + '">' + n + ' ' + fmtV(v) + '</text>';
      });
    }
    const pts = puntos.filter(p => p[0] >= t0).map(p => X(p[0]).toFixed(1) + ',' + Y(p[1]).toFixed(1));
    if (pts.length > 1) {
      const primero = pts[0].split(',')[0], ultimoX = pts[pts.length - 1].split(',')[0];
      s += '<polygon points="' + primero + ',' + Y(0) + ' ' + pts.join(' ') + ' ' + ultimoX + ',' + Y(0) + '" fill="' + C.area + '"/>';
      s += '<polyline points="' + pts.join(' ') + '" fill="none" stroke="' + C.dato + '" stroke-width="2" stroke-linejoin="round"/>';
      const u = puntos[puntos.length - 1];
      s += '<circle cx="' + X(u[0]) + '" cy="' + Y(u[1]) + '" r="4.5" fill="' + C.dato + '" stroke="' + C.fondo + '" stroke-width="2"/>';
    }
    s += '<text transform="translate(12 ' + (m.t + ih / 2) + ') rotate(-90)" text-anchor="middle" font-family="IBM Plex Sans Condensed,sans-serif" font-size="11" fill="' + C.eje + '">Voltaje (V)</text>';
    s += '</svg>';
    caja.insertAdjacentHTML('beforeend', s);
  }
  let ahoraServidor = Date.now();
  function cargarVoltaje() {
    return fetch('/api/voltaje?seg=' + rango, { cache: 'no-store' }).then(sesion).then(r => r.json()).then(d => {
      if (!d || !Array.isArray(d.puntos)) return;
      puntos = d.puntos; ahoraServidor = d.ahora; dibujar(d.ahora);
    }).catch(() => {});
  }
  document.querySelectorAll('.rangos button').forEach(b => b.onclick = () => {
    document.querySelectorAll('.rangos button').forEach(x => x.setAttribute('aria-pressed', 'false'));
    b.setAttribute('aria-pressed', 'true'); rango = +b.dataset.seg; cargarVoltaje();
  });
  window.addEventListener('resize', () => dibujar(ahoraServidor));

  const selTema = $('tema');
  selTema.value = document.documentElement.getAttribute('data-tema') || 'oscuro';
  selTema.onchange = () => {
    const t = selTema.value;
    document.documentElement.setAttribute('data-tema', t);
    try { localStorage.setItem('temaPanel', t); } catch (e) {}
    dibujar(ahoraServidor);
  };

  // ---------- Registro ----------
  function cargarRegistro() {
    return fetch('/api/eventos?n=60', { cache: 'no-store' }).then(sesion).then(r => r.json()).then(lista => {
      if (!Array.isArray(lista)) return;
      const tope = lista.length ? lista[0].id : 0;
      if (tope === ultimoEvento) return;
      ultimoEvento = tope;
      $('registro').innerHTML = lista.map(ev => {
        const [t, c] = ETQ[ev.tipo] || [ev.tipo, ''];
        return '<tr><td class="mono">' + esc(ev.hora) + '</td><td><span class="etq ' + c + '">' + esc(t) + '</span></td><td>' + esc(ev.mensaje) + '</td></tr>';
      }).join('');
      $('sinRegistro').hidden = lista.length > 0;
      $('regTxt').textContent = lista.length ? 'Últimos ' + lista.length : '';
    }).catch(() => {});
  }

  function actualizar() {
    const ctrl = new AbortController(); const t = setTimeout(() => ctrl.abort(), 4000);
    return fetch('/api/estado', { cache: 'no-store', signal: ctrl.signal }).then(sesion).then(r => r.json())
      .then(e => { clearTimeout(t); fallos = 0; pintar(e); })
      .catch(err => { clearTimeout(t); if (err.message === 'sesion') return;
        if (++fallos >= 3) { $('enlace').classList.add('caido'); $('enlaceTxt').textContent = 'Sin servidor'; } });
  }

  actualizar(); cargarRegistro(); cargarVoltaje();
  setInterval(actualizar, 1000);
  setInterval(cargarVoltaje, 2000);
  setInterval(cargarRegistro, 3000);
})();
</script>
</body>
</html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
