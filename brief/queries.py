"""
Queries agregadas para el Brief Mensual.

Módulo autocontenido a propósito: no importa streamlit_app (el import
ejecutaría su st.set_page_config y su CSS sobre la página del brief).
Los patrones SQL son los mismos del tablero en su variante sin filtros;
si se toca una definición allá (creador, publicador, alcance del tráfico),
hay que replicarla acá.

Las notas de agencias (usuario 'infobae': EFE, EuropaPress, Narrativa, etc.)
quedan afuera de todos los agregados salvo _sources, que es el capítulo que
compara Composer/Scribnews/Agencias. Sin esta exclusión las ~34 mil notas
automáticas mensuales sepultan a la redacción en producción, secciones y
tiempo de lectura (20,6 s de promedio de agencias vs 58,7 s de humanos).
"""

import calendar
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

TABLE_PRODUCTIVITY = "data-prod-454014.Gold.GA4_ARC_author_productivity_daily"
TABLE_EDITORIAL = "data-prod-454014.Silver.arc_editorial_activity"
TABLE_AUTHORS = "data-prod-454014.Bronze.authors_infobae_raw"

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
         "agosto", "septiembre", "octubre", "noviembre", "diciembre"]

# Exclusión de agencias. En editorial el usuario es 'infobae'; en Gold el
# creator_email puede venir NULL, por eso el COALESCE.
SIN_AGENCIAS_ED = "AND LOWER(e.email_editor) != 'infobae'"
SIN_AGENCIAS_GOLD = "AND LOWER(COALESCE(g.creator_email, '')) != 'infobae'"


@st.cache_resource
def get_bigquery_client():
    try:
        if "gcp_service_account" not in st.secrets:
            return None
        credentials = service_account.Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"])
        )
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def get_last_data_date(_client):
    """Última fecha con datos en Gold (misma lógica que el tablero)."""
    proyecto, dataset, tabla = TABLE_PRODUCTIVITY.split('.')
    queries = [
        f"""
        SELECT MAX(PARSE_DATE('%Y%m%d', partition_id)) as ultima_fecha
        FROM `{proyecto}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = '{tabla}' AND partition_id != '__NULL__' AND total_rows > 0
        """,
        f"SELECT MAX(date) as ultima_fecha FROM `{TABLE_PRODUCTIVITY}`",
    ]
    for query in queries:
        try:
            df = _client.query(query).to_dataframe()
            if not df.empty and df.iloc[0]['ultima_fecha'] is not None:
                fecha = df.iloc[0]['ultima_fecha']
                return fecha.date() if hasattr(fecha, 'date') else fecha
        except Exception:
            continue
    return None


def closed_months(last_data_date, limit=12):
    """Meses cerrados (el último día del mes ya tiene datos), el más reciente primero."""
    if last_data_date is None:
        return []
    months = []
    year, month = last_data_date.year, last_data_date.month
    # El mes de last_data_date solo cuenta si está completo
    if last_data_date.day < calendar.monthrange(year, month)[1]:
        year, month = (year - 1, 12) if month == 1 else (year, month - 1)
    while len(months) < limit:
        months.append((year, month))
        year, month = (year - 1, 12) if month == 1 else (year, month - 1)
    return months


def month_range(year, month):
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    return str(start), str(end)


def prev_month(year, month):
    return (year - 1, 12) if month == 1 else (year, month - 1)


def month_label(year, month):
    return f"{MESES[month - 1].capitalize()} {year}"


def _run_parallel(tasks: dict, max_workers: int = 10) -> dict:
    with ThreadPoolExecutor(max_workers=min(max_workers, len(tasks))) as pool:
        futures = {name: pool.submit(fn) for name, fn in tasks.items()}
        return {name: f.result() for name, f in futures.items()}


def _df(client, query) -> pd.DataFrame:
    try:
        return client.query(query).to_dataframe()
    except Exception:
        return pd.DataFrame()


# ───────────────────────────────── queries ─────────────────────────────────

def _production(client, start, end) -> dict:
    query = f"""
        SELECT
            COUNT(DISTINCT IF(e.action_type = 'CREATE', e.email_editor, NULL)) as creadores,
            COUNT(DISTINCT IF(e.action_type = 'FIRST_PUBLISH', e.email_editor, NULL)) as publicadores,
            COUNT(DISTINCT IF(e.action_type = 'FIRST_PUBLISH', e.note_id, NULL)) as notas
        FROM `{TABLE_EDITORIAL}` e
        WHERE e.action_type IN ('CREATE', 'FIRST_PUBLISH')
          AND DATE(e.event_timestamp) BETWEEN '{start}' AND '{end}'
          {SIN_AGENCIAS_ED}
    """
    df = _df(client, query)
    if df.empty:
        return {'creadores': 0, 'publicadores': 0, 'notas': 0}
    r = df.iloc[0]
    return {'creadores': int(r['creadores'] or 0),
            'publicadores': int(r['publicadores'] or 0),
            'notas': int(r['notas'] or 0)}


def _traffic(client, start, end) -> dict:
    """Tráfico del mes sobre las notas publicadas en el mes (mismo alcance que el tablero)."""
    base = f"""
        FROM `{TABLE_PRODUCTIVITY}` g
        WHERE g.date BETWEEN '{start}' AND '{end}'
          AND DATE(g.publish_date) BETWEEN '{start}' AND '{end}'
          {SIN_AGENCIAS_GOLD}
    """
    query_hll = f"""
        SELECT
            SUM(g.visits) as visitas, SUM(g.pageviews) as pageviews,
            SAFE_DIVIDE(SUM(g.total_time_seconds), SUM(g.visits)) as tiempo_seg,
            SAFE_DIVIDE(SUM(g.scrolls), SUM(g.visits)) as scroll_prom,
            HLL_COUNT.MERGE(g.users_sketch) as usuarios_hll,
            HLL_COUNT.MERGE(g.sessions_sketch) as sesiones_hll,
            COUNTIF(g.users_sketch IS NULL) as filas_sin_sketch
        {base}
    """
    query_plain = f"""
        SELECT
            SUM(g.visits) as visitas, SUM(g.pageviews) as pageviews,
            SAFE_DIVIDE(SUM(g.total_time_seconds), SUM(g.visits)) as tiempo_seg,
            SAFE_DIVIDE(SUM(g.scrolls), SUM(g.visits)) as scroll_prom
        {base}
    """
    result = {'visitas': 0, 'pageviews': 0, 'tiempo_min': 0.0, 'scroll_prom': 0.0,
              'usuarios_unicos': 0, 'sesiones_unicas': 0, 'metodo_unicos': 'suma'}
    df = _df(client, query_hll)
    if df.empty:
        df = _df(client, query_plain)
        if df.empty:
            return result
    r = df.iloc[0]
    result['visitas'] = int(r['visitas']) if pd.notna(r['visitas']) else 0
    result['pageviews'] = int(r['pageviews']) if pd.notna(r['pageviews']) else 0
    result['tiempo_min'] = (float(r['tiempo_seg']) if pd.notna(r['tiempo_seg']) else 0) / 60
    result['scroll_prom'] = float(r['scroll_prom']) if pd.notna(r['scroll_prom']) else 0
    # HLL solo si el mes entero tiene sketches (MERGE ignora NULLs en silencio)
    if 'usuarios_hll' in df.columns and int(r.get('filas_sin_sketch') or 0) == 0 \
            and pd.notna(r['usuarios_hll']):
        result['usuarios_unicos'] = int(r['usuarios_hll'])
        result['sesiones_unicas'] = int(r['sesiones_hll']) if pd.notna(r['sesiones_hll']) else 0
        result['metodo_unicos'] = 'hll'
    else:
        result['sesiones_unicas'] = result['visitas']
    return result


def _daily_notas(client, start, end) -> list:
    query = f"""
        SELECT DATE(e.event_timestamp) as fecha, COUNT(DISTINCT e.note_id) as valor
        FROM `{TABLE_EDITORIAL}` e
        WHERE e.action_type = 'FIRST_PUBLISH'
          AND DATE(e.event_timestamp) BETWEEN '{start}' AND '{end}'
          {SIN_AGENCIAS_ED}
        GROUP BY fecha ORDER BY fecha
    """
    df = _df(client, query)
    return [(str(r['fecha']), int(r['valor'])) for _, r in df.iterrows()]


def _daily_visitas(client, start, end) -> list:
    query = f"""
        SELECT g.date as fecha, SUM(g.visits) as valor
        FROM `{TABLE_PRODUCTIVITY}` g
        WHERE g.date BETWEEN '{start}' AND '{end}'
          AND DATE(g.publish_date) BETWEEN '{start}' AND '{end}'
          {SIN_AGENCIAS_GOLD}
        GROUP BY fecha ORDER BY fecha
    """
    df = _df(client, query)
    return [(str(r['fecha']), int(r['valor'])) for _, r in df.iterrows()]


def _top_authors(client, start, end, action, limit=10) -> list:
    nombre = 'Publicador' if action == 'FIRST_PUBLISH' else 'Creador'
    query = f"""
        SELECT
            COALESCE(a.complete_name, e.email_editor) as nombre,
            a.country as pais,
            COUNT(DISTINCT e.note_id) as notas
        FROM `{TABLE_EDITORIAL}` e
        LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)
        WHERE e.action_type = '{action}'
          AND DATE(e.event_timestamp) BETWEEN '{start}' AND '{end}'
          AND e.email_editor IS NOT NULL AND e.email_editor != ''
          {SIN_AGENCIAS_ED}
        GROUP BY nombre, pais
        ORDER BY notas DESC
        LIMIT {limit}
    """
    df = _df(client, query)
    return [{'nombre': r['nombre'], 'pais': r['pais'] if pd.notna(r['pais']) else '',
             'notas': int(r['notas']), 'rol': nombre} for _, r in df.iterrows()]


def _sections(client, start, end) -> list:
    query = f"""
        WITH editorial_stats AS (
            SELECT e.segment as seccion, COUNT(DISTINCT e.note_id) as notas
            FROM `{TABLE_EDITORIAL}` e
            WHERE e.action_type = 'FIRST_PUBLISH'
              AND DATE(e.event_timestamp) BETWEEN '{start}' AND '{end}'
              AND e.segment IS NOT NULL AND e.segment != ''
              {SIN_AGENCIAS_ED}
            GROUP BY e.segment
        ),
        traffic_stats AS (
            SELECT g.section as seccion, SUM(g.visits) as sesiones
            FROM `{TABLE_PRODUCTIVITY}` g
            WHERE g.date BETWEEN '{start}' AND '{end}'
              AND DATE(g.publish_date) BETWEEN '{start}' AND '{end}'
              AND g.section IS NOT NULL AND g.section != ''
              {SIN_AGENCIAS_GOLD}
            GROUP BY g.section
        )
        SELECT e.seccion, e.notas, COALESCE(t.sesiones, 0) as sesiones,
               SAFE_DIVIDE(COALESCE(t.sesiones, 0), e.notas) as productividad
        FROM editorial_stats e
        LEFT JOIN traffic_stats t ON e.seccion = t.seccion
        ORDER BY e.notas DESC
    """
    df = _df(client, query)
    return [{'seccion': r['seccion'], 'notas': int(r['notas']),
             'sesiones': int(r['sesiones']), 'productividad': float(r['productividad'] or 0)}
            for _, r in df.iterrows()]


def _section_country(client, start, end) -> list:
    """Notas por sección × país de la redacción (para el cruce de secciones cross)."""
    query = f"""
        SELECT e.segment as seccion,
               COALESCE(a.country, 'SIN ASIGNAR') as pais,
               COUNT(DISTINCT e.note_id) as notas
        FROM `{TABLE_EDITORIAL}` e
        LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)
        WHERE e.action_type = 'FIRST_PUBLISH'
          AND DATE(e.event_timestamp) BETWEEN '{start}' AND '{end}'
          AND e.segment IS NOT NULL AND e.segment != ''
          {SIN_AGENCIAS_ED}
        GROUP BY seccion, pais
    """
    df = _df(client, query)
    return [{'seccion': r['seccion'], 'pais': r['pais'], 'notas': int(r['notas'])}
            for _, r in df.iterrows()]


def _countries_production(client, start, end) -> list:
    query = f"""
        SELECT UPPER(a.country) as pais,
            COUNT(DISTINCT IF(e.action_type = 'CREATE', e.email_editor, NULL)) as creadores,
            COUNT(DISTINCT IF(e.action_type = 'FIRST_PUBLISH', e.email_editor, NULL)) as publicadores,
            COUNT(DISTINCT IF(e.action_type = 'FIRST_PUBLISH', e.note_id, NULL)) as notas
        FROM `{TABLE_EDITORIAL}` e
        INNER JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)
        WHERE e.action_type IN ('CREATE', 'FIRST_PUBLISH')
          AND DATE(e.event_timestamp) BETWEEN '{start}' AND '{end}'
          AND a.country IS NOT NULL AND a.country != ''
          {SIN_AGENCIAS_ED}
        GROUP BY pais
        ORDER BY notas DESC
    """
    df = _df(client, query)
    return [{'pais': r['pais'], 'creadores': int(r['creadores']),
             'publicadores': int(r['publicadores']), 'notas': int(r['notas'])}
            for _, r in df.iterrows()]


def _countries_traffic(client, start, end) -> dict:
    """Tráfico por país del creador. Devuelve {pais: {...}}."""
    base = f"""
        FROM `{TABLE_PRODUCTIVITY}` g
        INNER JOIN `{TABLE_AUTHORS}` a ON LOWER(g.creator_email) = LOWER(a.email)
        WHERE g.date BETWEEN '{start}' AND '{end}'
          AND DATE(g.publish_date) BETWEEN '{start}' AND '{end}'
          AND a.country IS NOT NULL AND a.country != ''
          {SIN_AGENCIAS_GOLD}
        GROUP BY pais
    """
    query_hll = f"""
        SELECT UPPER(a.country) as pais,
            SUM(g.visits) as visitas,
            HLL_COUNT.MERGE(g.users_sketch) as usuarios_hll,
            COUNTIF(g.users_sketch IS NULL) as filas_sin_sketch
        {base}
    """
    query_plain = f"""
        SELECT UPPER(a.country) as pais,
            SUM(g.visits) as visitas,
            NULL as usuarios_hll,
            1 as filas_sin_sketch
        {base}
    """
    df = _df(client, query_hll)
    if df.empty:
        df = _df(client, query_plain)
    out = {}
    for _, r in df.iterrows():
        con_hll = int(r['filas_sin_sketch'] or 0) == 0 and pd.notna(r['usuarios_hll'])
        out[r['pais']] = {
            'visitas': int(r['visitas']) if pd.notna(r['visitas']) else 0,
            'usuarios_unicos': int(r['usuarios_hll']) if con_hll else None,
        }
    return out


def _countries_top_creators(client, start, end, per_country=3) -> dict:
    query = f"""
        SELECT pais, nombre, notas FROM (
            SELECT UPPER(a.country) as pais,
                   COALESCE(a.complete_name, e.email_editor) as nombre,
                   COUNT(DISTINCT e.note_id) as notas,
                   ROW_NUMBER() OVER (PARTITION BY UPPER(a.country)
                                      ORDER BY COUNT(DISTINCT e.note_id) DESC) as rn
            FROM `{TABLE_EDITORIAL}` e
            INNER JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)
            WHERE e.action_type = 'CREATE'
              AND DATE(e.event_timestamp) BETWEEN '{start}' AND '{end}'
              AND a.country IS NOT NULL AND a.country != ''
              AND LOWER(e.email_editor) != 'infobae'
            GROUP BY pais, nombre
        )
        WHERE rn <= {per_country}
        ORDER BY pais, notas DESC
    """
    df = _df(client, query)
    out = {}
    for _, r in df.iterrows():
        out.setdefault(r['pais'], []).append({'nombre': r['nombre'], 'notas': int(r['notas'])})
    return out


def _sources(client, start, end) -> list:
    query = f"""
        WITH notas_por_fuente AS (
            SELECT e.note_id, e.story_url,
                CASE
                    WHEN LOWER(e.email_editor) = 'infobae' THEN 'Agencias'
                    WHEN LOWER(COALESCE(e.source, '')) LIKE '%scribnews%' THEN 'Scribnews'
                    WHEN LOWER(COALESCE(e.source, '')) LIKE '%composer%' THEN 'Composer'
                    ELSE 'Otros'
                END as fuente
            FROM `{TABLE_EDITORIAL}` e
            WHERE e.action_type = 'FIRST_PUBLISH'
              AND DATE(e.event_timestamp) BETWEEN '{start}' AND '{end}'
        ),
        metricas AS (
            SELECT n.fuente, COUNT(DISTINCT n.note_id) as notas, SUM(g.visits) as sesiones
            FROM notas_por_fuente n
            LEFT JOIN `{TABLE_PRODUCTIVITY}` g ON n.story_url = g.article_url
                AND g.date BETWEEN '{start}' AND '{end}'
            GROUP BY n.fuente
        )
        SELECT fuente, notas, COALESCE(sesiones, 0) as sesiones,
               SAFE_DIVIDE(COALESCE(sesiones, 0), notas) as sesiones_por_nota
        FROM metricas
        WHERE fuente != 'Otros'
        ORDER BY sesiones DESC
    """
    df = _df(client, query)
    return [{'fuente': r['fuente'], 'notas': int(r['notas']), 'sesiones': int(r['sesiones']),
             'sesiones_por_nota': float(r['sesiones_por_nota'] or 0)} for _, r in df.iterrows()]


# ───────────────────────────── carga completa ─────────────────────────────

@st.cache_data(ttl=86400, show_spinner=False)
def load_brief_data(_client, year: int, month: int) -> dict:
    """
    Todos los agregados del brief para un mes cerrado, mes actual y anterior.
    Un mes cerrado no cambia, así que el TTL es de 24 h.
    """
    start, end = month_range(year, month)
    py, pm = prev_month(year, month)
    pstart, pend = month_range(py, pm)

    tasks = {
        'production': lambda: _production(_client, start, end),
        'traffic': lambda: _traffic(_client, start, end),
        'daily_notas': lambda: _daily_notas(_client, start, end),
        'daily_visitas': lambda: _daily_visitas(_client, start, end),
        'top_publishers': lambda: _top_authors(_client, start, end, 'FIRST_PUBLISH'),
        'top_creators': lambda: _top_authors(_client, start, end, 'CREATE'),
        'sections': lambda: _sections(_client, start, end),
        'section_country': lambda: _section_country(_client, start, end),
        'countries_production': lambda: _countries_production(_client, start, end),
        'countries_traffic': lambda: _countries_traffic(_client, start, end),
        'countries_top': lambda: _countries_top_creators(_client, start, end),
        'sources': lambda: _sources(_client, start, end),
        'prev_production': lambda: _production(_client, pstart, pend),
        'prev_traffic': lambda: _traffic(_client, pstart, pend),
        'prev_sections': lambda: _sections(_client, pstart, pend),
        'prev_countries_production': lambda: _countries_production(_client, pstart, pend),
        'prev_sources': lambda: _sources(_client, pstart, pend),
    }
    data = _run_parallel(tasks)
    data['year'] = year
    data['month'] = month
    data['label'] = month_label(year, month)
    data['prev_label'] = month_label(py, pm)
    data['start'] = start
    data['end'] = end
    return data
