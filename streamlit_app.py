"""
📰 Infobae - Centro de Control Editorial
Dashboard Optimizado para Productividad e Impacto Editorial
Arquitectura: Queries Agregadas en BigQuery (NO descarga DataFrames grandes)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN GLOBAL
# ═══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Infobae | Centro de Control Editorial",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Paleta de colores Infobae
NARANJA_INFOBAE = "#F26522"
NEGRO_PERIODISTICO = "#231F20"
FONDO_CLARO = "#F8F9FA"
GRIS_TEXTO = "#6B7280"
BLANCO = "#FFFFFF"

# Tablas BigQuery
TABLE_PRODUCTIVITY = "data-prod-454014.Gold.GA4_ARC_author_productivity_daily"
TABLE_PRODUCTIVITY_SILVER = "data-prod-454014.Silver.GA4_productivity_cleaned"
TABLE_GEO_SOURCES = "data-prod-454014.Silver.GA4_geo_sources_metrics"
TABLE_EDITORIAL = "data-prod-454014.Silver.arc_editorial_activity"
TABLE_AUTHORS = "data-prod-454014.Bronze.authors_infobae_raw"

# ═══════════════════════════════════════════════════════════════════════════════
# CSS PERSONALIZADO
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown(f"""
<style>
    .stApp {{ background-color: {FONDO_CLARO}; }}
    h1, h2, h3 {{ color: {NEGRO_PERIODISTICO} !important; font-family: 'Helvetica Neue', Arial, sans-serif; font-weight: 700; }}
    h1 {{ font-size: 2.5rem !important; border-bottom: 4px solid {NARANJA_INFOBAE}; padding-bottom: 10px; margin-bottom: 20px !important; }}
    h2 {{ font-size: 1.5rem !important; color: {NARANJA_INFOBAE} !important; }}
    .main-header {{ font-size: 2.8rem; font-weight: 800; color: {NEGRO_PERIODISTICO}; text-align: center; padding: 20px 0; margin-bottom: 10px; }}
    .main-header .orange {{ color: {NARANJA_INFOBAE}; }}
    .subtitle {{ text-align: center; color: {GRIS_TEXTO}; font-size: 1rem; margin-bottom: 30px; }}
    .kpi-card {{ background: {BLANCO}; padding: 20px 25px; border-radius: 12px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.08); border-left: 4px solid {NARANJA_INFOBAE}; transition: transform 0.2s ease; }}
    .kpi-card:hover {{ transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.12); }}
    .kpi-card.highlight {{ background: linear-gradient(135deg, {NARANJA_INFOBAE} 0%, #FF8C42 100%); border-left: none; }}
    .kpi-card.highlight .kpi-value, .kpi-card.highlight .kpi-label {{ color: {BLANCO} !important; }}
    .kpi-value {{ font-size: 2.2rem; font-weight: 800; color: {NEGRO_PERIODISTICO}; line-height: 1.1; }}
    .kpi-value.orange {{ color: {NARANJA_INFOBAE}; }}
    .kpi-label {{ font-size: 0.75rem; color: {GRIS_TEXTO}; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 5px; font-weight: 600; }}
    .kpi-delta {{ font-size: 0.85rem; margin-top: 5px; font-weight: 600; }}
    .kpi-delta.positive {{ color: #10B981; }}
    .kpi-delta.negative {{ color: #EF4444; }}
    .section-title {{ font-size: 1.1rem; font-weight: 700; color: {NEGRO_PERIODISTICO}; margin-bottom: 15px; display: flex; align-items: center; gap: 8px; }}
    .section-title::before {{ content: ''; width: 4px; height: 20px; background: {NARANJA_INFOBAE}; border-radius: 2px; }}
    section[data-testid="stSidebar"] {{ background-color: {BLANCO}; border-right: 1px solid #E5E7EB; }}
    section[data-testid="stSidebar"] .stMarkdown h3 {{ color: {NARANJA_INFOBAE} !important; font-size: 0.9rem !important; text-transform: uppercase; letter-spacing: 1px; }}
    .status-badge {{ display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 0.75rem; font-weight: 600; }}
    .status-connected {{ background: #D1FAE5; color: #065F46; }}
    .status-error {{ background: #FEE2E2; color: #991B1B; }}
    .divider {{ height: 2px; background: linear-gradient(90deg, {NARANJA_INFOBAE} 0%, transparent 100%); margin: 30px 0; }}
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# CONEXIÓN BIGQUERY
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_bigquery_client():
    """Establece conexión con BigQuery usando credenciales de secrets."""
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

# ═══════════════════════════════════════════════════════════════════════════════
# QUERIES OPTIMIZADAS - LÓGICA DUAL DE FECHAS
# ═══════════════════════════════════════════════════════════════════════════════

def get_user_notes_cte(email_filter: str, start_date: str, end_date: str, include_urls: bool = False) -> str:
    """
    Genera las CTEs para identificar las notas "del usuario".
    Un usuario es dueño de una nota si:
    1. La CREÓ (evento CREATE)
    2. La PUBLICÓ (evento FIRST_PUBLISH)
    3. Fue el PRIMERO en hacer SAVE (si no hay CREATE de nadie)
    
    Returns: String con las CTEs (sin WITH inicial, para poder encadenar)
    """
    url_field = ", story_url" if include_urls else ""
    url_select = f", e.story_url" if include_urls else ""
    url_ps_select = f", ps.story_url" if include_urls else ""
    
    return f"""
        notas_create_{email_filter.replace('@', '_').replace('.', '_')[:10]} AS (
            SELECT DISTINCT note_id{url_select} FROM `{TABLE_EDITORIAL}` e
            WHERE email_editor = '{email_filter}'
              AND action_type = 'CREATE'
              AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
              {"AND story_url IS NOT NULL" if include_urls else ""}
        ),
        notas_publish_{email_filter.replace('@', '_').replace('.', '_')[:10]} AS (
            SELECT DISTINCT note_id{url_select} FROM `{TABLE_EDITORIAL}` e
            WHERE email_editor = '{email_filter}'
              AND action_type = 'FIRST_PUBLISH'
              AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
              {"AND story_url IS NOT NULL" if include_urls else ""}
        ),
        primer_save_{email_filter.replace('@', '_').replace('.', '_')[:10]} AS (
            SELECT note_id, email_editor{", story_url" if include_urls else ""},
                   ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
            FROM `{TABLE_EDITORIAL}`
            WHERE action_type = 'SAVE'
              AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
              {"AND story_url IS NOT NULL" if include_urls else ""}
        ),
        notas_con_create AS (
            SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'
        ),
        notas_primer_save_{email_filter.replace('@', '_').replace('.', '_')[:10]} AS (
            SELECT ps.note_id{url_ps_select} FROM primer_save_{email_filter.replace('@', '_').replace('.', '_')[:10]} ps
            WHERE ps.rn = 1 
              AND ps.email_editor = '{email_filter}'
              AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
        ),
        todas_notas_usuario AS (
            SELECT note_id{url_field} FROM notas_create_{email_filter.replace('@', '_').replace('.', '_')[:10]} UNION DISTINCT
            SELECT note_id{url_field} FROM notas_publish_{email_filter.replace('@', '_').replace('.', '_')[:10]} UNION DISTINCT
            SELECT note_id{url_field} FROM notas_primer_save_{email_filter.replace('@', '_').replace('.', '_')[:10]}
        ),
        notas_publicadas_periodo AS (
            SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
            WHERE action_type = 'FIRST_PUBLISH'
              AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
        )
    """


@st.cache_data(ttl=3600, show_spinner=False)
def load_production_metrics(_client, start_date: str, end_date: str, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> dict:
    """
    Carga métricas de PRODUCCIÓN desde arc_editorial_activity.
    
    Definición de "creador":
    - Tiene evento CREATE, O
    - Fue el PRIMERO en hacer SAVE en notas que no tienen CREATE de nadie
    
    Definición de "publicador":
    - Tiene evento FIRST_PUBLISH
    
    Notas "del usuario" = CREATE + FIRST_PUBLISH + PRIMER_SAVE (si no hay CREATE)
    Notas "publicadas" = notas "del usuario" que tienen FIRST_PUBLISH
    """
    seccion_clause = f"AND e.segment = '{seccion_filter}'" if seccion_filter else ""
    pais_clause = f"AND UPPER(a.country) = UPPER('{pais_filter}')" if pais_filter else ""
    join_clause = f"LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)" if pais_filter else ""
    
    creadores = 0
    publicadores = 0
    notas = 0
    
    if email_filter:
        # Query para contar creadores y publicadores de las notas del usuario
        query_counts = f"""
            WITH notas_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}'
                  AND action_type = 'CREATE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_publish AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}'
                  AND action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            primer_save AS (
                SELECT note_id, email_editor,
                       ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'SAVE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_con_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'
            ),
            notas_primer_save AS (
                SELECT ps.note_id FROM primer_save ps
                WHERE ps.rn = 1 
                  AND ps.email_editor = '{email_filter}'
                  AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
            ),
            todas_notas_usuario AS (
                SELECT note_id FROM notas_create UNION DISTINCT
                SELECT note_id FROM notas_publish UNION DISTINCT
                SELECT note_id FROM notas_primer_save
            ),
            -- Creadores reales: CREATE si existe, si no PRIMER_SAVE
            creadores_create AS (
                SELECT note_id, email_editor as creador_email FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'
            ),
            primer_save_all AS (
                SELECT note_id, email_editor, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
            ),
            creadores_primer_save AS (
                SELECT ps.note_id, ps.email_editor as creador_email FROM primer_save_all ps
                WHERE ps.rn = 1 AND ps.note_id NOT IN (SELECT note_id FROM creadores_create)
            ),
            creadores_reales AS (
                SELECT note_id, creador_email FROM creadores_create UNION ALL
                SELECT note_id, creador_email FROM creadores_primer_save
            ),
            -- Publicadores de las notas del usuario
            publicadores_notas AS (
                SELECT DISTINCT e.email_editor
                FROM `{TABLE_EDITORIAL}` e
                WHERE e.action_type = 'FIRST_PUBLISH'
                  AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND e.note_id IN (SELECT note_id FROM todas_notas_usuario)
            ),
            -- Creadores de las notas del usuario
            creadores_notas AS (
                SELECT DISTINCT cr.creador_email
                FROM creadores_reales cr
                WHERE cr.note_id IN (SELECT note_id FROM todas_notas_usuario)
            )
            SELECT 
                (SELECT COUNT(*) FROM creadores_notas) as total_creadores,
                (SELECT COUNT(*) FROM publicadores_notas) as total_publicadores
        """
        
        try:
            df_counts = _client.query(query_counts).to_dataframe()
            if not df_counts.empty:
                creadores = int(df_counts.iloc[0]['total_creadores'] or 0)
                publicadores = int(df_counts.iloc[0]['total_publicadores'] or 0)
        except:
            pass
        
        # Query para notas publicadas del usuario
        query_notas = f"""
            WITH notas_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}'
                  AND action_type = 'CREATE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_publish AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}'
                  AND action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            primer_save AS (
                SELECT note_id, email_editor,
                       ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'SAVE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_con_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'
            ),
            notas_primer_save AS (
                SELECT ps.note_id FROM primer_save ps
                WHERE ps.rn = 1 
                  AND ps.email_editor = '{email_filter}'
                  AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
            ),
            todas_notas_usuario AS (
                SELECT note_id FROM notas_create UNION DISTINCT
                SELECT note_id FROM notas_publish UNION DISTINCT
                SELECT note_id FROM notas_primer_save
            ),
            notas_publicadas AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            )
            SELECT COUNT(DISTINCT t.note_id) as notas_publicadas
            FROM todas_notas_usuario t
            INNER JOIN notas_publicadas p ON t.note_id = p.note_id
            INNER JOIN `{TABLE_EDITORIAL}` e ON t.note_id = e.note_id AND e.action_type = 'FIRST_PUBLISH'
            {f"LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)" if pais_filter else ""}
            WHERE 1=1 {seccion_clause} {pais_clause}
        """
    else:
        # Sin filtro de email: contar todos los creadores y publicadores únicos
        query_creadores = f"""
            SELECT COUNT(DISTINCT e.email_editor) as creadores_activos
            FROM `{TABLE_EDITORIAL}` e
            {join_clause}
            WHERE e.action_type = 'CREATE'
              AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
              {seccion_clause} {pais_clause}
        """
        query_publicadores = f"""
            SELECT COUNT(DISTINCT e.email_editor) as publicadores_activos
            FROM `{TABLE_EDITORIAL}` e
            {join_clause}
            WHERE e.action_type = 'FIRST_PUBLISH'
              AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
              {seccion_clause} {pais_clause}
        """
        query_notas = f"""
            SELECT COUNT(DISTINCT e.note_id) as notas_publicadas
            FROM `{TABLE_EDITORIAL}` e
            {join_clause}
            WHERE e.action_type = 'FIRST_PUBLISH'
              AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
              {seccion_clause} {pais_clause}
        """
        
        try:
            df_creadores = _client.query(query_creadores).to_dataframe()
            if not df_creadores.empty:
                creadores = int(df_creadores.iloc[0]['creadores_activos'] or 0)
        except Exception as e:
            st.error(f"Error cargando creadores activos: {e}")
        
        try:
            df_publicadores = _client.query(query_publicadores).to_dataframe()
            if not df_publicadores.empty:
                publicadores = int(df_publicadores.iloc[0]['publicadores_activos'] or 0)
        except Exception as e:
            st.error(f"Error cargando publicadores activos: {e}")
    
    try:
        df_notas = _client.query(query_notas).to_dataframe()
        if not df_notas.empty:
            notas = int(df_notas.iloc[0]['notas_publicadas'] or 0)
    except Exception as e:
        st.error(f"Error cargando notas publicadas: {e}")
    
    return {'creadores_activos': creadores, 'publicadores_activos': publicadores, 'notas_publicadas': notas}


@st.cache_data(ttl=3600, show_spinner=False)
def load_traffic_metrics(_client, start_date: str, end_date: str, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> dict:
    """
    Carga métricas de TRÁFICO desde GA4_ARC_author_productivity_daily.
    
    Con filtro de email:
    - Busca notas "del usuario" (CREATE + FIRST_PUBLISH + PRIMER_SAVE sin CREATE)
    - Solo las que están publicadas
    - Calcula tráfico sobre esas notas
    """
    TABLE_SILVER = "data-prod-454014.Silver.GA4_productivity_cleaned"
    
    seccion_clause = f"AND g.section = '{seccion_filter}'" if seccion_filter else ""
    pais_clause = f"AND UPPER(a.country) = UPPER('{pais_filter}')" if pais_filter else ""
    join_clause = f"LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(g.creator_email) = LOWER(a.email)" if pais_filter else ""
    
    result = {
        'visitas_totales': 0, 'pageviews_totales': 0, 
        'tiempo_promedio_min': 0, 'scroll_promedio': 0, 'scrolls_totales': 0,
        'usuarios_unicos': 0
    }
    
    if email_filter:
        # Con filtro de email: usar CTEs para identificar notas del usuario
        query = f"""
            WITH notas_create AS (
                SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}'
                  AND action_type = 'CREATE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND story_url IS NOT NULL
            ),
            notas_publish AS (
                SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}'
                  AND action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND story_url IS NOT NULL
            ),
            primer_save AS (
                SELECT note_id, email_editor, story_url,
                       ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'SAVE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND story_url IS NOT NULL
            ),
            notas_con_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'
            ),
            notas_primer_save AS (
                SELECT ps.note_id, ps.story_url FROM primer_save ps
                WHERE ps.rn = 1 
                  AND ps.email_editor = '{email_filter}'
                  AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
            ),
            todas_notas_usuario AS (
                SELECT note_id, story_url FROM notas_create UNION DISTINCT
                SELECT note_id, story_url FROM notas_publish UNION DISTINCT
                SELECT note_id, story_url FROM notas_primer_save
            ),
            notas_publicadas AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            urls_del_usuario AS (
                SELECT DISTINCT t.story_url
                FROM todas_notas_usuario t
                INNER JOIN notas_publicadas p ON t.note_id = p.note_id
            )
            SELECT
                SUM(g.visits) as visitas_totales,
                SUM(g.pageviews) as pageviews_totales,
                SAFE_DIVIDE(SUM(g.total_time_seconds), SUM(g.visits)) as tiempo_promedio_segundos,
                SAFE_DIVIDE(SUM(g.scrolls), SUM(g.visits)) as scroll_promedio,
                SUM(g.scrolls) as scrolls_totales
            FROM `{TABLE_PRODUCTIVITY}` g
            {join_clause}
            WHERE g.date BETWEEN '{start_date}' AND '{end_date}'
              AND g.article_url IN (SELECT story_url FROM urls_del_usuario)
              {seccion_clause} {pais_clause}
        """
        
        query_users = f"""
            WITH notas_create AS (
                SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}' AND action_type = 'CREATE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
            ),
            notas_publish AS (
                SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}' AND action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
            ),
            primer_save AS (
                SELECT note_id, email_editor, story_url,
                       ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
            ),
            notas_con_create AS (SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'),
            notas_primer_save AS (
                SELECT ps.note_id, ps.story_url FROM primer_save ps
                WHERE ps.rn = 1 AND ps.email_editor = '{email_filter}'
                  AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
            ),
            todas_notas_usuario AS (
                SELECT note_id, story_url FROM notas_create UNION DISTINCT
                SELECT note_id, story_url FROM notas_publish UNION DISTINCT
                SELECT note_id, story_url FROM notas_primer_save
            ),
            notas_publicadas AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'FIRST_PUBLISH' AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            urls_del_usuario AS (
                SELECT DISTINCT t.story_url FROM todas_notas_usuario t
                INNER JOIN notas_publicadas p ON t.note_id = p.note_id
            )
            SELECT SUM(s.daily_users) as usuarios_unicos
            FROM `{TABLE_SILVER}` s
            WHERE s.event_date BETWEEN '{start_date}' AND '{end_date}'
              AND s.article_url IN (SELECT story_url FROM urls_del_usuario)
        """
    else:
        # Sin filtro de email: query simple
        query = f"""
            SELECT
                SUM(g.visits) as visitas_totales,
                SUM(g.pageviews) as pageviews_totales,
                SAFE_DIVIDE(SUM(g.total_time_seconds), SUM(g.visits)) as tiempo_promedio_segundos,
                SAFE_DIVIDE(SUM(g.scrolls), SUM(g.visits)) as scroll_promedio,
                SUM(g.scrolls) as scrolls_totales
            FROM `{TABLE_PRODUCTIVITY}` g
            {join_clause}
            WHERE g.date BETWEEN '{start_date}' AND '{end_date}'
              AND DATE(g.publish_date) BETWEEN '{start_date}' AND '{end_date}'
              {seccion_clause} {pais_clause}
        """
        
        query_users = f"""
            SELECT SUM(s.daily_users) as usuarios_unicos
            FROM `{TABLE_SILVER}` s
            INNER JOIN `{TABLE_PRODUCTIVITY}` g ON s.article_url = g.article_url AND s.event_date = g.date
            {join_clause}
            WHERE s.event_date BETWEEN '{start_date}' AND '{end_date}'
              AND DATE(g.publish_date) BETWEEN '{start_date}' AND '{end_date}'
              {seccion_clause} {pais_clause}
        """
    
    try:
        df = _client.query(query).to_dataframe()
        if not df.empty:
            row = df.iloc[0]
            result['visitas_totales'] = int(row['visitas_totales']) if pd.notna(row['visitas_totales']) else 0
            result['pageviews_totales'] = int(row['pageviews_totales']) if pd.notna(row['pageviews_totales']) else 0
            result['tiempo_promedio_min'] = (float(row['tiempo_promedio_segundos']) if pd.notna(row['tiempo_promedio_segundos']) else 0) / 60
            result['scroll_promedio'] = float(row['scroll_promedio']) if pd.notna(row['scroll_promedio']) else 0
            result['scrolls_totales'] = int(row['scrolls_totales']) if pd.notna(row['scrolls_totales']) else 0
    except Exception as e:
        st.error(f"Error cargando métricas de tráfico: {e}")
    
    try:
        df_users = _client.query(query_users).to_dataframe()
        if not df_users.empty:
            result['usuarios_unicos'] = int(df_users.iloc[0]['usuarios_unicos']) if pd.notna(df_users.iloc[0]['usuarios_unicos']) else 0
    except:
        pass
    
    return result


@st.cache_data(ttl=3600, show_spinner=False)
def load_kpis(_client, start_date: str, end_date: str, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> dict:
    """
    Combina métricas de producción (arc_editorial_activity) y tráfico (GA4).
    """
    # Cargar métricas de producción desde arc_editorial_activity
    production = load_production_metrics(_client, start_date, end_date, email_filter, seccion_filter, pais_filter)
    
    # Cargar métricas de tráfico desde GA4
    traffic = load_traffic_metrics(_client, start_date, end_date, email_filter, seccion_filter, pais_filter)
    
    # Calcular productividad
    notas = production['notas_publicadas']
    visitas = traffic['visitas_totales']
    productividad = visitas / max(notas, 1)
    
    return {
        'creadores_activos': production['creadores_activos'],
        'publicadores_activos': production['publicadores_activos'],
        'notas_publicadas': production['notas_publicadas'],
        'visitas_totales': traffic['visitas_totales'],
        'usuarios_unicos': traffic['usuarios_unicos'],
        'pageviews_totales': traffic['pageviews_totales'],
        'tiempo_promedio_min': traffic['tiempo_promedio_min'],
        'scroll_promedio': traffic['scroll_promedio'],
        'scrolls_totales': traffic['scrolls_totales'],
        'productividad': productividad
    }


@st.cache_data(ttl=3600, show_spinner=False)
def load_previous_kpis(_client, start_date: str, end_date: str, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> dict:
    """Carga KPIs del período anterior para comparación."""
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    period_days = (end_dt - start_dt).days + 1
    
    prev_end = start_dt - timedelta(days=1)
    prev_start = prev_end - timedelta(days=period_days - 1)
    
    prev_start_str = prev_start.strftime('%Y-%m-%d')
    prev_end_str = prev_end.strftime('%Y-%m-%d')
    
    return load_kpis(_client, prev_start_str, prev_end_str, email_filter, seccion_filter, pais_filter)


@st.cache_data(ttl=3600, show_spinner=False)
def load_top_publishers(_client, start_date: str, end_date: str, limit: int = 10, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> pd.DataFrame:
    """
    Top 10 Publicadores:
    - Con filtro de email: muestra QUIÉN PUBLICÓ las notas del usuario
    - Sin filtro: muestra los publicadores con más FIRST_PUBLISH
    """
    seccion_clause = f"AND e.segment = '{seccion_filter}'" if seccion_filter else ""
    pais_clause = f"AND UPPER(a.country) = UPPER('{pais_filter}')" if pais_filter else ""
    
    if email_filter:
        # Mostrar QUIÉN PUBLICÓ las notas del usuario (no necesariamente el usuario)
        query = f"""
            WITH notas_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}'
                  AND action_type = 'CREATE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_publish AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}'
                  AND action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            primer_save AS (
                SELECT note_id, email_editor,
                       ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'SAVE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_con_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'
            ),
            notas_primer_save AS (
                SELECT ps.note_id FROM primer_save ps
                WHERE ps.rn = 1 
                  AND ps.email_editor = '{email_filter}'
                  AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
            ),
            todas_notas_usuario AS (
                SELECT note_id FROM notas_create UNION DISTINCT
                SELECT note_id FROM notas_publish UNION DISTINCT
                SELECT note_id FROM notas_primer_save
            )
            -- Buscar QUIÉN hizo FIRST_PUBLISH en las notas del usuario
            SELECT 
                CASE 
                    WHEN LOWER(e.email_editor) = 'infobae' THEN 'Infobae (agencias)'
                    ELSE COALESCE(a.complete_name, e.email_editor) 
                END as Publicador,
                a.country as Pais,
                COUNT(DISTINCT e.note_id) as notas_publicadas
            FROM `{TABLE_EDITORIAL}` e
            LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)
            WHERE e.action_type = 'FIRST_PUBLISH'
              AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
              AND e.note_id IN (SELECT note_id FROM todas_notas_usuario)
              AND e.email_editor IS NOT NULL AND e.email_editor != ''
            GROUP BY Publicador, Pais
            ORDER BY notas_publicadas DESC
            LIMIT {limit}
        """
    else:
        query = f"""
            SELECT 
                CASE 
                    WHEN LOWER(e.email_editor) = 'infobae' THEN 'Infobae (agencias)'
                    ELSE COALESCE(a.complete_name, e.email_editor) 
                END as Publicador,
                a.country as Pais,
                COUNT(*) as notas_publicadas
            FROM `{TABLE_EDITORIAL}` e
            LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)
            WHERE e.action_type = 'FIRST_PUBLISH'
              AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
              AND e.email_editor IS NOT NULL AND e.email_editor != ''
              {seccion_clause} {pais_clause}
            GROUP BY Publicador, Pais
            ORDER BY notas_publicadas DESC
        LIMIT {limit}
    """
    try:
        return _client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error cargando top publicadores: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_top_creators(_client, start_date: str, end_date: str, limit: int = 10, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> pd.DataFrame:
    """
    Top 10 Creadores:
    - Con filtro de email: muestra QUIÉN CREÓ las notas del usuario
    - Sin filtro: muestra los creadores con más notas
    """
    seccion_clause = f"AND e.segment = '{seccion_filter}'" if seccion_filter else ""
    pais_clause = f"AND UPPER(a.country) = UPPER('{pais_filter}')" if pais_filter else ""
    
    if email_filter:
        # Mostrar QUIÉN CREÓ las notas del usuario (CREATE o PRIMER_SAVE sin CREATE)
        query = f"""
            WITH notas_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}'
                  AND action_type = 'CREATE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_publish AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}'
                  AND action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            primer_save AS (
                SELECT note_id, email_editor,
                       ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'SAVE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_con_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'
            ),
            notas_primer_save AS (
                SELECT ps.note_id FROM primer_save ps
                WHERE ps.rn = 1 
                  AND ps.email_editor = '{email_filter}'
                  AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
            ),
            todas_notas_usuario AS (
                SELECT note_id FROM notas_create UNION DISTINCT
                SELECT note_id FROM notas_publish UNION DISTINCT
                SELECT note_id FROM notas_primer_save
            ),
            -- Creadores: CREATE si existe, si no PRIMER_SAVE
            creadores_create AS (
                SELECT note_id, email_editor as creador_email
                FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'CREATE'
            ),
            primer_save_all AS (
                SELECT note_id, email_editor, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
            ),
            creadores_primer_save AS (
                SELECT ps.note_id, ps.email_editor as creador_email
                FROM primer_save_all ps
                WHERE ps.rn = 1 AND ps.note_id NOT IN (SELECT note_id FROM creadores_create)
            ),
            creadores_reales AS (
                SELECT note_id, creador_email FROM creadores_create
                UNION ALL
                SELECT note_id, creador_email FROM creadores_primer_save
            )
            -- Buscar QUIÉN CREÓ las notas del usuario
            SELECT 
                CASE 
                    WHEN LOWER(cr.creador_email) = 'infobae' THEN 'Infobae (agencias)'
                    ELSE COALESCE(a.complete_name, cr.creador_email) 
                END as Creador,
                a.country as Pais,
                COUNT(DISTINCT cr.note_id) as notas_creadas
            FROM creadores_reales cr
            LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(cr.creador_email) = LOWER(a.email)
            WHERE cr.note_id IN (SELECT note_id FROM todas_notas_usuario)
              AND cr.creador_email IS NOT NULL AND cr.creador_email != ''
            GROUP BY Creador, Pais
            ORDER BY notas_creadas DESC
            LIMIT {limit}
        """
    else:
        # Sin filtro: mostrar todos los creadores (solo CREATE por simplicidad)
        query = f"""
            SELECT 
                CASE 
                    WHEN LOWER(e.email_editor) = 'infobae' THEN 'Infobae (agencias)'
                    ELSE COALESCE(a.complete_name, e.email_editor) 
                END as Creador,
                a.country as Pais,
                COUNT(*) as notas_creadas
            FROM `{TABLE_EDITORIAL}` e
            LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)
            WHERE e.action_type = 'CREATE'
              AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
              AND e.email_editor IS NOT NULL AND e.email_editor != ''
              {seccion_clause} {pais_clause}
            GROUP BY Creador, Pais
            ORDER BY notas_creadas DESC
            LIMIT {limit}
        """
    
    try:
        return _client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error cargando top creadores: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_daily_evolution(_client, start_date: str, end_date: str, metric: str = 'visits', email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> pd.DataFrame:
    """Carga evolución diaria agregada en BigQuery. Usa lógica de PRIMER_SAVE como creador."""
    TABLE_SILVER = "data-prod-454014.Silver.GA4_productivity_cleaned"
    seccion_clause = f"AND e.segment = '{seccion_filter}'" if seccion_filter else ""
    seccion_clause_gold = f"AND g.section = '{seccion_filter}'" if seccion_filter else ""
    pais_clause = f"AND UPPER(a.country) = UPPER('{pais_filter}')" if pais_filter else ""
    join_clause = f"LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)" if pais_filter else ""
    
    if metric == 'notas':
        if email_filter:
            # Usar CTEs con PRIMER_SAVE
            query = f"""
                WITH notas_create AS (
                    SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'CREATE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                notas_publish AS (
                    SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'FIRST_PUBLISH'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                primer_save AS (
                    SELECT note_id, email_editor, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                    FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                notas_con_create AS (SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'),
                notas_primer_save AS (
                    SELECT ps.note_id FROM primer_save ps
                    WHERE ps.rn = 1 AND ps.email_editor = '{email_filter}'
                      AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
                ),
                todas_notas_usuario AS (
                    SELECT note_id FROM notas_create UNION DISTINCT
                    SELECT note_id FROM notas_publish UNION DISTINCT
                    SELECT note_id FROM notas_primer_save
                )
                SELECT DATE(e.event_timestamp) as fecha, COUNT(DISTINCT e.note_id) as valor
                FROM `{TABLE_EDITORIAL}` e
                WHERE e.action_type = 'FIRST_PUBLISH'
                  AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND e.note_id IN (SELECT note_id FROM todas_notas_usuario)
                  {seccion_clause}
                GROUP BY fecha ORDER BY fecha
            """
        else:
            query = f"""
                SELECT DATE(e.event_timestamp) as fecha, COUNT(*) as valor
                FROM `{TABLE_EDITORIAL}` e {join_clause}
                WHERE e.action_type = 'FIRST_PUBLISH'
                  AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  {seccion_clause} {pais_clause}
                GROUP BY fecha ORDER BY fecha
            """
    elif metric == 'users':
        if email_filter:
            query = f"""
                WITH notas_create AS (
                    SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'CREATE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
                ),
                notas_publish AS (
                    SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'FIRST_PUBLISH'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
                ),
                primer_save AS (
                    SELECT note_id, email_editor, story_url, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                    FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
                ),
                notas_con_create AS (SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'),
                notas_primer_save AS (
                    SELECT ps.note_id, ps.story_url FROM primer_save ps
                    WHERE ps.rn = 1 AND ps.email_editor = '{email_filter}'
                      AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
                ),
                todas_notas_usuario AS (
                    SELECT note_id, story_url FROM notas_create UNION DISTINCT
                    SELECT note_id, story_url FROM notas_publish UNION DISTINCT
                    SELECT note_id, story_url FROM notas_primer_save
                ),
                notas_publicadas AS (
                    SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                    WHERE action_type = 'FIRST_PUBLISH' AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                urls_usuario AS (
                    SELECT DISTINCT t.story_url FROM todas_notas_usuario t
                    INNER JOIN notas_publicadas p ON t.note_id = p.note_id
                )
                SELECT s.event_date as fecha, SUM(s.daily_users) as valor
                FROM `{TABLE_SILVER}` s
                WHERE s.event_date BETWEEN '{start_date}' AND '{end_date}'
                  AND s.article_url IN (SELECT story_url FROM urls_usuario)
                GROUP BY s.event_date ORDER BY s.event_date
            """
        else:
            join_gold = f"LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(g.creator_email) = LOWER(a.email)" if pais_filter else ""
            query = f"""
                SELECT s.event_date as fecha, SUM(s.daily_users) as valor
                FROM `{TABLE_SILVER}` s
                INNER JOIN `{TABLE_PRODUCTIVITY}` g ON s.article_url = g.article_url AND s.event_date = g.date
                {join_gold}
                WHERE s.event_date BETWEEN '{start_date}' AND '{end_date}'
                  AND DATE(g.publish_date) BETWEEN '{start_date}' AND '{end_date}'
                  {seccion_clause_gold} {pais_clause}
                GROUP BY s.event_date ORDER BY s.event_date
            """
    else:
        if email_filter:
            query = f"""
                WITH notas_create AS (
                    SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'CREATE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
                ),
                notas_publish AS (
                    SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'FIRST_PUBLISH'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
                ),
                primer_save AS (
                    SELECT note_id, email_editor, story_url, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                    FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
                ),
                notas_con_create AS (SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'),
                notas_primer_save AS (
                    SELECT ps.note_id, ps.story_url FROM primer_save ps
                    WHERE ps.rn = 1 AND ps.email_editor = '{email_filter}'
                      AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
                ),
                todas_notas_usuario AS (
                    SELECT note_id, story_url FROM notas_create UNION DISTINCT
                    SELECT note_id, story_url FROM notas_publish UNION DISTINCT
                    SELECT note_id, story_url FROM notas_primer_save
                ),
                notas_publicadas AS (
                    SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                    WHERE action_type = 'FIRST_PUBLISH' AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                urls_usuario AS (
                    SELECT DISTINCT t.story_url FROM todas_notas_usuario t
                    INNER JOIN notas_publicadas p ON t.note_id = p.note_id
                )
                SELECT g.date as fecha, SUM(g.{metric}) as valor
                FROM `{TABLE_PRODUCTIVITY}` g
                WHERE g.date BETWEEN '{start_date}' AND '{end_date}'
                  AND g.article_url IN (SELECT story_url FROM urls_usuario)
                  {seccion_clause_gold}
                GROUP BY g.date ORDER BY g.date
            """
        else:
            join_gold = f"LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(g.creator_email) = LOWER(a.email)" if pais_filter else ""
            query = f"""
                SELECT g.date as fecha, SUM(g.{metric}) as valor
                FROM `{TABLE_PRODUCTIVITY}` g {join_gold}
                WHERE g.date BETWEEN '{start_date}' AND '{end_date}'
                  AND DATE(g.publish_date) BETWEEN '{start_date}' AND '{end_date}'
                  {seccion_clause_gold} {pais_clause}
                GROUP BY g.date ORDER BY g.date
            """
    
    try:
        df = _client.query(query).to_dataframe()
        df['fecha'] = pd.to_datetime(df['fecha'])
        return df
    except:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_section_stats(_client, start_date: str, end_date: str, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> pd.DataFrame:
    """
    Carga estadísticas completas por sección.
    Usa lógica de PRIMER_SAVE como creador cuando hay filtro de email.
    """
    seccion_clause_editorial = f"AND ed.segment = '{seccion_filter}'" if seccion_filter else ""
    seccion_clause_gold = f"AND g.section = '{seccion_filter}'" if seccion_filter else ""
    pais_clause_editorial = f"AND UPPER(a1.country) = UPPER('{pais_filter}')" if pais_filter else ""
    pais_clause_gold = f"AND UPPER(a2.country) = UPPER('{pais_filter}')" if pais_filter else ""
    join_editorial = f"LEFT JOIN `{TABLE_AUTHORS}` a1 ON LOWER(ed.email_editor) = LOWER(a1.email)" if pais_filter else ""
    join_gold = f"LEFT JOIN `{TABLE_AUTHORS}` a2 ON LOWER(g.creator_email) = LOWER(a2.email)" if pais_filter else ""
    
    if email_filter:
        # CTEs con PRIMER_SAVE
        notas_usuario_cte = f"""
            notas_create AS (
                SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}' AND action_type = 'CREATE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_publish AS (
                SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}' AND action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            primer_save AS (
                SELECT note_id, email_editor, story_url, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_con_create AS (SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'),
            notas_primer_save AS (
                SELECT ps.note_id, ps.story_url FROM primer_save ps
                WHERE ps.rn = 1 AND ps.email_editor = '{email_filter}'
                  AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
            ),
            todas_notas_usuario AS (
                SELECT note_id, story_url FROM notas_create UNION DISTINCT
                SELECT note_id, story_url FROM notas_publish UNION DISTINCT
                SELECT note_id, story_url FROM notas_primer_save
            ),
            notas_publicadas AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'FIRST_PUBLISH' AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_usuario_publicadas AS (
                SELECT t.note_id, t.story_url FROM todas_notas_usuario t
                INNER JOIN notas_publicadas p ON t.note_id = p.note_id
            ),
        """
        note_filter_editorial = "AND ed.note_id IN (SELECT note_id FROM notas_usuario_publicadas)"
        note_filter_gold = "AND g.article_url IN (SELECT story_url FROM notas_usuario_publicadas WHERE story_url IS NOT NULL)"
        pais_clause_editorial = ""
        pais_clause_gold = ""
    else:
        notas_usuario_cte = ""
        note_filter_editorial = ""
        note_filter_gold = ""
    
    # Cuando hay email_filter, usar Silver (GA4_productivity_cleaned) directamente para el tráfico
    if email_filter:
        query = f"""
            WITH {notas_usuario_cte}
            editorial_stats AS (
                SELECT 
                    ed.segment as seccion,
                    COUNT(DISTINCT ed.note_id) as notas,
                    COUNT(DISTINCT CASE WHEN LOWER(ed.source) LIKE '%composer%' THEN ed.note_id END) as composer,
                    COUNT(DISTINCT CASE WHEN LOWER(ed.source) LIKE '%scribnews%' THEN ed.note_id END) as scribnews
                FROM `{TABLE_EDITORIAL}` ed
                WHERE ed.action_type = 'FIRST_PUBLISH'
                  AND DATE(ed.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND ed.segment IS NOT NULL AND ed.segment != ''
                  {note_filter_editorial} {seccion_clause_editorial}
                GROUP BY ed.segment
            ),
            urls_por_seccion AS (
                SELECT DISTINCT ed.segment as seccion, ed.story_url
                FROM `{TABLE_EDITORIAL}` ed
                WHERE ed.action_type = 'FIRST_PUBLISH'
                  AND DATE(ed.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND ed.segment IS NOT NULL AND ed.segment != ''
                  AND ed.story_url IS NOT NULL
                  {note_filter_editorial} {seccion_clause_editorial}
            ),
            traffic_stats AS (
                SELECT 
                    u.seccion,
                    SUM(g.daily_sessions) as sesiones,
                    SUM(g.daily_pageviews) as pageviews,
                    SAFE_DIVIDE(SUM(g.sessions_with_scroll), SUM(g.daily_sessions)) as scroll_promedio
                FROM urls_por_seccion u
                INNER JOIN `{TABLE_PRODUCTIVITY_SILVER}` g ON u.story_url = g.article_url
                WHERE g.event_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY u.seccion
            )
            SELECT 
                e.seccion,
                e.notas,
                e.composer,
                e.scribnews,
                COALESCE(t.sesiones, 0) as sesiones,
                COALESCE(t.pageviews, 0) as pageviews,
                COALESCE(t.scroll_promedio, 0) as scroll_promedio,
                SAFE_DIVIDE(COALESCE(t.sesiones, 0), e.notas) as productividad
            FROM editorial_stats e
            LEFT JOIN traffic_stats t ON e.seccion = t.seccion
            ORDER BY e.notas DESC
        """
    else:
        query = f"""
            WITH editorial_stats AS (
                SELECT 
                    ed.segment as seccion,
                    COUNT(DISTINCT ed.note_id) as notas,
                    COUNT(DISTINCT CASE WHEN LOWER(ed.source) LIKE '%composer%' THEN ed.note_id END) as composer,
                    COUNT(DISTINCT CASE WHEN LOWER(ed.source) LIKE '%scribnews%' THEN ed.note_id END) as scribnews
                FROM `{TABLE_EDITORIAL}` ed
                {join_editorial}
                WHERE ed.action_type = 'FIRST_PUBLISH'
                  AND DATE(ed.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND ed.segment IS NOT NULL AND ed.segment != ''
                  {seccion_clause_editorial} {pais_clause_editorial}
                GROUP BY ed.segment
            ),
            traffic_stats AS (
                SELECT 
                    g.section as seccion,
                    SUM(g.visits) as sesiones,
                    SUM(g.pageviews) as pageviews,
                    SAFE_DIVIDE(SUM(g.scrolls), SUM(g.visits)) as scroll_promedio
                FROM `{TABLE_PRODUCTIVITY}` g
                {join_gold}
                WHERE g.date BETWEEN '{start_date}' AND '{end_date}'
                  AND DATE(g.publish_date) BETWEEN '{start_date}' AND '{end_date}'
                  AND g.section IS NOT NULL AND g.section != ''
                  {seccion_clause_gold} {pais_clause_gold}
                GROUP BY g.section
            )
            SELECT 
                e.seccion,
                e.notas,
                e.composer,
                e.scribnews,
                COALESCE(t.sesiones, 0) as sesiones,
                COALESCE(t.pageviews, 0) as pageviews,
                COALESCE(t.scroll_promedio, 0) as scroll_promedio,
                SAFE_DIVIDE(COALESCE(t.sesiones, 0), e.notas) as productividad
            FROM editorial_stats e
            LEFT JOIN traffic_stats t ON e.seccion = t.seccion
            ORDER BY e.notas DESC
        """
    try:
        return _client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error cargando secciones: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_geo_data(_client, start_date: str, end_date: str, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> pd.DataFrame:
    """Carga datos geográficos. Usa lógica de PRIMER_SAVE como creador."""
    
    if email_filter or seccion_filter or pais_filter:
        seccion_clause = f"AND e.segment = '{seccion_filter}'" if seccion_filter else ""
        pais_clause = f"AND UPPER(a.country) = UPPER('{pais_filter}')" if pais_filter else ""
        join_authors = f"LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)" if pais_filter else ""
        
        if email_filter:
            notas_usuario_cte = f"""
                notas_create AS (
                    SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'CREATE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
                ),
                notas_publish AS (
                    SELECT DISTINCT note_id, story_url FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'FIRST_PUBLISH'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
                ),
                primer_save AS (
                    SELECT note_id, email_editor, story_url, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                    FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}' AND story_url IS NOT NULL
                ),
                notas_con_create AS (SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'),
                notas_primer_save AS (
                    SELECT ps.note_id, ps.story_url FROM primer_save ps
                    WHERE ps.rn = 1 AND ps.email_editor = '{email_filter}'
                      AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
                ),
                todas_notas_usuario AS (
                    SELECT note_id, story_url FROM notas_create UNION DISTINCT
                    SELECT note_id, story_url FROM notas_publish UNION DISTINCT
                    SELECT note_id, story_url FROM notas_primer_save
                ),
                notas_publicadas AS (
                    SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                    WHERE action_type = 'FIRST_PUBLISH' AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                urls_usuario AS (
                    SELECT DISTINCT t.story_url FROM todas_notas_usuario t
                    INNER JOIN notas_publicadas p ON t.note_id = p.note_id
                    WHERE t.story_url IS NOT NULL
                ),
            """
            note_filter = "AND e.story_url IN (SELECT story_url FROM urls_usuario)"
            pais_clause = ""
        else:
            notas_usuario_cte = ""
            note_filter = ""
        
        query = f"""
            WITH {notas_usuario_cte}
            urls_filtradas AS (
                SELECT DISTINCT e.story_url
                FROM `{TABLE_EDITORIAL}` e
                {join_authors}
                WHERE e.action_type = 'FIRST_PUBLISH'
                  AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND e.story_url IS NOT NULL AND e.story_url != ''
                  {note_filter} {seccion_clause} {pais_clause}
            )
            SELECT 
                g.dimension_type,
                g.dimension_value,
                SUM(g.visits) as total_visits,
                COUNT(DISTINCT g.article_url) as article_count
            FROM `{TABLE_GEO_SOURCES}` g
            INNER JOIN urls_filtradas u ON g.article_url = u.story_url
            WHERE g.event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY g.dimension_type, g.dimension_value
            HAVING total_visits > 0
            ORDER BY total_visits DESC
            LIMIT 100
        """
    else:
        query = f"""
            SELECT 
                dimension_type,
                dimension_value,
                SUM(visits) as total_visits,
                COUNT(DISTINCT article_url) as article_count
            FROM `{TABLE_GEO_SOURCES}`
            WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY dimension_type, dimension_value
            HAVING total_visits > 0
            ORDER BY total_visits DESC
            LIMIT 100
        """
    
    try:
        return _client.query(query).to_dataframe()
    except:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_top_articles(_client, start_date: str, end_date: str, limit: int = 100, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> pd.DataFrame:
    """
    Carga top artículos. Usa lógica de PRIMER_SAVE como creador.
    """
    seccion_clause = f"AND g.section = '{seccion_filter}'" if seccion_filter else ""
    pais_clause = f"AND UPPER(a.country) = UPPER('{pais_filter}')" if pais_filter else ""
    join_authors = f"LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(g.creator_email) = LOWER(a.email)" if pais_filter else ""
    
    if email_filter:
        # Query especial para email_filter: muestra TODAS las notas del usuario (incluso sin tráfico)
        query = f"""
            WITH notas_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}' AND action_type = 'CREATE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_publish AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}' AND action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            primer_save AS (
                SELECT note_id, email_editor, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_con_create AS (SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'),
            notas_primer_save AS (
                SELECT ps.note_id FROM primer_save ps
                WHERE ps.rn = 1 AND ps.email_editor = '{email_filter}'
                  AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
            ),
            todas_notas_usuario AS (
                SELECT note_id FROM notas_create UNION DISTINCT
                SELECT note_id FROM notas_publish UNION DISTINCT
                SELECT note_id FROM notas_primer_save
            ),
            notas_publicadas AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'FIRST_PUBLISH' AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_usuario_pub AS (
                SELECT DISTINCT t.note_id FROM todas_notas_usuario t
                INNER JOIN notas_publicadas p ON t.note_id = p.note_id
            ),
            urls_notas AS (
                SELECT DISTINCT e.note_id, e.story_url FROM `{TABLE_EDITORIAL}` e
                WHERE e.note_id IN (SELECT note_id FROM notas_usuario_pub)
                  AND e.story_url IS NOT NULL
            ),
            -- Calcular el creador real: CREATE si existe, si no PRIMER_SAVE
            creadores_create AS (
                SELECT note_id, email_editor as creador_email
                FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'CREATE'
            ),
            primer_save_all AS (
                SELECT note_id, email_editor, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
            ),
            creadores_primer_save AS (
                SELECT ps.note_id, ps.email_editor as creador_email
                FROM primer_save_all ps
                WHERE ps.rn = 1 AND ps.note_id NOT IN (SELECT note_id FROM creadores_create)
            ),
            creadores_reales AS (
                SELECT note_id, creador_email FROM creadores_create
                UNION ALL
                SELECT note_id, creador_email FROM creadores_primer_save
            ),
            metadata_notas AS (
                SELECT 
                    u.note_id,
                    u.story_url as url,
                    ANY_VALUE(e.title) as titulo,
                    ANY_VALUE(e.segment) as seccion,
                    ANY_VALUE(cr.creador_email) as creador,
                    ANY_VALUE(CASE WHEN e.action_type = 'FIRST_PUBLISH' THEN e.email_editor END) as publicador,
                    ANY_VALUE(e.source) as fuente_produccion,
                    ANY_VALUE(e.title_word_count) as palabras_titulo,
                    ANY_VALUE(e.body_word_count) as palabras_body
                FROM urls_notas u
                LEFT JOIN `{TABLE_EDITORIAL}` e ON u.note_id = e.note_id
                LEFT JOIN creadores_reales cr ON u.note_id = cr.note_id
                GROUP BY u.note_id, u.story_url
            ),
            trafico AS (
                SELECT
                    g.article_url as url,
                    SUM(g.visits) as visitas,
                    SUM(g.pageviews) as pageviews,
                    SAFE_DIVIDE(SUM(g.scrolls), SUM(g.visits)) as scroll_promedio
                FROM `{TABLE_PRODUCTIVITY}` g
                WHERE g.date BETWEEN '{start_date}' AND '{end_date}'
                  AND g.article_url IN (SELECT story_url FROM urls_notas)
                GROUP BY g.article_url
            )
            SELECT
                m.url,
                m.titulo,
                m.seccion,
                COALESCE(m.creador, '-') as creador,
                COALESCE(m.publicador, '-') as publicador,
                CASE 
                    WHEN LOWER(COALESCE(m.fuente_produccion, '')) LIKE '%scribnews%' THEN 'Scribnews'
                    WHEN LOWER(COALESCE(m.fuente_produccion, '')) LIKE '%composer%' THEN 'Composer'
                    ELSE COALESCE(m.fuente_produccion, '-')
                END as fuente,
                COALESCE(t.visitas, 0) as visitas,
                COALESCE(t.pageviews, 0) as pageviews,
                ROUND(COALESCE(t.scroll_promedio, 0), 2) as scroll_promedio,
                COALESCE(m.palabras_titulo, 0) as palabras_titulo,
                COALESCE(m.palabras_body, 0) as palabras_body
            FROM metadata_notas m
            LEFT JOIN trafico t ON m.url = t.url
            WHERE m.titulo IS NOT NULL AND m.titulo != ''
            ORDER BY COALESCE(t.visitas, 0) DESC
            LIMIT {limit}
        """
    else:
        query = f"""
            WITH trafico AS (
                SELECT
                    g.article_url as url,
                    MAX(g.article_title) as titulo,
                    MAX(g.section) as seccion,
                    MAX(g.creator_email) as creador,
                    MAX(g.production_source) as fuente_produccion,
                    SUM(g.visits) as visitas,
                    SUM(g.pageviews) as pageviews,
                    SAFE_DIVIDE(SUM(g.scrolls), SUM(g.visits)) as scroll_promedio
                FROM `{TABLE_PRODUCTIVITY}` g
                {join_authors}
                WHERE g.date BETWEEN '{start_date}' AND '{end_date}'
                  AND DATE(g.publish_date) BETWEEN '{start_date}' AND '{end_date}'
                  {seccion_clause} {pais_clause}
                GROUP BY g.article_url
            ),
            publicadores AS (
                SELECT 
                    story_url as url,
                    ANY_VALUE(email_editor) as publicador,
                    ANY_VALUE(title_word_count) as palabras_titulo,
                    ANY_VALUE(body_word_count) as palabras_body
                FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY story_url
            )
            SELECT
                t.url,
                t.titulo,
                t.seccion,
                COALESCE(t.creador, '-') as creador,
                COALESCE(p.publicador, '-') as publicador,
                CASE 
                    WHEN LOWER(t.fuente_produccion) LIKE '%scribnews%' THEN 'Scribnews'
                    WHEN LOWER(t.fuente_produccion) LIKE '%composer%' THEN 'Composer'
                    ELSE COALESCE(t.fuente_produccion, '-')
                END as fuente,
                t.visitas,
                t.pageviews,
                ROUND(COALESCE(t.scroll_promedio, 0), 2) as scroll_promedio,
                COALESCE(p.palabras_titulo, 0) as palabras_titulo,
                COALESCE(p.palabras_body, 0) as palabras_body
            FROM trafico t
            LEFT JOIN publicadores p ON t.url = p.url
            WHERE t.titulo IS NOT NULL 
              AND t.titulo != ''
              AND LOWER(t.titulo) NOT LIKE '%hacemos periodismo%'
              AND LOWER(t.titulo) NOT LIKE '%infobae américa - infobae%'
              AND LOWER(t.titulo) NOT LIKE '%infobae america - infobae%'
              AND LOWER(t.url) NOT LIKE '%/homepage%'
              AND t.seccion IS NOT NULL
            ORDER BY t.visitas DESC
            LIMIT {limit}
        """
    try:
        return _client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error cargando artículos: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def get_last_data_date(_client) -> tuple:
    """
    Obtiene la última fecha con datos disponibles en la tabla Gold.
    Retorna (fecha_formateada, objeto_date) o (None, None) si no hay datos.
    """
    query = f"""
        SELECT MAX(date) as ultima_fecha
        FROM `{TABLE_PRODUCTIVITY}`
    """
    try:
        df = _client.query(query).to_dataframe()
        if not df.empty and df.iloc[0]['ultima_fecha'] is not None:
            fecha = df.iloc[0]['ultima_fecha']
            # Convertir a date si es datetime
            if hasattr(fecha, 'date'):
                fecha_date = fecha.date()
            else:
                fecha_date = fecha
            return (fecha_date.strftime('%d/%m/%Y'), fecha_date)
    except:
        pass
    return (None, None)


@st.cache_data(ttl=3600, show_spinner=False)
def load_filter_options(_client, start_date: str, end_date: str) -> dict:
    """Carga opciones para filtros (emails de creadores/publicadores y secciones)."""
    # Emails de creadores y publicadores con nombres (LEFT JOIN con autores)
    query_emails = f"""
        SELECT DISTINCT 
            e.email_editor,
            COALESCE(a.complete_name, e.email_editor) as display_name,
            a.country
        FROM `{TABLE_EDITORIAL}` e
        LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)
        WHERE e.action_type IN ('CREATE', 'FIRST_PUBLISH')
          AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
          AND e.email_editor IS NOT NULL AND e.email_editor != ''
        ORDER BY display_name
    """
    
    # Secciones desde arc_editorial_activity
    query_secciones = f"""
        SELECT DISTINCT segment
        FROM `{TABLE_EDITORIAL}`
        WHERE action_type = 'FIRST_PUBLISH'
          AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
          AND segment IS NOT NULL AND segment != ''
        ORDER BY segment
    """
    
    # Países únicos desde la tabla de autores
    query_paises = f"""
        SELECT DISTINCT a.country
        FROM `{TABLE_AUTHORS}` a
        INNER JOIN (
            SELECT DISTINCT email_editor
            FROM `{TABLE_EDITORIAL}`
            WHERE action_type IN ('CREATE', 'FIRST_PUBLISH')
              AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
              AND email_editor IS NOT NULL AND email_editor != ''
        ) e ON LOWER(a.email) = LOWER(e.email_editor)
        WHERE a.country IS NOT NULL AND a.country != ''
        ORDER BY a.country
    """
    
    try:
        df_emails = _client.query(query_emails).to_dataframe()
        # Crear diccionario {display_name: email} para el dropdown
        email_options = {}
        for _, row in df_emails.iterrows():
            email = row['email_editor']
            display = row['display_name']
            if email and display:
                email_options[display] = email
        # Ordenar por nombre
        email_options = dict(sorted(email_options.items()))
    except:
        email_options = {}
    
    try:
        df_secciones = _client.query(query_secciones).to_dataframe()
        secciones = sorted([s for s in df_secciones['segment'].dropna().unique() if s.strip()])
    except:
        secciones = []
    
    try:
        df_paises = _client.query(query_paises).to_dataframe()
        paises = sorted([p for p in df_paises['country'].dropna().unique() if p.strip()])
    except:
        paises = []
    
    return {'email_options': email_options, 'secciones': secciones, 'paises': paises}


@st.cache_data(ttl=3600, show_spinner=False)
def load_source_efficiency(_client, start_date: str, end_date: str, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None) -> pd.DataFrame:
    """
    Carga métricas de eficiencia por fuente de producción.
    Usa lógica de PRIMER_SAVE como creador.
    """
    seccion_clause = f"AND e.segment = '{seccion_filter}'" if seccion_filter else ""
    pais_clause = f"AND UPPER(a.country) = UPPER('{pais_filter}')" if pais_filter else ""
    join_authors = f"LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)" if pais_filter else ""
    
    if email_filter:
        email_cte = f"""
            notas_create AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}' AND action_type = 'CREATE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_publish AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE email_editor = '{email_filter}' AND action_type = 'FIRST_PUBLISH'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            primer_save AS (
                SELECT note_id, email_editor, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
                  AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_con_create AS (SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'),
            notas_primer_save AS (
                SELECT ps.note_id FROM primer_save ps
                WHERE ps.rn = 1 AND ps.email_editor = '{email_filter}'
                  AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
            ),
            todas_notas_usuario AS (
                SELECT note_id FROM notas_create UNION DISTINCT
                SELECT note_id FROM notas_publish UNION DISTINCT
                SELECT note_id FROM notas_primer_save
            ),
            notas_publicadas AS (
                SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                WHERE action_type = 'FIRST_PUBLISH' AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
            ),
            notas_usuario_publicadas AS (
                SELECT t.note_id FROM todas_notas_usuario t
                INNER JOIN notas_publicadas p ON t.note_id = p.note_id
            ),
        """
        email_filter_clause = "AND e.note_id IN (SELECT note_id FROM notas_usuario_publicadas)"
    else:
        email_cte = ""
        email_filter_clause = ""
    
    # Cuando hay email_filter, usar Silver directamente para mejor coincidencia de URLs
    if email_filter:
        query = f"""
            WITH {email_cte}
            notas_por_fuente AS (
                SELECT 
                    e.note_id,
                    e.story_url,
                    CASE 
                        WHEN LOWER(e.email_editor) = 'infobae' THEN 'Agencias'
                        WHEN LOWER(COALESCE(e.source, '')) LIKE '%scribnews%' THEN 'Scribnews'
                        WHEN LOWER(COALESCE(e.source, '')) LIKE '%composer%' THEN 'Composer'
                        ELSE 'Otros'
                    END as fuente
                FROM `{TABLE_EDITORIAL}` e
                WHERE e.action_type = 'FIRST_PUBLISH'
                  AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  {email_filter_clause} {seccion_clause}
            ),
            metricas AS (
                SELECT 
                    n.fuente,
                    COUNT(DISTINCT n.note_id) as notas,
                    SUM(g.daily_sessions) as sesiones,
                    SUM(g.daily_pageviews) as pageviews,
                    SAFE_DIVIDE(SUM(g.sessions_with_scroll), SUM(g.daily_sessions)) as scroll_promedio,
                    SAFE_DIVIDE(SUM(g.total_engagement_seconds), SUM(g.daily_sessions)) / 60 as tiempo_promedio_min
                FROM notas_por_fuente n
                LEFT JOIN `{TABLE_PRODUCTIVITY_SILVER}` g ON n.story_url = g.article_url
                    AND g.event_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY n.fuente
            )
            SELECT 
                fuente,
                notas,
                COALESCE(sesiones, 0) as sesiones,
                COALESCE(pageviews, 0) as pageviews,
                SAFE_DIVIDE(COALESCE(sesiones, 0), notas) as sesiones_por_nota,
                COALESCE(scroll_promedio, 0) as scroll_promedio,
                COALESCE(tiempo_promedio_min, 0) as tiempo_promedio_min
            FROM metricas
            WHERE fuente != 'Otros'
            ORDER BY sesiones DESC
        """
    else:
        query = f"""
            WITH notas_por_fuente AS (
                SELECT 
                    e.note_id,
                    e.story_url,
                    CASE 
                        WHEN LOWER(e.email_editor) = 'infobae' THEN 'Agencias'
                        WHEN LOWER(COALESCE(e.source, '')) LIKE '%scribnews%' THEN 'Scribnews'
                        WHEN LOWER(COALESCE(e.source, '')) LIKE '%composer%' THEN 'Composer'
                        ELSE 'Otros'
                    END as fuente
                FROM `{TABLE_EDITORIAL}` e
                {join_authors}
                WHERE e.action_type = 'FIRST_PUBLISH'
                  AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  {seccion_clause} {pais_clause}
            ),
            metricas AS (
                SELECT 
                    n.fuente,
                    COUNT(DISTINCT n.note_id) as notas,
                    SUM(g.visits) as sesiones,
                    SUM(g.pageviews) as pageviews,
                    SAFE_DIVIDE(SUM(g.scrolls), SUM(g.visits)) as scroll_promedio,
                    SAFE_DIVIDE(SUM(g.total_time_seconds), SUM(g.visits)) / 60 as tiempo_promedio_min
                FROM notas_por_fuente n
                LEFT JOIN `{TABLE_PRODUCTIVITY}` g ON n.story_url = g.article_url
                    AND g.date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY n.fuente
            )
            SELECT 
                fuente,
                notas,
                COALESCE(sesiones, 0) as sesiones,
                COALESCE(pageviews, 0) as pageviews,
                SAFE_DIVIDE(COALESCE(sesiones, 0), notas) as sesiones_por_nota,
                COALESCE(scroll_promedio, 0) as scroll_promedio,
                COALESCE(tiempo_promedio_min, 0) as tiempo_promedio_min
            FROM metricas
            WHERE fuente != 'Otros'
            ORDER BY sesiones DESC
        """
    try:
        return _client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error cargando eficiencia por fuente: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def load_author_productivity(_client, start_date: str, end_date: str, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None, metric_type: str = "created") -> pd.DataFrame:
    """
    Carga datos de productividad por autor para scatter plot.
    
    Con filtro de email:
    - created: muestra al usuario con sus notas creadas (CREATE + PRIMER_SAVE)
    - published: muestra QUIÉN PUBLICÓ las notas del usuario
    - participated: muestra al usuario con todas sus notas
    """
    seccion_clause = f"AND e.segment = '{seccion_filter}'" if seccion_filter else ""
    pais_clause = f"AND UPPER(a.country) = UPPER('{pais_filter}')" if pais_filter else ""
    min_notas = 1 if email_filter else 3
    
    if metric_type == "created":
        action_types = "('CREATE')"
    elif metric_type == "published":
        action_types = "('FIRST_PUBLISH')"
    else:
        action_types = "('CREATE', 'FIRST_PUBLISH')"
    
    if email_filter:
        if metric_type == "published":
            # Mostrar QUIÉN PUBLICÓ las notas del usuario
            query = f"""
                WITH notas_create AS (
                    SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'CREATE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                notas_publish AS (
                    SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'FIRST_PUBLISH'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                primer_save AS (
                    SELECT note_id, email_editor, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                    FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                notas_con_create AS (SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'),
                notas_primer_save AS (
                    SELECT ps.note_id FROM primer_save ps
                    WHERE ps.rn = 1 AND ps.email_editor = '{email_filter}'
                      AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
                ),
                todas_notas_usuario AS (
                    SELECT note_id FROM notas_create UNION DISTINCT
                    SELECT note_id FROM notas_publish UNION DISTINCT
                    SELECT note_id FROM notas_primer_save
                ),
                -- Publicadores de las notas del usuario con sus métricas
                publicadores AS (
                    SELECT 
                        e.email_editor,
                        COALESCE(a.complete_name, e.email_editor) as autor,
                        COALESCE(a.country, 'Desconocido') as pais,
                        COUNT(DISTINCT e.note_id) as notas
                    FROM `{TABLE_EDITORIAL}` e
                    LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)
                    WHERE e.action_type = 'FIRST_PUBLISH'
                      AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                      AND e.note_id IN (SELECT note_id FROM todas_notas_usuario)
                      AND e.email_editor IS NOT NULL AND e.email_editor != ''
                      AND LOWER(e.email_editor) != 'infobae'
                    GROUP BY e.email_editor, autor, pais
                ),
                notas_publicador AS (
                    SELECT DISTINCT e.email_editor, e.story_url
                    FROM `{TABLE_EDITORIAL}` e
                    WHERE e.action_type = 'FIRST_PUBLISH'
                      AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                      AND e.note_id IN (SELECT note_id FROM todas_notas_usuario)
                      AND e.story_url IS NOT NULL
                ),
                trafico_por_publicador AS (
                    SELECT 
                        np.email_editor,
                        SUM(g.daily_sessions) as sesiones_totales,
                        SAFE_DIVIDE(SUM(g.sessions_with_scroll), SUM(g.daily_sessions)) as scroll_promedio
                    FROM notas_publicador np
                    INNER JOIN `{TABLE_PRODUCTIVITY_SILVER}` g ON np.story_url = g.article_url
                    WHERE g.event_date BETWEEN '{start_date}' AND '{end_date}'
                    GROUP BY np.email_editor
                )
                SELECT 
                    p.autor,
                    p.pais,
                    p.notas as notas_creadas,
                    COALESCE(t.sesiones_totales, 0) as sesiones_totales,
                    COALESCE(t.scroll_promedio, 0) as scroll_promedio,
                    SAFE_DIVIDE(COALESCE(t.sesiones_totales, 0), p.notas) as eficiencia
                FROM publicadores p
                LEFT JOIN trafico_por_publicador t ON p.email_editor = t.email_editor
                ORDER BY sesiones_totales DESC
                LIMIT 100
            """
        else:
            # created o participated: mostrar al usuario con sus notas
            query = f"""
                WITH notas_create AS (
                    SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'CREATE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                notas_publish AS (
                    SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                    WHERE email_editor = '{email_filter}' AND action_type = 'FIRST_PUBLISH'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                primer_save AS (
                    SELECT note_id, email_editor, ROW_NUMBER() OVER (PARTITION BY note_id ORDER BY event_timestamp) as rn
                    FROM `{TABLE_EDITORIAL}` WHERE action_type = 'SAVE'
                      AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                notas_con_create AS (SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}` WHERE action_type = 'CREATE'),
                notas_primer_save AS (
                    SELECT ps.note_id FROM primer_save ps
                    WHERE ps.rn = 1 AND ps.email_editor = '{email_filter}'
                      AND ps.note_id NOT IN (SELECT note_id FROM notas_con_create)
                ),
                todas_notas_usuario AS (
                    SELECT note_id FROM notas_create UNION DISTINCT
                    SELECT note_id FROM notas_publish UNION DISTINCT
                    SELECT note_id FROM notas_primer_save
                ),
                notas_publicadas AS (
                    SELECT DISTINCT note_id FROM `{TABLE_EDITORIAL}`
                    WHERE action_type = 'FIRST_PUBLISH' AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ),
                notas_usuario_pub AS (
                    SELECT DISTINCT t.note_id FROM todas_notas_usuario t
                    INNER JOIN notas_publicadas p ON t.note_id = p.note_id
                ),
                urls_usuario AS (
                    SELECT DISTINCT e.story_url FROM `{TABLE_EDITORIAL}` e
                    WHERE e.note_id IN (SELECT note_id FROM notas_usuario_pub)
                      AND e.story_url IS NOT NULL
                ),
                info_autor AS (
                    SELECT 
                        COALESCE(a.complete_name, '{email_filter}') as autor,
                        COALESCE(a.country, 'Desconocido') as pais
                    FROM `{TABLE_AUTHORS}` a
                    WHERE LOWER(a.email) = LOWER('{email_filter}')
                ),
                trafico AS (
                    SELECT 
                        SUM(g.daily_sessions) as sesiones_totales,
                        SAFE_DIVIDE(SUM(g.sessions_with_scroll), SUM(g.daily_sessions)) as scroll_promedio
                    FROM `{TABLE_PRODUCTIVITY_SILVER}` g
                    WHERE g.article_url IN (SELECT story_url FROM urls_usuario)
                      AND g.event_date BETWEEN '{start_date}' AND '{end_date}'
                )
                SELECT 
                    COALESCE((SELECT autor FROM info_autor LIMIT 1), '{email_filter}') as autor,
                    COALESCE((SELECT pais FROM info_autor LIMIT 1), 'Desconocido') as pais,
                    (SELECT COUNT(*) FROM notas_usuario_pub) as notas_creadas,
                    COALESCE((SELECT sesiones_totales FROM trafico), 0) as sesiones_totales,
                    COALESCE((SELECT scroll_promedio FROM trafico), 0) as scroll_promedio,
                    SAFE_DIVIDE(COALESCE((SELECT sesiones_totales FROM trafico), 0), (SELECT COUNT(*) FROM notas_usuario_pub)) as eficiencia
            """
    else:
        query = f"""
            WITH autores AS (
                SELECT 
                    e.email_editor,
                    COALESCE(a.complete_name, e.email_editor) as autor,
                    COALESCE(a.country, 'Desconocido') as pais,
                    COUNT(DISTINCT e.note_id) as notas
                FROM `{TABLE_EDITORIAL}` e
                LEFT JOIN `{TABLE_AUTHORS}` a ON LOWER(e.email_editor) = LOWER(a.email)
                WHERE e.action_type IN {action_types}
                  AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND e.email_editor IS NOT NULL 
                  AND e.email_editor != ''
                  AND LOWER(e.email_editor) != 'infobae'
                  {seccion_clause} {pais_clause}
                GROUP BY e.email_editor, autor, pais
                HAVING notas >= {min_notas}
            ),
            notas_autor AS (
                SELECT DISTINCT 
                    e.email_editor,
                    e.story_url
                FROM `{TABLE_EDITORIAL}` e
                WHERE e.action_type IN {action_types}
                  AND DATE(e.event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
                  AND e.story_url IS NOT NULL
            ),
            trafico_por_autor AS (
                SELECT 
                    na.email_editor,
                    SUM(g.visits) as sesiones_totales,
                    SAFE_DIVIDE(SUM(g.scrolls), SUM(g.visits)) as scroll_promedio
                FROM notas_autor na
                INNER JOIN `{TABLE_PRODUCTIVITY}` g ON na.story_url = g.article_url
                WHERE g.date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY na.email_editor
            )
            SELECT 
                a.autor,
                a.pais,
                a.notas as notas_creadas,
                COALESCE(t.sesiones_totales, 0) as sesiones_totales,
                COALESCE(t.scroll_promedio, 0) as scroll_promedio,
                SAFE_DIVIDE(COALESCE(t.sesiones_totales, 0), a.notas) as eficiencia
            FROM autores a
            LEFT JOIN trafico_por_autor t ON a.email_editor = t.email_editor
            WHERE COALESCE(t.sesiones_totales, 0) > 0
            ORDER BY sesiones_totales DESC
            LIMIT 100
        """
    try:
        return _client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error cargando productividad por autor: {e}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE RENDERIZADO
# ═══════════════════════════════════════════════════════════════════════════════

def format_number(num: float) -> str:
    """Formatea números grandes de forma legible."""
    if pd.isna(num) or num is None:
        return "0"
    if num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    else:
        return f"{num:,.0f}"


def calculate_delta(current: float, previous: float) -> tuple:
    """Calcula el delta porcentual entre dos valores."""
    if previous == 0 or pd.isna(previous):
        return (0, "neutral")
    delta = ((current - previous) / previous) * 100
    direction = "positive" if delta >= 0 else "negative"
    return (delta, direction)


def render_kpi_card(label: str, value: str, delta: float = None, delta_dir: str = None, highlight: bool = False) -> str:
    """Genera HTML para una tarjeta KPI."""
    highlight_class = "highlight" if highlight else ""
    delta_html = ""
    if delta is not None and delta != 0:
        arrow = "↑" if delta_dir == "positive" else "↓" if delta_dir == "negative" else "→"
        delta_html = f'<div class="kpi-delta {delta_dir}">{arrow} {abs(delta):.1f}%</div>'
    
    return f"""
        <div class="kpi-card {highlight_class}">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            {delta_html}
        </div>
    """


def render_kpis(kpis: dict, prev_kpis: dict):
    """Renderiza la sección de KPIs principales."""
    
    # Fila superior - Volumen
    st.markdown("##### 📊 Volumen de Producción")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.markdown(render_kpi_card(
            "Creadores", 
            f"{kpis['creadores_activos']:,}"
        ), unsafe_allow_html=True)
    
    with col2:
        st.markdown(render_kpi_card(
            "Publicadores", 
            f"{kpis['publicadores_activos']:,}"
        ), unsafe_allow_html=True)
    
    with col3:
        st.markdown(render_kpi_card(
            "Notas Publicadas", 
            f"{kpis['notas_publicadas']:,}"
        ), unsafe_allow_html=True)
    
    with col4:
        st.markdown(render_kpi_card(
            "Sesiones Únicas", 
            format_number(kpis['visitas_totales'])
        ), unsafe_allow_html=True)
    
    with col5:
        st.markdown(render_kpi_card(
            "Usuarios Únicos", 
            format_number(kpis.get('usuarios_unicos', 0))
        ), unsafe_allow_html=True)
    
    with col6:
        st.markdown(render_kpi_card(
            "Tiempo Promedio", 
            f"{kpis['tiempo_promedio_min']:.1f} min"
        ), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Fila inferior - Eficiencia
    st.markdown("##### ⚡ Indicadores de Eficiencia")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(render_kpi_card(
            "Sesiones/Nota", 
            format_number(kpis['productividad']),
            highlight=True
        ), unsafe_allow_html=True)
    
    with col2:
        st.markdown(render_kpi_card(
            "Tasa de Scroll", 
            f"{kpis['scroll_promedio']:.2%}"
        ), unsafe_allow_html=True)
    
    with col3:
        st.markdown(render_kpi_card(
            "Pageviews", 
            format_number(kpis['pageviews_totales'])
        ), unsafe_allow_html=True)
    
    with col4:
        st.markdown(render_kpi_card(
            "Sesiones c/ Scroll", 
            format_number(kpis['scrolls_totales'])
        ), unsafe_allow_html=True)
    
    # Leyenda explicativa
    with st.expander("ℹ️ ¿Qué significa cada métrica?", expanded=False):
        st.markdown("""
        **Volumen de Producción:**
        - **Creadores**: Emails únicos que crearon notas en el período (si hay filtro de autor, muestra solo ese creador)
        - **Publicadores**: Emails únicos que publicaron notas (si hay filtro de autor, muestra quiénes publicaron las notas de ese creador)
        - **Notas Publicadas**: Total de publicaciones en el período
        - **Sesiones Únicas**: Cantidad de sesiones (1 usuario puede tener múltiples sesiones en el día)
        - **Usuarios Únicos**: Cantidad de personas distintas que visitaron las notas
        - **Tiempo Promedio**: Tiempo de lectura promedio por sesión
        
        **Indicadores de Eficiencia:**
        - **Sesiones/Nota**: Promedio de sesiones generadas por cada nota publicada
        - **Tasa de Scroll**: % de sesiones donde el usuario hizo scroll significativo
        - **Pageviews**: Total de páginas vistas (incluye recargas y navegación interna)
        - **Sesiones c/ Scroll**: Cantidad de sesiones con scroll significativo
        
        💡 *El scroll se mide cuando el usuario alcanza el 90% del largo del artículo, según el estándar de GA4.*
        """)


def render_impact_zone(top_publishers: pd.DataFrame, top_creators: pd.DataFrame, geo_df: pd.DataFrame):
    """Renderiza la Zona de Impacto: Top Publicadores, Top Creadores y Datos geográficos."""
    
    # Primera fila: Top Publicadores y Top Creadores
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="section-title">📰 Top 10 Publicadores</div>', unsafe_allow_html=True)
        
        if not top_publishers.empty:
            display_df = top_publishers.copy()
            # Renombrar columnas (ahora incluye País)
            display_df.columns = ['Publicador', 'País', 'Notas']
            # Formatear país para mostrar bandera
            pais_flags = {'ARGENTINA': '🇦🇷', 'COLOMBIA': '🇨🇴', 'PERU': '🇵🇪', 'ESPAÑA': '🇪🇸', 'MEXICO': '🇲🇽', 'AMERICA': 'AM'}
            display_df['País'] = display_df['País'].apply(lambda x: pais_flags.get(str(x).upper(), '') if pd.notna(x) else '')
            st.dataframe(display_df, hide_index=True, use_container_width=True, height=350)
        else:
            st.info("No hay datos de publicadores")
    
    with col2:
        st.markdown('<div class="section-title">✍️ Top 10 Creadores</div>', unsafe_allow_html=True)
        
        if not top_creators.empty:
            display_df = top_creators.copy()
            # Renombrar columnas (ahora incluye País)
            display_df.columns = ['Creador', 'País', 'Notas']
            # Formatear país para mostrar bandera
            pais_flags = {'ARGENTINA': '🇦🇷', 'COLOMBIA': '🇨🇴', 'PERU': '🇵🇪', 'ESPAÑA': '🇪🇸', 'MEXICO': '🇲🇽', 'AMERICA': 'AM'}
            display_df['País'] = display_df['País'].apply(lambda x: pais_flags.get(str(x).upper(), '') if pd.notna(x) else '')
            st.dataframe(display_df, hide_index=True, use_container_width=True, height=350)
        else:
            st.info("No hay datos de creadores")
    
    # Segunda fila: Datos geográficos
    st.markdown('<div class="section-title">🌍 ¿Desde dónde nos leen?</div>', unsafe_allow_html=True)
    
    # Traducción de países al español
    country_translations = {
        'Argentina': 'Argentina', 'Mexico': 'México', 'Colombia': 'Colombia', 'Spain': 'España',
        'United States': 'Estados Unidos', 'Peru': 'Perú', 'Chile': 'Chile', 'Ecuador': 'Ecuador',
        'Venezuela': 'Venezuela', 'Bolivia': 'Bolivia', 'Uruguay': 'Uruguay', 'Paraguay': 'Paraguay',
        'Guatemala': 'Guatemala', 'Cuba': 'Cuba', 'Dominican Republic': 'República Dominicana',
        'Honduras': 'Honduras', 'El Salvador': 'El Salvador', 'Nicaragua': 'Nicaragua',
        'Costa Rica': 'Costa Rica', 'Panama': 'Panamá', 'Puerto Rico': 'Puerto Rico',
        'Brazil': 'Brasil', 'Germany': 'Alemania', 'France': 'Francia', 'Italy': 'Italia',
        'United Kingdom': 'Reino Unido', 'Canada': 'Canadá', 'Australia': 'Australia',
        'Japan': 'Japón', 'China': 'China', 'India': 'India', 'Russia': 'Rusia',
        'Portugal': 'Portugal', 'Netherlands': 'Países Bajos', 'Belgium': 'Bélgica',
        'Switzerland': 'Suiza', 'Sweden': 'Suecia', 'Norway': 'Noruega', 'Denmark': 'Dinamarca',
        'Poland': 'Polonia', 'Austria': 'Austria', 'Ireland': 'Irlanda', 'Israel': 'Israel',
        '(not set)': 'No especificado'
    }
    
    if not geo_df.empty and 'dimension_type' in geo_df.columns:
        available_dims = sorted(geo_df['dimension_type'].dropna().unique().tolist())
        
        dim_labels = {
            'geo': '🌍 País', 'source': '🔗 Fuente', 'source_medium': '🔗 Fuente/Medio',
            'medium': '📱 Medio', 'device': '💻 Dispositivo', 'city': '🏙️ Ciudad', 'region': '📍 Región'
        }
        
        if available_dims:
            dim_type = st.selectbox(
                "Ver por:",
                options=available_dims,
                format_func=lambda x: dim_labels.get(x.lower(), x.capitalize()),
                key='geo_dimension'
            )
            
            filtered_geo = geo_df[geo_df['dimension_type'] == dim_type].nlargest(15, 'total_visits').copy()
            
            # Traducir países si es dimensión geográfica
            if dim_type == 'geo':
                filtered_geo['dimension_value'] = filtered_geo['dimension_value'].apply(
                    lambda x: country_translations.get(x, x)
                )
            
            if not filtered_geo.empty:
                # Determinar label según dimensión
                dim_hover_label = dim_labels.get(dim_type.lower(), dim_type.capitalize()).replace('🌍 ', '').replace('🔗 ', '').replace('📱 ', '').replace('💻 ', '').replace('🏙️ ', '').replace('📍 ', '')
                
                fig = px.bar(
                    filtered_geo.sort_values('total_visits', ascending=True),
                    x='total_visits', y='dimension_value', orientation='h',
                    color='total_visits', color_continuous_scale=['#FFE5D4', NARANJA_INFOBAE]
                )
                
                # Hover mejorado con formato claro
                fig.update_traces(
                    hovertemplate='<b>%{y}</b><br><br>' +
                                  '📊 Sesiones: <b>%{x:,.0f}</b><extra></extra>'
                )
                
                fig.update_layout(
                    height=350, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                    showlegend=False, coloraxis_showscale=False,
                    xaxis=dict(gridcolor='#E5E7EB', title='Sesiones', tickformat=',.0f'),
                    yaxis=dict(gridcolor='#E5E7EB', title=''),
                    margin=dict(l=0, r=20, t=20, b=40),
                    hoverlabel=dict(
                        bgcolor="white",
                        font_size=13,
                        font_family="Arial",
                        bordercolor="#E5E7EB"
                    )
                )
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Datos geográficos no disponibles")


def render_temporal_zone(client, start_date: str, end_date: str, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None):
    """Renderiza la Zona Temporal con línea de tendencia."""
    
    st.markdown('<div class="section-title">📈 Evolución Temporal</div>', unsafe_allow_html=True)
    
    # Descripción del gráfico
    st.markdown("""
        <p style="font-size: 0.85rem; color: #6B7280; margin-bottom: 15px;">
            Visualiza cómo evolucionan las métricas día a día. La <b>línea naranja</b> muestra los valores diarios 
            y la <b>línea punteada gris</b> indica la tendencia general del período.
        </p>
    """, unsafe_allow_html=True)
    
    # Opciones con descripciones más claras
    metric_options = {
        'Sesiones Únicas': 'visits', 
        'Usuarios Únicos': 'users',
        'Notas Publicadas': 'notas', 
        'Pageviews': 'pageviews', 
        'Scrolls Completos': 'scrolls'
    }
    
    selected_metric = st.selectbox("Ver evolución de:", options=list(metric_options.keys()), key="temporal_metric")
    
    daily_data = load_daily_evolution(client, start_date, end_date, metric_options[selected_metric], email_filter, seccion_filter, pais_filter)
    
    if daily_data.empty or len(daily_data) < 2:
        st.warning("No hay suficientes datos temporales")
        return
    
    # Gráfico con trendline
    fig = px.scatter(daily_data, x='fecha', y='valor', trendline='ols', trendline_color_override='#999999')
    
    # Hover mejorado con formato más claro en español
    hover_label = selected_metric
    fig.add_trace(go.Scatter(
        x=daily_data['fecha'], y=daily_data['valor'],
        mode='lines+markers', name=selected_metric,
        line=dict(color=NARANJA_INFOBAE, width=3),
        marker=dict(size=10, color=NARANJA_INFOBAE, line=dict(width=2, color='white')),
        hovertemplate='<b>%{x|%d %b %Y}</b><br>' + 
                      '📊 ' + hover_label + ': <b>%{y:,.0f}</b><extra></extra>'
    ))
    
    # Ocultar el scatter original y configurar trendline
    fig.data[0].visible = False
    fig.data[1].line.dash = 'dot'
    fig.data[1].name = 'Tendencia'
    fig.data[1].hovertemplate = '<b>Tendencia</b><extra></extra>'  # Simplificar hover de tendencia
    
    fig.update_layout(
        height=380, 
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        showlegend=True, 
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(gridcolor='#E5E7EB', title='', tickformat='%d %b'),
        yaxis=dict(gridcolor='#E5E7EB', title='', tickformat=',.0f'),
        margin=dict(l=60, r=20, t=40, b=40),
        hovermode='x',
        hoverlabel=dict(
            bgcolor="white",
            font_size=13,
            font_family="Arial",
            bordercolor="#E5E7EB"
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Indicador de tendencia con explicación
    try:
        results = px.get_trendline_results(fig)
        if not results.empty:
            slope = results.iloc[0]["px_fit_results"].params[1]
            if slope > 0:
                trend_text = "📈 Tendencia al ALZA"
                trend_explanation = "Los valores están creciendo en promedio día a día"
                color = "#10B981"
            else:
                trend_text = "📉 Tendencia a la BAJA"
                trend_explanation = "Los valores están decreciendo en promedio día a día"
                color = "#EF4444"
            
            st.markdown(f"""
                <div style='text-align:center; margin-top: 5px;'>
                    <span style='color:{color}; font-weight:600; font-size: 1rem;'>{trend_text}</span>
                    <span style='color: #9CA3AF; font-size: 0.8rem; margin-left: 10px;'>— {trend_explanation}</span>
                </div>
            """, unsafe_allow_html=True)
    except:
        pass


def render_section_analysis(section_stats: pd.DataFrame):
    """Renderiza el Análisis Seccional con métricas completas por sección."""
    
    st.markdown('<div class="section-title">📂 Rendimiento por Sección</div>', unsafe_allow_html=True)
    
    if section_stats.empty:
        st.warning("No hay datos de secciones disponibles")
        return
    
    # Preparar DataFrame para mostrar
    display_df = section_stats.copy()
    
    # Renombrar columnas
    display_df = display_df.rename(columns={
        'seccion': 'Sección',
        'notas': 'Notas',
        'composer': 'Composer',
        'scribnews': 'Scribnews',
        'sesiones': 'Sesiones',
        'pageviews': 'Pageviews',
        'scroll_promedio': 'Scroll',
        'productividad': 'Sesiones/Nota'
    })
    
    # Formatear números
    display_df['Notas'] = display_df['Notas'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "0")
    display_df['Composer'] = display_df['Composer'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "0")
    display_df['Scribnews'] = display_df['Scribnews'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "0")
    display_df['Sesiones'] = display_df['Sesiones'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "0")
    display_df['Pageviews'] = display_df['Pageviews'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "0")
    display_df['Scroll'] = display_df['Scroll'].apply(lambda x: f"{x:.2%}" if pd.notna(x) else "0.00%")
    display_df['Sesiones/Nota'] = display_df['Sesiones/Nota'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) and x > 0 else "-")
    
    st.dataframe(
        display_df, 
        hide_index=True, 
        use_container_width=True, 
        height=450,
        column_config={
            "Sección": st.column_config.TextColumn("Sección", width="medium"),
            "Notas": st.column_config.TextColumn("Notas", help="Total de notas publicadas"),
            "Composer": st.column_config.TextColumn("Composer", help="Notas creadas en Composer"),
            "Scribnews": st.column_config.TextColumn("Scribnews", help="Notas creadas en Scribnews"),
            "Sesiones": st.column_config.TextColumn("Sesiones", help="Visitas únicas totales"),
            "Pageviews": st.column_config.TextColumn("Pageviews", help="Páginas vistas totales"),
            "Scroll": st.column_config.TextColumn("Scroll", help="% promedio de scroll (90% del artículo)"),
            "Sesiones/Nota": st.column_config.TextColumn("Sesiones/Nota", help="Promedio de sesiones por nota publicada")
        }
    )


def render_source_efficiency(source_stats: pd.DataFrame):
    """Renderiza el análisis de eficiencia por fuente de producción."""
    
    st.markdown('<div class="section-title">🏭 Eficiencia por Fuente de Producción</div>', unsafe_allow_html=True)
    
    if source_stats.empty:
        st.warning("No hay datos de fuentes disponibles")
        return
    
    # Configuración de colores e iconos por fuente
    source_config = {
        'Composer': {'color': '#F26522', 'bg': '#FFF7ED', 'icon': '✍️'},
        'Scribnews': {'color': '#3B82F6', 'bg': '#EFF6FF', 'icon': '🤖'},
        'Agencias': {'color': '#10B981', 'bg': '#ECFDF5', 'icon': '📰'}
    }
    
    # Crear tarjetas para cada fuente
    cols = st.columns(len(source_stats))
    
    for idx, (_, row) in enumerate(source_stats.iterrows()):
        fuente = row['fuente']
        config = source_config.get(fuente, {'color': '#6B7280', 'bg': '#F3F4F6', 'icon': '📄'})
        
        notas_fmt = f"{int(row['notas']):,}".replace(",", ".")
        sesiones_fmt = format_number(row['sesiones'])
        eficiencia_fmt = f"{int(row['sesiones_por_nota']):,}".replace(",", ".")
        scroll_fmt = f"{row['scroll_promedio']:.1%}"
        tiempo_fmt = f"{row['tiempo_promedio_min']:.1f}"
        
        with cols[idx]:
            st.markdown(f"""
<div style="background:{config['bg']};padding:20px;border-radius:12px;border-top:4px solid {config['color']};text-align:center;">
<div style="font-size:2.5rem;margin-bottom:8px;">{config['icon']}</div>
<div style="font-size:1.2rem;font-weight:700;color:{config['color']};margin-bottom:15px;">{fuente}</div>
<table style="width:100%;border-collapse:collapse;">
<tr>
<td style="background:white;padding:10px;border-radius:8px;text-align:center;width:50%;">
<div style="font-size:0.7rem;color:#9CA3AF;text-transform:uppercase;">Notas</div>
<div style="font-size:1.3rem;font-weight:700;color:#1F2937;">{notas_fmt}</div>
</td>
<td style="width:8px;"></td>
<td style="background:white;padding:10px;border-radius:8px;text-align:center;width:50%;">
<div style="font-size:0.7rem;color:#9CA3AF;text-transform:uppercase;">Sesiones</div>
<div style="font-size:1.3rem;font-weight:700;color:#1F2937;">{sesiones_fmt}</div>
</td>
</tr>
</table>
<div style="background:white;padding:12px;border-radius:8px;margin-top:10px;">
<div style="font-size:0.7rem;color:#9CA3AF;text-transform:uppercase;">Sesiones/Nota</div>
<div style="font-size:1.8rem;font-weight:800;color:{config['color']};">{eficiencia_fmt}</div>
</div>
<table style="width:100%;border-collapse:collapse;margin-top:10px;">
<tr>
<td style="background:white;padding:8px;border-radius:8px;text-align:center;width:50%;">
<div style="font-size:0.65rem;color:#9CA3AF;text-transform:uppercase;">Scroll</div>
<div style="font-size:1rem;font-weight:600;color:#374151;">{scroll_fmt}</div>
</td>
<td style="width:8px;"></td>
<td style="background:white;padding:8px;border-radius:8px;text-align:center;width:50%;">
<div style="font-size:0.65rem;color:#9CA3AF;text-transform:uppercase;">Tiempo</div>
<div style="font-size:1rem;font-weight:600;color:#374151;">{tiempo_fmt} min</div>
</td>
</tr>
</table>
</div>
            """, unsafe_allow_html=True)
    
    # Gráfico comparativo de barras horizontales
    st.markdown("", unsafe_allow_html=True)
    
    fig = go.Figure()
    
    # Ordenar por eficiencia
    source_stats_sorted = source_stats.sort_values('sesiones_por_nota', ascending=True)
    
    for _, row in source_stats_sorted.iterrows():
        config = source_config.get(row['fuente'], {'color': '#6B7280'})
        fig.add_trace(go.Bar(
            y=[row['fuente']],
            x=[row['sesiones_por_nota']],
            orientation='h',
            marker_color=config['color'],
            text=f"{int(row['sesiones_por_nota']):,} ses/nota",
            textposition='outside',
            name=row['fuente'],
            hovertemplate=f"<b>{row['fuente']}</b><br>Eficiencia: %{{x:,.0f}} ses/nota<extra></extra>"
        ))
    
    fig.update_layout(
        title=dict(text='📊 Comparativa de Eficiencia', font=dict(size=14)),
        showlegend=False,
        height=150,
        margin=dict(l=20, r=100, t=40, b=20),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Arial", size=11),
        xaxis=dict(showgrid=True, gridcolor='#E5E7EB', title=''),
        yaxis=dict(showgrid=False, title=''),
        bargap=0.4
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_author_scatter(_client, start_date: str, end_date: str, email_filter: str = None, seccion_filter: str = None, pais_filter: str = None):
    """Renderiza el scatter plot de productividad por autor con selector de métrica."""
    
    st.markdown('<div class="section-title">👤 Productividad por Autor</div>', unsafe_allow_html=True)
    
    # Selector de tipo de métrica
    col_selector, col_info = st.columns([2, 3])
    with col_selector:
        metric_options = {
            "Notas Creadas": "created",
            "Notas Publicadas": "published", 
            "Total Participación": "participated"
        }
        selected_metric = st.selectbox(
            "Ver por:",
            options=list(metric_options.keys()),
            index=0,
            key="author_metric_selector"
        )
        metric_type = metric_options[selected_metric]
    
    with col_info:
        # Descripciones dinámicas según si hay filtro de email
        if email_filter and metric_type == "published":
            description = "Muestra **quién publicó** las notas del usuario seleccionado"
        elif metric_type == "created":
            description = "Muestra autores según las notas que **crearon**"
        elif metric_type == "published":
            description = "Muestra autores según las notas que **publicaron**"
        else:
            description = "Muestra autores según las notas en las que **crearon o publicaron**"
        
        st.markdown(f"""
            <p style='color: #6B7280; font-size: 0.8rem; margin-top: 25px;'>
                ℹ️ {description}
            </p>
        """, unsafe_allow_html=True)
    
    # Cargar datos con el tipo seleccionado
    author_stats = load_author_productivity(_client, start_date, end_date, email_filter, seccion_filter, pais_filter, metric_type)
    
    if author_stats.empty:
        st.warning("No hay datos de autores disponibles para esta selección")
        return
    
    # Labels dinámicos según la métrica y si hay filtro
    if email_filter and metric_type == "published":
        x_label = "Notas Publicadas (del usuario)"
        point_description = "Cada punto representa un **publicador** de las notas del usuario seleccionado."
    else:
        x_label_map = {
            "created": "Notas Creadas",
            "published": "Notas Publicadas",
            "participated": "Notas (Creadas + Publicadas)"
        }
        x_label = x_label_map[metric_type]
        point_description = "Cada punto representa un autor."
    
    st.markdown(f"""
        <p style='color: #6B7280; font-size: 0.85rem; margin-bottom: 15px;'>
            {point_description} <b>Eje X:</b> {x_label} | <b>Eje Y:</b> Sesiones generadas | 
            <b>Tamaño:</b> Tasa de scroll | <b>Color:</b> País
        </p>
    """, unsafe_allow_html=True)
    
    # Crear scatter plot
    fig = px.scatter(
        author_stats,
        x='notas_creadas',
        y='sesiones_totales',
        size='scroll_promedio',
        color='pais',
        hover_name='autor',
        hover_data={
            'notas_creadas': True,
            'sesiones_totales': ':,.0f',
            'scroll_promedio': ':.2%',
            'eficiencia': ':,.0f',
            'pais': True
        },
        labels={
            'notas_creadas': x_label,
            'sesiones_totales': 'Sesiones Totales',
            'scroll_promedio': 'Scroll',
            'eficiencia': 'Ses/Nota',
            'pais': 'País'
        },
        title=''
    )
    
    # Agregar línea de tendencia
    fig.update_layout(
        height=450,
        margin=dict(l=20, r=20, t=20, b=20),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Arial", size=11),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            title=""
        ),
        xaxis=dict(
            title=x_label,
            gridcolor='#E5E7EB',
            zeroline=False
        ),
        yaxis=dict(
            title="Sesiones Totales",
            gridcolor='#E5E7EB',
            zeroline=False
        )
    )
    
    # Ajustar tamaño de burbujas
    fig.update_traces(
        marker=dict(
            sizemin=5,
            sizeref=2.*max(author_stats['scroll_promedio'].fillna(0))/(40.**2) if not author_stats.empty else 1,
            line=dict(width=1, color='white')
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Insights automáticos
    if not author_stats.empty and len(author_stats) >= 3:
        col1, col2, col3 = st.columns(3)
        
        # Etiqueta dinámica para "más productivo"
        productive_label_map = {
            "created": "notas creadas",
            "published": "notas publicadas",
            "participated": "notas"
        }
        productive_label = productive_label_map[metric_type]
        
        # Autor más eficiente (mayor sesiones/nota)
        top_efficient = author_stats.nlargest(1, 'eficiencia').iloc[0]
        with col1:
            st.markdown(f"""
                <div style='background: #FEF3C7; padding: 12px 15px; border-radius: 8px; border-left: 4px solid #F59E0B;'>
                    <div style='font-size: 0.7rem; color: #92400E; text-transform: uppercase; font-weight: 600;'>🏆 Más Eficiente</div>
                    <div style='font-weight: 700; color: #78350F; font-size: 0.95rem; margin: 5px 0;'>{top_efficient['autor']}</div>
                    <div style='font-size: 0.9rem; color: #92400E; font-weight: 600;'>{int(top_efficient['eficiencia']):,} ses/nota</div>
                </div>
            """, unsafe_allow_html=True)
        
        # Autor más productivo (más notas)
        top_productive = author_stats.nlargest(1, 'notas_creadas').iloc[0]
        with col2:
            st.markdown(f"""
                <div style='background: #DBEAFE; padding: 12px 15px; border-radius: 8px; border-left: 4px solid #3B82F6;'>
                    <div style='font-size: 0.7rem; color: #1E40AF; text-transform: uppercase; font-weight: 600;'>📝 Más Productivo</div>
                    <div style='font-weight: 700; color: #1E3A8A; font-size: 0.95rem; margin: 5px 0;'>{top_productive['autor']}</div>
                    <div style='font-size: 0.9rem; color: #1E40AF; font-weight: 600;'>{int(top_productive['notas_creadas'])} {productive_label}</div>
                </div>
            """, unsafe_allow_html=True)
        
        # Autor con más alcance (más sesiones)
        top_reach = author_stats.nlargest(1, 'sesiones_totales').iloc[0]
        with col3:
            st.markdown(f"""
                <div style='background: #D1FAE5; padding: 12px 15px; border-radius: 8px; border-left: 4px solid #10B981;'>
                    <div style='font-size: 0.7rem; color: #065F46; text-transform: uppercase; font-weight: 600;'>🌍 Mayor Alcance</div>
                    <div style='font-weight: 700; color: #064E3B; font-size: 0.95rem; margin: 5px 0;'>{top_reach['autor']}</div>
                    <div style='font-size: 0.9rem; color: #065F46; font-weight: 600;'>{format_number(top_reach['sesiones_totales'])} sesiones</div>
                </div>
            """, unsafe_allow_html=True)


def render_data_grid(top_articles: pd.DataFrame):
    """Renderiza el Data Grid de auditoría con todas las métricas y links clickeables."""
    
    st.markdown('<div class="section-title">📋 Detalle de Notas</div>', unsafe_allow_html=True)
    
    if top_articles.empty:
        st.warning("No hay datos para mostrar")
        return
    
    # Preparar DataFrame para mostrar
    display_df = top_articles.copy()
    
    # Renombrar columnas
    display_df = display_df.rename(columns={
        'url': 'URL',
        'titulo': 'Título',
        'seccion': 'Sección',
        'creador': 'Creador',
        'publicador': 'Publicador',
        'fuente': 'Fuente',
        'visitas': 'Sesiones',
        'pageviews': 'Pageviews',
        'scroll_promedio': 'Scroll',
        'palabras_titulo': 'Pal. Título',
        'palabras_body': 'Pal. Body'
    })
    
    # Reordenar columnas
    cols_order = ['Título', 'Sección', 'Creador', 'Publicador', 'Fuente', 'Pal. Título', 'Pal. Body', 'Sesiones', 'Pageviews', 'Scroll', 'URL']
    display_df = display_df[[c for c in cols_order if c in display_df.columns]]
    
    # Formatear números con separador de miles
    display_df['Sesiones'] = display_df['Sesiones'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "0")
    display_df['Pageviews'] = display_df['Pageviews'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "0")
    display_df['Scroll'] = display_df['Scroll'].apply(lambda x: f"{x:.2%}" if pd.notna(x) else "0.00%")
    display_df['Pal. Título'] = display_df['Pal. Título'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) and x > 0 else "-")
    display_df['Pal. Body'] = display_df['Pal. Body'].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) and x > 0 else "-")
    
    # Configuración de columnas
    column_config = {
        "Título": st.column_config.TextColumn(
            "Título",
            width="large",
            help="Título del artículo"
        ),
        "URL": st.column_config.LinkColumn(
            "🔗 Ver",
            help="Abrir artículo",
            display_text="Abrir"
        ),
        "Sesiones": st.column_config.TextColumn(
            "Sesiones",
            help="Visitas únicas (una sesión = un usuario en un dispositivo)"
        ),
        "Pageviews": st.column_config.TextColumn(
            "Pageviews",
            help="Total de páginas vistas (incluye recargas)"
        ),
        "Scroll": st.column_config.TextColumn(
            "Scroll",
            help="% de sesiones que llegaron al 90% del artículo (estándar GA4)"
        ),
        "Sección": st.column_config.TextColumn("Sección", width="small"),
        "Creador": st.column_config.TextColumn("Creador", width="medium", help="Email del creador del artículo"),
        "Publicador": st.column_config.TextColumn("Publicador", width="medium", help="Email de quien publicó el artículo"),
        "Fuente": st.column_config.TextColumn("Fuente", width="small", help="Sistema de producción (Composer/Scribnews)"),
        "Pal. Título": st.column_config.TextColumn("Pal. Título", width="small", help="Cantidad de palabras en el título"),
        "Pal. Body": st.column_config.TextColumn("Pal. Body", width="small", help="Cantidad de palabras en el cuerpo del artículo")
    }
    
    st.dataframe(
        display_df, 
        hide_index=True, 
        use_container_width=True, 
        height=500, 
        column_config=column_config
    )
    
    # Botón de descarga
    csv = display_df.to_csv(index=False)
    st.download_button("📥 Descargar CSV", csv, "infobae_editorial_data.csv", "text/csv", key='download-csv')

# ═══════════════════════════════════════════════════════════════════════════════
# APLICACIÓN PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    # Header
    st.markdown('''
        <h1 class="main-header">📰 <span class="orange">INFOBAE</span> Centro de Control Editorial</h1>
        <p class="subtitle">Dashboard de Productividad e Impacto Editorial</p>
    ''', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 🔌 Conexión")
        client = get_bigquery_client()
        
        if client:
            st.markdown('<span class="status-badge status-connected">✓ Conectado a BigQuery</span>', unsafe_allow_html=True)
            
            # Obtener última fecha de datos disponibles
            ultima_fecha_str, ultima_fecha_date = get_last_data_date(client)
            if ultima_fecha_str:
                st.markdown(f"""
                    <div style="background: #FFF7ED; border-left: 3px solid #F26522; padding: 8px 12px; margin-top: 10px; border-radius: 4px;">
                        <span style="font-size: 0.75rem; color: #9A3412;">📅 <b>Datos hasta:</b> {ultima_fecha_str}</span><br>
                        <span style="font-size: 0.65rem; color: #78716C;">Actualización diaria ~13hs (AR)</span>
                    </div>
                """, unsafe_allow_html=True)
            
            # Botón para refrescar caché
            if st.button("🔄 Refrescar datos", help="Limpia el caché y recarga los datos desde BigQuery"):
                st.cache_data.clear()
                st.rerun()
        else:
            st.markdown('<span class="status-badge status-error">✗ Sin conexión</span>', unsafe_allow_html=True)
            st.info("⚙️ Configura las credenciales en Settings → Secrets")
            return
        
        st.markdown("---")
        st.markdown("### 📅 Período de Análisis")
        
        # Por defecto: últimos 7 días según datos disponibles
        if ultima_fecha_date:
            end_date = ultima_fecha_date
        else:
            end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)  # 6 días atrás = 7 días totales
        
        col1, col2 = st.columns(2)
        with col1:
            start = st.date_input("Desde", start_date, key="start_date")
        with col2:
            end = st.date_input("Hasta", end_date, key="end_date")
        
        if start > end:
            st.error("⚠️ La fecha inicial debe ser anterior a la final")
            return
        
        start_str = str(start)
        end_str = str(end)
        
        st.markdown("---")
        st.markdown("### 🎯 Filtros")
        st.caption("💡 Escribí en el dropdown para buscar")
        
        # Cargar opciones de filtros (query ligera)
        filter_options = load_filter_options(client, start_str, end_str)
        
        # Dropdown de Creador/Publicador (muestra nombre, filtra por email)
        email_display_options = ["Todos"] + list(filter_options['email_options'].keys())
        selected_display_name = st.selectbox(
            "Creador/Publicador", 
            options=email_display_options,
            index=0, 
            key="email_filter"
        )
        
        # Filtro por país
        pais_options = ["Todos"] + filter_options['paises']
        selected_pais = st.selectbox(
            "País del autor",
            options=pais_options,
            index=0,
            key="pais_filter"
        )
        
        selected_section = st.selectbox(
            "Sección", 
            options=["Todas"] + filter_options['secciones'],
            index=0, 
            key="section_filter"
        )
        
        st.markdown("---")
        st.markdown(f"**Período:** {(end - start).days + 1} días")
    
    # Preparar filtros para queries
    # Convertir display_name de vuelta a email para el filtro
    if selected_display_name != "Todos":
        email_filter = filter_options['email_options'].get(selected_display_name)
    else:
        email_filter = None
    seccion_filter = selected_section if selected_section != "Todas" else None
    pais_filter = selected_pais if selected_pais != "Todos" else None
    
    # Cargar datos optimizados (con filtros aplicados)
    with st.spinner("🔄 Cargando datos..."):
        kpis = load_kpis(client, start_str, end_str, email_filter, seccion_filter, pais_filter)
        prev_kpis = load_previous_kpis(client, start_str, end_str, email_filter, seccion_filter, pais_filter)
        top_publishers = load_top_publishers(client, start_str, end_str, 10, email_filter, seccion_filter, pais_filter)
        top_creators = load_top_creators(client, start_str, end_str, 10, email_filter, seccion_filter, pais_filter)
        geo_df = load_geo_data(client, start_str, end_str, email_filter, seccion_filter, pais_filter)
        section_stats = load_section_stats(client, start_str, end_str, email_filter, seccion_filter, pais_filter)
        top_articles = load_top_articles(client, start_str, end_str, 100, email_filter, seccion_filter, pais_filter)
        # Nuevas métricas de análisis
        source_efficiency = load_source_efficiency(client, start_str, end_str, email_filter, seccion_filter, pais_filter)
    
    # Renderizar dashboard
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    render_kpis(kpis, prev_kpis)
    
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    render_impact_zone(top_publishers, top_creators, geo_df)
    
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    render_temporal_zone(client, start_str, end_str, email_filter, seccion_filter, pais_filter)
    
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    render_section_analysis(section_stats)
    
    # Nuevas secciones de análisis estratégico
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    render_source_efficiency(source_efficiency)
    
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    render_author_scatter(client, start_str, end_str, email_filter, seccion_filter, pais_filter)
    
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    render_data_grid(top_articles)
    
    # Footer
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.markdown(f"""
        <p style="text-align: center; color: {GRIS_TEXTO}; font-size: 0.8rem;">
            📰 Infobae Centro de Control Editorial | {datetime.now().strftime('%d/%m/%Y %H:%M')}
        </p>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
