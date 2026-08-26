"""
📋 Brief Mensual — informe fijo por mes cerrado del Centro de Control Editorial.

Réplica de la estética del Informe de Audiencias de monitor-tendencias, con los
datos del tablero de Seguimiento de Autores. El HTML es autocontenido: se ve
embebido acá y se puede descargar para compartir.
"""

import streamlit as st
import streamlit.components.v1 as components

from brief.queries import (closed_months, get_bigquery_client, get_last_data_date,
                           load_brief_data, month_label)
from brief.template import render_brief_html

st.set_page_config(
    page_title="Infobae | Brief Mensual de Autores",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* La página se pinta oscura y el brief ocupa todo el ancho, sin chrome de Streamlit */
    .stApp { background-color: #0A0A0C; }
    header[data-testid="stHeader"] { background: transparent; }
    section[data-testid="stSidebar"] { background-color: #141416; }
    section[data-testid="stSidebar"] * { color: #C3C2B7; }
    .block-container { padding: 0 !important; max-width: 100% !important; }
    /* El iframe del brief llena el viewport: un solo scroll, el de adentro */
    .block-container iframe { height: calc(100vh - 4rem) !important; width: 100% !important; }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

client = get_bigquery_client()
if client is None:
    st.error("Sin conexión a BigQuery. Configurá las credenciales en Settings → Secrets.")
    st.stop()

ultima_fecha = get_last_data_date(client)
meses = closed_months(ultima_fecha, limit=12)
if not meses:
    st.warning("Todavía no hay ningún mes cerrado con datos.")
    st.stop()

with st.sidebar:
    st.markdown("### 📋 Brief mensual")
    st.caption("Informe fijo por mes cerrado. Compara siempre contra el mes anterior completo.")
    seleccion = st.selectbox(
        "Mes",
        options=meses,
        format_func=lambda ym: month_label(*ym),
        key="brief_month",
    )

year, month = seleccion
with st.spinner(f"Generando el brief de {month_label(year, month)}… (la primera vez tarda un rato)"):
    data = load_brief_data(client, year, month)
    html = render_brief_html(data)

with st.sidebar:
    st.download_button(
        "⬇️ Descargar HTML",
        data=html.encode("utf-8"),
        file_name=f"brief-autores-{year}-{month:02d}.html",
        mime="text/html",
        help="El archivo es autocontenido: se puede abrir suelto o compartir por mail/Slack.",
        use_container_width=True,
    )
    if st.button("🔄 Regenerar", help="Limpia el caché del brief y vuelve a consultar BigQuery",
                 use_container_width=True):
        load_brief_data.clear()
        st.rerun()
    st.markdown("---")
    if hasattr(st, "page_link"):
        try:
            st.page_link("streamlit_app.py", label="⬅️ Volver al tablero")
        except Exception:
            pass

# La altura real la fija el CSS de arriba (100vh); este valor es solo el fallback.
components.html(html, height=900, scrolling=True)
