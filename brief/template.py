"""
Template HTML del Brief Mensual.

Genera un HTML autocontenido (CSS inline, charts SVG armados en Python, sin
CDNs): el mismo string se muestra embebido en la página de Streamlit y se
descarga como archivo. Estética calcada del Informe de Audiencias de
monitor-tendencias: fondo #0A0A0C, acento #F68E1E, capítulos numerados,
tarjetas KPI con delta contra el mes anterior.
"""

import html as _html

# Paleta (relevada del informe de audiencias)
BG = "#0A0A0C"
PANEL = "#141416"
PANEL2 = "#1A1A1D"
BORDER = "#26262A"
TEXT = "#FFFFFF"
TEXT2 = "#C3C2B7"
MUTED = "#898981"
ACCENT = "#F68E1E"
UP = "#0CA30C"
DOWN = "#D03B3B"

PAIS_FLAGS = {
    'ARGENTINA': '🇦🇷', 'COLOMBIA': '🇨🇴', 'PERU': '🇵🇪', 'ESPAÑA': '🇪🇸',
    'MEXICO': '🇲🇽', 'AMERICA': '🌎', 'CENTROAMERICA': '🌎', 'COLABORADOR': '✍️',
    'EL SALVADOR': '🇸🇻', 'GUATEMALA': '🇬🇹', 'HONDURAS': '🇭🇳', 'PANAMA': '🇵🇦',
    'VENEZUELA': '🇻🇪', 'COSTA RICA': '🇨🇷', 'NICARAGUA': '🇳🇮',
}


def esc(s) -> str:
    return _html.escape(str(s), quote=True)


def flag(pais) -> str:
    return PAIS_FLAGS.get(str(pais).upper(), '')


# ─────────────────────────── formato de números ───────────────────────────

def fmt_int(n) -> str:
    """12345 -> '12.345' (formato es-AR)."""
    try:
        return f"{int(round(n)):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def fmt_dec(n, dec=1) -> str:
    try:
        return f"{n:.{dec}f}".replace(".", ",")
    except (TypeError, ValueError):
        return "0"


def fmt_big(n) -> str:
    """65059686 -> '65,1 M' · 861200 -> '861,2 mil' · 950 -> '950'."""
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "0"
    if abs(n) >= 1_000_000:
        return f"{fmt_dec(n / 1_000_000)} M"
    if abs(n) >= 10_000:
        return f"{fmt_dec(n / 1_000)} mil"
    return fmt_int(n)


def delta_pct(cur, prev):
    """Delta % contra el período anterior, o None si no se puede calcular."""
    if prev in (None, 0) or cur is None:
        return None
    return (cur - prev) / prev * 100


def delta_html(cur, prev, vs_label) -> str:
    d = delta_pct(cur, prev)
    if d is None:
        return f'<span class="delta muted">— <span class="vs">{esc(vs_label)}</span></span>'
    color, arrow = (UP, "▲") if d >= 0 else (DOWN, "▼")
    return (f'<span class="delta" style="color:{color}">{arrow} {fmt_dec(abs(d))} %</span>'
            f' <span class="vs">vs. {esc(vs_label)}</span>')


# ───────────────────────────── componentes ─────────────────────────────

def kpi_card(label, value, delta="", highlight=False) -> str:
    cls = "kpi highlight" if highlight else "kpi"
    return (f'<div class="{cls}"><div class="kpi-label">{esc(label)}</div>'
            f'<div class="kpi-value">{value}</div>'
            f'<div class="kpi-delta">{delta}</div></div>')


def hbar_rows(rows, unidad="notas") -> str:
    """rows: [{'label','flag','value','extra'}] -> barras horizontales estilo informe."""
    if not rows:
        return '<p class="muted">Sin datos para este período.</p>'
    max_v = max(r['value'] for r in rows) or 1
    out = ['<div class="hbars">']
    for r in rows:
        pct = max(2.0, r['value'] / max_v * 100)
        extra = f'<span class="hbar-extra">{r["extra"]}</span>' if r.get('extra') else ''
        fl = f'{r["flag"]} ' if r.get('flag') else ''
        tooltip = f'{r["label"]} · {fmt_int(r["value"])} {unidad}'
        out.append(
            f'<div class="hbar" data-label="{esc(tooltip)}">'
            f'<div class="hbar-label" title="{esc(r["label"])}">{fl}{esc(r["label"])}</div>'
            f'<div class="hbar-track"><div class="hbar-fill" style="width:{pct:.1f}%"></div></div>'
            f'<div class="hbar-value">{fmt_big(r["value"])}</div>{extra}</div>')
    out.append('</div>')
    return "".join(out)


def svg_line_chart(points, width=1060, height=260, color=ACCENT) -> str:
    """points: [(fecha 'YYYY-MM-DD', valor)] -> línea con área, ticks y máximo."""
    if len(points) < 2:
        return '<p class="muted">Sin datos diarios para este período.</p>'
    vals = [v for _, v in points]
    vmax = max(vals) or 1
    pad_l, pad_r, pad_t, pad_b = 56, 16, 18, 34
    iw, ih = width - pad_l - pad_r, height - pad_t - pad_b
    n = len(points)

    def x(i):
        return pad_l + i * iw / (n - 1)

    def y(v):
        return pad_t + ih - (v / vmax) * ih

    line = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, (_, v) in enumerate(points))
    area = f"{pad_l:.1f},{pad_t + ih:.1f} {line} {x(n - 1):.1f},{pad_t + ih:.1f}"

    # Ticks del eje x: ~6 fechas
    ticks = []
    step = max(1, (n - 1) // 5)
    for i in range(0, n, step):
        d = points[i][0]
        ticks.append(f'<text x="{x(i):.0f}" y="{height - 10}" class="tick" text-anchor="middle">{int(d[8:10])}</text>')
    imax = vals.index(max(vals))

    # Columnas invisibles por día: alimentan el tooltip de hover (JS del documento)
    hits = []
    half = iw / (n - 1) / 2
    for i, (d, v) in enumerate(points):
        x0 = max(pad_l, x(i) - half)
        x1 = min(pad_l + iw, x(i) + half)
        hits.append(f'<rect class="hit" x="{x0:.1f}" y="{pad_t}" width="{x1 - x0:.1f}" height="{ih}" '
                    f'data-label="{esc(fecha_es(d))} · {fmt_int(v)}"/>')

    return f"""
    <svg viewBox="0 0 {width} {height}" style="width:100%;height:auto" xmlns="http://www.w3.org/2000/svg" role="img">
      <defs><linearGradient id="area{abs(hash(line)) % 9999}" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="{color}" stop-opacity="0.28"/>
        <stop offset="100%" stop-color="{color}" stop-opacity="0"/>
      </linearGradient></defs>
      <line x1="{pad_l}" y1="{pad_t + ih}" x2="{width - pad_r}" y2="{pad_t + ih}" stroke="{BORDER}" stroke-width="1"/>
      <line x1="{pad_l}" y1="{pad_t}" x2="{width - pad_r}" y2="{pad_t}" stroke="{BORDER}" stroke-width="1" stroke-dasharray="3 4"/>
      <text x="{pad_l - 8}" y="{pad_t + 4}" class="tick" text-anchor="end">{fmt_big(vmax)}</text>
      <text x="{pad_l - 8}" y="{pad_t + ih + 4}" class="tick" text-anchor="end">0</text>
      <polygon points="{area}" fill="url(#area{abs(hash(line)) % 9999})"/>
      <polyline points="{line}" fill="none" stroke="{color}" stroke-width="2.5"
                stroke-linejoin="round" stroke-linecap="round"/>
      <circle cx="{x(imax):.1f}" cy="{y(vals[imax]):.1f}" r="4" fill="{color}"/>
      <text x="{x(imax):.0f}" y="{y(vals[imax]) - 10:.0f}" class="tick" style="fill:{TEXT2}" text-anchor="middle">{fmt_big(vals[imax])}</text>
      {"".join(ticks)}
      {"".join(hits)}
    </svg>"""


def chapter_header(num, kicker, title, measure_note="") -> str:
    note = (f'<div class="measure"><span class="dot"></span>'
            f'<b>Qué se mide acá:</b>&nbsp;{esc(measure_note)}</div>') if measure_note else ''
    return (f'<div class="chapter-kicker"><span>{num} — {esc(kicker)}</span><span class="rule"></span></div>'
            f'<h2 class="chapter-title">{esc(title)}</h2>{note}')


MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
         "agosto", "septiembre", "octubre", "noviembre", "diciembre"]


def fecha_es(iso) -> str:
    return f"{int(iso[8:10])} de {MESES[int(iso[5:7]) - 1]}"


# ─────────────────────────────── documento ───────────────────────────────

def render_brief_html(data: dict) -> str:
    label = data['label']            # "Julio 2026"
    prev_label = data['prev_label']  # "Junio 2026"
    mes_corto = label.split()[0].lower()        # "julio"
    prev_corto = prev_label.split()[0].lower()

    p, pp = data['production'], data['prev_production']
    t, pt = data['traffic'], data['prev_traffic']
    hll = t.get('metodo_unicos') == 'hll'

    # ── Portada ──
    hero_delta = delta_html(p['notas'], pp['notas'], prev_corto)
    usuarios_label = "usuarios únicos alcanzados" if hll else "alcance (suma diaria, puede repetir lectores)"

    resumen_cards = _resumen_cards(data, prev_corto)

    # ── 01 · Pulso ──
    kpis = [
        kpi_card("Notas publicadas", fmt_int(p['notas']), delta_html(p['notas'], pp['notas'], prev_label)),
        kpi_card("Creadores activos", fmt_int(p['creadores']), delta_html(p['creadores'], pp['creadores'], prev_label)),
        kpi_card("Publicadores activos", fmt_int(p['publicadores']), delta_html(p['publicadores'], pp['publicadores'], prev_label)),
        kpi_card("Usuarios únicos" if hll else "Alcance (suma diaria)", fmt_big(t['usuarios_unicos']),
                 delta_html(t['usuarios_unicos'], pt['usuarios_unicos'], prev_label), highlight=True),
        kpi_card("Sesiones únicas" if hll else "Sesiones (suma diaria)", fmt_big(t['sesiones_unicas']),
                 delta_html(t['sesiones_unicas'], pt['sesiones_unicas'], prev_label)),
        kpi_card("Visitas", fmt_big(t['visitas']), delta_html(t['visitas'], pt['visitas'], prev_label)),
        kpi_card("Pageviews", fmt_big(t['pageviews']), delta_html(t['pageviews'], pt['pageviews'], prev_label)),
        kpi_card("Tiempo promedio", f"{fmt_dec(t['tiempo_min'])} min", delta_html(t['tiempo_min'], pt['tiempo_min'], prev_label)),
        kpi_card("Tasa de scroll", f"{fmt_dec(t['scroll_prom'] * 100)} %", delta_html(t['scroll_prom'], pt['scroll_prom'], prev_label)),
        kpi_card("Sesiones por nota", fmt_dec(t['visitas'] / max(p['notas'], 1), 0),
                 delta_html(t['visitas'] / max(p['notas'], 1), pt['visitas'] / max(pp['notas'], 1), prev_label)),
    ]

    daily_notas = data['daily_notas']
    daily_line = svg_line_chart(daily_notas)
    daily_foot = ""
    if daily_notas:
        vals = [v for _, v in daily_notas]
        promedio = sum(vals) / len(vals)
        imax, imin = vals.index(max(vals)), vals.index(min(vals))
        daily_foot = (f"Promedio diario: <b>{fmt_int(promedio)}</b> notas. "
                      f"Día más alto: <b>{fecha_es(daily_notas[imax][0])}</b> ({fmt_int(vals[imax])}). "
                      f"Más bajo: <b>{fecha_es(daily_notas[imin][0])}</b> ({fmt_int(vals[imin])}).")

    visitas_line = svg_line_chart(data['daily_visitas'])

    # ── 02 · Autores ──
    top_creators = hbar_rows([
        {'label': r['nombre'], 'flag': flag(r['pais']), 'value': r['notas']}
        for r in data['top_creators']])
    top_publishers = hbar_rows([
        {'label': r['nombre'], 'flag': flag(r['pais']), 'value': r['notas']}
        for r in data['top_publishers']])

    # ── 03 · Secciones ──
    prev_secs = {s['seccion']: s for s in data['prev_sections']}
    sec_rows = []
    for s in data['sections'][:10]:
        prev_n = prev_secs.get(s['seccion'], {}).get('notas')
        sec_rows.append({'label': s['seccion'], 'value': s['notas'],
                         'extra': delta_html(s['notas'], prev_n, prev_corto)})
    sections_bars = hbar_rows(sec_rows)
    cross_html = _cross_sections(data)

    # ── 04 · País por país ──
    countries_html = _country_cards(data, prev_label)

    # ── 05 · Fuentes ──
    sources_html = _sources_block(data, prev_corto)

    metodo_nota = ("Usuarios y sesiones únicas se calculan con sketches HLL (cada lector cuenta una sola vez en el mes)."
                   if hll else
                   "Este mes no tiene cobertura completa de sketches HLL: usuarios y sesiones son la suma de únicos "
                   "por nota y por día, que puede contar más de una vez al mismo lector.")

    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Infobae · Brief de Autores · {esc(label)}</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:{BG}; color:{TEXT}; font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
         -webkit-font-smoothing:antialiased; }}
  .progress {{ position:fixed; top:0; left:0; height:3px; background:{ACCENT}; width:0; z-index:10; }}
  .wrap {{ max-width:1160px; margin:0 auto; padding:0 48px 80px; }}
  .logo {{ color:{ACCENT}; font-size:2.6rem; font-weight:800; letter-spacing:-1px; padding-top:64px; }}
  .kicker {{ color:{ACCENT}; font-size:.8rem; font-weight:700; letter-spacing:.22em; margin-top:40px;
             text-transform:uppercase; }}
  h1 {{ font-size:clamp(2.6rem, 7vw, 5rem); font-weight:700; line-height:1.05; margin:16px 0 36px;
        letter-spacing:-.02em; max-width:820px; }}
  .hero {{ display:flex; align-items:baseline; gap:18px; flex-wrap:wrap; margin:48px 0 8px; }}
  .hero-num {{ color:{ACCENT}; font-size:clamp(3.4rem, 9vw, 6.4rem); font-weight:800; letter-spacing:-.03em;
               line-height:1; }}
  .hero-side {{ max-width:280px; color:{TEXT2}; font-size:1rem; line-height:1.4; }}
  .delta {{ font-weight:700; font-size:.85rem; }}
  .vs {{ color:{MUTED}; font-weight:400; font-size:.8rem; }}
  .muted {{ color:{MUTED}; }}
  .cards6 {{ display:grid; grid-template-columns:repeat(3, 1fr); gap:16px; margin:56px 0 28px; }}
  .card6 {{ background:{PANEL}; border:1px solid {BORDER}; border-radius:12px; padding:20px 22px; }}
  .card6 .n {{ color:{ACCENT}; font-size:.72rem; font-weight:700; letter-spacing:.18em; margin-bottom:10px; }}
  .card6 p {{ color:{TEXT2}; font-size:.9rem; line-height:1.5; }}
  .card6 b {{ color:{TEXT}; }}
  .scroll-hint {{ text-align:center; color:{MUTED}; font-size:.75rem; letter-spacing:.25em; margin:48px 0 20px;
                  text-transform:uppercase; }}
  .chapter-kicker {{ display:flex; align-items:center; gap:16px; margin-top:110px; color:{ACCENT};
                     font-size:.78rem; font-weight:700; letter-spacing:.2em; text-transform:uppercase; }}
  .chapter-kicker .rule {{ flex:0 0 120px; height:1px; background:linear-gradient(90deg,{ACCENT},transparent); }}
  .chapter-title {{ font-size:clamp(1.9rem, 4.5vw, 3.1rem); font-weight:700; line-height:1.12;
                    letter-spacing:-.02em; margin:14px 0 18px; max-width:820px; }}
  .measure {{ display:inline-flex; align-items:center; gap:8px; border:1px solid {BORDER}; border-radius:999px;
              padding:8px 16px; color:{TEXT2}; font-size:.82rem; margin-bottom:28px; }}
  .measure b {{ color:{ACCENT}; font-weight:600; }}
  .measure .dot {{ width:7px; height:7px; border-radius:50%; background:{ACCENT}; flex:none; }}
  .panel {{ background:{PANEL}; border:1px solid {BORDER}; border-radius:14px; padding:26px 28px; margin:22px 0; }}
  .panel h3 {{ font-size:1.05rem; font-weight:700; margin-bottom:4px; }}
  .panel .sub {{ color:{MUTED}; font-size:.82rem; margin-bottom:18px; }}
  .panel-foot {{ border-top:1px solid {BORDER}; margin-top:18px; padding-top:12px; color:{MUTED};
                 font-size:.8rem; line-height:1.5; }}
  .panel-foot b {{ color:{TEXT2}; }}
  .kpi-grid {{ display:grid; grid-template-columns:repeat(5, 1fr); gap:14px; margin:26px 0; }}
  .kpi {{ background:{PANEL}; border:1px solid {BORDER}; border-radius:12px; padding:16px 18px;
          transition:border-color .15s ease, transform .15s ease; }}
  .kpi:hover {{ border-color:#3E3E45; transform:translateY(-2px); }}
  .kpi.highlight {{ border-color:{ACCENT}; background:linear-gradient(160deg, {PANEL2}, #201709); }}
  .kpi-label {{ color:{TEXT2}; font-size:.78rem; margin-bottom:8px; }}
  .kpi-value {{ font-size:1.7rem; font-weight:800; letter-spacing:-.01em; }}
  .kpi.highlight .kpi-value {{ color:{ACCENT}; }}
  .kpi-delta {{ margin-top:6px; font-size:.8rem; }}
  .two-col {{ display:grid; grid-template-columns:1fr 1fr; gap:22px; }}
  .hbars {{ display:flex; flex-direction:column; gap:10px; }}
  .hbar {{ display:grid; grid-template-columns:200px 1fr 74px auto; gap:12px; align-items:center; }}
  .hbar-label {{ color:{TEXT2}; font-size:.85rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
  .hbar-track {{ background:{PANEL2}; border-radius:4px; height:14px; overflow:hidden; }}
  .hbar-fill {{ background:linear-gradient(90deg, #b35f0e, {ACCENT}); height:100%; border-radius:4px; }}
  .hbar-value {{ font-weight:700; font-size:.88rem; text-align:right; }}
  .hbar-extra {{ font-size:.78rem; white-space:nowrap; }}
  .hbar:hover .hbar-fill {{ filter:brightness(1.3); }}
  .hbar:hover .hbar-label {{ color:{TEXT}; }}
  .tick {{ fill:{MUTED}; font-size:11px; font-family:inherit; }}
  .hit {{ fill:transparent; }}
  .hit:hover {{ fill:rgba(246,142,30,0.10); }}
  .cross-bar div {{ transition:filter .1s ease; }}
  .cross-bar div:hover {{ filter:brightness(1.35); }}
  #tip {{ position:fixed; display:none; background:{PANEL2}; border:1px solid {BORDER}; color:{TEXT};
          border-radius:8px; padding:7px 12px; font-size:.82rem; pointer-events:none; z-index:20;
          box-shadow:0 6px 18px rgba(0,0,0,.5); white-space:nowrap; }}
  .country-grid {{ display:grid; grid-template-columns:repeat(3, 1fr); gap:16px; }}
  .country {{ background:{PANEL}; border:1px solid {BORDER}; border-radius:14px; padding:22px; }}
  .country h4 {{ font-size:1.05rem; margin-bottom:2px; }}
  .country .big {{ color:{ACCENT}; font-size:1.9rem; font-weight:800; margin:10px 0 2px; }}
  .country .row {{ display:flex; justify-content:space-between; color:{TEXT2}; font-size:.84rem;
                   padding:5px 0; border-bottom:1px dashed {BORDER}; }}
  .country .row:last-child {{ border-bottom:none; }}
  .country .top3 {{ margin-top:12px; color:{MUTED}; font-size:.78rem; text-transform:uppercase;
                    letter-spacing:.12em; }}
  .country ol {{ margin:8px 0 0 18px; color:{TEXT2}; font-size:.85rem; line-height:1.6; }}
  .cross {{ display:flex; flex-direction:column; gap:14px; }}
  .cross-row .name {{ font-size:.9rem; font-weight:600; color:{TEXT}; margin-bottom:6px; }}
  .cross-bar {{ display:flex; height:16px; border-radius:4px; overflow:hidden; background:{PANEL2}; }}
  .cross-leg {{ color:{MUTED}; font-size:.78rem; margin-top:6px; }}
  .cross-leg b {{ color:{TEXT2}; font-weight:600; }}
  .closing {{ margin-top:110px; border-top:1px solid {BORDER}; padding-top:40px; }}
  .closing h2 {{ font-size:1.8rem; margin:10px 0 16px; }}
  .closing p {{ color:{TEXT2}; font-size:.95rem; line-height:1.6; max-width:760px; margin-bottom:14px; }}
  .closing a {{ color:{ACCENT}; font-weight:600; text-decoration:none; }}
  .method {{ color:{MUTED}; font-size:.8rem; line-height:1.6; max-width:760px; margin-top:26px; }}
  @media (max-width: 900px) {{
    .wrap {{ padding:0 20px 60px; }}
    .cards6, .country-grid {{ grid-template-columns:1fr; }}
    .kpi-grid {{ grid-template-columns:repeat(2, 1fr); }}
    .two-col {{ grid-template-columns:1fr; }}
    .hbar {{ grid-template-columns:110px 1fr 64px auto; }}
  }}
</style></head><body>
<div class="progress" id="progress"></div>
<div class="wrap">

  <div class="logo">infobae</div>
  <div class="kicker">Brief de autores · {esc(label)} · mes completo</div>
  <h1>El mes de la redacción, en números</h1>

  <div class="hero">
    <div class="hero-num">{fmt_int(p['notas'])}</div>
    <div class="hero-side">notas publicadas por la redacción en {esc(mes_corto)}, sin agencias<br>{hero_delta}</div>
  </div>
  <div class="hero" style="margin-top:10px">
    <div class="hero-num" style="font-size:clamp(2rem,5vw,3.4rem); color:{TEXT}">{fmt_big(t['usuarios_unicos'])}</div>
    <div class="hero-side">{esc(usuarios_label)}<br>{delta_html(t['usuarios_unicos'], pt['usuarios_unicos'], prev_corto)}</div>
  </div>

  <div class="kicker" style="margin-top:64px">{esc(mes_corto)} en seis cifras</div>
  <div class="cards6">{resumen_cards}</div>
  <div class="scroll-hint">Scrolleá para empezar</div>

  {chapter_header("01", "El pulso del mes", "Producción y alcance de la redacción",
                  "la producción de la redacción en el mes, todos los países y secciones juntos. "
                  "Las notas de agencias quedan afuera de todo el informe salvo el capítulo 05.")}
  <div class="kpi-grid">{"".join(kpis)}</div>
  <div class="panel">
    <h3>Notas publicadas día por día</h3>
    <div class="sub">{esc(label)} completo.</div>
    {daily_line}
    <div class="panel-foot">{daily_foot}</div>
  </div>
  <div class="panel">
    <h3>Visitas día por día</h3>
    <div class="sub">Tráfico sobre las notas publicadas en el mes. El tráfico a notas anteriores no se incluye.</div>
    {visitas_line}
  </div>

  {chapter_header("02", "Quiénes crearon y publicaron", "Los autores del mes",
                  "notas distintas por autor. El nombre sale de la tabla de autores, matcheada por email.")}
  <div class="two-col">
    <div class="panel"><h3>Top 10 creadores</h3><div class="sub">Por notas creadas.</div>{top_creators}</div>
    <div class="panel"><h3>Top 10 publicadores</h3><div class="sub">Por notas publicadas (FIRST_PUBLISH).</div>{top_publishers}</div>
  </div>

  {chapter_header("03", "Qué secciones se movieron", "La agenda del mes, sección por sección",
                  "la sección de la nota publicada, sin importar el país del autor.")}
  <div class="panel">
    <h3>Top 10 secciones por notas publicadas</h3>
    <div class="sub">La variación contra {esc(prev_corto)} está a la derecha de cada barra.</div>
    {sections_bars}
  </div>
  {cross_html}

  {chapter_header("04", "País por país", "Cada redacción en su mercado",
                  "el país del AUTOR según la tabla de autores; no el país del lector.")}
  <div class="country-grid">{countries_html}</div>

  {chapter_header("05", "Fuentes de producción", "Composer, Scribnews y agencias",
                  "por dónde se produjo cada nota publicada, y cuánto tráfico trajo cada canal.")}
  {sources_html}

  <div class="closing">
    <div class="kicker">Para cerrar</div>
    <h2>Con qué seguir</h2>
    <p>Todo lo que ves acá sale del tablero de Seguimiento de Autores, que sigue vivo y actualizado a diario.
       Si necesitás filtrar por autor, sección o país, o mirar otro rango de fechas, entrá directo:</p>
    <p><a href="https://seguimiento-autores-infobae.streamlit.app/" target="_blank">Abrir el tablero completo →</a></p>
    <div class="method">
      <b>Brief de Autores · {esc(label)} (mes completo), comparado contra {esc(prev_label)} completo.</b><br><br>
      La producción sale de la actividad editorial de ARC (creadores = evento CREATE; publicadores y notas =
      FIRST_PUBLISH). Las notas de agencias (usuario "infobae": EFE, EuropaPress, Narrativa, etc.) quedan
      afuera de todos los números —producción, tráfico y tiempo de lectura incluidos— salvo del capítulo 05,
      que las compara contra Composer y Scribnews. El tráfico sale de GA4 y cubre únicamente las notas
      publicadas dentro del mes: el tráfico a notas de meses anteriores no se incluye. {esc(metodo_nota)}
      El país del autor viene de la tabla de autores de Infobae, matcheada por email; los autores sin fila
      en esa tabla no aparecen en el capítulo 04. Elaborado automáticamente por el Centro de Control Editorial.
    </div>
  </div>
</div>
<div id="tip"></div>
<script>
  addEventListener('scroll', () => {{
    const h = document.documentElement;
    document.getElementById('progress').style.width =
      (h.scrollTop / (h.scrollHeight - h.clientHeight) * 100) + '%';
  }}, {{passive: true}});

  // Tooltip de hover: cualquier elemento con data-label lo muestra junto al cursor
  const tip = document.getElementById('tip');
  document.querySelectorAll('[data-label]').forEach(el => {{
    el.addEventListener('mousemove', e => {{
      tip.textContent = el.dataset.label;
      tip.style.display = 'block';
      const w = tip.offsetWidth;
      tip.style.left = Math.min(e.clientX + 14, innerWidth - w - 8) + 'px';
      tip.style.top = (e.clientY - 34) + 'px';
    }});
    el.addEventListener('mouseleave', () => {{ tip.style.display = 'none'; }});
  }});
</script>
</body></html>"""


# ───────────────────────── bloques auxiliares ─────────────────────────

def _resumen_cards(data, prev_corto) -> str:
    p, pp = data['production'], data['prev_production']
    t, pt = data['traffic'], data['prev_traffic']
    hll = t.get('metodo_unicos') == 'hll'
    secs = data['sections']
    sources = {s['fuente']: s for s in data['sources']}
    countries = data['countries_production']

    def frase_delta(cur, prev):
        d = delta_pct(cur, prev)
        if d is None:
            return "sin comparación disponible"
        return f"{'+' if d >= 0 else '−'}{fmt_dec(abs(d))} % vs. {prev_corto}"

    top_sec_notas = secs[0]['seccion'] if secs else "—"
    top_sec_traf = max(secs, key=lambda s: s['sesiones'])['seccion'] if secs else "—"
    top_country = countries[0]['pais'].title() if countries else "—"
    composer = sources.get('Composer', {}).get('notas', 0)
    scribnews = sources.get('Scribnews', {}).get('notas', 0)
    agencias = sources.get('Agencias', {}).get('notas', 0)
    unicos_txt = "usuarios únicos" if hll else "de alcance (suma diaria)"

    cards = [
        ("01 · PRODUCCIÓN",
         f"La redacción publicó <b>{fmt_int(p['notas'])} notas</b> ({frase_delta(p['notas'], pp['notas'])}), "
         f"sin contar agencias."),
        ("02 · AUTORES",
         f"<b>{fmt_int(p['creadores'])} creadores</b> y <b>{fmt_int(p['publicadores'])} publicadores</b> "
         f"estuvieron activos ({frase_delta(p['creadores'], pp['creadores'])} en creadores)."),
        ("03 · ALCANCE",
         f"Las notas del mes juntaron <b>{fmt_big(t['usuarios_unicos'])} {unicos_txt}</b> y "
         f"<b>{fmt_big(t['visitas'])} visitas</b> ({frase_delta(t['visitas'], pt['visitas'])})."),
        ("04 · LECTURA",
         f"<b>{fmt_dec(t['tiempo_min'])} min</b> de tiempo promedio por sesión y "
         f"<b>{fmt_dec(t['scroll_prom'] * 100)} %</b> de tasa de scroll."),
        ("05 · SECCIONES",
         f"<b>{esc(top_sec_notas)}</b> fue la sección con más notas; "
         f"<b>{esc(top_sec_traf)}</b>, la de más tráfico."),
        ("06 · ORIGEN",
         f"<b>{top_country}</b> fue la redacción con más notas: <b>{fmt_int(composer)}</b> por Composer y "
         f"<b>{fmt_int(scribnews)}</b> por Scribnews. Las agencias sumaron <b>{fmt_int(agencias)}</b> notas "
         f"automáticas, fuera del resto del informe."),
    ]
    return "".join(f'<div class="card6"><div class="n">{n}</div><p>{txt}</p></div>' for n, txt in cards)


CROSS_COLORS = ["#F68E1E", "#c96f10", "#8f5511", "#5c3a10", "#3a2a12", "#26262A"]


def _cross_sections(data) -> str:
    """Para las secciones top: qué redacción (país) puso las notas."""
    by_sec = {}
    for row in data['section_country']:
        by_sec.setdefault(row['seccion'], []).append(row)
    top_secs = [s['seccion'] for s in data['sections'][:6]]
    out = ['<div class="panel"><h3>¿Qué redacción empujó cada sección?</h3>'
           '<div class="sub">Reparto de las notas publicadas por país del autor, en las 6 secciones con más '
           'notas. Sirve para ver secciones cross: quién le está pegando al tema.</div><div class="cross">']
    for sec in top_secs:
        rows = sorted(by_sec.get(sec, []), key=lambda r: -r['notas'])
        total = sum(r['notas'] for r in rows) or 1
        segs, leg = [], []
        for i, r in enumerate(rows[:5]):
            share = r['notas'] / total * 100
            color = CROSS_COLORS[min(i, len(CROSS_COLORS) - 1)]
            segs.append(f'<div style="width:{share:.2f}%;background:{color}" '
                        f'data-label="{esc(str(r["pais"]).title())} · {fmt_int(r["notas"])} notas ({fmt_dec(share)} %)"></div>')
            if i < 3:
                leg.append(f'<b>{esc(str(r["pais"]).title())}</b> {fmt_dec(share)} %')
        resto = total - sum(r['notas'] for r in rows[:5])
        if resto > 0:
            segs.append(f'<div style="width:{resto / total * 100:.2f}%;background:{CROSS_COLORS[-1]}" '
                        f'data-label="Otros · {fmt_int(resto)} notas"></div>')
        out.append(f'<div class="cross-row"><div class="name">{esc(sec)} · {fmt_int(total)} notas</div>'
                   f'<div class="cross-bar">{"".join(segs)}</div>'
                   f'<div class="cross-leg">{" · ".join(leg)}</div></div>')
    out.append('</div></div>')
    return "".join(out)


def _country_cards(data, prev_label) -> str:
    prev = {c['pais']: c for c in data['prev_countries_production']}
    traffic = data['countries_traffic']
    tops = data['countries_top']
    out = []
    for c in data['countries_production']:
        pais = c['pais']
        tr = traffic.get(pais, {})
        pv = prev.get(pais, {})
        usuarios = tr.get('usuarios_unicos')
        usuarios_row = (f'<div class="row"><span>Usuarios únicos</span><b>{fmt_big(usuarios)}</b></div>'
                        if usuarios is not None else '')
        top_list = "".join(f'<li>{esc(a["nombre"])} · {fmt_int(a["notas"])}</li>'
                           for a in tops.get(pais, [])[:3])
        top_block = f'<div class="top3">Top creadores</div><ol>{top_list}</ol>' if top_list else ''
        out.append(f"""
        <div class="country">
          <h4>{flag(pais)} {esc(pais.title())}</h4>
          <div class="big">{fmt_int(c['notas'])}</div>
          <div style="color:{TEXT2};font-size:.82rem;margin-bottom:10px">notas publicadas ·
            {delta_html(c['notas'], pv.get('notas'), prev_label)}</div>
          <div class="row"><span>Creadores activos</span><b>{fmt_int(c['creadores'])}</b></div>
          <div class="row"><span>Publicadores</span><b>{fmt_int(c['publicadores'])}</b></div>
          <div class="row"><span>Visitas</span><b>{fmt_big(tr.get('visitas', 0))}</b></div>
          {usuarios_row}
          {top_block}
        </div>""")
    return "".join(out)


def _sources_block(data, prev_corto) -> str:
    prev = {s['fuente']: s for s in data['prev_sources']}
    rows_notas, rows_ses = [], []
    for s in data['sources']:
        pv = prev.get(s['fuente'], {})
        rows_notas.append({'label': s['fuente'], 'value': s['notas'],
                           'extra': delta_html(s['notas'], pv.get('notas'), prev_corto)})
        rows_ses.append({'label': s['fuente'], 'value': s['sesiones'],
                         'extra': f'<span class="muted">{fmt_int(s["sesiones_por_nota"])} sesiones/nota</span>'})
    return (f'<div class="two-col">'
            f'<div class="panel"><h3>Notas publicadas por fuente</h3>'
            f'<div class="sub">La variación contra {esc(prev_corto)} está a la derecha.</div>{hbar_rows(rows_notas)}</div>'
            f'<div class="panel"><h3>Visitas por fuente</h3>'
            f'<div class="sub">Con la eficiencia (sesiones por nota) a la derecha.</div>{hbar_rows(rows_ses, unidad="sesiones")}</div>'
            f'</div>')
