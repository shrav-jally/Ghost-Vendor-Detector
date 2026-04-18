import json
from pathlib import Path

import networkx as nx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh


st.set_page_config(
    page_title="Ghost Vendor Intelligence Platform",
    page_icon="GV",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = Path(__file__).resolve().parent
FLAGGED_DIR = BASE_DIR / "output" / "flagged_cases"
FLAGGED_CASES_CSV = FLAGGED_DIR / "flagged_cases.csv"
INVESTIGATION_LOG = FLAGGED_DIR / "investigation_log.jsonl"
RUNTIME_STATUS_DIR = BASE_DIR / "output" / "runtime_status"
PRODUCER_STATUS_PATH = RUNTIME_STATUS_DIR / "producer_status.json"
CONSUMER_STATUS_PATH = RUNTIME_STATUS_DIR / "consumer_status.json"
NEO4J_STATUS_PATH = RUNTIME_STATUS_DIR / "neo4j_sink_status.json"
MODEL_PATH = BASE_DIR / "output" / "models" / "isolation_forest_fraud.joblib"


st_autorefresh(interval=4000, key="dashboard-refresh")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=Manrope:wght@400;600;700;800&display=swap');

    :root {
        --ink: #10243e;
        --muted: #5f6f82;
        --card: rgba(255,255,255,0.88);
        --line: rgba(16, 36, 62, 0.12);
        --blue: #0f62fe;
        --green: #0f9d58;
        --amber: #dd8c00;
        --red: #d92d20;
    }

    .stApp {
        color: var(--ink);
        font-family: 'Manrope', sans-serif;
        background:
            radial-gradient(circle at 10% 14%, rgba(76, 161, 255, 0.17), transparent 28%),
            radial-gradient(circle at 90% 8%, rgba(255, 127, 80, 0.15), transparent 24%),
            linear-gradient(160deg, #f6fbff 0%, #f8fff8 50%, #fff8ee 100%);
    }

    .block-container {
        padding-top: 1.2rem;
    }

    .hero-title {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 2.5rem;
        font-weight: 800;
        letter-spacing: -0.03em;
        color: var(--ink);
        margin-bottom: 0.25rem;
    }

    .hero-subtitle {
        color: var(--muted);
        font-size: 1.03rem;
        margin-bottom: 0.75rem;
    }

    .metric-card {
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 0.95rem 1rem;
        box-shadow: 0 10px 24px rgba(14, 35, 61, 0.08);
        min-height: 122px;
    }

    .metric-label {
        color: #4f6178;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        font-size: 0.8rem;
        font-weight: 800;
    }

    .metric-value {
        font-size: 2rem;
        font-weight: 800;
        line-height: 1.05;
        margin-top: 0.35rem;
        margin-bottom: 0.25rem;
    }

    .metric-note {
        color: #6d7d90;
        font-size: 0.84rem;
    }

    .status-card {
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 0.95rem 1rem;
        min-height: 140px;
        box-shadow: 0 10px 24px rgba(14, 35, 61, 0.08);
    }

    .status-title {
        color: #4f6178;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        font-size: 0.79rem;
        font-weight: 800;
        margin-bottom: 0.4rem;
    }

    .status-live {
        color: var(--green);
        font-weight: 800;
        font-size: 1.1rem;
    }

    .status-lag {
        color: var(--amber);
        font-weight: 800;
        font-size: 1.1rem;
    }

    .status-offline {
        color: var(--red);
        font-weight: 800;
        font-size: 1.1rem;
    }

    .status-meta {
        color: #617386;
        font-size: 0.9rem;
        margin-top: 0.25rem;
    }

    .section-box {
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: 18px;
        padding: 1rem 1rem;
        box-shadow: 0 10px 22px rgba(14, 35, 61, 0.06);
    }

    .section-title {
        font-size: 1.14rem;
        font-weight: 800;
        margin-bottom: 0.65rem;
        color: var(--ink);
    }

    .analytics-banner {
        border-radius: 16px;
        border: 1px solid rgba(15, 98, 254, 0.25);
        background: linear-gradient(90deg, rgba(15, 98, 254, 0.12), rgba(15, 157, 88, 0.12));
        padding: 0.8rem 1rem;
        font-weight: 700;
        color: #153153;
        margin: 0.7rem 0 1rem 0;
    }

    @media (max-width: 960px) {
        .hero-title {
            font-size: 2rem;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def load_flagged_cases():
    if not FLAGGED_CASES_CSV.exists():
        return pd.DataFrame(
            columns=[
                "batch_id",
                "loop_id",
                "step_order",
                "loop_path",
                "from_id",
                "to_id",
                "amount",
                "timestamp",
                "confidence_score",
                "transactions_scanned",
                "audit_reason",
                "risk_score",
                "is_anomaly",
            ]
        )

    df = pd.read_csv(FLAGGED_CASES_CSV)
    for col in ["amount", "confidence_score", "risk_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    if "batch_id" in df.columns:
        df["batch_id"] = df["batch_id"].astype(str)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    return df


def load_logs():
    if not INVESTIGATION_LOG.exists():
        return []

    entries = []
    with INVESTIGATION_LOG.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def load_status(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def parse_ts(ts):
    if not ts:
        return None
    try:
        return pd.Timestamp(ts)
    except Exception:
        return None


def process_health(payload, stale_seconds):
    ts = parse_ts(payload.get("last_heartbeat"))
    if ts is None:
        return "Unknown", "status-offline"

    age = (pd.Timestamp.now(tz="UTC") - ts.tz_convert("UTC")).total_seconds()
    state = str(payload.get("status", "unknown")).lower()

    if state == "error":
        return "Error", "status-offline"
    if age <= stale_seconds and state in {"running", "starting", "idle", "warning"}:
        if state == "warning":
            return "Lagging", "status-lag"
        return "Live", "status-live"
    if age <= stale_seconds * 3:
        return "Lagging", "status-lag"
    return "Offline", "status-offline"


def status_card(title, payload, stale_seconds, meta_line):
    label, css_class = process_health(payload, stale_seconds)
    event = str(payload.get("last_event", "No telemetry yet")).replace("<", "[").replace(">", "]")
    st.markdown(
        f"""
        <div class="status-card">
            <div class="status-title">{title}</div>
            <div class="{css_class}">{label}</div>
            <div class="status-meta">{meta_line}</div>
            <div class="status-meta">{event}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def batch_sort_key(value):
    try:
        return (0, int(value))
    except Exception:
        return (1, str(value))


def filter_batch(df, selected_batch):
    if df.empty:
        return df
    if selected_batch in (None, "All batches"):
        return df
    return df[df["batch_id"].astype(str) == str(selected_batch)].copy()


def metric_card(label, value, note, color):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value" style="color:{color};">{value}</div>
            <div class="metric-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_graph_figure(df):
    graph = nx.DiGraph()
    for _, row in df.iterrows():
        graph.add_edge(str(row.get("from_id")), str(row.get("to_id")), amount=row.get("amount", 0.0))

    if graph.number_of_nodes() == 0:
        return None

    positions = nx.spring_layout(graph, seed=42, k=0.92)

    edge_x = []
    edge_y = []
    arrows = []
    for source, target in graph.edges():
        x0, y0 = positions[source]
        x1, y1 = positions[target]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
        arrows.append(
            dict(
                ax=x0,
                ay=y0,
                x=x1,
                y=y1,
                xref="x",
                yref="y",
                axref="x",
                ayref="y",
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                arrowwidth=1.4,
                arrowcolor="#90a4b8",
            )
        )

    node_x, node_y, node_text, node_color, node_size, hover = [], [], [], [], [], []
    degree = dict(graph.degree())

    for node in graph.nodes():
        x, y = positions[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(node)
        is_emp = str(node).startswith("EMP_")
        is_vendor = str(node).startswith("VEND_")
        if is_emp:
            node_color.append("#0f62fe")
        elif is_vendor:
            node_color.append("#d92d20")
        else:
            node_color.append("#0f9d58")
        node_size.append(18 + degree.get(node, 1) * 4)
        hover.append(f"{node}<br>Degree: {degree.get(node, 0)}")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=edge_x,
            y=edge_y,
            mode="lines",
            line=dict(width=1.4, color="#b2bfd0"),
            hoverinfo="none",
            name="Money flow",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            text=node_text,
            textposition="top center",
            hovertext=hover,
            hoverinfo="text",
            marker=dict(size=node_size, color=node_color, line=dict(width=2, color="#ffffff"), opacity=0.95),
            name="Entities",
        )
    )

    fig.update_layout(
        title="Fraud Loop Graph Explorer",
        annotations=arrows,
        template="plotly_white",
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        showlegend=False,
        dragmode="pan",
        margin=dict(l=10, r=10, t=48, b=10),
        height=660,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig


def build_funnel(logs):
    scans = [entry for entry in logs if entry.get("event") == "scan"]
    warnings = [entry for entry in logs if entry.get("event") == "warning"]

    tx_seen = sum(int(entry.get("transactions_scanned", 0) or 0) for entry in scans)
    flagged_batches = len(warnings)
    investigations = int(flagged_batches * 0.7)
    closed = int(investigations * 0.45)

    fig = go.Figure(
        go.Funnel(
            y=["Transactions Seen", "Flagged Batches", "Investigations", "Closed Cases"],
            x=[tx_seen, flagged_batches, investigations, closed],
            marker={"color": ["#0f62fe", "#ff9800", "#26a69a", "#7e57c2"]},
            textposition="inside",
            textinfo="value+percent initial",
        )
    )
    fig.update_layout(template="plotly_white", margin=dict(l=10, r=10, t=40, b=10), height=330)
    return fig


def build_batch_risk(df):
    if df.empty:
        return None
    working = df.copy()
    if "confidence_score" not in working.columns:
        return None

    agg = working.groupby("batch_id", as_index=False).agg(
        avg_risk=("confidence_score", "mean"),
        flagged_steps=("loop_id", "count"),
    )
    agg["batch_num"] = pd.to_numeric(agg["batch_id"], errors="coerce")
    agg = agg.sort_values(["batch_num", "batch_id"], na_position="last")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=agg["batch_id"],
            y=agg["avg_risk"],
            mode="lines+markers",
            line=dict(color="#d92d20", width=2.4),
            marker=dict(size=8, color="#d92d20"),
            name="Average Risk",
        )
    )
    fig.add_trace(
        go.Bar(
            x=agg["batch_id"],
            y=agg["flagged_steps"],
            yaxis="y2",
            opacity=0.42,
            marker_color="#0f62fe",
            name="Flagged Steps",
        )
    )
    fig.update_layout(
        title="Batch Risk Pulse",
        template="plotly_white",
        margin=dict(l=10, r=10, t=42, b=10),
        height=320,
        yaxis=dict(title="Avg Risk", rangemode="tozero"),
        yaxis2=dict(title="Steps", overlaying="y", side="right", rangemode="tozero"),
        legend=dict(orientation="h", y=1.12, x=0),
    )
    return fig


def build_amount_hist(df):
    if df.empty or "amount" not in df.columns:
        return None
    data = pd.to_numeric(df["amount"], errors="coerce").dropna()
    if data.empty:
        return None

    fig = px.histogram(data, nbins=24)
    fig.update_traces(marker_color="#0f62fe", marker_line_color="#ffffff", marker_line_width=1)
    fig.update_layout(
        title="Flagged Amount Distribution",
        template="plotly_white",
        margin=dict(l=10, r=10, t=42, b=10),
        height=320,
        xaxis_title="Amount",
        yaxis_title="Count",
    )
    return fig


def build_entity_bar(df):
    if df.empty or "from_id" not in df.columns or "to_id" not in df.columns:
        return None

    entities = pd.concat([df["from_id"].astype(str), df["to_id"].astype(str)], ignore_index=True)
    top = entities.value_counts().head(12).rename_axis("entity").reset_index(name="mentions")
    if top.empty:
        return None

    fig = px.bar(top, x="mentions", y="entity", orientation="h", color="mentions", color_continuous_scale="Blues")
    fig.update_layout(
        title="Top Mentioned Entities in Fraud Loops",
        template="plotly_white",
        margin=dict(l=10, r=10, t=42, b=10),
        height=360,
        yaxis=dict(categoryorder="total ascending"),
    )
    return fig


def styled_flag_table(df):
    if df.empty:
        return df

    display_df = df.copy()
    if "confidence_score" in display_df.columns:
        display_df["confidence_score"] = pd.to_numeric(display_df["confidence_score"], errors="coerce").fillna(0).round(2)

    def color_row(row):
        score = float(row.get("confidence_score", 0) or 0)
        if score >= 90:
            style = "background-color: #ffe3e3; color: #a40000;"
        elif score >= 60:
            style = "background-color: #fff4cc; color: #7a5c00;"
        else:
            style = ""
        return [style] * len(row)

    return display_df.style.apply(color_row, axis=1)


flagged_cases = load_flagged_cases()
logs = load_logs()
producer_status = load_status(PRODUCER_STATUS_PATH)
consumer_status = load_status(CONSUMER_STATUS_PATH)
neo4j_status = load_status(NEO4J_STATUS_PATH)

scan_events = [entry for entry in logs if entry.get("event") == "scan"]
transactions_scanned = sum(int(entry.get("transactions_scanned", 0) or 0) for entry in scan_events)
flagged_loops = int(flagged_cases["loop_id"].nunique()) if (not flagged_cases.empty and "loop_id" in flagged_cases.columns) else 0
avg_risk = float(flagged_cases["confidence_score"].mean()) if (not flagged_cases.empty and "confidence_score" in flagged_cases.columns) else 0.0
model_status = "Available" if MODEL_PATH.exists() else "Missing"


st.markdown('<div class="hero-title">Ghost Vendor Intelligence Platform</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="hero-subtitle">A real-time command center for Kafka ingestion, Spark stream analytics, graph forensics, Neo4j persistence, and machine-learning risk intelligence.</div>',
    unsafe_allow_html=True,
)

pages = [
    "Overview",
    "Live Transactions",
    "Analytics Hub",
    "Graph Lab",
    "ML Insights",
    "Tech Stack",
    "About and Demo Guide",
]
selected_page = st.sidebar.radio("Navigate", pages, index=0)

st.sidebar.markdown("---")
st.sidebar.write("Auto-refresh: every 4 seconds")
st.sidebar.write(f"Model file: {model_status}")
st.sidebar.write(f"Flagged loops: {flagged_loops}")

if selected_page == "Overview":
    status_cols = st.columns(3)
    with status_cols[0]:
        status_card(
            "Producer (Kafka)",
            producer_status,
            stale_seconds=8,
            meta_line=f"Sent: {int(producer_status.get('tx_sent_total', 0) or 0):,} | Topic: {producer_status.get('topic', 'n/a')}",
        )
    with status_cols[1]:
        status_card(
            "Spark Consumer",
            consumer_status,
            stale_seconds=18,
            meta_line=f"Batch: {consumer_status.get('last_batch_id', 'n/a')} | Retry Queue: {consumer_status.get('neo4j_retry_queue_size', 0)}",
        )
    with status_cols[2]:
        status_card(
            "Neo4j Sink",
            neo4j_status,
            stale_seconds=30,
            meta_line=f"Rows in last write: {int(neo4j_status.get('rows', 0) or 0):,}",
        )

    st.markdown("<div style='height: 0.8rem;'></div>", unsafe_allow_html=True)

    metric_cols = st.columns(4)
    with metric_cols[0]:
        metric_card("Transactions Scanned", f"{transactions_scanned:,}", "Count from Spark scan logs", "#0f62fe")
    with metric_cols[1]:
        metric_card("Flagged Loops", f"{flagged_loops:,}", "Unique loop ids detected", "#0f9d58")
    with metric_cols[2]:
        metric_card("Average Risk", f"{avg_risk:.2f}", "Mean confidence across flagged steps", "#d92d20")
    with metric_cols[3]:
        metric_card("Model Status", model_status, "Isolation Forest model availability", "#7e57c2")

    st.markdown("<div style='height: 0.8rem;'></div>", unsafe_allow_html=True)
    c1, c2 = st.columns([1.05, 1], gap="large")
    with c1:
        st.markdown('<div class="section-box"><div class="section-title">Forensic Workflow Funnel</div></div>', unsafe_allow_html=True)
        st.plotly_chart(build_funnel(logs), width="stretch", config={"displaylogo": False})
    with c2:
        st.markdown('<div class="section-box"><div class="section-title">Process Telemetry Snapshot</div></div>', unsafe_allow_html=True)
        st.json(
            {
                "producer": {
                    "status": producer_status.get("status", "unknown"),
                    "tx_sent_total": producer_status.get("tx_sent_total", 0),
                    "fraud_queue_depth": producer_status.get("fraud_queue_depth", 0),
                },
                "spark_consumer": {
                    "status": consumer_status.get("status", "unknown"),
                    "last_batch_id": consumer_status.get("last_batch_id", "n/a"),
                    "model_loaded": consumer_status.get("model_loaded", False),
                    "neo4j_retry_queue_size": consumer_status.get("neo4j_retry_queue_size", 0),
                },
                "neo4j_sink": {
                    "status": neo4j_status.get("status", "unknown"),
                    "rows": neo4j_status.get("rows", 0),
                },
            },
            expanded=True,
        )

if selected_page == "Live Transactions":
    st.markdown('<div class="analytics-banner">Live transactions and stream activity. Use this page for operational monitoring during your demo.</div>', unsafe_allow_html=True)

    row1 = st.columns(3)
    row1[0].metric("Producer tx sent", int(producer_status.get("tx_sent_total", 0) or 0))
    row1[1].metric("Latest Spark batch", consumer_status.get("last_batch_id", "n/a"))
    row1[2].metric("Rows in latest Neo4j write", int(neo4j_status.get("rows", 0) or 0))

    st.markdown("<div style='height: 0.6rem;'></div>", unsafe_allow_html=True)

    scans_df = pd.DataFrame(scan_events)
    if not scans_df.empty:
        if "timestamp" in scans_df.columns:
            scans_df["timestamp"] = pd.to_datetime(scans_df["timestamp"], errors="coerce", utc=True)
            scans_df = scans_df.sort_values("timestamp")
        scans_df["transactions_scanned"] = pd.to_numeric(scans_df.get("transactions_scanned", 0), errors="coerce").fillna(0)

        left, right = st.columns(2, gap="large")
        with left:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=scans_df["timestamp"],
                    y=scans_df["transactions_scanned"],
                    mode="lines+markers",
                    line=dict(color="#0f62fe", width=2.2),
                    marker=dict(size=7),
                    name="Batch volume",
                )
            )
            fig.update_layout(
                title="Micro-batch Transactions Over Time",
                template="plotly_white",
                margin=dict(l=10, r=10, t=44, b=10),
                height=330,
            )
            st.plotly_chart(fig, width="stretch", config={"displaylogo": False})

        with right:
            minute_df = scans_df.copy()
            minute_df["minute"] = minute_df["timestamp"].dt.strftime("%H:%M")
            minute_agg = minute_df.groupby("minute", as_index=False)["transactions_scanned"].sum().tail(20)
            fig2 = px.bar(minute_agg, x="minute", y="transactions_scanned", color="transactions_scanned", color_continuous_scale="Viridis")
            fig2.update_layout(
                title="Recent Throughput by Minute",
                template="plotly_white",
                margin=dict(l=10, r=10, t=44, b=10),
                height=330,
            )
            st.plotly_chart(fig2, width="stretch", config={"displaylogo": False})
    else:
        st.info("No scan events found yet. Keep producer and consumer running for 1-2 minutes.")

    st.markdown("<div style='height: 0.6rem;'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-box"><div class="section-title">Live Investigation Log Stream</div></div>', unsafe_allow_html=True)
    if not logs:
        st.write("No log events yet.")
    else:
        for entry in logs[-30:][::-1]:
            ts = entry.get("timestamp", "unknown")
            msg = entry.get("message", "")
            evt = entry.get("event", "scan")
            prefix = "SCAN" if evt == "scan" else "ALERT"
            st.caption(f"[{prefix}] [{ts}] {msg}")

if selected_page == "Analytics Hub":
    st.markdown('<div class="analytics-banner">Analytics Hub: risk trends, distributions, leaderboards, and tabular forensic outputs.</div>', unsafe_allow_html=True)

    grid1 = st.columns(2, gap="large")
    risk_fig = build_batch_risk(flagged_cases)
    amt_fig = build_amount_hist(flagged_cases)

    with grid1[0]:
        if risk_fig is None:
            st.info("Risk trend appears after loops are detected.")
        else:
            st.plotly_chart(risk_fig, width="stretch", config={"displaylogo": False})

    with grid1[1]:
        if amt_fig is None:
            st.info("Amount distribution appears when flagged rows are available.")
        else:
            st.plotly_chart(amt_fig, width="stretch", config={"displaylogo": False})

    grid2 = st.columns(2, gap="large")
    ent_fig = build_entity_bar(flagged_cases)
    with grid2[0]:
        if ent_fig is None:
            st.info("Entity leaderboard appears after loop detections.")
        else:
            st.plotly_chart(ent_fig, width="stretch", config={"displaylogo": False})

    with grid2[1]:
        st.markdown('<div class="section-box"><div class="section-title">Top Loop Paths</div></div>', unsafe_allow_html=True)
        if flagged_cases.empty or "loop_path" not in flagged_cases.columns:
            st.info("No loop paths available yet.")
        else:
            top_paths = (
                flagged_cases["loop_path"]
                .astype(str)
                .value_counts()
                .head(10)
                .rename_axis("loop_path")
                .reset_index(name="occurrences")
            )
            st.dataframe(top_paths, width="stretch", hide_index=True)

    st.markdown("<div style='height: 0.6rem;'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-box"><div class="section-title">Fraudulent Loop Table</div></div>', unsafe_allow_html=True)
    if flagged_cases.empty:
        st.warning("No fraudulent loops detected yet. Fraud bursts are injected periodically in producer cycles.")
    else:
        cols = [
            c
            for c in [
                "batch_id",
                "loop_id",
                "step_order",
                "from_id",
                "to_id",
                "amount",
                "confidence_score",
                "timestamp",
                "audit_reason",
            ]
            if c in flagged_cases.columns
        ]
        st.dataframe(styled_flag_table(flagged_cases[cols]), width="stretch", hide_index=True)

if selected_page == "Graph Lab":
    st.markdown('<div class="analytics-banner">Graph Lab: interactive fraud network exploration and batch-scoped loop visualization.</div>', unsafe_allow_html=True)

    options = ["All batches"]
    if not flagged_cases.empty and "batch_id" in flagged_cases.columns:
        options.extend(sorted(flagged_cases["batch_id"].dropna().astype(str).unique().tolist(), key=batch_sort_key))
    selected_batch = st.selectbox("Select batch scope", options=options, index=len(options) - 1)
    graph_frame = filter_batch(flagged_cases, selected_batch)

    gfig = build_graph_figure(graph_frame)
    if gfig is None:
        st.info("No graph to display yet. Keep stream running until a fraud loop is flagged.")
    else:
        st.plotly_chart(gfig, width="stretch", config={"displaylogo": False, "scrollZoom": True})

    st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    c1.metric("Graph nodes", 0 if graph_frame.empty else int(pd.concat([graph_frame["from_id"], graph_frame["to_id"]]).nunique()))
    c2.metric("Graph edges", 0 if graph_frame.empty else int(len(graph_frame)))
    c3.metric("Selected batch", selected_batch)

if selected_page == "ML Insights":
    st.markdown('<div class="analytics-banner">Machine learning diagnostics: model state, risk signal quality, and anomaly summary.</div>', unsafe_allow_html=True)

    ml_cols = st.columns(3)
    ml_cols[0].metric("Model artifact", model_status)
    ml_cols[1].metric("Flagged rows", len(flagged_cases))
    ml_cols[2].metric("Mean confidence", f"{avg_risk:.2f}")

    left, right = st.columns(2, gap="large")
    with left:
        st.markdown('<div class="section-box"><div class="section-title">Model Information</div></div>', unsafe_allow_html=True)
        st.write(
            {
                "algorithm": "IsolationForest",
                "feature_set": ["amount", "pair_frequency", "sender_receiver_centrality"],
                "online_inference": True,
                "spark_integration": "foreachBatch",
                "artifact_path": str(MODEL_PATH),
                "artifact_exists": MODEL_PATH.exists(),
            }
        )

    with right:
        if flagged_cases.empty or "confidence_score" not in flagged_cases.columns:
            st.info("Risk band chart appears when flagged data is available.")
        else:
            bands = pd.cut(
                flagged_cases["confidence_score"],
                bins=[-0.01, 50, 75, 90, 100],
                labels=["Low", "Medium", "High", "Critical"],
            )
            band_df = bands.value_counts().rename_axis("risk_band").reset_index(name="count")
            pie = px.pie(band_df, names="risk_band", values="count", color="risk_band", color_discrete_map={
                "Low": "#90caf9",
                "Medium": "#ffe082",
                "High": "#ffab91",
                "Critical": "#ef5350",
            })
            pie.update_layout(title="Risk Band Distribution", height=340, margin=dict(l=10, r=10, t=42, b=10))
            st.plotly_chart(pie, width="stretch", config={"displaylogo": False})

    st.markdown("<div style='height: 0.6rem;'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-box"><div class="section-title">ML Summary Notes</div></div>', unsafe_allow_html=True)
    st.write("1. Transactions are scored per micro-batch before cycle analysis.")
    st.write("2. Confidence score is used as risk score in forensic outputs.")
    st.write("3. If model is missing, pipeline falls back to heuristic scoring.")
    st.write("4. Model can be retrained offline from historical datasets.")

if selected_page == "Tech Stack":
    st.markdown('<div class="analytics-banner">Technical summary of tools and technologies used in this project.</div>', unsafe_allow_html=True)

    tech = pd.DataFrame(
        [
            ["Apache Kafka", "Event ingestion and transport", "producer/transaction_producer.py, consumer/spark_stream_processor.py"],
            ["Apache Spark Structured Streaming", "Micro-batch processing and stream orchestration", "consumer/spark_stream_processor.py"],
            ["NetworkX", "Directed graph construction and cycle detection", "consumer/spark_stream_processor.py, graph/graph_analysis.py"],
            ["scikit-learn IsolationForest", "Anomaly scoring model", "graph/graph_analysis.py"],
            ["Neo4j + neo4j driver", "Persistent graph sink for investigative Cypher queries", "neo4j_sink.py, consumer/spark_stream_processor.py"],
            ["Pandas", "Batch shaping, feature transforms, tabular output", "app_ui.py, graph/graph_analysis.py, consumer/spark_stream_processor.py"],
            ["Plotly", "Interactive funnel, trends, histograms, graph analytics", "app_ui.py"],
            ["Streamlit", "Presentation-grade multi-page analytics dashboard", "app_ui.py"],
            ["Faker", "Synthetic transaction entity generation", "producer/transaction_producer.py"],
            ["Joblib", "Model artifact persistence", "graph/graph_analysis.py"],
            ["PowerShell", "Operational command orchestration on Windows", "README.md runbook"],
        ],
        columns=["Technology", "Purpose", "Where Used"],
    )
    st.dataframe(tech, width="stretch", hide_index=True)

    st.markdown("<div style='height: 0.6rem;'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-box"><div class="section-title">Architecture Flow</div></div>', unsafe_allow_html=True)
    st.write("1. Producer emits transactions to Kafka topic financial_transactions.")
    st.write("2. Spark consumes topic in 5-second micro-batches.")
    st.write("3. Feature engineering and anomaly scoring are applied.")
    st.write("4. Graph cycles are searched and suspicious loops are flagged.")
    st.write("5. Flagged outputs are persisted and surfaced in dashboard.")
    st.write("6. Optional Neo4j sink stores loops for deep graph forensics.")

if selected_page == "About and Demo Guide":
    st.markdown('<div class="analytics-banner">Presentation page: what to say, what to show, and what to click during the live demo.</div>', unsafe_allow_html=True)

    st.markdown('<div class="section-box"><div class="section-title">Project Story</div></div>', unsafe_allow_html=True)
    st.write("This platform simulates enterprise-grade forensic analytics where fraud patterns are detected from streaming transaction relationships instead of static monthly reports.")

    st.markdown('<div class="section-box"><div class="section-title">Demo Walkthrough (5 minutes)</div></div>', unsafe_allow_html=True)
    st.write("1. Show Overview page and confirm all three process status cards are Live.")
    st.write("2. Open Live Transactions page to show stream throughput and logs.")
    st.write("3. Open Analytics Hub to explain funnel and risk charts.")
    st.write("4. Open Graph Lab and inspect loop topology when available.")
    st.write("5. Open ML Insights to explain model-based anomaly scoring.")
    st.write("6. Open Tech Stack page for architecture and implementation depth.")

    st.markdown('<div class="section-box"><div class="section-title">Faculty Questions You Can Answer</div></div>', unsafe_allow_html=True)
    st.write("1. Why Kafka: durable decoupled event streaming for real-time systems.")
    st.write("2. Why Spark: scalable, fault-tolerant streaming analytics with micro-batches.")
    st.write("3. Why graph analytics: cycle structures reveal collusion patterns SQL misses.")
    st.write("4. Why Neo4j: persistent graph history for post-incident investigations.")
    st.write("5. Why ML: outlier risk scoring adds behavioral intelligence beyond deterministic rules.")


with st.sidebar.expander("Recent Investigation Logs", expanded=True):
    if not logs:
        st.write("No events yet.")
    else:
        for entry in logs[-18:][::-1]:
            st.caption(f"[{entry.get('timestamp', 'unknown')}] {entry.get('message', '')}")

st.caption(pd.Timestamp.now(tz="UTC").strftime("Last refresh: %H:%M:%S UTC"))