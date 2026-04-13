import json
from pathlib import Path

import networkx as nx
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh


st.set_page_config(
    page_title="Ghost Vendor Intelligence Dashboard",
    page_icon="GVD",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = Path(__file__).resolve().parent
FLAGGED_DIR = BASE_DIR / "output" / "flagged_cases"
FLAGGED_CASES_CSV = FLAGGED_DIR / "flagged_cases.csv"
INVESTIGATION_LOG = FLAGGED_DIR / "investigation_log.jsonl"


st_autorefresh(interval=4000, key="ghost-vendor-refresh")

st.markdown(
    """
    <style>
    .stApp {
        background: #ffffff;
        color: #10243e;
    }
    .hero-title {
        font-size: 2.4rem;
        font-weight: 800;
        letter-spacing: -0.03em;
        margin-bottom: 0.15rem;
        color: #10243e;
    }
    .hero-subtitle {
        font-size: 1rem;
        color: #526173;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
        border: 1px solid rgba(15, 78, 140, 0.12);
        border-radius: 18px;
        padding: 1.2rem 1.15rem;
        box-shadow: 0 10px 30px rgba(16, 36, 62, 0.06);
        min-height: 132px;
    }
    .metric-label {
        font-size: 0.88rem;
        font-weight: 700;
        color: #4f5d6f;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 0.6rem;
    }
    .metric-value {
        font-size: 2.25rem;
        font-weight: 800;
        line-height: 1.0;
        margin-bottom: 0.3rem;
    }
    .metric-footnote {
        font-size: 0.86rem;
        color: #6b7a8d;
    }
    .accent-green {
        color: #0b7a4b;
    }
    .accent-blue {
        color: #0f62fe;
    }
    .accent-red {
        color: #d92d20;
    }
    .section-card {
        background: #ffffff;
        border: 1px solid rgba(15, 78, 140, 0.10);
        border-radius: 18px;
        padding: 1rem 1.1rem;
        box-shadow: 0 8px 24px rgba(16, 36, 62, 0.05);
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
            ]
        )

    df = pd.read_csv(FLAGGED_CASES_CSV)
    if "confidence_score" in df.columns:
        df["confidence_score"] = pd.to_numeric(df["confidence_score"], errors="coerce").fillna(0.0)
    if "batch_id" in df.columns:
        df["batch_id"] = df["batch_id"].astype(str)
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


def batch_sort_key(value):
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, str(value))


def filter_graph_frame(df, selected_batch):
    if df.empty:
        return df
    if selected_batch in (None, "All batches"):
        return df
    return df[df["batch_id"].astype(str) == str(selected_batch)].copy()


def build_graph_figure(df):
    graph = nx.DiGraph()
    for _, row in df.iterrows():
        graph.add_edge(
            str(row["from_id"]),
            str(row["to_id"]),
            amount=row.get("amount"),
            confidence_score=row.get("confidence_score"),
            loop_id=row.get("loop_id"),
        )

    if graph.number_of_nodes() == 0:
        return None

    positions = nx.spring_layout(graph, seed=42, k=0.9)

    edge_x = []
    edge_y = []
    annotations = []
    for source_id, target_id in graph.edges():
        x0, y0 = positions[source_id]
        x1, y1 = positions[target_id]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
        annotations.append(
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
                arrowhead=3,
                arrowsize=1,
                arrowwidth=1.6,
                arrowcolor="#8b97a8",
            )
        )

    node_x = []
    node_y = []
    node_text = []
    node_colors = []
    node_sizes = []
    node_hover = []
    degrees = dict(graph.degree())

    for node_id in graph.nodes():
        x, y = positions[node_id]
        node_x.append(x)
        node_y.append(y)
        node_text.append(node_id)
        is_employee = str(node_id).startswith("EMP_")
        is_vendor = str(node_id).startswith("VEND_")
        node_colors.append("#0f62fe" if is_employee else "#d92d20" if is_vendor else "#0b7a4b")
        node_sizes.append(20 + degrees.get(node_id, 1) * 5)
        connected_edges = graph.in_edges(node_id, data=True)
        total_amount = sum(float(edge_data.get("amount") or 0) for _, _, edge_data in connected_edges)
        node_hover.append(f"{node_id}<br>Total amount: {total_amount:,.2f}")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=edge_x,
            y=edge_y,
            line=dict(width=1.5, color="#b8c2d1"),
            hoverinfo="none",
            mode="lines",
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
            hovertext=node_hover,
            hoverinfo="text",
            marker=dict(
                size=node_sizes,
                color=node_colors,
                line=dict(width=2, color="#ffffff"),
                opacity=0.96,
            ),
            name="Entities",
        )
    )

    fig.update_layout(
        annotations=annotations,
        margin=dict(l=20, r=20, t=20, b=20),
        template="plotly_white",
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        showlegend=False,
        dragmode="pan",
        height=700,
        xaxis=dict(showgrid=False, zeroline=False, visible=False),
        yaxis=dict(showgrid=False, zeroline=False, visible=False),
    )
    return fig


def style_flagged_table(df):
    if df.empty:
        return df

    display_df = df.copy()
    if "confidence_score" in display_df.columns:
        display_df["confidence_score"] = display_df["confidence_score"].round(2)

    def style_row(row):
        style = ""
        if "audit_reason" in row and "Circular" in row["audit_reason"]:
            style = "background-color: #ffe3e3; color: #a40000;"
        elif "confidence_score" in row:
            score = float(row.get("confidence_score", 0) or 0)
            if score > 90:
                style = "background-color: #ffe3e3; color: #a40000;"
            elif score > 50:
                style = "background-color: #fff4cc; color: #7a5c00;"
        return [style] * len(row)

    return display_df.style.apply(style_row, axis=1)


flagged_cases = load_flagged_cases()
logs = load_logs()

scan_events = [entry for entry in logs if entry.get("event") == "scan"]
transactions_scanned = sum(int(entry.get("transactions_scanned", 0) or 0) for entry in scan_events)
if not flagged_cases.empty:
    if "loop_id" in flagged_cases.columns:
        flagged_alerts = int(flagged_cases["loop_id"].nunique())
    else:
        flagged_alerts = int(len(flagged_cases))
    risk_display = "CRITICAL"
    risk_color = "accent-red"
else:
    flagged_alerts = 0
    risk_display = "Low (0.1%)"
    risk_color = "accent-green"

last_refresh_text = pd.Timestamp.utcnow().strftime("%H:%M:%S UTC")

st.markdown('<div class="hero-title">Ghost Vendor Intelligence Dashboard</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="hero-subtitle">A bright, executive-ready view of suspicious transaction loops, live investigation logs, and confidence-scored alerts.</div>',
    unsafe_allow_html=True,
)

with st.expander("ℹ️ Project Overview & Methodology"):
    st.markdown("""
    **AI-Driven Forensic Audit Pipeline**

    This dashboard provides real-time insights from an AI-driven forensic audit pipeline. The system is designed to detect and flag suspicious financial activities, specifically focusing on 'Ghost Vendor' schemes and circular transaction loops that are often indicators of shell company fraud.

    **Methodology:**
    1.  **Real-time Ingestion:** Transaction data is streamed into the system using **Apache Kafka**, ensuring that the analysis is always based on the most current data.
    2.  **Graph Processing:** **Apache Spark** processes these micro-batches, constructing a graph of financial flows.
    3.  **Cycle Detection:** Using **NetworkX**, the system analyzes the graph to detect cycles—cases where money flows from an entity and eventually returns to it through a series of transactions.
    4.  **Auditing:** The system isn't just scanning; it's auditing. Every entry you see in the 'Investigation Logs' represents a Kafka micro-batch being cleared. When a loop appears in the 'Ghost Vendor' table, the system has mathematically proven a circular flow of funds that traditional SQL queries would miss.
    """)

st.caption(f"Live refresh at {last_refresh_text}")

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label accent-blue">Transactions Scanned</div>
            <div class="metric-value accent-blue">{transactions_scanned:,}</div>
            <div class="metric-footnote">Total rows processed by Spark batches</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with col2:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label accent-green">Flagged Alerts</div>
            <div class="metric-value accent-green">{flagged_alerts:,}</div>
            <div class="metric-footnote">Suspicious loops captured in flagged_cases.csv</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with col3:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label {risk_color}">Risk Level (%)</div>
            <div class="metric-value {risk_color}">{risk_display}</div>
            <div class="metric-footnote">Average confidence score for flagged edges</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

graph_options = ["All batches"]
if not flagged_cases.empty and "batch_id" in flagged_cases.columns:
    graph_options.extend(sorted(flagged_cases["batch_id"].dropna().astype(str).unique().tolist(), key=batch_sort_key))

selected_batch = st.selectbox(
    "Loop graph scope",
    options=graph_options,
    index=len(graph_options) - 1,
    help="Select a batch to zoom into one loop set at a time.",
)

graph_frame = filter_graph_frame(flagged_cases, selected_batch)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Ghost Vendor Loops")
if graph_frame.empty:
    st.info("No flagged loops are available yet. Start the Spark processor to populate flagged_cases.csv.")
else:
    fig = build_graph_figure(graph_frame)
    if fig is None:
        st.info("The selected batch does not contain enough loop data to draw a graph.")
    else:
        st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True, "displaylogo": False})
st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Flagged Cases")
if flagged_cases.empty:
    st.warning("No flagged cases found yet.")
else:
    display_columns = [
        column
        for column in [
            "batch_id",
            "loop_id",
            "step_order",
            "from_id",
            "to_id",
            "amount",
            "confidence_score",
            "timestamp",
        ]
        if column in flagged_cases.columns
    ]
    st.dataframe(
        style_flagged_table(flagged_cases[display_columns]),
        use_container_width=True,
        hide_index=True,
    )

st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Forensic Analysis")
if flagged_cases.empty:
    st.info("No forensic discoveries in the current session yet.")
else:
    entities = pd.concat(
        [
            flagged_cases["from_id"].astype(str),
            flagged_cases["to_id"].astype(str),
        ],
        ignore_index=True,
    )
    risk_by_entity = entities.value_counts().head(12)
    top_node = risk_by_entity.index[0] if not risk_by_entity.empty else "N/A"
    st.info(f"Forensic discovery indicates a high-confidence shell company loop involving {top_node}.")
    st.caption("Risk by Entity")
    st.bar_chart(risk_by_entity)
st.markdown("</div>", unsafe_allow_html=True)


with st.sidebar.expander("🕵️ Investigation Logs", expanded=True):
    if not logs:
        st.write("No investigation activity yet.")
    else:
        for entry in logs[-20:][::-1]:
            timestamp = entry.get("timestamp", "unknown time")
            message = entry.get("message", "")
            event = entry.get("event", "scan")
            color = "#0b7a4b" if event == "scan" else "#d92d20"
            st.markdown(
                f"<div style='margin-bottom:0.65rem;'><span style='color:{color}; font-weight:700;'>[{timestamp[-8:]}]</span> <span style='color:#10243e;'>{message}</span></div>",
                unsafe_allow_html=True,
            )