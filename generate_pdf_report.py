"""
Comprehensive PDF Report Generator for Smart Meter Streaming Analytics Project.
"""
import os
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, HRFlowable
)
from reportlab.pdfgen import canvas

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
OUTPUT_PDF = os.path.join(BASE_DIR, "Smart_Meter_Streaming_Analytics_Complete_Project_Report.pdf")

class NumberedCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_header_footer(num_pages)
            super().showPage()
        super().save()

    def draw_header_footer(self, page_count):
        self.saveState()
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#64748b"))

        if self._pageNumber > 1:
            self.drawString(54, letter[1] - 36, "Smart Meter Real-Time Streaming Analytics — Project Report")
            self.setStrokeColor(colors.HexColor("#cbd5e1"))
            self.setLineWidth(0.5)
            self.line(54, letter[1] - 42, letter[0] - 54, letter[1] - 42)

        page_str = f"Page {self._pageNumber} of {page_count}"
        self.drawRightString(letter[0] - 54, 30, page_str)
        self.drawString(54, 30, "7th Semester B.Tech | AI & ML / DSA Mini-Project | Author: Rohan")
        self.setStrokeColor(colors.HexColor("#cbd5e1"))
        self.setLineWidth(0.5)
        self.line(54, 40, letter[0] - 54, 40)
        self.restoreState()


def create_pdf():
    doc = SimpleDocTemplate(
        OUTPUT_PDF,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=54,
        bottomMargin=54
    )

    styles = getSampleStyleSheet()

    PRIMARY = colors.HexColor("#0f172a")
    ACCENT_CYAN = colors.HexColor("#0284c7")
    ACCENT_DARK = colors.HexColor("#1e293b")
    TEXT_MUTED = colors.HexColor("#475569")
    BG_LIGHT = colors.HexColor("#f8fafc")

    title_style = ParagraphStyle(
        'DocTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=21,
        leading=26,
        textColor=PRIMARY,
        spaceAfter=6
    )

    subtitle_style = ParagraphStyle(
        'DocSubtitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=11,
        leading=15,
        textColor=ACCENT_CYAN,
        spaceAfter=14
    )

    h1_style = ParagraphStyle(
        'H1',
        parent=styles['Heading1'],
        fontName='Helvetica-Bold',
        fontSize=14,
        leading=18,
        textColor=PRIMARY,
        spaceBefore=14,
        spaceAfter=6,
        keepWithNext=True
    )

    h2_style = ParagraphStyle(
        'H2',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=11.5,
        leading=15,
        textColor=ACCENT_DARK,
        spaceBefore=10,
        spaceAfter=4,
        keepWithNext=True
    )

    body_style = ParagraphStyle(
        'Body',
        parent=styles['BodyText'],
        fontName='Helvetica',
        fontSize=9.5,
        leading=13.5,
        textColor=colors.HexColor("#1e293b"),
        spaceAfter=6
    )

    bullet_style = ParagraphStyle(
        'Bullet',
        parent=body_style,
        leftIndent=15,
        firstLineIndent=-10,
        spaceAfter=4
    )

    caption_style = ParagraphStyle(
        'Caption',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=8.5,
        leading=11,
        textColor=TEXT_MUTED,
        alignment=1,
        spaceAfter=8
    )

    story = []

    # Title Banner
    story.append(Paragraph("Real-Time Smart-Meter Streaming Analytics", title_style))
    story.append(Paragraph("Complete Technical Project Explanation, Architectural Rationale & Visual Output Analysis", subtitle_style))
    story.append(HRFlowable(width="100%", thickness=1.5, color=ACCENT_CYAN, spaceAfter=10))

    meta_info = [
        [
            Paragraph("<b>Author / Student:</b> Rohan", body_style),
            Paragraph("<b>Semester / Degree:</b> 7th Sem B.Tech (AI & ML / DSA)", body_style)
        ],
        [
            Paragraph("<b>Pipeline Stack:</b> Kafka → Python (deque) → Isolation Forest → PostgreSQL → Streamlit", body_style),
            Paragraph("<b>Dataset:</b> UCI Household Electric Power Consumption", body_style)
        ]
    ]
    meta_tbl = Table(meta_info, colWidths=[250, 254])
    meta_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), BG_LIGHT),
        ('PADDING', (0,0), (-1,-1), 6),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor("#e2e8f0")),
    ]))
    story.append(meta_tbl)
    story.append(Spacer(1, 10))

    # 1. Problem Statement
    story.append(Paragraph("1. Problem Statement & How This Project Solves It", h1_style))
    story.append(Paragraph(
        "<b>The Real-World Smart Grid Problem:</b> In electrical utility distribution networks, traditional meters are read manually "
        "or processed in daily/monthly batches. This introduces a latency window of 24 hours to 30 days. When anomalous power surges "
        "occur—caused by equipment insulation breakdowns, unauthorized illegal power tapping, hazardous appliance short-circuits, or transformer overloads—"
        "utility providers cannot detect them until hardware catches fire or outages cascade across the grid.",
        body_style
    ))
    story.append(Paragraph("• <b>Sub-Second Reaction Latency (< 15 ms):</b> Replaces sluggish batch processing with an event-driven streaming pipeline where each electrical reading is ingested, processed, scored by machine learning, and stored within 15 milliseconds.", bullet_style))
    story.append(Paragraph("• <b>Dynamic Baseline Tracking:</b> Maintains a dynamic 30-reading moving baseline via collections.deque to flag contextual surges at any hour of the day.", bullet_style))
    story.append(Paragraph("• <b>Unsupervised ML Anomaly Detection:</b> Utilizes an Isolation Forest that requires zero prior incident labels to isolate extreme spikes in real time.", bullet_style))
    story.append(Paragraph("• <b>Live Operator Telemetry:</b> Delivers an auto-refreshing dashboard displaying real-time power curves and glowing red alert diamonds.", bullet_style))

    # 2. Languages Used
    story.append(Spacer(1, 4))
    story.append(Paragraph("2. Programming Languages Used & Justification", h1_style))
    lang_data = [
        [
            Paragraph("<b>Language</b>", body_style),
            Paragraph("<b>Components Used In</b>", body_style),
            Paragraph("<b>Why This Language Was Chosen (Justification)</b>", body_style)
        ],
        [
            Paragraph("<b>Python 3.10</b>", body_style),
            Paragraph("Producer, Stream Processor, ML Training & Inference, Streamlit UI", body_style),
            Paragraph("Unified ecosystem connecting Kafka (kafka-python-ng), Scikit-Learn ML, Psycopg2 PostgreSQL driver, and Streamlit in one runtime with C-accelerated numeric extensions.", body_style)
        ],
        [
            Paragraph("<b>SQL (PostgreSQL)</b>", body_style),
            Paragraph("Database Schema, Table Definitions, Indexed Queries", body_style),
            Paragraph("ACID transactions guaranteeing data integrity, atomic ON CONFLICT upserts, and B-Tree indexing on reading_pk DESC for sub-millisecond range queries.", body_style)
        ]
    ]
    lang_tbl = Table(lang_data, colWidths=[80, 130, 294])
    lang_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), PRIMARY),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('PADDING', (0,0), (-1,-1), 5),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor("#94a3b8")),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
        ('BACKGROUND', (0,1), (-1,1), BG_LIGHT),
        ('BACKGROUND', (0,2), (-1,2), colors.white),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ]))
    story.append(lang_tbl)

    # 3. Technology Stack
    story.append(Spacer(1, 6))
    story.append(Paragraph("3. Technology Stack & Detailed Tool Rationale", h1_style))
    tools_data = [
        [Paragraph("<b>Tool</b>", body_style), Paragraph("<b>Layer</b>", body_style), Paragraph("<b>Rationale</b>", body_style)],
        [Paragraph("<b>Apache Kafka</b>", body_style), Paragraph("Message Broker", body_style), Paragraph("Decoupled, fault-tolerant message queue with persistent offset tracking and backpressure handling.", body_style)],
        [Paragraph("<b>collections.deque</b>", body_style), Paragraph("Sliding Window", body_style), Paragraph("Guaranteed O(1) constant-time append and left-eviction ring buffer preventing memory leaks.", body_style)],
        [Paragraph("<b>Isolation Forest</b>", body_style), Paragraph("ML Model", body_style), Paragraph("Unsupervised outlier detection partitioning anomalies in < 0.4 ms without labeled training data.", body_style)],
        [Paragraph("<b>PostgreSQL 18</b>", body_style), Paragraph("Database", body_style), Paragraph("ACID-compliant relational DBMS handling concurrent stream writes and dashboard reads.", body_style)],
        [Paragraph("<b>Streamlit + Plotly</b>", body_style), Paragraph("Dashboard", body_style), Paragraph("Reactive Web UI auto-refreshing every 2s with interactive hardware-accelerated Plotly charts.", body_style)]
    ]
    tools_tbl = Table(tools_data, colWidths=[100, 110, 294])
    tools_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), PRIMARY),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('PADDING', (0,0), (-1,-1), 4.5),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor("#94a3b8")),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [BG_LIGHT, colors.white]),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ]))
    story.append(tools_tbl)

    story.append(PageBreak())

    # 4. Visual Output Analysis with Screenshots
    story.append(Paragraph("4. Visual Output Analysis & Interpretation with Screenshots", h1_style))
    story.append(Paragraph(
        "Below are the verified live visual outputs captured during real-time streaming execution.",
        body_style
    ))

    img1_path = os.path.join(ASSETS_DIR, "dashboard_overview.png")
    if os.path.exists(img1_path):
        story.append(Image(img1_path, width=6.5*inch, height=3.53*inch))
        story.append(Paragraph("<b>Figure 1:</b> Live Streamlit Dashboard showing active pipeline status, glassmorphic metric cards, and dynamic Plotly power curve.", caption_style))

    story.append(Paragraph("<b>Breakdown of Figure 1 Telemetry:</b>", h2_style))
    story.append(Paragraph("• <b>CURRENT POWER (2.398 kW):</b> Instantaneous load from the latest Kafka event.", bullet_style))
    story.append(Paragraph("• <b>30-PT AVERAGE (1.895 kW):</b> Dynamic baseline tracked across 30 readings via deque.", bullet_style))
    story.append(Paragraph("• <b>PEAK POWER (7.706 kW):</b> All-time session peak load.", bullet_style))
    story.append(Paragraph("• <b>TOTAL ENERGY (18.96 kWh):</b> Cumulative energy computed via integration (Delta E = P / 60).", bullet_style))
    story.append(Paragraph("• <b>ANOMALIES (19):</b> Cumulative count of unusual power spikes flagged by Isolation Forest.", bullet_style))

    story.append(Spacer(1, 6))

    img2_path = os.path.join(ASSETS_DIR, "dashboard_anomalies.png")
    if os.path.exists(img2_path):
        story.append(Image(img2_path, width=6.5*inch, height=3.54*inch))
        story.append(Paragraph("<b>Figure 2:</b> Detected Anomaly Log showing timestamped surge deltas alongside Isolation Forest hyperparameters and stream statistics.", caption_style))

    story.append(Paragraph("<b>Breakdown of Figure 2 Telemetry:</b>", h2_style))
    story.append(Paragraph("• <b>Overall Stream Stats:</b> 513 readings streamed, with 24 anomalies detected (4.68% anomaly ratio).", bullet_style))
    story.append(Paragraph("• <b>Detected Anomaly Log:</b> Shows acute surges reaching <b>5.282 kW</b> against a baseline of <b>2.025 kW</b>, yielding a <b>+3.257 kW surge delta</b>.", bullet_style))
    story.append(Paragraph("• <b>Model Insights:</b> Scikit-Learn IsolationForest with 200 trees and 1.0% contamination threshold.", bullet_style))

    # 5. Conclusion
    story.append(Spacer(1, 6))
    story.append(Paragraph("5. Conclusion", h1_style))
    story.append(Paragraph(
        "This project successfully developed an end-to-end streaming data analytics pipeline for smart meter monitoring. "
        "By integrating Apache Kafka for fault-tolerant messaging, Python's collections.deque for sliding-window computation, "
        "an Isolation Forest for unsupervised outlier detection, PostgreSQL for relational time-series storage, and Streamlit "
        "for interactive visualization, the architecture demonstrates sub-15ms operational latency and dependable anomaly detection.",
        body_style
    ))

    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"✅ Generated: {OUTPUT_PDF}")

if __name__ == "__main__":
    create_pdf()
