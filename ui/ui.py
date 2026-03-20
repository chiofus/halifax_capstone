# ── make sure main.py is importable from the same directory ──────────
def add_venv_dir_to_modules(path_to_check: str = '') -> None:
    import os, sys
    from pathlib import Path

    if not path_to_check: path_to_check = os.path.dirname(os.path.abspath(__file__))
    p = Path(path_to_check)

    dirs = [item.name for item in p.iterdir() if item.is_dir()]
    if '.venv' in dirs:
        sys.path.append(path_to_check)
        return None
    
    add_venv_dir_to_modules(p.parent.as_posix()) 

add_venv_dir_to_modules() #adds venv dir to modules

#IMPORTS
import streamlit as st
from objects.objects import INTERNAL_KEY
from ui_main import run_cq, HPCDMIndex, HPCDM_FILE
from ui_main import _extract_parcel_id, _detect_template, _run_direct_sparql, _format_answer, _infer_category, extract_keywords

# ── page config ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Halifax.Ai",
    page_icon="🏠",
    layout="centered",
)

# ── custom CSS ───────────────────────────────────────────────────────
st.markdown("""
<style>
/* hide default streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }

/* prevent Send button text from wrapping */
div[data-testid="column"]:last-child button {
    white-space: nowrap;
    min-width: 80px;
    height: 38px;
    padding: 0 16px;
}

/* chat bubbles */
.user-bubble {
    background: #185FA5;
    color: #E6F1FB;
    padding: 10px 16px;
    border-radius: 18px 18px 4px 18px;
    margin: 4px 0 4px 15%;
    font-size: 15px;
    line-height: 1.6;
}
.bot-bubble {
    background: #F1EFE8;
    color: #2C2C2A;
    padding: 10px 16px;
    border-radius: 18px 18px 18px 4px;
    margin: 4px 15% 4px 0;
    font-size: 15px;
    line-height: 1.6;
}
.meta-row {
    margin: 4px 15% 12px 0;
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
}
.tag {
    font-size: 11px;
    padding: 2px 9px;
    border-radius: 99px;
    border: 1px solid #B4B2A9;
    color: #5F5E5A;
    background: white;
    display: inline-block;
}
.tag-sparql {
    background: #E6F1FB;
    color: #185FA5;
    border-color: #B5D4F4;
}
.tag-llm {
    background: #FAEEDA;
    color: #854F0B;
    border-color: #FAC775;
}
</style>
""", unsafe_allow_html=True)

# ── load ontology index once (cached across reruns) ──────────────────
@st.cache_resource(show_spinner="Loading ontology index…")
def load_index():
    return HPCDMIndex(HPCDM_FILE)

index = load_index()

# ── session state ────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []   # list of {role, text, category, method}

# ── header ───────────────────────────────────────────────────────────
st.markdown("## 🏠 Halifax.Ai")
st.caption("Ask natural language questions about parcels, zoning, ownership, services, and building regulations.")
st.divider()

# ── suggested questions ──────────────────────────────────────────────
SUGGESTIONS = [
    "Where in the city are there empty parcels of land?",
    "Who owns parcel 489967?",
    "What is the perimeter of parcel 512976?",
    "Where is parcel 490700?",
    "What is the are of parcel 394769?",
    "What bylaws apply to parcel 389914?",
]

if not st.session_state.messages:
    st.markdown("**Try asking:**")
    cols = st.columns(3)
    for i, suggestion in enumerate(SUGGESTIONS):
        if cols[i % 3].button(suggestion, key=f"sug_{i}", use_container_width=True):
            st.session_state.pending_question = suggestion
            st.rerun()

# ── render chat history ───────────────────────────────────────────────

#Import convention
from objects.objects import CURR_STYLE

for msg in st.session_state.messages:
    try:
        if msg["role"] == CURR_STYLE: continue #skip dev messages

        if INTERNAL_KEY in msg["content"]: continue #some msgs are internal and should not be displayed
    except Exception as e:
        pass

    if msg["role"] == "user":
        st.markdown(f'<div class="user-bubble">{msg["content"]}</div>',
                    unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="bot-bubble">{msg["content"]}</div>',
                    unsafe_allow_html=True)
        tag_class = "tag-sparql" if msg.get("method") == "direct" else "tag-llm"
        method_label = "direct SPARQL" if msg.get("method") == "direct" else "LLM chain"
        st.markdown(
            f'<div class="meta-row">'
            f'<span class="tag {tag_class}">{method_label}</span>'
            f'<span class="tag">{msg.get("category", "")}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

# ── input bar ─────────────────────────────────────────────────────────
with st.form("chat_form", clear_on_submit=True):
    col1, col2 = st.columns([8, 1])
    with col1:
        user_input = st.text_input(
            "question",
            placeholder="Ask about a parcel, zone, owner, or service…",
            label_visibility="collapsed",
        )
    with col2:
        submitted = st.form_submit_button("Send")

# ── handle submitted question (form or suggestion button) ─────────────
question = None
if submitted and user_input.strip():
    question = user_input.strip()
elif hasattr(st.session_state, "pending_question"):
    question = st.session_state.pending_question
    del st.session_state.pending_question

if question:
    # Save user message
    st.session_state.messages.append({"role": "user", "content": question})

    # Detect method for display tag
    with st.spinner("Generating answer..."):
        category, st.session_state.messages = run_cq(question, index, st.session_state.messages) #now also updates all session messages

    parcel_id    = _extract_parcel_id(question)
    template_key = _detect_template(question) if parcel_id else None
    method       = "direct" if template_key and parcel_id else "llm"
    
    st.rerun()

# ── clear button ──────────────────────────────────────────────────────
if st.session_state.messages:
    st.divider()
    if st.button("Clear conversation", type="secondary"):
        st.session_state.messages = []
        st.rerun()