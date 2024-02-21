"""
A homepage for the graphical interface, integrating the autoFHIR functionality.
"""

import streamlit as st
import os
import openai
import pandas as pd

from streamlit_extras.switch_page_button import switch_page

streamlit_style = """<style>
textarea, html *  {
font-family: -apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif !important;
} a { color: #0e7569 !important; }
</style>"""  # noqa: E501

if "data_dict" not in st.session_state:
    st.session_state.data_dict = pd.DataFrame()


def V_SPACE(lines):
    for _ in range(lines):
        st.write("&nbsp;")


SIDEBAR_TEXT = """## About

**[autoFHIR](https://github.com/globaldothealth/isaric-pipeline/autofhir) generates
mapping files to enable data conversion into the Global.HealthxISARIC FHIRflat data
format.**

<span style="background-color: palegoldenrod">:warning: Uses Generative AI, answers may contain errors</span>
"""  # noqa: E501

st.set_page_config(
    page_title="autoFHIR",
    page_icon="https://global.health/wp-content/uploads/2020/10/gs-favicon-green.png",
    layout="wide",
)
st.markdown(streamlit_style, unsafe_allow_html=True)

image_col, title_col = st.columns([2, 5])

with image_col:
    st.markdown("")
    st.markdown("")
    st.image("graphics/gh_isaric_logo.png")

with title_col:
    st.title(":blue[autoFHIR]")
    st.subheader("Automated data transformation to FHIRflat")

with st.sidebar:
    st.markdown(SIDEBAR_TEXT, unsafe_allow_html=True)

    api_key_input = st.text_input(
        "OpenAI API Key",
        type="password",
        placeholder="Paste your OpenAI API key here (sk-...)",
        help="You can get your API key from https://platform.openai.com/account/api-keys",  # noqa: E501
        value=os.environ.get("OPENAI_API_KEY", None)
        or st.session_state.get("OPENAI_API_KEY", ""),
    )

    st.session_state["OPENAI_API_KEY"] = api_key_input
    openai.api_key = api_key_input

# -------------
# Main content
# -------------

if st.session_state.data_dict.empty:
    st.write(
        "This webapp provides semi-automated parser generation for new clinical\
            datasets."
    )

    st.write(
        "To start, please upload the data dictionary (NOT THE RAW DATA FILE) as a csv\
        file using the button below."
    )

    _, col1, _ = st.columns([1, 2, 1])
    ap_file = col1.file_uploader("Upload data dictionary", type=["csv"])

    if ap_file is not None:

        @st.cache_data
        def uploaded_dd():
            st.session_state.dd_fp = ap_file
            ap_df = pd.read_csv(ap_file)

            return ap_df

        df = uploaded_dd()
        st.session_state.data_dict = df

if not st.session_state.data_dict.empty:
    st.write("Thank you for uploading your data dictionary.")
    st.write(
        "Below is a preview of the data dictionary you have uploaded, where completely\
        empty columns have been removed.\n\
        Please ensure that the headers are\
            correctly formatted before continuing."
    )

    df = st.session_state.data_dict
    # df.dropna(axis=1, how="all", inplace=True)

    st.data_editor(df, use_container_width=True, disabled=True, hide_index=True)

    _, col1, col2, _ = st.columns([1, 2, 2, 1], gap="medium")
    clicked_tf = col1.button("Use TF-IDF", type="primary", use_container_width=True)
    clicked_gpt = col2.button("Use LLM", type="primary", use_container_width=True)

    if clicked_tf:
        switch_page("map-tf-idf")
        st.rerun()

    if clicked_gpt:
        switch_page("map-gpt")
        st.rerun()
