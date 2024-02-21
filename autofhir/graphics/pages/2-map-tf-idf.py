# map-tf-idf.py
# Uses the tf-idf autofhir functionality to map data to FHIR resources

import streamlit as st

from home import V_SPACE

from autofhir.csv_mapping import generate_csv_local


st.header("Mapping data with TF-IDF")

V_SPACE(1)

target_format = st.radio(
    "Choose a target data format:", ["ISARIC Original", "FHIRflat"]
)


if target_format == "FHIRflat":
    use_descriptions = st.checkbox("Use target field descriptions", value=False)

    resources = generate_csv_local(
        "autofhir/config/ARCH.toml", st.session_state.data_dict, use_descriptions
    )
elif target_format == "ISARIC Original":
    resources = generate_csv_local(
        "autofhir/config/redcap-en.toml", st.session_state.data_dict, False
    )

for title, resource in resources.items():
    st.subheader(title)
    df = resource
    st.data_editor(df, hide_index=True, key=title)
