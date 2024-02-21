# fhir.py
# Stores the subject table

import streamlit as st

from home import V_SPACE

import pandas as pd

st.header("Draft FHIR mapped data")

st.write(
    """
    The tables below show how fields from the source data are mapped to FHIR resources.
    """
)
st.write(
    """
    One table is provided per FHIR resource, in FHIRflat format.
    Please check and edit the field names as necessary to ensure the data is mapped
     correctly.
    """
)

st.write(
    """
    If data is in a single wide column, each FHIR property will match to a single
     column.  \n\
    For long data, mapping will be shown as 'column[name]', e.g. DSDECODE['DEATH'] for
     a "death" entry in the DSDECODE column. \n\
    If multiple data files are provided, mapping will be shown as 'file.column[name]'
    """
)

st.write(
    "In cases where data from more than one file will be mapped to a FHIR table,\
         multiple rows will be shown, one for each source file."
)

st.write(
    """
    Once you are happy with the data mapping click 'Generate Parser' to create the
     parser file.
    """
)

V_SPACE(1)

st.subheader("Patient Resource")

df_patient = pd.read_csv("graphics/example_patient.csv", header=0)
st.data_editor(df_patient, hide_index=True)

st.subheader("Observation Resource")

df_observation = pd.read_csv("graphics/example_obs.csv", header=0)
st.data_editor(
    df_observation,
    hide_index=True,
    column_config={
        "category": st.column_config.SelectboxColumn(
            "category",
            options=[
                "Vital Signs",
                "Social History",
                "Laboratory",
            ],
            required=True,
        )
    },
)

_, col2, col3, _ = st.columns(4)

if col2.button(
    "Generate Parser", type="primary", use_container_width=True, key="parsergen-subject"
):
    pass
