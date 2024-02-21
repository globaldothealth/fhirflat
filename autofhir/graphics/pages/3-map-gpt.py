# fhir.py
# Stores the subject table

import streamlit as st
import ast
import pandas as pd
import re

from home import V_SPACE

# with st.sidebar:
#     st.markdown(SIDEBAR_TEXT, unsafe_allow_html=True)

#     api_key_input = st.text_input(
#         "OpenAI API Key",
#         type="password",
#         placeholder="Paste your OpenAI API key here (sk-...)",
#         help="You can get your API key from https://platform.openai.com/account/api-keys",  # noqa: E501
#         value=os.environ.get("OPENAI_API_KEY", None)
#         or st.session_state.get("OPENAI_API_KEY", ""),
#     )

#     st.session_state["OPENAI_API_KEY"] = api_key_input
#     openai.api_key = api_key_input

from openai import OpenAI

client = OpenAI(api_key=st.session_state["OPENAI_API_KEY"])

if "field_column" not in st.session_state:
    st.session_state.field_column = None


@st.cache_data
def load_dd(dd):
    messages = []
    data_message = {"role": "user", "content": dd.to_csv(index=False)}
    messages.append(data_message)
    dd_response = client.chat.completions.create(
        model="gpt-3.5-turbo", messages=messages
    )
    messages.append(
        {"role": "system", "content": dd_response.choices[0].message.content}
    )

    return messages


st.header("Using chatGPT to map data to FHIR resources")
st.write(
    """
    Once produced, each mapping file can be downloaded, edited if necessary and used to
     generate FHIR resources.
    """
)

V_SPACE(1)

if st.session_state.field_column is None:
    st.session_state.field_column = st.selectbox(
        "Select the column in the data dictionary which represents the field names",
        st.session_state.data_dict.columns,
        index=None,
    )

if st.session_state.field_column is not None:
    if st.session_state.field_column in st.session_state.data_dict.columns:
        st.session_state.data_dict = st.session_state.data_dict.rename(
            columns={st.session_state.field_column: "field"}
        )
    st.subheader("Patient Resource")

    @st.cache_data
    def get_patient_dict():
        pt_messages = load_dd(st.session_state.data_dict)

        pt_messages.append(
            {
                "role": "user",
                "content": """
                This file is a data dictionary describing the structure of a dataset
                 containing patient information.
                Please create a python dictionary with the following keys
                id, gender, birthDate, deceasedBoolean, deceasedDateTime,
                 generalPractitioner, managingOrganization, maritalStatus,
                 multipleBirthBoolean, multipleBirthInteger
                The values of this dictionary should be taken from the "field"
                column, and should represent the fields from the file that match the key
                 most closely. If there isn't a good match for the key in the data file,
                set the value to "". Data describing if a patient has died might be
                 listed as an 'outcome'.
                Only provide the dictionary, no other text.
                """,
            }
        )

        pt_response = client.chat.completions.create(
            model="gpt-3.5-turbo", messages=pt_messages
        )
        patient_dict = pt_response.choices[0].message.content
        return patient_dict

    patient_dict = get_patient_dict()
    try:
        try:
            patient_dict = ast.literal_eval(patient_dict)
            patient_df = pd.DataFrame(
                {
                    "schema_field": patient_dict.keys(),
                    "field": patient_dict.values(),
                }
            )
        except SyntaxError:
            patient_dict = ast.literal_eval(
                ast.literal_eval(re.search("({.+})", patient_dict).group(0))
            )

            patient_df = pd.DataFrame(
                {
                    "schema_field": patient_dict.keys(),
                    "field": patient_dict.values(),
                }
            )
        patient_df = patient_df.merge(
            st.session_state.data_dict, on="field", how="left"
        )

        st.data_editor(
            patient_df, use_container_width=True, disabled=True, hide_index=True
        )

    except Exception as e:
        st.error(e)

    # -- Condition
    st.subheader("Condition Resource")

    @st.cache_data
    def get_condition_dict():
        cn_messages = load_dd(st.session_state.data_dict)
        cn_messages.append(
            {
                "role": "user",
                "content": """
                This file is a data dictionary describing the structure of a dataset
                containing patient information.
                Please create a python dictionary with the following keys
                bodySite, category, code, severity, stage, subject
                The values of this dictionary should be taken from the "field"
                column, and should represent the fields from the file that match the key
                 most closely. If there isn't a good match for the key in the data file,
                set the value to "".
                The `code` value should contain a list of all the fields that describe
                 medical conditions a patient could have.
                Only provide the dictionary, no other text.
                """,
            }
        )

        cn_response = client.chat.completions.create(
            model="gpt-3.5-turbo", messages=cn_messages
        )
        condition_dict = cn_response.choices[0].message.content
        return condition_dict

    condition_dict = get_condition_dict()

    try:
        try:
            condition_dict = ast.literal_eval(condition_dict)
        except SyntaxError:
            condition_dict = ast.literal_eval(
                ast.literal_eval(re.search("({.+})", condition_dict).group(0))
            )
        condition_df = pd.DataFrame(
            {
                "schema_field": condition_dict.keys(),
                "field": condition_dict.values(),
            }
        )
        condition_df = condition_df.explode("field")
        condition_df = condition_df.merge(
            st.session_state.data_dict, on="field", how="left"
        )

        st.data_editor(
            condition_df, use_container_width=True, disabled=True, hide_index=True
        )

    except Exception as e:
        st.error(e)
