# Utility functions for FHIRflat
from __future__ import annotations

import datetime
import importlib
import re
from collections.abc import KeysView
from itertools import groupby
from typing import TYPE_CHECKING

import fhir.resources
import numpy as np
import pandas as pd

import fhirflat
from fhirflat.resources import extensions
from fhirflat.resources.extensions import _ISARICExtension

if TYPE_CHECKING:
    from .resources.base import FHIRFlatBase


def group_keys(data_keys: list[str] | KeysView) -> dict[str, list[str]]:
    """
    Finds columns with a '.' in the name denoting data that has been flattened and
     groups them together.

    ["code.code", "code.text", "value.code", "value.text", "fruitcake"]
    returns
    {"code": ["code.code", "code.text"], "value": ["value.code", "value.text"]}
    """
    grouped_keys = [k for k in data_keys if "." in k]
    grouped_keys.sort()
    groups = {k: list(g) for k, g in groupby(grouped_keys, lambda x: x.split(".")[0])}
    return groups


def get_fhirtype(t: str | list[str]):
    """
    Finds the relevant class from fhir.resources for a given string.
    """

    if isinstance(t, list):
        return [get_fhirtype(x) for x in t]

    if not (hasattr(extensions, t) or hasattr(extensions, t.capitalize())):
        try:
            return getattr(getattr(fhir.resources, t.lower()), t)
        except AttributeError:
            file_words = re.findall(r"[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))", t)

            for i in range(-1, (len(file_words) * -1), -1):
                try:
                    file = "".join(file_words[:i]).lower()
                    return getattr(getattr(fhir.resources, file), t)
                except AttributeError:
                    continue

            try:
                module = importlib.import_module(f"fhir.resources.{t.lower()}")
                return getattr(module, t)
            except (ImportError, ModuleNotFoundError) as e:
                # Handle the case where the module does not exist.
                raise AttributeError(f"Could not find {t} in fhir.resources") from e

    else:
        return get_local_extension_type(t)


def get_local_extension_type(t: str):
    """
    Finds the relevant class from local extensions for a given string.
    """

    try:
        return getattr(extensions, t)
    except AttributeError:
        try:
            return getattr(extensions, t.capitalize())
        except AttributeError as ae:
            raise AttributeError(f"Could not find {t} in fhirflat extensions") from ae


def get_local_resource(t: str, case_insensitive: bool = False):
    if case_insensitive is False:
        return getattr(fhirflat, t)
    else:
        for a in dir(fhirflat):
            if a.lower() == t.lower():
                return getattr(fhirflat, a)


def find_data_class_options(
    data_class: FHIRFlatBase | list[FHIRFlatBase], k: str
) -> FHIRFlatBase | list[FHIRFlatBase]:
    """
    Finds the type class(es) for item k within the data class.

    Parameters
    ----------
    data_class
        The data class to search within. If a list, the function will search for the
        a class with a matching title to k.
    k
        The property to search for within the data class

    Returns
    -------
    The relevant FHIR class.
    """

    if isinstance(data_class, list):
        title_matches = [k.lower() == c.__name__.lower() for c in data_class]
        result = [x for x, y in zip(data_class, title_matches, strict=True) if y]
        if len(result) == 1:
            return get_fhirtype(k)
        else:
            raise ValueError(f"Couldn't find a matching class for {k} in {data_class}")

    else:
        k_schema = data_class.schema()["properties"][k]

        base_class = (
            k_schema.get("items").get("type")
            if k_schema.get("items") is not None
            else k_schema.get("type")
        )

        if base_class is None:
            assert k_schema.get("type") == "array"

            base_class = [opt.get("type") for opt in k_schema["items"]["anyOf"]]

        return get_fhirtype(base_class)


def is_codeable_concept(
    col_name: str, resource: FHIRFlatBase | list[FHIRFlatBase] | _ISARICExtension
) -> bool:
    """
    Determines if a column is a code or a codeable concept.
    """
    search_terms = col_name.split(".")

    if not issubclass(
        resource if not isinstance(resource, list) else type(str),
        _ISARICExtension,
    ):
        fhir_type = find_data_class_options(resource, search_terms[0])

    else:
        prop = get_local_extension_type(search_terms[0]).schema()["properties"]
        value_type = [key for key in prop.keys() if key.startswith("value")]
        if len(value_type) == 1:
            return True if "codeableconcept" in value_type[0].lower() else False
        elif "valueCodeableConcept" in value_type:
            # if longer it's a value with 'code' in it, e.g. Quantity.
            return True if len(search_terms) == 2 else False

    if len(search_terms) == 2:  # e.g. "code.code", "age.code"
        schema = fhir_type.schema()["properties"]
        codeable_concepts = [
            key
            for key in schema.keys()
            if "codeableconcept" in key.lower() or "coding" in key.lower()
        ]
        if codeable_concepts:
            return True
        else:
            return False
    else:
        return is_codeable_concept(".".join(search_terms[1:]), fhir_type)


def format_flat(flat_df: pd.DataFrame, resource: FHIRFlatBase) -> pd.DataFrame:
    """
    Performs formatting on dates/lists in FHIRflat resources.
    """

    for date_cols in [
        x
        for x in flat_df.columns
        if ("date" in x.lower() or "period" in x.lower() or "time" in x.lower())
    ]:
        # replace nan with None
        flat_df[date_cols] = flat_df[date_cols].replace(np.nan, None)

        # convert datetime objects to ISO strings
        # (stops unwanted parquet conversions)
        # but skips over extensions that have floats/strings rather than dates
        flat_df[date_cols] = flat_df[date_cols].apply(
            lambda x: (
                x.isoformat()
                if isinstance(x, datetime.datetime) or isinstance(x, datetime.date)
                else x
            )
        )

    for coding_column in [
        x
        for x in flat_df.columns
        if (
            (x.lower().endswith(".code") or x.lower().endswith(".text"))
            and is_codeable_concept(x, resource)
        )
    ]:
        flat_df[coding_column] = flat_df[coding_column].apply(
            lambda x: [x] if isinstance(x, str) else x
        )

    return flat_df


def condense_codes(row: pd.Series, code_col: str) -> pd.Series:
    raw_codes = row[(code_col + ".code")]
    if isinstance(raw_codes, (str, int, float)) and raw_codes == raw_codes:
        formatted_code = (
            raw_codes if isinstance(raw_codes, str) else str(int(raw_codes))
        )
        codes = row[code_col + ".system"] + "|" + formatted_code
    elif isinstance(raw_codes, list):
        formatted_codes = [
            c if (isinstance(c, str) or c is None) else str(int(c)) for c in raw_codes
        ]
        codes = [
            s + "|" + c
            for s, c in zip(row[code_col + ".system"], formatted_codes, strict=True)
        ]
    else:
        codes = None

    row[code_col + ".code"] = codes
    return row


def json_type_matching(t: str):
    """
    Matches a JSON type to a Python type.
    """
    tps = {
        "string": str,
        "integer": float,
        "number": float,
        "boolean": bool,
        "array": list,
        "object": dict,
        "null": None,
    }

    return tps[t]
