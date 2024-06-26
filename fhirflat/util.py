# Utility functions for FHIRflat
import datetime
import importlib
import re
from collections.abc import KeysView
from itertools import groupby

import fhir.resources
import numpy as np

import fhirflat
from fhirflat.resources import extensions


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
            file = "".join(file_words[:-1]).lower()

            try:
                return getattr(getattr(fhir.resources, file), t)
            except AttributeError:
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


def get_local_resource(t: str):
    return getattr(fhirflat, t)


def format_flat(flat_df):
    """
    Performs formatting ondates/lists in FHIRflat resources.
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
        if (x.lower().endswith(".code") or x.lower().endswith(".text"))
        and "Quantity" not in x
    ]:
        flat_df[coding_column] = flat_df[coding_column].apply(
            lambda x: [x] if isinstance(x, str) else x
        )

    return flat_df


def condense_codes(row, code_col):
    raw_codes = row[(code_col + ".code")]
    if isinstance(raw_codes, (str, float)) and raw_codes == raw_codes:
        formatted_code = (
            raw_codes if isinstance(raw_codes, str) else str(int(raw_codes))
        )
        codes = row[code_col + ".system"] + "|" + formatted_code
    elif np.isnan(raw_codes) or raw_codes is None:
        codes = None
    else:
        formatted_codes = [
            c if (isinstance(c, str) or c is None) else str(int(c)) for c in raw_codes
        ]
        codes = [
            s + "|" + c
            for s, c in zip(row[code_col + ".system"], formatted_codes, strict=True)
        ]

    row[code_col + ".code"] = codes
    return row
