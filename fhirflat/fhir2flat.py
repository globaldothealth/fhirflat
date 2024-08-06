"""
Convert FHIR resources as JSON files to FHIRflat CSV files.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .resources.base import FHIRFlatBase  # pragma: no cover


def flatten_column(
    data: pd.DataFrame | pd.Series, column_name: str
) -> pd.DataFrame | pd.Series:
    """
    Takes a column of a dataframe or series containing dictionaries and flattens it
    into multiple columns.
    """

    expanded_col: pd.DataFrame = pd.json_normalize(data[column_name])
    if not expanded_col.empty:
        expanded_col.columns = [
            column_name + "." + str(col) for col in expanded_col.columns
        ]

        if isinstance(data, pd.DataFrame):
            i = data.columns.get_loc(column_name)
            data = data.drop(column_name, axis=1)
            new_data = pd.concat(
                [data.iloc[:, :i], expanded_col, data.iloc[:, i:]], axis=1
            )
            return new_data
        elif isinstance(data, pd.Series):
            data = data.drop(column_name)
            new_data = pd.concat([data, expanded_col.iloc[0]], axis=0)
            return new_data
        else:
            raise TypeError("Input data must be a pandas DataFrame or Series.")
    else:
        return data


def explode_and_flatten(df: pd.DataFrame, list_cols: list[str]) -> pd.DataFrame:
    """
    Recursively explodes and flattens a dataframe.
    Columns containing a 'coding' or 'extension' list are left intact for later
    processing.

    df: flattened fhir resource
    lists: list of columns containing lists in the dataframe
    """

    list_lengths = [len(df[x][0]) for x in list_cols]
    long_list_cols = [x for x, y in zip(list_cols, list_lengths, strict=True) if y > 1]

    if long_list_cols:
        df.rename(columns={x: x + "_dense" for x in long_list_cols}, inplace=True)
        list_cols = [x for x in list_cols if x not in long_list_cols]

    df = df.explode(list_cols)

    assert len(df) == 1, "List with more than one concept has slipped through."

    for lc in list_cols:
        df = flatten_column(df, lc)

    # check if any cols remain containing lists that aren't 'coding' chunks, extension
    # or dense columns (lists of nested data we don't want to explode)
    list_columns = df.map(lambda x: isinstance(x, list))
    new_list_cols = [
        col
        for col in df.columns
        if (
            list_columns[col].any()
            and not col.endswith("coding")
            and not col.endswith("extension")
            and not col.endswith("_dense")
        )
    ]
    if new_list_cols:
        df = explode_and_flatten(df, new_list_cols)

    return df


def implode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Implodes a dataframe back to one row per resource instance.
    """

    def single_or_list(x):
        if x.apply(lambda x: isinstance(x, list)).any():
            x_unique = x.drop_duplicates()
            if len(x_unique) == 1:
                return x_unique
            elif len(x_unique.dropna()) == 1:
                return x_unique.dropna()
            else:
                return list(x)
        else:
            # Check if the column contains nan values
            if x.isnull().any():
                # If the column contains a single non-nan value, return it
                non_nan_values = x.dropna()
                if non_nan_values.nunique() == 1:
                    return non_nan_values
                else:
                    return list(non_nan_values)
            else:
                return x.iat[0] if x.nunique() == 1 else list(x)

    return df.groupby(df.index).agg(single_or_list)


def condenseCoding(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Turns a column containing a list of dictionaries with coding information into
    2 columns containing a list of strings with the coding information, and the text.

    [ {"system": "http://loinc.org", "code": "1234", "display": "Test"} ]
    becomes
    [ "http://loinc.org|1234" ], ["Test"]

    If a "text" field has already been provided, this overrides the display.
    """

    def condense(
        row: pd.Series, column_name: str, text_present: bool = False
    ) -> pd.Series:
        codes = row[column_name]
        new_codes = []
        new_names = []
        for c in codes:
            if c.get("code") and c.get("system"):
                new_codes.append(c.get("system") + "|" + c.get("code"))
            new_names.append(c.get("display"))

        # empty list if no alphanumeric code is present
        row[column_name] = new_codes
        if not text_present:
            row[column_name.removesuffix(".coding") + ".text"] = new_names
        return row

    text_present = False
    if column_name.removesuffix(".coding") + ".text" in df.columns:
        text_present = True

    df = df.apply(lambda x: condense(x, column_name, text_present), axis=1)

    if not text_present:
        df.insert(
            df.columns.get_loc(column_name) + 1,
            column_name.removesuffix(".coding") + ".text",
            df.pop(column_name.removesuffix(".coding") + ".text"),
        )

    df.rename(
        columns={column_name: column_name.removesuffix(".coding") + ".code"},
        inplace=True,
    )

    return df


def condenseReference(df: pd.DataFrame, reference: str) -> pd.DataFrame:
    """
    Condenses a reference into a string containing the reference type and the reference
    id.

    References are already present as "Patient/f201", so just need to rename the column.
    """
    # if reference is still nested in a dictionary, retrieve it
    ref = df[reference].str["reference"]
    if all(ref.isna()):
        df.rename(
            columns={reference: "".join(reference.rsplit(".reference", 1))},
            inplace=True,
        )
    else:
        df[reference] = ref

    # drop any display text for references, might contain identifying information
    if reference.removesuffix(".reference") + ".display" in df.columns:
        df.drop(columns=reference.removesuffix(".reference") + ".display", inplace=True)
    return df


def condenseSystem(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    For instances of coding not wrapped in a "coding" block, condenses the system and
     code columns into one.
    """
    base_name = col_name.removesuffix(".system")
    df[base_name + ".code"] = df[col_name] + "|" + df[base_name + ".code"]
    df.drop(col_name, axis=1, inplace=True)
    return df


def flattenExtensions(df: pd.DataFrame, extension: str) -> pd.DataFrame:
    """
    Flattens extensions in a FHIR resource.

    [
     {"url": "relativeDay", "valueInteger": 2},
     {"url":"approximateDate", "valueDate": "2012-09"}
    ]
    becomes
    [2], [ "2012-09" ]

    """

    def expand_and_redefine(df, extension):
        def redefine(
            row: pd.Series | pd.DataFrame, extension: str
        ) -> pd.Series | pd.DataFrame:
            """Expands out simple extensions and leaves complex ones as is.
            To be dealt with later in the pipeline."""

            ext = row[extension]

            name = extension.removesuffix(".extension") + "." + ext["url"]

            if "extension" in ext.keys():
                row[extension] = ext["extension"]
                row.rename({extension: name}, inplace=True)
                row = expand_and_redefine(row, name)

            if isinstance(row, pd.DataFrame):
                row = implode(row)
                assert len(row) == 1
                return row.iloc[0]

            try:
                # The fixed index will probably cause issues
                value = ext[next(key for key in ext if key.startswith("value"))]
            except IndexError as e:
                raise IndexError("Extension does not contain a single value.") from e

            row[name] = value

            if isinstance(row[name], dict) or issubclass(type(row[name]), dict):
                row = flatten_column(row, name)

            return row

        if isinstance(df, pd.DataFrame):
            df_ext = df.explode(extension)

        elif isinstance(df, pd.Series):
            # convert to dataframe, transpose then explode
            df_ext = df.to_frame().T.explode(extension)

        df_ext = df_ext.apply(lambda x: redefine(x, extension), axis=1)
        df_ext.drop(
            columns=extension, inplace=True, errors="ignore"
        )  # will stay silent if column doesn't exist

        return df_ext

    df_ext = expand_and_redefine(df, extension)

    df_ext_single = implode(df_ext)

    return df_ext_single


def fhir2flat(resource: FHIRFlatBase, lists: list | None = None) -> pd.DataFrame:
    """
    Converts a FHIR JSON file into a FHIRflat file.

    resource: fhir.resource.Resource
    lists: list
        List of columns that are lists of FHIR concepts that need to be expanded.
    """

    # Flatten JSON and convert to DataFrame
    df = pd.json_normalize(resource.dict())

    if lists:
        # extensions are dealt with seperately while still in a list
        list_cols = [n for n in lists if n in df.columns if n != "extension"]
        if list_cols:
            df = explode_and_flatten(df, list_cols)

    # condense all extensions
    for ext in df.columns[df.columns.str.endswith("extension")]:
        df = flattenExtensions(df, ext)

    # expand all instances of the "coding" list
    for coding in df.columns[df.columns.str.endswith("coding")]:
        df = condenseCoding(df, coding)

    # condense all references
    for reference in df.columns[df.columns.str.endswith("reference")]:
        df = condenseReference(df, reference)

    # condense any remaining codes not wrapped in a "coding" block
    for col in df.columns[df.columns.str.endswith(".system")]:
        df = condenseSystem(df, col)

    return df
