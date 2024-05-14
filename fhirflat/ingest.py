"""
Stores the main functions for converting clinical data (initally from RedCap-ARCH) to
FHIRflat.

Assumes two files are provided: one with the clinical data and one containing the
mappings. PL: Actually, maybe rather than the mappings it's either a file or a
dictionary showing the location of each mapping file (one per resource type).

TODO: Eventually, this ahould link to a google sheet file that contains the mappings
"""

import pandas as pd
import numpy as np
import warnings

# 1:1 (single row, single resource) mapping: Patient, Encounter
# 1:M (single row, multiple resources) mapping: Observation, Condition, Procedure, ...

"""
1. Create one input-data dataframe per resource type, using the column names from
the mapping file

2. For 1:1 mappings: use an apply function to create a fhir-like (or maybe
fhir-flat-like?) input data dictionary in one column, then a resource object in another.
Then follow format similar to fhir_file_to_flat to create the flat representation.

3. For 1:M mappings: (PL: not sure about this) Group columns by single_resource column
(to be created in the mapping file), explode the dataframe by these groups, then follow
the 1:1 process.
"""

"""
TODO
* sort out reference formatting
* cope with 'if' statements - e.g. for date overwriting.
* deal with duplicates/how to add multiple values to a single field - list options.
* Consider using pandarallel (https://pypi.org/project/pandarallel/) to parallelize
    the apply function, particularly for one to many mappings.
"""


def find_field_value(row, response, map, raw_data=None):
    """Returns the data for a given field, given the map."""
    if map == "<FIELD>":
        return response
    elif "+" in map:
        map = map.split("+")
        results = [find_field_value(row, response, m) for m in map]
        results = [x for x in results if x == x]
        return " ".join(results)
    else:
        col = map.lstrip("<").rstrip(">")
        try:
            return row[col]
        except KeyError:
            return raw_data.loc[row["index"], col]


def create_dictionary(data: pd.DataFrame, map_file: pd.DataFrame) -> pd.DataFrame:
    """
    Given a data file and a single mapping file for one FHIR resource type,
    returns a single column dataframe with the mapped data in a FHIRflat-like
    format, ready for further processing.
    """

    data = pd.read_csv(data, header=0)
    map_df = pd.read_csv(map_file, header=0)

    filtered_data = data[map_df["redcap_variable"].dropna().unique()].copy()

    # Fills the na redcap variables with the previous value
    map_df["redcap_variable"] = map_df["redcap_variable"].ffill()

    # strips the text answers out of the redcap_response column
    map_df["redcap_response"] = map_df["redcap_response"].apply(
        lambda x: x.split(",")[0] if isinstance(x, str) else x
    )

    # Set multi-index for easier access
    map_df.set_index(["redcap_variable", "redcap_response"], inplace=True)

    def create_dict_from_row(row):
        """
        Iterates through the columns of the row, applying the mapping to each columns
        and produces a fhirflat-like dictionary to initialize the resource object.
        """

        result = {}
        for column in row.index:
            if column in map_df.index.get_level_values(0):
                response = row[column]
                if pd.notna(response):  # Ensure there is a response to map
                    try:
                        # Retrieve the mapping for the given column and response
                        if pd.isna(map_df.loc[column].index).all():
                            mapping = map_df.loc[(column, np.nan)].dropna()
                        else:
                            mapping = map_df.loc[(column, str(int(response)))].dropna()
                        snippet = {
                            k: (
                                v
                                if "<" not in str(v)
                                else find_field_value(row, response, v)
                            )
                            for k, v in mapping.items()
                        }
                    except KeyError:
                        # No mapping found for this column and response despite presence
                        # in mapping file
                        warnings.warn(
                            f"No mapping for column {column} response {response}",
                            UserWarning,
                        )
                        continue
                else:
                    continue
            else:
                raise ValueError(f"Column {column} not found in mapping file")
            duplicate_keys = set(result.keys()).intersection(snippet.keys())
            if not duplicate_keys:
                result = result | snippet
            else:
                if all(
                    result[key] == snippet[key] for key in duplicate_keys
                ):  # Ignore duplicates if they are the same
                    continue
                else:
                    raise ValueError(
                        "Duplicate keys in mapping:"
                        f" {set(result.keys()).intersection(snippet.keys())}"
                    )
        return result

    # Apply the function across the DataFrame rows
    filtered_data["flat_dict"] = filtered_data.apply(create_dict_from_row, axis=1)
    return filtered_data


def load_data(data, mapping_files, resource_type, file_name):

    df = create_dictionary(data, mapping_files)

    resource_type.ingest_to_flat(df, file_name)


def create_one_to_many_dictionary(data: pd.DataFrame, map_file: pd.DataFrame):
    """
    Given a data file and a single mapping file for one FHIR resource type,
    returns a single column dataframe with the mapped data in a FHIRflat-like
    format, ready for further processing.
    """
    data = pd.read_csv(data, header=0)
    map_df = pd.read_csv(map_file, header=0)

    # setup the data -----------------------------------------------------------
    relevant_cols = map_df["redcap_variable"].dropna().unique()
    filtered_data = data.loc[
        :, data.columns.isin(relevant_cols)
    ].reset_index()  # .copy()

    melted_data = filtered_data.melt(id_vars="index", var_name="column")

    # set up the mappings -------------------------------------------------------

    # Fills the na redcap variables with the previous value
    map_df["redcap_variable"] = map_df["redcap_variable"].ffill()

    # strips the text answers out of the redcap_response column
    map_df["redcap_response"] = map_df["redcap_response"].apply(
        lambda x: x.split(",")[0] if isinstance(x, str) else x
    )

    # Set multi-index for easier access
    map_df.set_index(["redcap_variable", "redcap_response"], inplace=True)

    def create_dict_from_cell(row, full_df=data):
        """
        Iterates through the columns of the row, applying the mapping to each columns
        and produces a fhirflat-like dictionary to initialize the resource object.
        """

        column = row["column"]
        response = row["value"]
        if pd.notna(response):  # Ensure there is a response to map
            try:
                # Retrieve the mapping for the given column and response
                if pd.isna(map_df.loc[column].index).all():
                    mapping = map_df.loc[(column, np.nan)].dropna()
                else:
                    mapping = map_df.loc[(column, str(int(response)))].dropna()
                snippet = {
                    k: (
                        v
                        if "<" not in str(v)
                        else find_field_value(row, response, v, raw_data=full_df)
                    )
                    for k, v in mapping.items()
                }
                return snippet
            except KeyError:
                # No mapping found for this column and response despite presence
                # in mapping file
                warnings.warn(
                    f"No mapping for column {column} response {response}",
                    UserWarning,
                )
                return None

    # Apply the function across the DataFrame rows
    melted_data["flat_dict"] = melted_data.apply(create_dict_from_cell, axis=1)
    return melted_data


def load_data_one_to_many(data, mapping_files, resource_type, file_name):

    df = create_one_to_many_dictionary(data, mapping_files)

    resource_type.ingest_to_flat(df.dropna(), file_name)
