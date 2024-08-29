"""
Stores the main functions for converting clinical data (initally from RedCap-ARCH) to
FHIRflat.
"""

import argparse
import hashlib
import logging
import os
import shutil
import timeit
import warnings
from datetime import datetime
from glob import glob
from math import isnan
from pathlib import Path
from typing import Literal, TypedDict
from zoneinfo import ZoneInfo

import dateutil.parser
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from pyarrow.lib import ArrowTypeError

import fhirflat
from fhirflat.util import get_local_resource, group_keys

logger = logging.getLogger(__name__)

# 1:1 (single row, single resource) mapping: Patient, Encounter
# 1:M (single row, multiple resources) mapping: Observation, Condition, Procedure, ...

"""
TODO
* sort out how to choose ID's e.g. location within encounter etc
* cope with 'if' statements - e.g. for date overwriting.
* deal with how to check if lists are appropriate when adding multiple values to a
    single field - list options.
* Consider using pandarallel (https://pypi.org/project/pandarallel/) to parallelize
    the apply function, particularly for one to many mappings.
"""


class FlatMetadata(TypedDict):
    N: int | Literal["NA"]
    generator: str
    checksum: str
    checksum_file: str


def find_field_value(
    row, response, fhir_attr, mapp, date_format, timezone, raw_data=None
):
    """
    Returns the data for a given field, given the mapping.
    For one to many resources the raw data is provided to allow for searching for other
    fields than in the melted data.
    """
    if mapp == "<FIELD>":
        return_val = response
    elif "+" in mapp:
        mapp = mapp.split("+")
        results = [
            find_field_value(row, response, "", m, date_format, timezone, raw_data)
            for m in mapp
        ]
        results = [str(x) for x in results if not (isinstance(x, float) and isnan(x))]
        return_val = " ".join(results) if "/" not in results[0] else "".join(results)
    elif "if not" in mapp:
        mapp = mapp.replace(" ", "").split("ifnot")
        results = [
            find_field_value(row, response, "", m, date_format, timezone, raw_data)
            for m in mapp
        ]
        x, y = results
        if isinstance(y, float):
            return_val = x if isnan(y) else None
        else:
            return_val = x if not y else None
    elif "<" in mapp:
        col = mapp.lstrip("<").rstrip(">")
        try:
            return_val = row[col]
        except KeyError as e:
            if raw_data is not None:
                try:
                    return_val = raw_data.loc[row["index"], col]
                except KeyError:
                    raise KeyError(f"Column {col} not found in data") from e
            else:
                raise KeyError(f"Column {col} not found in the filtered data") from e
    else:
        return_val = mapp

    if "date" in fhir_attr.lower() or "period" in fhir_attr.lower():
        return format_dates(return_val, date_format, timezone)
    return return_val


def format_dates(date_str: str | float, date_format: str, timezone: str) -> str:
    """
    Converts dates into ISO8601 format with timezone information.
    """

    if date_str is None or date_str is np.nan:
        return date_str

    new_tz = ZoneInfo(timezone)

    try:
        date_time = datetime.strptime(date_str, date_format)
        date_time_aware = date_time.replace(tzinfo=new_tz)
        if "%H" not in date_format:
            date_time_aware = date_time_aware.date()
    except ValueError:
        try:
            # Unconverted data remains in the string (i.e. time is present)
            date, time = date_str.split(" ")
            date = datetime.strptime(date, date_format)
            time = dateutil.parser.parse(time).time()
            date_time = datetime.combine(date, time)
            date_time_aware = date_time.replace(tzinfo=new_tz)
        except ValueError:
            # Can't convert data, pass to FHIR to create validation error
            warnings.warn(
                f"Date {date_str} could not be converted using date format"
                f" {date_format}",
                UserWarning,
                stacklevel=1,
            )
            return date_str

    return date_time_aware.isoformat()


def create_dict_wide(
    row: pd.Series, map_df: pd.DataFrame, date_format: str, timezone: str
) -> dict:
    """
    Takes a wide-format dataframe and iterates through the columns of the row,
    applying the mapping to each column and produces a fhirflat-like dictionary to
    initialize the resource object for each row.
    """

    result: dict = {}
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
                            else find_field_value(
                                row, response, k, v, date_format, timezone
                            )
                        )
                        for k, v in mapping.items()
                    }
                except KeyError:
                    # No mapping found for this column and response despite presence
                    # in mapping file
                    warnings.warn(
                        f"No mapping for column {column} response {response}",
                        UserWarning,
                        stacklevel=1,
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
            # Ignore duplicates if they are the same
            # stringify lists/lists of numbers to get this to work without value errors
            if all(str(result[key]) == str(snippet[key]) for key in duplicate_keys):
                continue
            # replace placeholders with actual values
            elif all(result[key] is None for key in duplicate_keys):
                result.update(snippet)
            else:
                for key in duplicate_keys:
                    if isinstance(result[key], list):
                        result[key].append(snippet[key])
                    else:
                        result[key] = [result[key], snippet[key]]

                # Keys that were not previously in the result still need to be added
                remaining_keys = set(snippet.keys()) ^ duplicate_keys
                if remaining_keys:
                    key_length = max(len(result[k]) for k in duplicate_keys)
                    empty_list = [None] * (key_length - 1)
                    for key in remaining_keys:
                        result[key] = [*empty_list, snippet[key]]

                # Check for existing keys that might need to be extended
                snippet_keys = list(snippet.keys())
                result_groups = group_keys(result.keys())
                for k_list in result_groups.values():
                    if set(snippet_keys).issubset(set(k_list)):
                        relevant_result = {
                            k: (
                                [result[k]]
                                if not isinstance(result[k], list)
                                else result[k]
                            )
                            for k in k_list
                        }
                        all_vals_same_length = (
                            len(set(map(len, relevant_result.values()))) == 1
                        )
                        if not all_vals_same_length:
                            target_length = max(map(len, relevant_result.values()))
                            for k, v in relevant_result.items():
                                if len(v) < target_length:
                                    result[k] = relevant_result[k] + [None] * (
                                        target_length - len(v)
                                    )
    return result


def create_dict_long(
    row: pd.Series,
    full_df: pd.DataFrame,
    map_df: pd.DataFrame,
    date_format: str,
    timezone: str,
) -> dict | None:
    """
    Takes a long-format dataframe and a mapping file, and produces a fhirflat-like
    dictionary for each row in the dataframe.
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
                    else find_field_value(
                        row, response, k, v, date_format, timezone, raw_data=full_df
                    )
                )
                for k, v in mapping.items()
            }
            return snippet
        except KeyError:
            # No mapping found for this column and response despite presence
            # in mapping file
            if response == 0.0:
                # mostly this is ignoring unfilled responses
                logger.info(f"No mapping for column {column} response {response}")
            else:
                warnings.warn(
                    f"No mapping for column {column} response {response}",
                    UserWarning,
                    stacklevel=1,
                )
            return None
    return None


def create_dictionary(
    data_file: str,
    map_file: str,
    resource: str,
    one_to_one=False,
    subject_id="subjid",
    date_format="%Y-%m-%d",
    timezone="UTC",
) -> pd.DataFrame | None:
    """
    Given a data file and a single mapping file for one FHIR resource type,
    returns a single column dataframe with the mapped data in a FHIRflat-like
    format, ready for further processing.

    Parameters
    ----------
    data: str
        The path to the data file containing the clinical data.
    map_file: pd.DataFrame
        The path to the mapping file containing the mapping of the clinical data to the
        FHIR resource.
    resource: str
        The name of the resource being mapped.
    one_to_one: bool
        Whether the resource should be mapped as one-to-one or one-to-many.
    subject_id: str
        The name of the column containing the subject ID in the data file.
    date_format: str
        The format of the dates in the data file. E.g. "%Y-%m-%d"
    timezone: str
        The timezone of the dates in the data file. E.g. "Europe/London"
    """

    data: pd.DataFrame = pd.read_csv(data_file, header=0)
    map_df: pd.DataFrame = pd.read_csv(map_file, header=0)

    # setup the data -----------------------------------------------------------
    relevant_cols = map_df["raw_variable"].dropna().unique()
    filtered_data = data.loc[:, data.columns.isin(relevant_cols)].copy()

    if filtered_data.empty:
        warnings.warn(
            f"No data found for the {resource} resource.", UserWarning, stacklevel=2
        )
        return None

    if one_to_one:

        def condense(x):
            """
            In case where data is actually multi-row per subject, condenses the relevant
            data into a single row for 1:1 mapping.
            """

            # Check if the column contains nan values
            if x.isnull().any():
                # If the column contains a single non-nan value, return it
                non_nan_values = x.dropna()
                if non_nan_values.nunique() == 1:
                    return (
                        non_nan_values
                        if len(non_nan_values) == 1
                        else non_nan_values.unique()[0]
                    )
                elif non_nan_values.empty:
                    return np.nan
                else:
                    raise ValueError("Multiple values found in one-to-one mapping")
            else:
                if len(x) == 1:
                    return x
                elif x.nunique() == 1:
                    return x.unique()[0]
                else:
                    raise ValueError("Multiple values found in one-to-one mapping")

        if filtered_data.drop([subject_id], axis=1).empty:
            warnings.warn(
                f"No data found for the {resource} resource.", UserWarning, stacklevel=2
            )
            return None
        filtered_data = filtered_data.groupby(subject_id, as_index=False).agg(condense)

    if not one_to_one:
        filtered_data = filtered_data.reset_index()
        melted_data = filtered_data.melt(id_vars="index", var_name="column")
        melted_data.dropna(subset=["value"], inplace=True)

    # set up the mappings -------------------------------------------------------

    # Fills the na input variables with the previous value
    map_df["raw_variable"] = map_df["raw_variable"].ffill()

    # strips the text answers out of the response column
    map_df["raw_response"] = map_df["raw_response"].apply(
        lambda x: x.split(",")[0] if isinstance(x, str) else x
    )

    # Set multi-index for easier access
    map_df.set_index(["raw_variable", "raw_response"], inplace=True)
    map_df.sort_index(inplace=True)  # for performance improvements

    if not map_df.index.is_unique:
        raise ValueError(
            f"Mapping file for the {resource} resource has duplicate entries "
            f"{map_df.index[map_df.index.duplicated()]}"
        )

    # Generate the flat_like dictionary
    if one_to_one:
        filtered_data["flat_dict"] = filtered_data.apply(
            create_dict_wide, args=[map_df, date_format, timezone], axis=1
        )
        return filtered_data
    else:
        melted_data["flat_dict"] = melted_data.apply(
            create_dict_long, args=[data, map_df, date_format, timezone], axis=1
        )
        return melted_data["flat_dict"].to_frame()


def checksum(file: str) -> str:
    "Calculate the SHA-256 checksum of a file"
    h = hashlib.sha256()
    with open(file, "rb") as fp:
        while True:
            data = fp.read(4096)
            if len(data) == 0:
                break
            h.update(data)
    return h.hexdigest()


def checksum_text(checksums: dict[str, str]) -> str:
    return "\n".join(f"{checksums[k]}  {k}" for k in sorted(checksums)) + "\n"


def generate_metadata(folder_name: str) -> tuple[FlatMetadata, dict[str, str]]:
    "Generate metadata for a FHIRflat folder"

    patient_file = os.path.join(folder_name, "patient.parquet")
    if not os.path.exists(patient_file):
        N = "NA"
    else:
        N = len(pd.read_parquet(patient_file).id.unique())
    if isinstance(N, int):
        assert N > 0, "patient.parquet file is empty"
    checksums = {
        os.path.basename(f): checksum(f) for f in glob(f"{folder_name}/*.parquet")
    }
    m = hashlib.sha256()
    m.update(checksum_text(checksums).encode("utf-8"))

    # write checksums file
    return {
        "N": N,
        "generator": f"fhirflat/{fhirflat.__version__}",
        "checksum": m.hexdigest(),
        "checksum_file": "sha256sums.txt",
    }, checksums


def write_metadata(
    metadata: FlatMetadata, checksums: dict[str, str], metadata_path: Path
):
    metadata_text = f"""[metadata]
N = {metadata['N']}
generator = "{metadata['generator']}"
checksum = "{metadata['checksum']}"
checksum_file = "{metadata['checksum_file']}"
"""
    metadata_path.write_text(metadata_text)
    (metadata_path.parent / "sha256sums.txt").write_text(checksum_text(checksums))


def convert_data_to_flat(
    data: str,
    date_format: str,
    timezone: str,
    folder_name: str = "fhirflat_output",
    mapping_files_types: tuple[dict, dict] | None = None,
    sheet_id: str | None = None,
    subject_id="subjid",
    validate: bool = True,
    compress_format: None | str = None,
    parallel: bool = False,
):
    """
    Takes raw clinical data (currently assumed to be a one-row-per-patient format like
    RedCap exports) and produces a folder of FHIRflat files, one per resource. Takes
    either local mapping files, or a Google Sheet ID containing the mapping files.

    Parameters
    ----------
    data
        The path to the raw clinical data file.
    date_format
        The format of the dates in the data file. E.g. "%Y-%m-%d"
    timezone
        The timezone of the dates in the data file. E.g. "Europe/London"
    folder_name
        The name of the folder to store the FHIRflat files.
    mapping_files_types
        A tuple containing two dictionaries, one with the mapping files for each
        resource type and one with the mapping type (either one-to-one or one-to-many)
        for each resource type.
    sheet_id
        The Google Sheet ID containing the mapping files. The first sheet must contain
        the mapping types - one column listing the resource name, and another describing
        whether the mapping is one-to-one or one-to-many. The subsequent sheets must
        be named by resource, and contain the mapping for that resource.
    subject_id
        The name of the column containing the subject ID in the data file.
    validate
        Whether to validate the FHIRflat files after creation.
    compress_format
        If the output folder should be zipped, and if so with what format.
    parallel
        Whether to parallelize the data conversion over different resources.
    """

    if not mapping_files_types and not sheet_id:
        raise TypeError("Either mapping_files_types or sheet_id must be provided")

    if not validate:
        warnings.warn(
            "Validation of the FHIRflat files has been disabled. ",
            UserWarning,
            stacklevel=2,
        )

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    if mapping_files_types:
        mappings, types = mapping_files_types
    else:
        sheet_link = (
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        )

        df_types = pd.read_csv(sheet_link, header=0, index_col="Resources")
        types = dict(zip(df_types.index, df_types["Resource Type"], strict=True))
        sheet_keys = {r: df_types.loc[r, "Sheet ID"] for r in types.keys()}
        mappings = {
            get_local_resource(r): sheet_link + f"&gid={i}"
            for r, i in sheet_keys.items()
        }

    def convert_resource(
        resource, data, map_file, t, subject_id, date_format, timezone
    ):
        start_time = timeit.default_timer()
        o2o = t == "one-to-one"

        if t in ["one-to-one", "one-to-many"]:
            df = create_dictionary(
                data,
                map_file,
                resource.__name__,
                one_to_one=o2o,
                subject_id=subject_id,
                date_format=date_format,
                timezone=timezone,
            )
            if df is None or df.empty:
                return None
        else:
            raise ValueError(f"Unknown mapping type {t}")

        dict_time = timeit.default_timer()
        print(
            f"creates {resource.__name__} dictionary in " + str(dict_time - start_time)
        )

        flat_nonvalidated = resource.ingest_to_flat(df)

        ingest_time = timeit.default_timer()
        print(f"{resource.__name__} ingestion in " + str(ingest_time - dict_time))

        if validate:
            valid_flat, errors = resource.validate_fhirflat(flat_nonvalidated)

            valid_flat.to_parquet(
                f"{os.path.join(folder_name, resource.__name__.lower())}.parquet"
            )
        else:
            errors = None
            try:
                flat_nonvalidated.to_parquet(
                    f"{os.path.join(folder_name, resource.__name__.lower())}.parquet"
                )
            except ArrowTypeError as e:
                warnings.warn(
                    f"Error writing {resource.__name__.lower()}.parquet: {e}\n"
                    "This is likely due to a validation error, re-run without "
                    "--no-validate.",
                    UserWarning,
                    stacklevel=2,
                )
                return None

        valid_time = timeit.default_timer()
        print(f"{resource.__name__} validation in " + str(valid_time - dict_time))

        end_time = timeit.default_timer()
        total_time = end_time - start_time
        print(
            f"{resource.__name__} took {total_time:.2f} seconds to convert"
            f" {len(df)} rows. "
        )
        if errors is not None:
            errors.to_csv(
                os.path.join(folder_name, f"{resource.__name__.lower()}_errors.csv"),
                index=False,
            )
            error_length = len(errors)
            print(
                f"{error_length} resources not created due to validation errors. "
                f"Errors saved to {resource.__name__.lower()}_errors.csv"
            )

    total_t = timeit.default_timer()
    _ = Parallel(n_jobs=-1 if parallel else 1)(
        delayed(convert_resource)(
            resource,
            data,
            map_file,
            types[resource.__name__],
            subject_id,
            date_format,
            timezone,
        )
        for resource, map_file in mappings.items()
    )

    print(f"Total time: {timeit.default_timer() - total_t}")

    write_metadata(*generate_metadata(folder_name), Path(folder_name) / "fhirflat.toml")
    if compress_format:
        shutil.make_archive(folder_name, compress_format, folder_name)
        shutil.rmtree(folder_name)


def validate(folder_name: str, compress_format: str | None = None):
    """
    Takes a folder containing (optionally compressed) FHIRflat files and validates them
    against the FHIR. File names **must** correspond to the FHIR resource types they
    represent. E.g. a file containing Patient resources must be named "patient.parquet".

    Parameters
    ----------
    folder_name
        The path to the folder containing the FHIRflat files, or compressed file.
    compress_format
        The format to compress the validated files into.
    """

    if Path(folder_name).is_file():
        directory = Path(folder_name).with_suffix("")
        shutil.unpack_archive(folder_name, extract_dir=directory)
    else:
        directory = folder_name

    for file in Path(directory).glob("*.parquet"):
        df = pd.read_parquet(file)
        resource = file.stem
        resource_type = get_local_resource(resource, case_insensitive=True)

        valid_flat, errors = resource_type.validate_fhirflat(df, return_frames=True)

        if errors is not None:

            valid_flat.to_parquet(os.path.join(directory, f"{resource}_valid.parquet"))
            errors.to_csv(
                os.path.join(directory, f"{resource}_errors.csv"), index=False
            )
            error_length = len(errors)
            print(
                f"{error_length} rows in {file.name} have validation errors. "
                f"Errors saved to {resource}_errors.csv. "
                f"Valid rows saved to {resource}_valid.parquet"
            )
        else:
            print(f"{file.name} is valid")
    print("Validation complete")

    if compress_format:
        new_directory = str(directory) + "_validated"
        shutil.make_archive(
            new_directory,
            format=compress_format,
            root_dir=directory,
        )
        shutil.rmtree(directory)
        print(f"Validated files saved as {new_directory}.{compress_format}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert data to FHIRflat parquet files",
        prog="fhirflat transform",
    )
    parser.add_argument("data", help="Data to be transformed")
    parser.add_argument(
        "sheet_id", help="Alphanumeric ID of the Google Sheet containing the mappings"
    )
    parser.add_argument(
        "date_format", help="Date format used within the data, e.g. '%%Y-%%m-%%d'"
    )
    parser.add_argument(
        "timezone", help="Timezone the data is in, e.g. 'Europe/London'"
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Name to use for output folder",
        default="fhirflat_output",
    )
    parser.add_argument(
        "-s",
        "--subject_id",
        help="Column header denoting the subject ID",
        type=str,
        default="subjid",
    )

    parser.add_argument(
        "--no-validate",
        help="Do the data conversion without validation",
        dest="validate",
        action="store_false",
    )

    parser.add_argument(
        "-c",
        "--compress",
        help="Compress the output folder using this format",
        choices=["zip", "tar", "gztar", "bztar", "xztar"],
    )

    parser.add_argument(
        "-p",
        "--parallel",
        help="Parallelize the data conversion over different reosurces",
        action="store_true",
    )

    args = parser.parse_args()

    convert_data_to_flat(
        args.data,
        args.date_format,
        args.timezone,
        folder_name=args.output,
        sheet_id=args.sheet_id,
        subject_id=args.subject_id,
        validate=args.validate,
        compress_format=args.compress,
        parallel=args.parallel,
    )


def validate_cli():
    parser = argparse.ArgumentParser(
        description="Validate FHIRflat parquet files against the FHIR schema",
        prog="fhirflat validate",
    )
    parser.add_argument("folder", help="File path to folder containing FHIRflat files")

    parser.add_argument(
        "-c",
        "--compress_format",
        help="Format to compress the output into",
        choices=["zip", "tar", "gztar", "bztar", "xztar"],
    )

    args = parser.parse_args()

    validate(
        args.folder,
        compress_format=args.compress_format,
    )


if __name__ == "__main__":
    main()
