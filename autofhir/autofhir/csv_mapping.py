"""
Create draft intermediate mapping in CSV from source dataset to target dataset
"""

import os
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List, Union

import tomli
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

from .util import maybe, DEFAULT_CONFIG


def matches_redcap(
    config: Dict[str, Any],
    data_dictionary: Union[pd.DataFrame, str],
    table: str,
    num_matches: int = 6,
    use_descrip=False,
) -> pd.DataFrame:
    column_mappings = {v: k for k, v in config["column_mappings"].items()}
    stopwords = config["lang"]["stopwords"]
    schema = get_fields(config, table)
    scores = config["scores"]

    if isinstance(data_dictionary, str):
        df = pd.read_csv(data_dictionary)
    else:
        df = data_dictionary

    df = df.rename(columns=column_mappings)[list(column_mappings.values())]
    df["description"] = df.description.map(str.strip, na_action="ignore")

    # Drop field types like 'banner' which are purely informative
    _allowed_field_types = config["categorical_types"] + config["text_types"]
    df = df[df.type.isin(_allowed_field_types)]

    # Initial scoring (tf_rank column) using TF-IDF similarity
    vec = TfidfVectorizer(
        stop_words=stopwords,
        max_df=0.9,
        ngram_range=(1, 2),
    )
    properties = [p for p in list(schema["properties"].keys()) if "_id" not in p]
    # # this turns "birth_date" into "birth date"
    # descriptions = [" ".join(attr.split("_")) for attr in properties]

    if use_descrip:
        descriptions = [t["title"] for t in list(schema["properties"].values())]
        propdescrip = [p + " " + d for p, d in zip(properties, descriptions)]
    else:
        propdescrip = [" ".join(attr.split("_")) for attr in properties]

    # Use the data dictionary to set up the vocabulary and do the initial TF-IDF
    # which is used to transform the field names
    X = vec.fit_transform(df.description)
    # Y = vec.transform(descriptions)
    Y = vec.transform(propdescrip)

    # Similarity (this is the tf-idf bit.)
    D = Y.dot(X.T)

    # Keep max 'num_matches' top matches in terms of similarity
    # S is the length of properties in FHIR,
    # each array gives the index of the field in the dd, ranked in order of similarity.

    sorted_indices = np.argsort(D.toarray(), axis=1)[:, ::-1]
    sorted_values = np.take_along_axis(D.toarray(), sorted_indices, axis=1)
    mask = sorted_values != 0
    filtered_indices = np.where(mask, sorted_indices, np.nan)
    # nans are given if no other similarities are found
    S = filtered_indices[:, :num_matches]

    # First draft of match data
    match_df = pd.DataFrame(
        columns=["schema_field", "field", "tf_rank"],
        data=sum(
            [
                [
                    [
                        properties[i],
                        df.iloc[int(k)].field,
                        num_matches - j,
                    ]
                    for j, k in enumerate(S[i])
                    if not np.isnan(k)
                ]
                for i in range(len(propdescrip))
            ],
            [],
        ),
    )
    match_df["table"] = table

    # If no matches are found, we still want to return a file with all the target fields
    if match_df.empty:
        match_df["schema_field"] = properties
        return match_df

    # Merge data dictionary into match_df for further scoring
    match_df = match_df.merge(df, on="field")

    # We can return match_df here, but we can put in some manual hints for
    # refining tf_rank, based on matching field types:
    #
    # score=3 for type match (dropdown/radio == booleans/enums, text otherwise)
    # score=3 for both being dates
    # score=-3 for only one of the fields being a date, extremely unlikely match
    # score=1 for every token that is not a stopword appearing in the source field
    #  / description

    # scoring using pre-defined rules
    def scorer(row) -> int:
        "Returns a score for a match row"
        score = 1  # default score, every match gets this
        attributes = schema["properties"][row["schema_field"]]
        T = attributes.get("type")  # PL: How is this type matching working?
        if T == "boolean":
            score += (
                scores["type-match"]
                if row["type"] in ["dropdown", "radio", "yesno"]
                else scores["type-mismatch"]
            )
        if (
            (T == "string" and row["type"] == "text")
            or (T == "number" and row["type"] == "decimal")
            or ("enum" in attributes and row["type"] == "categorical")
            or T == row["type"] == "integer"
        ):
            score += scores["type-match"]
        if attributes.get("format") == "date":
            score += (
                scores["type-match"]
                if ("date_" in str(row.get("valid_type", "")) or T == "date")
                else scores["date-mismatch"]
            )
        if (
            "follow" in row["category"]
        ):  # de-emphasise followup, usually only required in observation
            score += scores["is-followup"]
        words = row["schema_field"].split("_")
        score += sum(
            w in row["field"] + " " + row["description"]
            for w in set(words) - set(stopwords)
        )
        return score * row["tf_rank"]

    match_df.insert(match_df.columns.get_loc("tf_rank") + 1, "score", "")
    match_df["score"] = match_df.apply(scorer, axis=1)

    if use_descrip:
        match_df.insert(1, "schema_description", "")
        match_df["schema_description"] = match_df.apply(
            lambda x: schema["properties"][x["schema_field"]]["title"], axis=1
        )
    return match_df.sort_values(["schema_field", "score"], ascending=[True, False])


def read_json(file: str) -> Dict:
    with (Path(__file__).parent / file).open() as fp:
        return json.load(fp)


def get_fields(config: Dict[str, Any], table: str) -> List[str]:
    # TODO: aim to generate these on demand from the FHIRflat resources
    schemas = config.get("schemas", [])
    if table not in schemas:
        raise ValueError(f"Schema not found for table: {table}")
    return read_json(config["schema-path"] / schemas[table])


def generate_csv(args):
    with maybe(args.config, Path, default=Path(__file__).parent / DEFAULT_CONFIG).open(
        "rb"
    ) as fp:
        config = tomli.load(fp)
        config["schema-path"] = (
            maybe(args.schema_path, Path)
            or maybe(os.getenv("ISARIC_SCHEMA_PATH"), Path)
            or Path.cwd()
        )
    tables = args.tables.split(",") if args.tables else config["schemas"].keys()
    for table in tables:
        df = matches_redcap(
            config,
            args.dictionary,
            table,
            num_matches=args.num_matches,
        )
        df.to_csv(f"{args.output}-{table}.csv", index=False)


def generate_csv_local(config_path, data_dict, use_descrip=False):
    config = tomli.load(open(config_path, "rb"))
    config["schema-path"] = Path(config["schema-path"])
    tables = config["schemas"].keys()
    resources = {}
    for table in tables:
        df = matches_redcap(
            config, data_dict, table, num_matches=6, use_descrip=use_descrip
        )
        resources[table] = df
    return resources


def main():
    parser = argparse.ArgumentParser(
        description="Generate intermediate CSV used by make_toml.py to create TOML"
    )
    parser.add_argument("dictionary", help="Data dictionary to use")
    parser.add_argument(
        "-o", "--output", help="Name to use for output files", default="isaric"
    )
    parser.add_argument(
        "-n",
        "--num_matches",
        help="Number of matches to output for each field",
        type=int,
        default=6,
    )
    parser.add_argument("--schema-path", help="Path where ISARIC schemas are located")
    parser.add_argument(
        "-t", "--tables", help="Only match for tables (comma separated list, no spaces)"
    )
    parser.add_argument(
        "-c", "--config", help=f"Configuration file to use (default={DEFAULT_CONFIG})"
    )
    args = parser.parse_args()
    generate_csv(args)


if __name__ == "__main__":
    main()
