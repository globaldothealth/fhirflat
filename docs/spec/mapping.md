# Mapping file specification

The mapping file enables **transformation of raw data** into FHIRflat format.
Mapping is usually done using a Google Sheet, though using a local file is also
possible.

Qualifiers for requirement levels (MUST, SHOULD) are interpreted as in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

Once a mapping file or a Google Sheet is prepared, the
[`fhirflat.convert_data_to_flat()`](../fhirflat.rst) function can be used to
transform the source data into a folder of FHIRflat resources in parquet format.

```{note}
The usual location for a mapping file is a Google Sheet, which has a URL like
`https://docs.google.com/spreadsheets/d/{SHEET_URL}/edit`
```

## Metadata

The first sheet of the Google Sheets document MUST be a table with the following
column headers: `Resources` [cell A1], `Sheet ID` [cell B1] and `Resource Type`
[cell C1].

Cell A2 MUST be `=getAllSheetNames()` which populates the column with the names
of the other sheets. Cell B2 MUST be `=getOtherSheetIDs()` which populates the
column with the sheet IDs of the other sheets.

The rest of the sheets MUST be named according to the FHIR resource they are
mapping, e.g. `Patient`, `Encounter`.

The last column, `Resource Type` indicates whether the mapping type and MUST be
one of these types:

- `one-to-one`: Information in the mapping sheet pertains to one record of the
  corresponding FHIRflat resource. One row in the raw data generates a single
  resource.
- `one-to-many`: Information in the mapping sheet pertains to multiple records
  of the corresponding FHIRflat resource. One row in the raw data generates
  multiple resources.

## FHIRflat resource mapping sheet

Each resource mapping sheet MUST be named the same name as the corresponding FHIRflat resource.

These columns are mandatory:
- `raw_variable`: Name of column in raw data to be transformed
- `raw_response`: Assignment to `raw_variable` for which the transformation rule
  should be applied. The `raw_response` entry MUST be of the following pattern
  `response, comment` where `response` MAY be an integer and `comment` is free
  text, separated by a comma. For example if `raw_variable` is `gender` then
  `raw_response` could be `2, Female` which indicates that the transformation
  rule corresponding to this row should be invoked when `gender=2`.

**Rules**: Each row in a resource mapping sheet represents a transformation
rule, invoked when the source entry for the variable in `raw_variable`
matches that in `raw_response`. The transformation target variables are the rest
of the columns in the mapping sheet.

Successive `raw_response` entries corresponding to the same `raw_variable` MAY
keep the corresponding `raw_variable` entry blank, example:

| raw_variable | raw_response |   gender.code   | gender.system          |
|--------------|--------------|-----------------|-------------------------
| demog_gender | 1, Man       | 446151000124109 | http://snomed.info/sct |
|              | 2, Woman     | 446141000124107 | http://snomed.info/sct |
|              | 99, Unknown  |                 |                        |

### Column headers

The rest of the column headers MUST correspond to assignments to FHIRflat
variables. Nested data in FHIR are represented as flattened versions in
FHIRflat, with the parent and child fields joined by a period (`.`):

```json
"gender": {
    "system": "http://snomed.info/sct",
    "code": 446141000124107
}
```
would flatten to
```json
{
    "gender.system": "http://snomed.info/sct",
    "gender.code": 446141000124107
}
```

[ISARIC specific FHIR extensions](isaric-fhir-extensions.rst) MUST be specified
with an `extension.` prefix as required in the FHIR standard.

### Column values

Values in a column (other than `raw_response` and `raw_variable`) are constants
(such as the preceding example) or refer to column names in the raw data. Column
names MUST be delimited by angle brackets, such as `<column>`.
Concatenation with constants or other field names uses the plus (`+`)
operator. The special column name `<FIELD>` refers to the value in the column
referred to by `raw_variable`.

Examples:
- For a patient ID field, `raw_variable = subjid` and `id = Patient/+<FIELD>`
  which assigns the value of the subject ID to the `id` column in FHIRflat with
  a namespace prefix.
- To combine date of admission and time of admission, `raw_variable =
  dates_admdate`, and `actualPeriod.start = <FIELD>+<dates_admtime>`.

#### Conditional assignments

Conditional assignments are made using the **`if not`** statement. A value of
`<A> if not <B>` selects field A if B is blank.

### Creating list assignments

FHIR resource entries can be lists, such as a set of codes. This is handled in
the mapping specification by allowing multiple matches for a (`raw_variable`,
`raw_response`) tuple. The corresponding resource assignments (such as
`coding.code`) are then collected into lists.
