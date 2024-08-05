# How to convert clinical data into FHIRflat

fhirflat can be used from the command line, if you wish solely to transform your raw
data into FHIRflat files, or as a python library.

## Command line

```bash
fhirflat transform data-file google-sheet-id date-format timezone-name
```

Here *data-file* data file that fhirflat will transform, and *google-sheet-id* is the unique
ID of the google sheet containing the mapping information (found in the url; the format
if usually https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id},
you want the spreadsheet_id. The sheet has to be public, i.e. share settings must be set
to 'Anyone with the link' for this to work). *date-format* is the format dates follow in
the raw data, e.g. a "2020-04-20" date has a date format of "%Y-%m-%d", and *timezone*
is the time zone the data was recorded in, e.g. "America/New_York". A full list of
timezones can be found [here](https://nodatime.org/timezones).

Further information on the structure of the mapping file can be found
[in the specification](../spec/mapping.md)

## Library

The equivalent function to the CLI described above can be used as

```
fhirflat.convert_data_to_flat("data_file_path", "sheet_id", "%Y-%m-%d", "Brazil/East")
```

## Conversion without validation

If you wish to convert your data into FHIRflat, but not perform validation to check the
converted data conforms to the FHIR spec, you can add the `--no-validate` flag:

```bash
fhirflat transform data-file google-sheet-id date-format timezone-name --no-validate
```

The equivalent library function is
```python
fhirflat.convert_data_to_flat(<data_file_path>, <sheet_id>, <date_format>, <timezone>, validate=False)
```

We strongly recommend you don't do this unless necessary for time constraints; some
errors in conversion can cause the parquet file to fail to save (e.g. if columns contain
mixed types due to errors which would be caught during validation).

Data which is already in a FHIRflat format can be validated against the schema using

```bash
fhirflat validate <folder_name>
```

where `folder_name` is the path to the folder containing your flat files. The files **must**
be named according to the corresponding FHIR resource, e.g. the folder containing flat
Encounter data must be named `encounter.parquet`.

The folder can be provided in a compressed format, e.g. zipped; you can specifiy this
using
```bash
fhirflat validate <folder_name> -c "zip"
```

The output folder of validated data will be compressed using the same format.

The equivalent library function is

```python
fhirflat.validate(<folder_name>, compress_format="zip")
```
