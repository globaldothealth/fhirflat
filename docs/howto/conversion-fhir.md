# How to convert between FHIR and FHIRflat

FHIR resources can be initialised using a data dictionary:
```python
from fhirflat import Patient
data= {
    "id": "f001",
    "name": [{"text": "Micky Mouse"}],
    "gender": "male",
    "deceasedBoolean": False,
    "address": [{"country": "Switzerland"}],
    "birthDate": "1996-05-30",
}
patient = Patient(**data)
```

or in bulk from a FHIR export as an .ndjson file.
```
from fhirflat import Patient

patients = Patient.fhir_bulk_import("patient_export.ndjson")
```

## To FHIRflat
Once initialised, FHIR resources can be transformed to FHIRflat files using the `to_flat()` function like this
```python
patient.to_flat("patient_flat.parquet")
```
which produces a [parquet file](https://towardsdatascience.com/demystifying-the-parquet-file-format-13adb0206705) which can be read in pandas, producing a dataframe with the following structure
| resourceType | id   | gender | birthDate  | deceasedBoolean |
|--------------|------|--------|------------|-----------------|
| Patient      | f001 | male   | 1996-05-30 | False           |

or a FHIRflat file can be generated directly from a FHIR .ndjson export file.
```python
from fhirflat import Patient

Patient.fhir_file_to_flat("patient_export.ndjson")
```
creates a "patient_export.parquet" FHIRflat file.
This first initialises a `Patient` data class for each row to make use of the Pydantic
data validation, then creates a FHIRflat file.

## From FHIRflat
FHIR resources can also be created directly from FHIRflat files
```
Patient.from_flat("patient_flat.parquet")
```
which returns either a single Patient resource, or a list of Patient resources if
the Parquet file contains multiple rows of data.
