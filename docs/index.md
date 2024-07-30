# FHIRflat -- flat file structure library for FHIR resources

FHIRflat is a library for transforming FHIR resources in NDJSON or native Python
dictionaries to a flat structure that can be written to a Parquet file.

```{warning}
FHIRflat is a **prototype library**, subject to **major revisions** that is used
for the [ISARIC](https://isaric.org) 3.0 project and data pipelines. Portions of
the code are specific to the ISARIC project, such as ISARIC specific FHIR
extensions.
```

```{note}
This library is used to transform source data into the FHIRflat format.
To analyse data, there is a companion library: [polyflame](https://polyflame.readthedocs.io)
```
## Motivation

We are working on a reproducible analytical pipeline (RAP) that converts raw
data using a mapping file to the [FHIR R5](https://hl7.org/fhir/R5/) standard
along with ISARIC specific extensions. FHIR is generally stored in a database
with a specialised application (a FHIR server) serving resources. While this is
useful for developing applications, it is less so for developing RAP which
requires snapshots of the source data and pipeline to ensure reproducibility.

Converting FHIR resources (approximately tables in a database) to a flat file format both makes it possible to:
1. Utilise checksums to guarantee reproducibility and guard against data
   corruption.
2. Enable use in low resource settings, by not  requiring specialised
   infrastructure. Most datasets are not large, and by not requiring backend
   infrastructure, researchers can work with data pipelines locally.

## Components

FHIRflat comprises of these components:

- ISARIC specific **FHIR resources** in the
  [`fhirflat.resources`](resources.rst) library, that include
  [ISARIC specific extensions](spec/isaric-fhir-extensions.rst).
- **CONVERSION** modules `fhirflat.fhir2flat` and `fhirflat.flat2fhir` that
  implement conversion to and from FHIRflat to a nested FHIR resource. These do
  not need to be directly accessed, as they are exposed as `to_flat()` and
  `from_flat()` in [`FHIRFlatBase`](resources_base.rst) which all FHIR resources
  derive from.
- **TRANSFORMATION** from raw data to FHIRflat format using a mapping file or a
  mapping Google sheet that follows the [mapping
  specification](spec/mapping.md).

Schematic diagram:
```
                                                                        ┌────────────┐
         EXTENSION                  CONVERSION          TRANSFORMATION  │            │
                                                                   ┌────│  Raw data  │
                                                                   │    │            │
┌─────────┐      ┌─────────────┐     fhirflat.    ┌────────────┐   │    └────────────┘
│  FHIR   │      │ ISARIC FHIR │     fhir2flat    │            │◀──┘
│  fhir.  │─────▶│  fhirflat.  │◀────────────────▶│  FHIRflat  │
│resources│      │  resources  │     fhirflat.    │            │◀──┐    ┌────────────┐
└─────────┘      └─────────────┘     flat2fhir    └────────────┘   │    │            │
                                                                   │    │  Mapping   │
                                                                   └────│   sheet    │
                                                                        │            │
                                                                        └────────────┘
```

## Installing

You can use `pip` to install `fhirflat`:
```
pip install git+https://github.com/globaldothealth/fhirflat
```

If you are using `requirements.txt`, then add this to your file and rerun `pip
install -r requirements.txt`:
```
https://github.com/globaldothealth/fhirflat/main.tar.gz
```
