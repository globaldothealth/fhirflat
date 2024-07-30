# FHIRflat

[![](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![tests](https://github.com/globaldothealth/adtl/actions/workflows/tests.yml/badge.svg)](https://github.com/globaldothealth/fhirflat/actions/workflows/tests.yml)
[![docs](https://readthedocs.org/projects/fhirflat/badge/)](https://fhirflat.readthedocs.io)
[![codecov](https://codecov.io/gh/globaldothealth/fhirflat/graph/badge.svg?token=AINU8PNJE3)](https://codecov.io/gh/globaldothealth/fhirflat)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

fhirflat is a library for transforming FHIR resources in NDJSON or native Python
dictionaries to a flat structure that can be written to a Parquet file.

fhirflat is a **prototype library**, subject to **major revisions** that is used
for the [ISARIC](https://isaric.org) 3.0 project and data pipelines. Portions of
the code are specific to the ISARIC project, such as ISARIC specific FHIR
extensions.

The FHIRflat FHIR resources are derived from the
[fhir.resources](https://github.com/nazrulworld/fhir.resources) package.

For more information, howtos and tutorials, see the
[**documentation**](https://fhirflat.readthedocs.io).

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

## Development

To test and develop fhirflat, from a cloned version of fhirflat use an editable install
including the development dependencies(`pip install -e ".[dev]"`). This will allow you
to test the packages, and installs formatting and linting tools, and
[pre-commit](https://pre-commit.com).

Setup pre-commit hooks (`pre-commit install`) which will do linting checks before commit.
