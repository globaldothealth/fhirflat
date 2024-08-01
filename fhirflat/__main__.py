import sys

from .ingest import main as ingest_to_flat
from .ingest import validate_cli as validate


def main():
    if len(sys.argv) < 2:
        print(
            """fhirflat: specify subcommand to run

                Available subcommands:
                transform - Convert raw data into FHIRflat files
                validate - Validate FHIRflat files against FHIR schemas
            """
        )
        sys.exit(1)
    subcommand = sys.argv[1]
    if subcommand not in ["transform", "validate"]:
        print("fhirflat: unrecognised subcommand", subcommand)
        sys.exit(1)
    sys.argv = sys.argv[1:]
    if subcommand == "transform":
        ingest_to_flat()
    elif subcommand == "validate":
        validate()
    else:
        pass


if __name__ == "__main__":
    main()
