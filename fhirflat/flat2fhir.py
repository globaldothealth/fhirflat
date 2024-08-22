# Converts FHIRflat files into FHIR resources
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.domainresource import DomainResource as _DomainResource
from fhir.resources.fhirprimitiveextension import FHIRPrimitiveExtension
from fhir.resources.period import Period
from fhir.resources.quantity import Quantity
from pydantic.v1.error_wrappers import ValidationError

from .resources.extensions import _ISARICExtension
from .util import (
    find_data_class_options,
    get_fhirtype,
    get_local_extension_type,
    group_keys,
    json_type_matching,
)


def step_down(data: dict) -> dict:
    """
    Splits column names on the first '.' to step 'down' one level into the nested data.

    Parameters
    ----------
    data
        {
        "timingPhaseDetail.timingPhase.code": ["http://snomed.info/sct|281379000"],
        "timingPhaseDetail.timingPhase.text": ["pre-admission"],
        }

    Returns
    -------
    dict
        {
        "timingPhase.code": ["http://snomed.info/sct|281379000"],
        "timingPhase.text": ["pre-admission"],
        }
    """
    return {s.split(".", 1)[1]: data[s] for s in data}


def create_codeable_concept(
    old_dict: dict[str, list[str] | str | float | None], name: str
) -> dict[str, list[str]]:
    """
    Re-creates a codeableConcept structure from the FHIRflat representation.

    Parameters
    ----------
    old_dict
        The dictionary containing the flattened codings and text. E.g.,
        {"bodySite.code": ["SNOMED-CT|123456"], "bodySite.text": "Left arm"}
    name
        The base name of the data, e.g. "bodySite"

    Returns
    -------
    dict
        The FHIR representation of the codeableConcept. E.g.,
        {
        "bodySite": {
            "coding": [{"system": "SNOMED-CT", "code": "123456", "display": "Left arm"}]
            }
        }
    """

    # for creating backbone elements
    if name + ".code" in old_dict and name + ".system" in old_dict:
        raw_codes: str | float | list[str | None] = old_dict.get(name + ".code")
        if raw_codes is not None and not isinstance(raw_codes, list):
            formatted_code = (
                raw_codes if isinstance(raw_codes, str) else str(int(raw_codes))
            )
            codes = [old_dict[name + ".system"] + "|" + formatted_code]
        elif not raw_codes:
            codes = raw_codes
        else:
            formatted_codes = [
                c if (isinstance(c, str) or c is None) else str(int(c))
                for c in raw_codes
            ]
            codes = [
                s + "|" + c
                for s, c in zip(
                    old_dict[name + ".system"], formatted_codes, strict=True
                )
            ]
    else:
        # From FHIRflat file
        codes = old_dict.get(name + ".code")

    if codes is None:
        return {
            "text": (
                old_dict[name + ".text"][0]
                if isinstance(old_dict[name + ".text"], list)
                else old_dict[name + ".text"]
            )
        }

    if len(codes) == 1:
        system, code = codes[0].split("|")
        display = (
            old_dict[name + ".text"][0]
            if isinstance(old_dict[name + ".text"], list)
            else old_dict[name + ".text"]
        )
        new_dict = {"coding": [{"system": system, "code": code, "display": display}]}
    elif not codes:
        display = (
            old_dict[name + ".text"][0]
            if isinstance(old_dict[name + ".text"], list)
            else old_dict[name + ".text"]
        )
        new_dict = {"coding": [{"display": display}]}
    else:
        new_dict = {"coding": []}
        for cd, nme in zip(codes, old_dict[name + ".text"], strict=True):
            system, code = cd.split("|")
            display = nme

            subdict = {"system": system, "code": code, "display": display}

            new_dict["coding"].append(subdict)

    return new_dict


def create_quantity(df: dict, group: str) -> dict:
    """
    Re-creates a Quantity structure from the FHIRflat representation.
    Ensures that any flattened codes are correctly unpacked.

    Parameters
    ----------
    df
        The dictionary containing the flattened quantity data. E.g.,
        {
            "doseQuantity.value": 5,
            "doseQuantity.code": "http://unitsofmeasure.org|mg"
        }
    group
        The base name of the data, e.g. "doseQuantity"

    Returns
    -------
    dict
        The FHIR representation of the quantity. E.g.,
        {"value": 5, "system": "http://unitsofmeasure.org", "code": "mg"}
    """

    quant = {}

    for attribute in df.keys():
        attr = attribute.split(".")[-1]
        if attr == "code":
            if group + ".system" in df.keys():
                # reading in from ingestion pipeline
                quant["code"] = df[group + ".code"]
                quant["system"] = df[group + ".system"]
            else:
                system, code = df[group + ".code"].split("|")
                quant["code"] = code
                quant["system"] = system
        else:
            quant[attr] = df[group + "." + attr]

    return quant


def create_single_extension(k: str, v: dict | str | float | bool) -> dict:
    """
    Creates a single ISARIC extension, by inferring the datatype from the value.
    Nested extensions aren't dealt with here, they're found in 'create_extension'.

    Parameters
    ----------
    k
        The key of the data, e.g. "approximateDate", "birthSex", "timingDetail"
    v
        The value of the data, e.g.
        "month 3",
        {"code": ["http://snomed.info/sct|1234"], "text": ["female"]},
        {"low.value": -7, "low.unit": "days", "high.value": 0, "high.unit": "days"}

    Returns
    -------
    dict
        The formatted data, e.g.,
        {'url': 'approximateDate', 'valueString': 'month 3'} \
        {
            "url": "birthSex",
            "valueCodeableConcept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "1234",
                        "display": "female",
                    }
                ]
            },
        } \
        {
            "url": "timingDetail",
            "valueRange": {
                "low": {"value": -7, "unit": "days"},
                "high": {"value": 0, "unit": "days"},
            },
        }

    """

    klass = get_local_extension_type(k)

    prop = klass.schema()["properties"]
    value_type = [key for key in prop.keys() if key.startswith("value")]

    if not value_type:  # pragma: no cover
        raise RuntimeError("Inappropriate entry into create_single_extension")

    for v_type in value_type:
        data_type = prop[v_type]["type"]
        try:
            data_class = get_fhirtype(data_type)
            # unpack coding etc
            if isinstance(v, dict) and len(group_keys(v.keys())) > 1:
                # if there are still groups to organise, e.g. valueRange
                new_dict = expand_concepts(v, data_class)
            elif isinstance(v, dict):
                # single group needs formatting, e.g. valueCodeableConcept
                # requires the format to include the k in the dict names
                v_appended = {f"{k}.{ki}": vi for ki, vi in v.items()}
                new_dict = set_datatypes(k, v_appended, data_class)
            else:
                # standard json type, e.g. valueInteger
                new_dict = v

            try:
                data_class.parse_obj(new_dict)
                return {"url": k, f"{v_type}": new_dict}
            except ValidationError:
                continue
        except AttributeError as e:
            # should be a standard json type as a string
            if isinstance(v, json_type_matching(data_type)):
                try:
                    klass.parse_obj({"url": k, f"{v_type}": v})
                    return {"url": k, f"{v_type}": v}
                except ValidationError:
                    continue
            else:
                raise e  # pragma: no cover

    raise RuntimeError(f"extension not created from {k, v}")  # pragma: no cover


def create_extension(k: str, v_dict: dict, klass: _ISARICExtension) -> dict:
    """
    Formats ISARIC extensions into the correct FHIR structure, while finding the correct
    value type for the data.
    Can handle both nested and simple extensions.

    Parameters
    ----------
    k
        The key of the data, e.g. "timingPhaseDetail"
    v_dict
        The value of the data, e.g.
            {
                "timingDetail.high.unit": "days",
                "timingDetail.high.value": 0.0,
                "timingDetail.low.unit": "days",
                "timingDetail.low.value": -7.0,
                "timingPhase.code": ["http://snomed.info/sct|281379000"],
                "timingPhase.text": ["pre-admission"],
            }
    klass
        The class of the data, e.g. <timingPhaseDetail>.

    Returns
    -------
    dict
        The formatted data, e.g.
        {
            "url": "timingPhaseDetail",
            "extension": [
                {
                    "url": "timingDetail",
                    "valueRange": {
                        "low": {"value": -7, "unit": "days"},
                        "high": {"value": 0, "unit": "days"},
                    },
                },
                {
                    "url": "timingPhase",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "281379000",
                                "display": "pre-admission",
                            }
                        ]
                    },
                },
            ],
        }
    """

    if klass.nested_extension:
        classes = find_data_class_options(klass, "extension")
        short_extensions = [s for s in v_dict.keys() if s.count(".") == 0]
        expanded_short_extensions = []
        if short_extensions:
            # these get skipped over in expand_concepts because they don't get grouped
            # so have to be dealt with here
            for se in short_extensions:
                short_ext_dict = {se: v_dict[se]}
                expanded_short_extensions.append(
                    create_single_extension(se, short_ext_dict[se])
                )
                v_dict.pop(se)
        return {
            "url": k,
            "extension": list(expand_concepts(v_dict, classes).values())
            + expanded_short_extensions,
        }

    return create_single_extension(k, v_dict)


def set_datatypes(k: str, v_dict: dict, klass: type[_DomainResource]) -> dict:
    """
    Once the final datatype is found, this function formats the data into the correct
    FHIR structure.

    Parameters
    ----------
    k
        The key of the data, e.g. "bodySite"
    v_dict
        The value of the data, e.g.
        {"bodySite.code": ["SNOMED-CT|123456"], "bodySite.text": "Left arm"}
    klass
        The class of the data, e.g. Quantity, CodeableConcept. Should be present in
        either this library, or the fhir.resources module.

    Returns
    -------
    dict
        The formatted data, e.g.
        {
        "bodySite": {
            "coding": [{"system": "SNOMED-CT", "code": "123456", "display": "Left arm"}]
            }
        }
    """

    if klass == Quantity:
        return create_quantity(v_dict, k)
    elif klass == CodeableConcept:
        return create_codeable_concept(v_dict, k)
    elif klass == Period:
        return {"start": v_dict.get(k + ".start"), "end": v_dict.get(k + ".end")}
    elif issubclass(klass, FHIRPrimitiveExtension):
        stripped_dict = step_down(v_dict)
        return {
            "extension": [
                create_single_extension(ki, vi) for ki, vi in stripped_dict.items()
            ],
        }
    elif issubclass(klass, _ISARICExtension):
        if klass.nested_extension:
            stripped_dict = step_down(v_dict)
            return {
                "url": k,
                "extension": [
                    create_single_extension(ki, vi) for ki, vi in stripped_dict.items()
                ],
            }

        return create_single_extension(k, step_down(v_dict))

    return step_down(v_dict)


def expand_concepts(data: dict[str, dict], data_class: type[_DomainResource]) -> dict:
    """
    Combines columns containing flattened FHIR concepts back into
    JSON-like structures.
    """

    groups = group_keys(data.keys())
    group_classes = {}

    for k in groups.keys():
        group_classes[k] = find_data_class_options(data_class, k)

    expanded = {}
    keys_to_replace = []

    for k, v in groups.items():
        is_single_fhir_extension = not isinstance(
            group_classes[k], list
        ) and issubclass(group_classes[k], _ISARICExtension)
        keys_to_replace += v
        v_dict = {k: data[k] for k in v}
        # step into nested groups
        if any(s.count(".") > 1 for s in v):
            # strip the outside group name
            stripped_dict = step_down(v_dict)
            if not is_single_fhir_extension:
                # call recursively
                new_v_dict = expand_concepts(stripped_dict, data_class=group_classes[k])
                # add outside group key back on
                v_dict = {f"{k}." + old_k: v for old_k, v in new_v_dict.items()}
            elif is_single_fhir_extension:
                # column name will be missing one or more datatype layers, e.g.
                # valueString, valueRange that need to be inferred
                expanded[k] = create_extension(k, stripped_dict, group_classes[k])
                continue

        if all(isinstance(v, dict) for v in v_dict.values()):
            # coming back out of nested recursion
            expanded[k] = step_down(v_dict)

        elif any(isinstance(v, dict) for v in v_dict.values()) and isinstance(
            group_classes[k], list
        ):
            # extensions, where some classes are just values and others have codes etc
            non_dict_items = {
                k: v for k, v in v_dict.items() if not isinstance(v, dict)
            }
            stripped_dict = step_down(non_dict_items)
            for k1, v1 in stripped_dict.items():
                v_dict[k + "." + k1] = create_single_extension(k1, v1)

            expanded[k] = step_down(v_dict)

        else:
            expanded[k] = set_datatypes(k, v_dict, group_classes[k])

        if isinstance(data_class, list):
            continue
        elif data_class.schema()["properties"][k].get("type") == "array":
            if k == "extension":
                expanded[k] = list(expanded[k].values())
            else:
                expanded[k] = [expanded[k]]

    dense_cols = {
        k: k.removesuffix("_dense") for k in data.keys() if k.endswith("_dense")
    }
    if dense_cols:
        for old_k, new_k in dense_cols.items():
            data[new_k] = data[old_k]
            del data[old_k]

    for k in keys_to_replace:
        data.pop(k)
    data.update(expanded)
    return data
