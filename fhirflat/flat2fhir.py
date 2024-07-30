# Converts FHIRflat files into FHIR resources
from fhir.resources.backbonetype import BackboneType as _BackboneType
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.datatype import DataType as _DataType
from fhir.resources.domainresource import DomainResource as _DomainResource
from fhir.resources.fhirprimitiveextension import FHIRPrimitiveExtension
from fhir.resources.period import Period
from fhir.resources.quantity import Quantity
from pydantic.v1.error_wrappers import ValidationError

from .util import find_data_class, get_fhirtype, get_local_extension_type, group_keys


def create_codeable_concept(
    old_dict: dict[str, list[str] | str | float | None], name: str
) -> dict[str, list[str]]:
    """Re-creates a codeableConcept structure from the FHIRflat representation."""

    # for reading in from ingestion pipeline
    if name + ".code" in old_dict and name + ".system" in old_dict:
        raw_codes: str | float | list[str | None] = old_dict.get(name + ".code")
        if raw_codes is not None and not isinstance(raw_codes, list):
            formatted_code = (
                raw_codes if isinstance(raw_codes, str) else str(int(raw_codes))
            )
            codes = [old_dict[name + ".system"] + "|" + formatted_code]
        elif raw_codes is None:
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


def createQuantity(df, group):
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


def createExtension(exts: dict):
    """
    Searches through the schema of the extensions to find the correct datatype

    Covers the scenario where there is a list of extensions,e.g.
    [{'type': 'approximateDate'}, {'type': 'relativeDay'}, {'type': 'Extension'}]
    and finds the appropriate class for the data provided.

    Args:
    exts: dict
        e.g. {"relativeDay": 3, "approximateDate": "month 6"}
    """

    extensions = []

    extension_classes = {e: get_local_extension_type(e) for e in exts.keys()}

    for e, v in exts.items():
        properties = extension_classes[e].schema()["properties"]
        data_options = [key for key in properties.keys() if key.startswith("value")]
        if len(data_options) == 1:
            extensions.append({"url": e, data_options[0]: v})
        else:
            for opt in data_options:
                try:
                    extension_classes[e](**{opt: v})
                    extensions.append({"url": e, opt: v})
                    break
                except ValidationError:
                    continue

    return extensions


def set_datatypes(k, v_dict, klass) -> dict:
    if klass == Quantity:
        return createQuantity(v_dict, k)
    elif klass == CodeableConcept:
        return create_codeable_concept(v_dict, k)
    elif klass == Period:
        return {"start": v_dict.get(k + ".start"), "end": v_dict.get(k + ".end")}
    elif issubclass(klass, FHIRPrimitiveExtension):
        return {
            "extension": createExtension(
                {s.split(".", 1)[1]: v_dict[s] for s in v_dict}
            ),
        }
    elif issubclass(klass, _DataType) and not issubclass(klass, _BackboneType):
        # not quite
        prop = klass.schema()["properties"]
        value_type = [key for key in prop.keys() if key.startswith("value")]
        if not value_type:
            # nested extension
            return {
                "url": k,
                "extension": createExtension(
                    {s.split(".", 1)[1]: v_dict[s] for s in v_dict}
                ),
            }

        data_type = prop[value_type[0]]["type"]
        try:
            data_class = get_fhirtype(data_type)
            return {"url": k, f"{value_type[0]}": set_datatypes(k, v_dict, data_class)}
        except AttributeError:
            # datatype should be a primitive
            return {"url": k, f"{value_type[0]}": v_dict[k]}

    return {s.split(".", 1)[1]: v_dict[s] for s in v_dict}


def expand_concepts(data: dict[str, str], data_class: type[_DomainResource]) -> dict:
    """
    Combines columns containing flattened FHIR concepts back into
    JSON-like structures.
    """

    groups = group_keys(data.keys())
    group_classes = {}

    for k in groups.keys():
        group_classes[k] = find_data_class(data_class, k)

    expanded = {}
    keys_to_replace = []
    for k, v in groups.items():
        keys_to_replace += v
        v_dict = {k: data[k] for k in v}
        if any(s.count(".") > 1 for s in v):
            # strip the outside group name
            stripped_dict = {s.split(".", 1)[1]: v_dict[s] for s in v}
            # call recursively
            new_v_dict = expand_concepts(stripped_dict, data_class=group_classes[k])
            # add outside group key back on
            v_dict = {f"{k}." + old_k: v for old_k, v in new_v_dict.items()}

        if all(isinstance(v, dict) for v in v_dict.values()):
            # coming back out of nested recursion
            expanded[k] = {s.split(".", 1)[1]: v_dict[s] for s in v_dict}

        elif any(isinstance(v, dict) for v in v_dict.values()) and isinstance(
            group_classes[k], list
        ):
            # extensions, where some classes are just values and others have codes etc
            non_dict_items = {
                k: v for k, v in v_dict.items() if not isinstance(v, dict)
            }
            stripped_dict = {
                s.split(".", 1)[1]: non_dict_items[s] for s in non_dict_items.keys()
            }
            for k1, v1 in stripped_dict.items():
                klass = find_data_class(group_classes[k], k1)
                v_dict[k + "." + k1] = set_datatypes(k1, {k1: v1}, klass)

            expanded[k] = {s.split(".", 1)[1]: v_dict[s] for s in v_dict}

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
