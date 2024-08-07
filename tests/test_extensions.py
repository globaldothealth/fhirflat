import pytest
import datetime
from fhir.resources.extension import Extension
from fhir.resources.datatype import DataType
from fhir.resources.fhirprimitiveextension import FHIRPrimitiveExtension
from fhir.resources.codeableconcept import CodeableConcept as _CodeableConcept
from fhir.resources.quantity import Quantity as _Quantity
from fhirflat.resources.extensions import (
    timingPhase,
    timingDetail,
    timingPhaseDetail,
    relativeDay,
    relativeStart,
    relativeEnd,
    relativePeriod,
    approximateDate,
    Duration,
    dateTimeExtension,
)
from pydantic.v1.error_wrappers import ValidationError

timing_phase_data = {
    "url": "timingPhase",
    "valueCodeableConcept": {
        "coding": [
            {
                "system": "http://snomed.info/sct",
                "code": "307168008",
                "display": "During admission (qualifier value)",
            }
        ]
    },
}


def test_timingPhase():
    timing_phase = timingPhase(**timing_phase_data)
    assert isinstance(timing_phase, DataType)
    assert timing_phase.resource_type == "timingPhase"
    assert timing_phase.url == "timingPhase"
    assert type(timing_phase.valueCodeableConcept) is _CodeableConcept


@pytest.mark.parametrize(
    "data",
    [
        {
            "url": "timingDetail",
            "valueCodeableConcept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "708353007",
                        "display": "Since last encounter (qualifier value)",
                    }
                ]
            },
        },
        {
            "url": "timingDetail",
            "valueRange": {
                "low": {"value": -7, "unit": "days"},
                "high": {"value": 0, "unit": "days"},
            },
        },
        {"url": "timingDetail", "valueString": "Ever"},
    ],
)
def test_timingDetail(data):
    timing_detail = timingDetail(**data)
    assert isinstance(timing_detail, DataType)
    assert timing_detail.resource_type == "timingDetail"
    assert timing_detail.url == "timingDetail"


tpd_data = {
    "url": "timingPhaseDetail",
    "extension": [timing_phase_data, {"url": "timingDetail", "valueString": "Ever"}],
}


def test_timingPhaseDetail():
    timing_phase_detail = timingPhaseDetail(**tpd_data)
    assert isinstance(timing_phase_detail, DataType)
    assert timing_phase_detail.resource_type == "timingPhaseDetail"
    assert timing_phase_detail.url == "timingPhaseDetail"


tpd_data_error = {
    "url": "timingPhaseDetail",
    "extension": [
        timing_phase_data,
        {"url": "timingDetail", "valueString": "Ever"},
        {
            "url": "timingDetail",
            "valueRange": {
                "low": {"value": -7, "unit": "days"},
                "high": {"value": 0, "unit": "days"},
            },
        },
    ],
}


def test_timingPhaseDetail_error():
    with pytest.raises(
        ValidationError, match="timingPhase and timingDetail can only appear once"
    ):
        _ = timingPhaseDetail(**tpd_data_error)


rel_day = {"url": "relativeDay", "valueInteger": 3}


def test_relativeDay():
    relative_day = relativeDay(**rel_day)
    assert isinstance(relative_day, DataType)
    assert relative_day.resource_type == "relativeDay"
    assert relative_day.url == "relativeDay"
    assert type(relative_day.valueInteger) is int


start_date = {"url": "relativeStart", "valueInteger": 3}


def test_relativeStart():
    relative_start = relativeStart(**start_date)
    assert isinstance(relative_start, DataType)
    assert relative_start.resource_type == "relativeStart"
    assert relative_start.url == "relativeStart"
    assert type(relative_start.valueInteger) is int


end_date = {"url": "relativeEnd", "valueInteger": 5}


def test_relativeEnd():
    relative_end = relativeEnd(**end_date)
    assert isinstance(relative_end, DataType)
    assert relative_end.resource_type == "relativeEnd"
    assert relative_end.url == "relativeEnd"
    assert type(relative_end.valueInteger) is int


relative_phase_data = {"url": "relativePeriod", "extension": [start_date, end_date]}


def test_relativePeriod():
    relative_phase = relativePeriod(**relative_phase_data)
    assert isinstance(relative_phase, DataType)
    assert relative_phase.resource_type == "relativePeriod"
    assert relative_phase.url == "relativePeriod"
    assert isinstance(relative_phase.extension, list)
    assert all(
        isinstance(ext, (relativeStart, relativeEnd))
        for ext in relative_phase.extension
    )


@pytest.mark.parametrize(
    "data, expected_type_date, expected_type_str",
    [
        (
            {"url": "approximateDate", "valueDate": "2021-01-01"},
            datetime.date,
            type(None),
        ),
        ({"url": "approximateDate", "valueString": "month 3"}, type(None), str),
    ],
)
def test_approximateDate(data, expected_type_date, expected_type_str):
    approximate_date = approximateDate(**data)
    assert isinstance(approximate_date, DataType)
    assert approximate_date.resource_type == "approximateDate"
    assert approximate_date.url == "approximateDate"
    assert type(approximate_date.valueDate) is expected_type_date
    assert type(approximate_date.valueString) is expected_type_str


dur = {"url": "duration", "valueQuantity": {"value": 3, "unit": "days"}}


def test_duration():
    duration_inst = Duration(**dur)
    assert isinstance(duration_inst, DataType)
    assert duration_inst.resource_type == "Duration"
    assert duration_inst.url == "duration"
    assert type(duration_inst.valueQuantity) is _Quantity


dte = {"extension": [{"url": "approximateDate", "valueDate": "2021-01-01"}, rel_day]}


def test_dateTimeExtension():
    date_time_extension = dateTimeExtension(**dte)
    assert isinstance(date_time_extension, FHIRPrimitiveExtension)
    assert date_time_extension.resource_type == "dateTimeExtension"
    assert isinstance(date_time_extension.extension, list)
    assert all(
        isinstance(ext, (approximateDate, relativeDay, Extension))
        for ext in date_time_extension.extension
    )


@pytest.mark.parametrize(
    "ext_class, data",
    [
        (timingPhase, {"url": "timing"}),
        (relativeDay, {"url": "day"}),
        (relativeStart, {"url": "startdate"}),
        (relativeEnd, {"url": "enddate"}),
        (relativePeriod, {"url": "phase"}),
        (approximateDate, {"url": "approx"}),
        (Duration, {"url": "dur"}),
    ],
)
def test_extension_name_error(ext_class, data):
    with pytest.raises(ValueError):
        ext_class(**data)


@pytest.mark.parametrize(
    "ext_class, data",
    [
        (timingPhase, {"valueQuantity": {}}),
        (relativeDay, {"valueFloat": 2.5}),
        (relativeStart, {"valueInteger": "startdate"}),
        (relativeEnd, {"valueFloat": 2.5}),
        (relativePeriod, {"valueFloat": 2.5}),
        # not date format
        (approximateDate, {"valueDate": "month 3"}),
        # can't have both
        (approximateDate, {"valueDate": "2021-09", "valueString": "month 3"}),
        (Duration, {"valuePeriod": "middle"}),
        (dateTimeExtension, {"extension": [{"valueDate": "month 3"}]}),
    ],
)
def test_extension_validation_error(ext_class, data):
    with pytest.raises(ValidationError):
        ext_class(**data)(**data)
