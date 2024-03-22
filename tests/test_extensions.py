import pytest
import datetime
from fhir.resources.extension import Extension
from fhir.resources.datatype import DataType
from fhir.resources.fhirprimitiveextension import FHIRPrimitiveExtension
from fhir.resources.codeableconcept import CodeableConcept as _CodeableConcept
from fhir.resources.quantity import Quantity as _Quantity
from fhirflat.resources.extensions import (
    timingPhase,
    relativeDay,
    relativeStart,
    relativeEnd,
    relativePhase,
    approximateDate,
    Duration,
    dateTimeExtension,
    timingPhaseExtension,
    relativePhaseExtension,
    relativeTimingPhaseExtension,
    procedureExtension,
)

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


rel_day = {"url": "relativeDay", "valueInteger": 3}


def test_relativeDay():
    relative_day = relativeDay(**rel_day)
    assert isinstance(relative_day, DataType)
    assert relative_day.resource_type == "relativeDayExtension"
    assert relative_day.url == "relativeDay"
    assert type(relative_day.valueInteger) is int


start_date = {"url": "start", "valueInteger": 3}


def test_relativeStart():
    relative_start = relativeStart(**start_date)
    assert isinstance(relative_start, DataType)
    assert relative_start.resource_type == "relativeStartExtension"
    assert relative_start.url == "start"
    assert type(relative_start.valueInteger) is int


end_date = {"url": "end", "valueInteger": 5}


def test_relativeEnd():
    relative_end = relativeEnd(**end_date)
    assert isinstance(relative_end, DataType)
    assert relative_end.resource_type == "relativeEndExtension"
    assert relative_end.url == "end"
    assert type(relative_end.valueInteger) is int


relative_phase_data = {"url": "relativePhase", "extension": [start_date, end_date]}


def test_relativePhase():
    relative_phase = relativePhase(**relative_phase_data)
    assert isinstance(relative_phase, DataType)
    assert relative_phase.resource_type == "relativePhase"
    assert relative_phase.url == "relativePhase"
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
    assert approximate_date.resource_type == "approximateDateExtension"
    assert approximate_date.url == "approximateDate"
    assert type(approximate_date.valueDate) is expected_type_date
    assert type(approximate_date.valueString) is expected_type_str


dur = {"url": "duration", "valueQuantity": {"value": 3, "unit": "days"}}


def test_Duration():
    duration = Duration(**dur)
    assert isinstance(duration, DataType)
    assert duration.resource_type == "Duration"
    assert duration.url == "duration"
    assert type(duration.valueQuantity) is _Quantity


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


time_phase = {"extension": [timing_phase_data]}


def test_timingPhaseExtension():
    timing_phase_extension = timingPhaseExtension(**time_phase)
    assert isinstance(timing_phase_extension, FHIRPrimitiveExtension)
    assert timing_phase_extension.resource_type == "timingPhaseExtension"
    assert isinstance(timing_phase_extension.extension, list)
    assert all(
        isinstance(ext, (timingPhase, Extension))
        for ext in timing_phase_extension.extension
    )


rel_phase = {"extension": [relative_phase_data]}


def test_relativePhaseExtension():
    relative_phase_extension = relativePhaseExtension(**rel_phase)
    assert isinstance(relative_phase_extension, FHIRPrimitiveExtension)
    assert relative_phase_extension.resource_type == "relativePhaseExtension"
    assert isinstance(relative_phase_extension.extension, list)
    assert all(
        isinstance(ext, (relativePhase, Extension))
        for ext in relative_phase_extension.extension
    )


rel_time_phase = {"extension": [relative_phase_data, timing_phase_data]}


def test_relativeTimingPhaseExtension():
    relative_phase_extension = relativeTimingPhaseExtension(**rel_time_phase)
    assert isinstance(relative_phase_extension, FHIRPrimitiveExtension)
    assert relative_phase_extension.resource_type == "relativeTimingPhaseExtension"
    assert isinstance(relative_phase_extension.extension, list)
    assert all(
        isinstance(ext, (relativePhase, timingPhase, Extension))
        for ext in relative_phase_extension.extension
    )


proc = {"extension": [dur, timing_phase_data]}


def test_procedureExtension():
    procedure_extension = procedureExtension(**proc)
    assert isinstance(procedure_extension, FHIRPrimitiveExtension)
    assert procedure_extension.resource_type == "procedureExtension"
    assert isinstance(procedure_extension.extension, list)
    assert all(
        isinstance(ext, (timingPhase, Duration, Extension))
        for ext in procedure_extension.extension
    )


@pytest.mark.parametrize(
    "ext_class, data",
    [
        (timingPhase, {"url": "timing"}),
        (relativeDay, {"url": "day"}),
        (relativeStart, {"url": "startdate"}),
        (relativeEnd, {"url": "enddate"}),
        (relativePhase, {"url": "phase"}),
        (approximateDate, {"url": "approx"}),
        (Duration, {"url": "dur"}),
    ],
)
def test_extension_name_error(ext_class, data):
    with pytest.raises(ValueError):
        ext_class(**data)
