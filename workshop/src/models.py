import json
from dataclasses import dataclass, asdict
from typing import Optional


def is_null(value) -> bool:
    return value is None or value != value   


def to_iso(value) -> Optional[str]:
    if is_null(value):
        return None
    if hasattr(value, "to_pydatetime"):  
        value = value.to_pydatetime()
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def to_int(value) -> Optional[int]:
    if is_null(value):
        return None
    return int(value)


def to_float(value) -> Optional[float]:
    if is_null(value):
        return None
    return float(value)


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    lpep_pickup_datetime: Optional[str]
    lpep_dropoff_datetime: Optional[str]
    passenger_count: Optional[int]
    tip_amount: Optional[float]


def ride_from_row(row):
    return Ride(
        PULocationID=int(row["PULocationID"]),
        DOLocationID=int(row["DOLocationID"]),
        trip_distance=float(row["trip_distance"]),
        total_amount=float(row["total_amount"]),
        lpep_pickup_datetime=to_iso(row["lpep_pickup_datetime"]),
        lpep_dropoff_datetime=to_iso(row["lpep_dropoff_datetime"]),
        passenger_count=to_int(row["passenger_count"]),
        tip_amount=to_float(row["tip_amount"]),
    )


def ride_serializer(ride: Ride):
    return json.dumps(asdict(ride)).encode("utf-8")


def ride_deserializer(data):
    ride_dict = json.loads(data.decode("utf-8"))
    return Ride(**ride_dict)