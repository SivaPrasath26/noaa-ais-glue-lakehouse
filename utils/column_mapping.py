"""
Column name normalization map for NOAA AIS raw data.
Used to standardize source variations before schema enforcement.
"""

COLUMN_MAPPING = {
    "latitude": "LAT",
    "lat": "LAT",
    "longitude": "LON",
    "lon": "LON",
    "base_date_time": "BaseDateTime",
    "basedatetime": "BaseDateTime",
    "vessel_name": "VesselName",
    "vesselname": "VesselName",
    "call_sign": "CallSign",
    "callsign": "CallSign",
    "vessel_type": "VesselType",
    "vesseltype": "VesselType",
    "transceiver": "TransceiverClass",
    "transceiver_class": "TransceiverClass"
}
