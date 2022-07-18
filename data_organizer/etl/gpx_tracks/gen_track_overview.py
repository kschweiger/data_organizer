"""
ETL process for extracting general information from gpx tracks and adding them to a
database table using the gpx_track_analyzer package.
See. https://github.com/kschweiger/track_analyzer

Attention: Currently not delivered in this repository's requirements.
"""

import logging
from typing import Any, Dict

from gpx_track_analyzer.track import ByteTrack

from data_organizer.db.connection import DatabaseConnection
from data_organizer.db.model import get_table_setting_from_dict
from data_organizer.etl.exceptions import ETLConfigurationException

logger = logging.getLogger(__name__)


def gen_track_overview(
    db: DatabaseConnection,
    src_table: str,
    tgt_table: str,
    skip_existing: bool,
):

    tgt_table_dict: Dict[str, Any] = {
        "name": tgt_table,
        "id_track": {"ctype": "INT", "is_primary": True, "is_unique": True},
        "id_segment": {"ctype": "INT", "is_primary": True},
        "moving_time_seconds": {"ctype": "FLOAT"},
        "total_time_seconds": {"ctype": "FLOAT"},
        "moving_distance": {"ctype": "FLOAT"},
        "total_distance": {"ctype": "FLOAT"},
        "max_velocity": {"ctype": "FLOAT"},
        "avg_velocity": {"ctype": "FLOAT"},
        "max_elevation": {"ctype": "FLOAT", "is_nullable": True},
        "min_elevation": {"ctype": "FLOAT", "is_nullable": True},
        "uphill_elevation": {"ctype": "FLOAT", "is_nullable": True},
        "downhill_elevation": {"ctype": "FLOAT", "is_nullable": True},
        "moving_distance_km": {"ctype": "FLOAT"},
        "total_distance_km": {"ctype": "FLOAT"},
        "max_velocity_kmh": {"ctype": "FLOAT"},
        "avg_velocity_kmh": {"ctype": "FLOAT"},
    }

    tgt_table_setting = get_table_setting_from_dict(tgt_table_dict)

    existing_ids = []
    if skip_existing:
        if not db.has_table(tgt_table):
            raise ETLConfigurationException(
                "If skip_existing is True, the target table need to exist"
            )

        existing_query = db.pypika_query.from_(tgt_table).select("id_track")
        with db.engine.connect() as conn:
            existing_tracks = conn.execute(existing_query.get_sql())

        for id_track in existing_tracks:
            existing_ids.append(id_track[0])

    if not db.has_table(tgt_table):
        db.create_table_from_table_info([tgt_table_setting])

    in_data_query = db.pypika_query.from_(src_table).select("*")

    with db.engine.connect() as conn:
        input_track_data = conn.execute(in_data_query.get_sql())

    new_data_to_insert = []
    for id_track, _, track_ in input_track_data:
        if id_track in existing_ids:
            logger.debug("Track %s already exists and will be skipped", id_track)
            continue
        logger.info("Processing track %s", id_track)
        track = bytes(track_)
        byte_track = ByteTrack(track)

        for i_segment in range(byte_track.n_segments):
            this_track_overview = byte_track.get_segment_overview(i_segment)
            new_data_to_insert.append(
                [
                    id_track,
                    i_segment,
                    this_track_overview.moving_time_seconds,
                    this_track_overview.total_time_seconds,
                    this_track_overview.moving_distance,
                    this_track_overview.total_distance,
                    this_track_overview.max_velocity,
                    this_track_overview.avg_velocity,
                    this_track_overview.max_elevation,
                    this_track_overview.min_elevation,
                    this_track_overview.uphill_elevation,
                    this_track_overview.downhill_elevation,
                    this_track_overview.moving_distance_km,
                    this_track_overview.total_distance_km,
                    this_track_overview.max_velocity_kmh,
                    this_track_overview.avg_velocity_kmh,
                ]
            )

    if new_data_to_insert:
        db.insert(tgt_table_setting, new_data_to_insert)
    else:
        logger.info("No data to insert")
