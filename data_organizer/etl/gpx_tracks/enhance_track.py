"""
ETL process for enhancing gpx tracks saved in the database using the gpx_track_analyzer
package. See. https://github.com/kschweiger/track_analyzer
Attention: Currently not delivered in this repository's requirements.
"""

import logging
from copy import deepcopy

import gpxpy
from gpx_track_analyzer.enhancer import OpenTopoElevationEnhancer
from gpx_track_analyzer.track import ByteTrack

from data_organizer.db.connection import DatabaseConnection
from data_organizer.db.model import TableSetting
from data_organizer.etl.exceptions import ETLConfigurationException

logger = logging.getLogger(__name__)


def enhance_tracks(
    db: DatabaseConnection,
    src_table_setting: TableSetting,
    tgt_table: str,
    skip_existing: bool,
    enhancer_url: str,
    enhancer_dataset: str,
):
    logger.info("Starting to enhance tracks")

    enhancer = OpenTopoElevationEnhancer(url=enhancer_url, dataset=enhancer_dataset)

    src_table = src_table_setting.name

    tgt_table_setting = deepcopy(src_table_setting)
    tgt_table_setting.name = tgt_table
    tgt_table_setting.disable_auto_insert_columns = True

    if skip_existing and not db.has_table(tgt_table):
        raise ETLConfigurationException(
            "If skip_existing is True, the target table need to exist"
        )

    if not db.has_table(tgt_table):
        db.create_table_from_table_info([tgt_table_setting])

    existing_ids = []
    if skip_existing:
        existing_query = db.pypika_query.from_(tgt_table).select("id_track", "id_ride")
        with db.engine.connect() as conn:
            existing_tracks = conn.execute(existing_query.get_sql())

        for id_track, id_ride in existing_tracks:
            existing_ids.append((id_track, id_ride))

    in_data_query = db.pypika_query.from_(src_table).select("*")

    with db.engine.connect() as conn:
        input_track_data = conn.execute(in_data_query.get_sql())

    new_data_to_insert = []
    for id_track, id_ride, track_ in input_track_data:
        if (id_track, id_ride) in existing_ids:
            logger.debug(
                "Track %s/%s already exists and will be skipped", id_track, id_ride
            )
            continue
        logger.info("Processing track %s", id_track)
        track = bytes(track_)
        byte_track = ByteTrack(track)

        enhanced_track = enhancer.enhance_track(byte_track.track)
        out_gpx = gpxpy.gpx.GPX()
        out_gpx.tracks = [enhanced_track]

        enhanced_gpx_track = out_gpx.to_xml()

        new_data_to_insert.append([id_track, id_ride, enhanced_gpx_track.encode()])

    if new_data_to_insert:
        db.insert(tgt_table_setting, new_data_to_insert)
    else:
        logger.info("No data to insert")
