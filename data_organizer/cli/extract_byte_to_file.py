import logging
from pathlib import Path
from typing import List, Tuple

import click
from pypika import Table, Tables
from sqlalchemy.exc import ProgrammingError

from data_organizer.config import OrganizerConfig
from data_organizer.db.connection import DatabaseConnection
from data_organizer.utils import init_logging

logger = logging.getLogger("ExtractByteCli")


@click.command()
@click.argument("config_files", nargs=-1)
@click.option(
    "--conf_base",
    default="conf/",
    show_default=True,
    help="Base directory containing all config files. All other condif file paths will "
    "be interpreted relative to this",
)
@click.option(
    "--default_settings",
    default="settings.toml",
    show_default=True,
    help="Main settings file defining e.g. DB options and table configuration. Pass "
    "the file relative to --conf_base.",
)
@click.option(
    "--secrets",
    default=".secrets.toml",
    show_default=True,
    help="File containing the secrets (e.g. Database password).",
)
@click.option(
    "--path",
    required=True,
    type=click.Path(exists=True),
    help="Full output path. Must exist!",
)
@click.option("--prefix", required=True, help="Prefix of the output file")
@click.option("--ext", required=True, help="Extension of the output file")
@click.option(
    "--data_column",
    required=True,
    help="Name of the column that will be saved to file. Expecting byte column type",
)
@click.option(
    "--columns",
    required=True,
    help="Comma separated list of columns of the main table (defined with --table) "
    "that will be used for the output file name.",
)
@click.option(
    "--table",
    required=True,
    help="Main table that contains the data that should be written to file.",
)
@click.option(
    "--join_tables",
    default=None,
    multiple=True,
    help="Optional table that will be joined to the main table yia the column defined "
    "in --join_on. Can be passed multiple time to join multiple tables. Attention: All "
    "join are relative to the main table",
)
@click.option(
    "--join_on",
    default=None,
    multiple=True,
    help="Column that will be used in the join of the main table to the join tables. "
    "It is assumed that the column is present in the main and the join table. Can be "
    "passed multiple time to join multiple tables.",
)
@click.option(
    "--join_columns",
    default=None,
    multiple=True,
    help="Comma separated list of columns selected from the joined table. Can be "
    "passed multiple time to join multiple tables.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def cli(
    ctx,
    config_files: Tuple[str, ...],
    conf_base: str,
    default_settings: str,
    secrets: str,
    path: str,
    prefix: str,
    ext: str,
    data_column: str,
    columns: str,
    table: str,
    join_tables: Tuple[str, ...],
    join_on: Tuple[str, ...],
    join_columns: Tuple[str, ...],
    debug: bool,
):
    """
    Utility that saves the values in a byte column to a file. The filename is based on
    the columns that are returned by the query to the database. Pass additional one or
    more CONFIG_FILES as first arguments.
    """
    init_logging("DEBUG" if debug else "INFO")

    for passed_file in list(config_files) + [default_settings, secrets]:
        if not Path(f"{conf_base}/{passed_file}").is_file():
            raise RuntimeError("File %s does not exist" % passed_file)

    config = OrganizerConfig(
        config_dir_base=conf_base,
        default_settings=default_settings,
        secrets=secrets,
        additional_configs=list(config_files),
    )

    conn_name = "DataOrganizerCli-ExtractByte"

    join_tables_ = list(join_tables)
    join_on_ = list(join_on)
    join_columns_ = []
    for cols in join_columns:
        join_columns_.append(cols.replace(" ", "").split(","))

    assert len(join_tables_) == len(join_on_)
    assert len(join_tables_) == len(join_columns_)

    with DatabaseConnection(**config.settings.db.to_dict(), name=conn_name) as db:
        status_code = extract(
            db,
            path=path,
            prefix=prefix,
            file_ext=ext,
            data_column=data_column,
            columns=columns.replace(" ", "").split(","),
            table=table,
            join_tables=join_tables_,
            join_on=join_on_,
            join_columns=join_columns_,
        )

    ctx.exit(code=status_code)


def extract(
    db: DatabaseConnection,
    path: str,
    prefix: str,
    file_ext: str,
    data_column: str,
    columns: List[str],
    table: str,
    join_tables: List[str],
    join_on: List[str],
    join_columns: List[List[str]],
) -> int:
    logger.info("Will select data from table **%s**", table)
    for jo, jt in zip(join_on, join_tables):
        logger.info("Joining **%s** on **%s**", jt, jo)
    # Build the query based on the passed options
    join_tables_: List[Table] = Tables(*join_tables)
    q = db.pypika_query.from_(Table(table))
    for joc, jt in zip(join_on, join_tables_):
        q = q.join(jt).on_field(joc)
    q = q.select(*[data_column], *columns)
    for jc, jt in zip(join_columns, join_tables_):
        q = q.select(*[jt[c] for c in jc])  # type: ignore

    try:
        data = db.query(q)
    except ProgrammingError as e:
        logger.error("Got a sql ProgrammingError error. Details in debug log. Exiting")
        logger.debug(e)
        return 1

    for res in data:
        data_value = res[0]
        identifier = "_".join([str(r) for r in res[1::]])
        outfile_name = f"{path}/{prefix}_{identifier}.{file_ext}"
        logger.info("Writing file: %s", outfile_name)
        try:
            with open(outfile_name, "wb") as f:
                f.write(bytes(data_value))
        except Exception as e:
            logger.error("Writing to file failed. Details in debug log. Exiting")
            logger.debug(e)
            return 1

    return 0


if __name__ == "__main__":
    cli()
