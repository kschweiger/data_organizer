import logging
from pathlib import Path
from typing import Tuple

import click

from data_organizer.config import OrganizerConfig
from data_organizer.data.populator import get_table_data_from_user_input
from data_organizer.db.connection import DatabaseConnection
from data_organizer.utils import echo_and_log, init_logging_to_file

logger = logging.getLogger(__name__)


@click.command()
@click.argument("config_files", nargs=-1)
@click.option("--conf_base", default="conf/", show_default=True)
@click.option("--default_settings", default="settings.toml", show_default=True)
@click.option("--secrets", default=".secrets.toml", show_default=True)
def cli(
    config_files: Tuple[str, ...], conf_base: str, default_settings: str, secrets: str
):
    """
    CLI app to add data to a table defined in the passed CONFIG_FILES.
    """

    init_logging_to_file("cli_edit.txt", "DEBUG")

    for passed_file in list(config_files) + [default_settings, secrets]:
        if not Path(f"{conf_base}/{passed_file}").is_file():
            raise RuntimeError("File %s does not exist" % passed_file)

    config = OrganizerConfig(
        config_dir_base=conf_base,
        default_settings=default_settings,
        secrets=secrets,
        additional_configs=list(config_files),
    )

    if not config.tables:
        click.echo("No tables set in the initialized configuration")

    conn_name = "DataOrganizerCli-EditTable"

    with DatabaseConnection(**config.settings.db.to_dict(), name=conn_name) as db:
        edit(config, db)


def edit(config: OrganizerConfig, db: DatabaseConnection):
    exit_cond_met = False
    while not exit_cond_met:
        # TODO: Decide how to work with short and full table name here
        table_to_edit = click.prompt(
            "Please enter a table",
            type=click.Choice(list(config.tables.keys())),
        )
        logger.debug("Table %s entered", table_to_edit)

        if not db.has_table(config.tables[table_to_edit].name):
            echo_and_log(
                logger.info,
                "Table %s not yet created in database",
                table_to_edit,
            )
            # click.echo("Table %s not yet created in database")
            if click.confirm("Create table?"):
                db.create_table_from_table_info([config.tables[table_to_edit]])

        if db.has_table(config.tables[table_to_edit].name):
            edit_table(config, db, table_to_edit)

        if not click.confirm("Do you want to edit another table?"):
            exit_cond_met = True


def edit_table(config: OrganizerConfig, db: DatabaseConnection, table: str):
    exit_cond_met = False
    while not exit_cond_met:
        action = "add"  # Replace with a prompt if more actions are available
        if action == "add":
            data_to_add = get_table_data_from_user_input(
                config, table, prompt_func=click.prompt
            )
            success, err = db.insert(config.tables[table].name, data_to_add)
            # TODO: Error is printed twice. Error and warning from logging still shown
            if not success:
                click.echo("Data could not be inserted: %s" % err)

        if click.confirm(
            "Are you finished applying actions to this table?", default=True
        ):
            exit_cond_met = True


if __name__ == "__main__":
    cli()
