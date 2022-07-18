import logging
from typing import Any, Dict, List, Optional

from dynaconf import Dynaconf, LazySettings, ValidationError, Validator

from data_organizer.db.model import TableSetting, get_table_setting_from_dict

logger = logging.getLogger(__name__)


class OrganizerConfig:
    def __init__(
        self,
        name: str = "DataOrganizer",
        config_dir_base: str = "conf/",
        default_settings: str = "settings.toml",
        secrets: str = ".secrets.toml",
        additional_configs: List[str] = [],
        overwrite_settings: Dict[str, Any] = {},
        **kwargs,
    ):
        self.settings = get_settings(
            name=name,
            config_dir_base=config_dir_base,
            default_settings=default_settings,
            secrets=secrets,
            additional_configs=additional_configs,
            overwrite_settings=overwrite_settings,
            **kwargs,
        )

        self.tables: Dict[str, TableSetting] = {}
        if self.settings.tables:
            for table in self.settings.tables:
                table_dict = self.settings[table].to_dict()
                for key in table_dict.keys():
                    if isinstance(table_dict[key], dict):
                        if (
                            table_dict[key]["ctype"]
                            in self.settings.table_settings.auto_fill_ctypes
                        ):
                            table_dict[key].update({"is_inserted": False})
                self.tables[table] = get_table_setting_from_dict(table_dict)


def get_settings(
    name: str = "DataOrganizer",
    config_dir_base: str = "conf/",
    default_settings: str = "settings.toml",
    secrets: str = ".secrets.toml",
    additional_configs: List[str] = [],
    overwrite_settings: Dict[str, Any] = {},
    **kwargs,
) -> LazySettings:
    """
    Get the config based on the passed parameters

    Args:
        name: Name of the config. Uppercase will be used for env var prefix
        config_dir_base: base directory of the config files
        default_settings: File with all default settings
        secrets: File with the secrets (e.g. DB password)
        additional_configs: Additional configuration files

    Returns: A DynaConf Settings object
    """
    logger.debug("Initializing config with:")
    logger.debug("Name: %s", name)
    logger.debug("Base dir: %s", config_dir_base)
    logger.debug("Default settings: %s", default_settings)
    logger.debug("Secrets: %s", secrets)
    logger.debug("Additional files: %s", additional_configs)
    files = [
        f"{config_dir_base}/{default_settings}",
        f"{config_dir_base}/{secrets}",
    ]
    for additional_config in additional_configs:
        files.append(
            f"{config_dir_base}/{additional_config}",
        )
    settings = Dynaconf(
        settings_files=files,
        envvar_prefix=name.upper(),
    )

    settings.update(kwargs, merge=True)

    if overwrite_settings:
        logger.info("Got setting overwrite data")
        logger.debug("Replacing: %s", overwrite_settings)
        settings.update(overwrite_settings)

    validate_settings(settings)

    # Explicitly cast to str if the password is purely numeric
    settings.DB.password = str(settings.DB.password)

    return settings


def validate_settings(settings: LazySettings) -> None:
    settings.validators.register(
        # Validate DB settings
        Validator("db.user", must_exist=True),
        Validator("db.password", must_exist=True),
        Validator("db.database", must_exist=True),
        Validator("db.host", must_exist=True),
        Validator("db.port", must_exist=True),
        Validator("db.prefix", must_exist=True),
        Validator("db.schema", default=None),
        Validator("tables", default=None),
    )
    settings.validators.validate()

    # Validate table settings
    if settings.tables:
        validate_table(settings)


def validate_table(settings: LazySettings) -> None:
    """

    Args:
        setting: Initialized settings from dynaconf

    Raises:
        ValidationError if some criteria is not met
    """
    mandatory_columns = settings.table_settings.mandatory_columns
    optional_columns = settings.table_settings.optional_columns

    type_map = {"str": str, "bool": bool, "int": int, "float": float, "any": None}

    key_types: Dict[str, Optional[type]] = {
        key: type_map[cfg_type]
        for key, cfg_type in settings.table_settings.key_types.items()
    }

    top_level_table_options = ["name", "rel_table", "rel_table_common_column"]

    for table in settings.tables:
        table_settings = settings[table]
        # Check if table as a name
        if "name" not in table_settings.keys():
            raise ValidationError("name not defined in table %s" % table)
        if "rel_table" in table_settings.keys():
            if table_settings["rel_table"] not in settings.tables:
                raise ValidationError(
                    "**%s** set as rel_table in **%s** "
                    % (table_settings["rel_table"], table),
                    "but **%s** is not defined in tables" % table_settings["rel_table"],
                )
            if "rel_table_common_column" not in table_settings.keys():
                raise ValidationError(
                    "rel_table_common_column has to be set because "
                    "rel_table is set in **%s**" % table
                )

            if (
                table_settings["rel_table_common_column"]
                not in settings[table_settings["rel_table"]].keys()
            ):
                raise ValidationError(
                    "Common column **%s** set in table **%s** not present in **%s**"
                    % (
                        table_settings["rel_table_common_column"],
                        table,
                        table_settings["rel_table"],
                    )
                )

        for key in [
            key for key in table_settings.keys() if key not in top_level_table_options
        ]:
            # Check that the item for all other keys is a dict
            if not isinstance(table_settings[key], dict):
                raise ValidationError(
                    "Key %s in table %s is not a dict." % (key, table)
                )
            for column_key in table_settings[key].keys():
                # Check that all kes (expect name) are in the mandatory or optional
                # keys defined above
                if column_key not in mandatory_columns + optional_columns:
                    raise ValidationError(
                        "Column %s in table %s has invalid key %s"
                        % (key, table, column_key)
                    )
                # Check the type of the keys
                if key_types[column_key] is not None:
                    if not isinstance(
                        table_settings[key][column_key],
                        key_types[column_key],  # type: ignore
                    ):
                        raise ValidationError(
                            "%s / column %s / table %s - Wrong type. Expected %s"
                            % (
                                column_key,
                                key,
                                table,
                                key_types[column_key],
                            )
                        )
            # Check that all mandatory keys are set in each column
            if not all(
                [
                    mandatory_key in table_settings[key].keys()
                    for mandatory_key in mandatory_columns
                ]
            ):
                raise ValidationError(
                    "Column %s in table %s is missing mandatory keys. %s are needed"
                    % (key, table, mandatory_columns)
                )
