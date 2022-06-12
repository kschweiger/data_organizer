from typing import List

from dynaconf import Dynaconf, LazySettings, Validator


def get_config(
    name: str = "DataOrganizer",
    config_dir_base: str = "conf/",
    default_settings: str = "settings.toml",
    secrets: str = ".secrets.toml",
    additional_configs: List[str] = [],
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
    settings.validators.register(
        # Validate DB settings
        Validator("db.user", must_exist=True),
        Validator("db.password", must_exist=True),
        Validator("db.database", must_exist=True),
        Validator("db.host", must_exist=True),
        Validator("db.port", must_exist=True),
        Validator("db.prefix", must_exist=True),
        Validator("db.schema", default=None),
    )
    settings.validators.validate()

    return settings
