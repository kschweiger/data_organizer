##  (2023-01-10)




## 1.4.0 (2023-01-10)

* Bump version: 1.3.0 → 1.4.0 ([3b5e1fb](https://github.com/kschweiger/data_organizer/commit/3b5e1fb))
* feat(DatabaseConnection): Added generic sql execution method ([477568b](https://github.com/kschweiger/data_organizer/commit/477568b))
* doc: Updated CHANGELOG.md ([4bc2b98](https://github.com/kschweiger/data_organizer/commit/4bc2b98))



##  (2023-01-04)




## 1.3.0 (2023-01-04)

* Bump version: 1.2.0 → 1.3.0 ([e210ce3](https://github.com/kschweiger/data_organizer/commit/e210ce3))
* feat: Dataclass from TableSettings (#10) ([0db1ec7](https://github.com/kschweiger/data_organizer/commit/0db1ec7)), closes [#10](https://github.com/kschweiger/data_organizer/issues/10)
* doc: Updated CHANGELOG.md ([ee5d6c4](https://github.com/kschweiger/data_organizer/commit/ee5d6c4))



##  (2022-10-23)




## 1.2.0 (2022-10-23)

* Bump version: 1.1.0 → 1.2.0 ([f845411](https://github.com/kschweiger/data_organizer/commit/f845411))
* refactor: Use SQLAlchemy 2.0 style (#6) ([77704df](https://github.com/kschweiger/data_organizer/commit/77704df)), closes [#6](https://github.com/kschweiger/data_organizer/issues/6) [#5](https://github.com/kschweiger/data_organizer/issues/5) [#5](https://github.com/kschweiger/data_organizer/issues/5)
* doc: Updated CHANGELOG.md ([dbc9a53](https://github.com/kschweiger/data_organizer/commit/dbc9a53))



##  (2022-09-05)




## 1.1.0 (2022-09-05)

* Bump version: 1.0.2 → 1.1.0 ([9370b19](https://github.com/kschweiger/data_organizer/commit/9370b19))
* feat: Add support for foreign keys (#4) ([a41903b](https://github.com/kschweiger/data_organizer/commit/a41903b)), closes [#4](https://github.com/kschweiger/data_organizer/issues/4)
* chore: Added MIT License ([b4245ec](https://github.com/kschweiger/data_organizer/commit/b4245ec))
* doc: Updated CHANGELOG.md ([0ac81b8](https://github.com/kschweiger/data_organizer/commit/0ac81b8))



##  (2022-08-07)




## <small>1.0.2 (2022-08-07)</small>

* Bump version: 1.0.1 → 1.0.2 ([1bd7e3c](https://github.com/kschweiger/data_organizer/commit/1bd7e3c))
* chore: Updated setup.py to fix none working import ([ee2f902](https://github.com/kschweiger/data_organizer/commit/ee2f902))
* doc: Updated CHANGELOG.md ([22b8972](https://github.com/kschweiger/data_organizer/commit/22b8972))



##  (2022-08-07)




## <small>1.0.1 (2022-08-07)</small>

* Bump version: 1.0.0 → 1.0.1 ([a012a69](https://github.com/kschweiger/data_organizer/commit/a012a69))
* build: Removed unnecessary dependency ([db47643](https://github.com/kschweiger/data_organizer/commit/db47643))
* doc: Updated CHANGELOG.md ([a684998](https://github.com/kschweiger/data_organizer/commit/a684998))



##  (2022-08-07)




## 1.0.0 (2022-08-07)

* Bump version: 0.0.1 → 1.0.0 ([8defaa5](https://github.com/kschweiger/data_organizer/commit/8defaa5))
* Moved to setup.cfg+pyproject.toml ([b330f9f](https://github.com/kschweiger/data_organizer/commit/b330f9f))
* chore: Added .bumpversion.cfg and failsave to release script ([61014b2](https://github.com/kschweiger/data_organizer/commit/61014b2))
* chore: Initial commit with base file ([d00e653](https://github.com/kschweiger/data_organizer/commit/d00e653))
* feat: Added basic configuration ([a374ab6](https://github.com/kschweiger/data_organizer/commit/a374ab6))
* feat: Added basic database connection functionality ([65f08e3](https://github.com/kschweiger/data_organizer/commit/65f08e3))
* feat: Added cli tool for editing a table; Some related changes to DatabaseConnection and get_table_d ([653b15f](https://github.com/kschweiger/data_organizer/commit/653b15f))
* feat: Added default values to table config and populator ([2c46069](https://github.com/kschweiger/data_organizer/commit/2c46069))
* feat: Added ETL for extraction genenral information from gpx tracks ([ea6fccf](https://github.com/kschweiger/data_organizer/commit/ea6fccf))
* feat: Added etl process for enhancing gpx tracks ([a3d4345](https://github.com/kschweiger/data_organizer/commit/a3d4345))
* feat: Added function to generate user interaction for filling data base on config ([a13ae7a](https://github.com/kschweiger/data_organizer/commit/a13ae7a))
* feat: Added is_inserted flag to model. ([e0b05f8](https://github.com/kschweiger/data_organizer/commit/e0b05f8))
* feat: Added optional possibility to pass a schema to the insert method ([b40ec0b](https://github.com/kschweiger/data_organizer/commit/b40ec0b))
* feat: Added relative mapping between tables that can be used to fill multiple tables with one shared ([c98b949](https://github.com/kschweiger/data_organizer/commit/c98b949))
* feat: Added release script ([23527ae](https://github.com/kschweiger/data_organizer/commit/23527ae))
* feat: Added script for extraction data to file ([4c3661c](https://github.com/kschweiger/data_organizer/commit/4c3661c))
* feat: Added support for SERIAL type ([94fd000](https://github.com/kschweiger/data_organizer/commit/94fd000))
* feat: Added validation for table configuration + configuration test ([7ca7023](https://github.com/kschweiger/data_organizer/commit/7ca7023))
* feat: Wrapped dynaconfig in a objct containing the resolved and validated table objects ([e2c46d3](https://github.com/kschweiger/data_organizer/commit/e2c46d3))
* feat(DatabaseConnection): Added general purpose insert method _insert ([c5aaecb](https://github.com/kschweiger/data_organizer/commit/c5aaecb))
* feat(DatabaseConnection): Added insert method with preprocessing ([ead7b88](https://github.com/kschweiger/data_organizer/commit/ead7b88))
* feat(DatabaseConnection): Added table creation method ([7f594ac](https://github.com/kschweiger/data_organizer/commit/7f594ac))
* feat(DatabaseConnection): Passing a valid path as value to bytea column will assume to read the file ([92d084d](https://github.com/kschweiger/data_organizer/commit/92d084d))
* feat(OrganizerConfig): Elements can be added to the config by passing undefined kwargs to the init m ([8fb8d50](https://github.com/kschweiger/data_organizer/commit/8fb8d50))
* feat(OrganizerConfig): kwargs added to config are now merged into the configuration from file. Added ([84ba8d1](https://github.com/kschweiger/data_organizer/commit/84ba8d1))
* feat(populator): Added valiadtion for INTERVAL ctype ([9d2b80a](https://github.com/kschweiger/data_organizer/commit/9d2b80a))
* refactor: Added flag to TableSetting that determines if the default behaviour for columns that do no ([4dfa63d](https://github.com/kschweiger/data_organizer/commit/4dfa63d))
* refactor: Added logging to edit-table cli and made logging more flexible ([8cf3b13](https://github.com/kschweiger/data_organizer/commit/8cf3b13))
* refactor: Changes how the connections are handled and DatabaseConnection can be used with with ([f00a319](https://github.com/kschweiger/data_organizer/commit/f00a319))
* refactor: Moved tests ([9b2d3d6](https://github.com/kschweiger/data_organizer/commit/9b2d3d6))
* refactor: Moved to general purpose insert method in edit_table.py ([e45cf3b](https://github.com/kschweiger/data_organizer/commit/e45cf3b))
* refactor: Refactored logging functions ([043b526](https://github.com/kschweiger/data_organizer/commit/043b526))
* refactor: Renamed TableInfo to TableSetting and ColumnInfo to ColumnSetting ([d22c799](https://github.com/kschweiger/data_organizer/commit/d22c799))
* refactor(DatabaseConnection): Renamed insert to insert_df and query to query_to_df ([b40346d](https://github.com/kschweiger/data_organizer/commit/b40346d))
* refactor(OrganizerConfig): Minor changes to internal structure ([e417339](https://github.com/kschweiger/data_organizer/commit/e417339))
* ci: Added yml for tests (#1) ([916e2fa](https://github.com/kschweiger/data_organizer/commit/916e2fa)), closes [#1](https://github.com/kschweiger/data_organizer/issues/1)
* ci: Fixed action config ([c61480c](https://github.com/kschweiger/data_organizer/commit/c61480c))
* test: Added .coveragerc ([5c55258](https://github.com/kschweiger/data_organizer/commit/5c55258))
* test: Added test combining config and DatabaseConnection ([89aee80](https://github.com/kschweiger/data_organizer/commit/89aee80))
* test: Added test for function defined in data/populator.py ([bfa208f](https://github.com/kschweiger/data_organizer/commit/bfa208f))
* doc: Updated README.md ([126d3b9](https://github.com/kschweiger/data_organizer/commit/126d3b9))
* fix: Fixed wrong optional column name in default settings ([68d4d50](https://github.com/kschweiger/data_organizer/commit/68d4d50))



