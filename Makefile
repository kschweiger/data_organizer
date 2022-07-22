$(eval INTERNALREQ=$(shell python -c "import configparser; cfg=configparser.ConfigParser(); cfg.read('setup.cfg'); u = ' '.join([f'--upgrade-package {p}' for p in cfg['options']['install_requires'].split('\n') if p != '']); print(u)"))

sync:
	pip-sync requirements/prod.txt requirements/dev.txt requirements/test.txt
	pip install -e . --no-deps
compile-prod:
	pip-compile --no-header setup.cfg --output-file requirements/prod.txt $(INTERNALREQ)
compile-test:
	pip-compile --no-header requirements/test.in --output-file requirements/test.txt $(INTERNALREQ)
compile-dev:
	pip-compile --no-header requirements/dev.in --output-file requirements/dev.txt $(INTERNALREQ)
full-compile:	compile-prod compile-test compile-dev
compile-prod-no-update:
	pip-compile --no-header setup.cfg --output-file requirements/prod.txt
compile-test-no-update:
	pip-compile --no-header requirements/test.in --output-file requirements/test.txt
compile-dev-no-update:
	pip-compile --no-header requirements/dev.in --output-file requirements/dev.txt
full-compile-no-update:	compile-prod-no-update compile-test-no-update compile-dev-no-update