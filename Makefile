$(eval INTERNALREQ=$(shell python -c "import configparser; cfg=configparser.ConfigParser(); cfg.read('setup.cfg'); u = ' '.join([f'--upgrade-package {p}' for p in cfg['options']['install_requires'].split('\n') if p != '']); print(u)"))

sync:
	pip-sync requirements/prod.txt requirements/dev.txt requirements/test.txt
	pip install -e . --no-deps
compile-prod:
	pip-compile --no-header setup.cfg --output-file requirements/prod.txt
compile-test:
	pip-compile --no-header requirements/test.in --output-file requirements/test.txt
compile-dev:
	pip-compile --no-header requirements/dev.in --output-file requirements/dev.txt
full-compile:	compile-prod compile-test compile-dev
compile-prod-update:
	pip-compile --no-header setup.cfg --output-file requirements/prod.txt $(INTERNALREQ)
compile-test-update:
	pip-compile --no-header requirements/test.in --output-file requirements/test.txt $(INTERNALREQ)
compile-dev-update:
	pip-compile --no-header requirements/dev.in --output-file requirements/dev.txt $(INTERNALREQ)
full-compile-update:	compile-prod-update compile-test-update compile-dev-update
