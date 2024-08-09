dev-up-build-detach:
	$(MAKE) dev-base-build
	$(MAKE) dev-build -d
	$(MAKE) dev-migrate
	$(MAKE) dev-collect-static

dev-up-build:
	$(MAKE) dev-base-build
	$(MAKE) dev-build
	$(MAKE) dev-migrate
	$(MAKE) dev-collect-static

dev-base-build:
	echo "Building base image.."
	docker build -f docker_management/backend/dev.base.Dockerfile -t beiwe-server-dev-base .

dev-build:
	echo "Building images and running containers.."
	@ docker compose -f docker_management/dev.docker-compose.yml --env-file ./.envs/.env.dev up --build

dev-migrate:
	echo "Migrating database.."
	docker compose -f docker_management/dev.docker-compose.yml --env-file .envs/.env.dev exec -u 0 web python manage.py migrate --noinput

dev-collect-static:
	echo "Collecting static files.."
	docker compose -f docker_management/dev.docker-compose.yml --env-file .envs/.env.dev exec -u 0 web python manage.py collectstatic --no-input --clear

#will be updated on the production code's PR
#prod-up-build-detach:
#	$(MAKE) prod-base-build
#	$(MAKE) prod-build -d
#	$(MAKE) prod-migrate
#	$(MAKE) prod-collect-static
#
#prod-up-build:
#	$(MAKE) prod-base-build
#	$(MAKE) prod-build
#	$(MAKE) prod-migrate
#	$(MAKE) prod-collect-static
#
#prod-base-build:
#	echo "Building base image.."
#	docker build -f docker_management/backend/prod.base.Dockerfile -t beiwe-server-prod-base .
#
#prod-build:
#	echo "Building images and running containers.."
#	@ docker compose -f docker_management/prod.docker-compose.yml --env-file ./.envs/.env.prod up --build
#
#prod-migrate:
#	echo "Migrating database.."
#	docker compose -f docker_management/prod.docker-compose.yml --env-file .envs/.env.prod exec -u 0 web python manage.py migrate --noinput
#
#prod-collect-static:
#	echo "Collecting static files.."
#	docker compose -f docker_management/prod.docker-compose.yml --env-file .envs/.env.prod exec -u 0 web python manage.py collectstatic --no-input --clear