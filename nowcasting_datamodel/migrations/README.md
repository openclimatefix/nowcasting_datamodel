# Migrations

When we change the datamodel of the PV or Forecast database we will need to run a database migration

## 1. old local database
start local database of database before data model changes. This can be done by setting `DB_URL` and `DB_URL_PV` and running
by running ```python nowcasting_datamodel/migrations/app.py --make-migrations```

## 2. run migrations scripts
by running ```python nowcasting_datamodel/migrations/app.py --run-migrations```

## 3. migrations revision
commit migrations revision to repo

## 4. Start AWS task on ECS with docker container from this repo
This will run all the migrations an update the database in production / development
TODO Set up task definition using terraform
The current solution is to oopen ssh tunnel and run the migrations from there
