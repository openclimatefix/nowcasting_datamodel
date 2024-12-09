# Migrations

When we change the datamodel of the PV or Forecast database we will need to run a database migration

## 1. Connect to database via SSH
Open an SSH tunnel from the target database instance to your local machine. Then, set
`DB_URL` environment variable to point to the local end of the tunnel e.g.
```bash
$ export DB_URL=postgresql://<user>:<password>@localhost:<port>/forecastdevelopment
$ python nowcasting_datamodel/migrations/app.py --make-migrations
```

## 2. Commit new migration file
Commit migrations revision to repo using git

## 3a. Run migrations manually
```
$ python nowcasting_datamodel/migrations/app.py --run-migrations
```

## 3b. Start AWS task on ECS with docker container from this repo
This will run all the migrations an update the database in production / development
TODO Set up task definition using terraform
The current solution is to oopen ssh tunnel and run the migrations from there
