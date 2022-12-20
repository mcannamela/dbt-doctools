This is a library for generating and manipulating [dbt documentation](https://docs.getdbt.com/docs/building-a-dbt-project/documentation) in Python. 

## Requirements

Python 3.7+

dbt 1.0+

## Installation

Install this package in the same environment as your dbt project with pip:

<div class="termy">

```console
$ pip install dbt-doctools
```

</div>

## Development

Assuming you have installed docker and docker compose, build the image:

```shell
docker build -t dbt-doctools:local -f docker/Dockerfile .
```

Then initialize the database: 
```shell
docker compose up -f docker-compose.yml -f docker-compose.init-db.yml
```

and the dbt project: 
```shell
docker compose up -f docker-compose.yml -f docker-compose.init-dbt.yml
```

Now you can bring up the database:
```shell
docker compose up
```

And separately run the tests:
```shell
docker compose run dbt-doctools pytest tests
```

### Pycharm integration
Complete the above, then add a remote interpreter  using docker compose, selecting `dbt-doctools` as the service. 

You may need to adjust the `pytest` run configuration template to set the working directory to e.g/ `/home/your_user/path/to/project` rather than `/home/your_user/path/to/project/tests` or you may find tests failing to find the fixtures in `test_support/`