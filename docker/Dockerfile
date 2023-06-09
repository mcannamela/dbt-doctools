FROM python:3.9
ENV PYTHONIOENCODING=utf-8
ENV LANG C.UTF-8
ENV PATH="${PATH}:/root/.local/bin"
RUN apt-get update
RUN apt-get install -y postgresql-client
RUN curl --retry 5 -sSL https://install.python-poetry.org | python3 - --preview

RUN poetry config virtualenvs.create false
RUN poetry config installer.parallel false

COPY pyproject.toml .
COPY poetry.lock .

RUN poetry install --no-root
COPY . /dbt-doctools
WORKDIR /dbt-doctools
RUN git config --global --add safe.directory /dbt-doctools