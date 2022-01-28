# Use Python 3.9 - there is no binary version of hiredis for 3.10 yet
FROM python:3.9-slim

LABEL maintainer="Michał Rokita <mrokita@macrosystem.pl>"
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_HOME=/usr

RUN apt-get update \
    && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/b47de09/install-poetry.py | python -


RUN mkdir /src
WORKDIR /src
COPY poetry.lock pyproject.toml /src/
RUN poetry config virtualenvs.create false && poetry install --no-dev --no-root --no-interaction --no-ansi
COPY pybrook /src/pybrook
COPY README.md locustfile.py  ztm_dump.json /src/
ENV TZ=CET
RUN poetry install --no-dev --no-interaction --no-ansi
CMD bash