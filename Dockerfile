ARG PYTHON_TAG="3.12-slim"
FROM python:$PYTHON_TAG AS builder

LABEL maintainer="Michał Rokita <mrokita@macrosystem.pl>"
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    PDM_CHECK_UPDATE=false

RUN pip install -U pdm

COPY README.md pdm.lock pyproject.toml README.md /project/

WORKDIR /project
RUN pdm install --check --dev --no-editable
COPY pybrook /project/pybrook

FROM python:$PYTHON_TAG

COPY --from=builder /project/.venv/ /project/.venv
ENV TZ=CET \
    PATH="/project/.venv/bin:$PATH" \
    PYTHONPATH="/project:$PYTHONPATH"

WORKDIR /project
COPY pybrook /project/pybrook
COPY README.md locustfile.py ztm_dump.json /project/
CMD bash