# Launcher image for the Beam cleaning pipeline (Dataflow Flex Template).
# Build context: etl/  (needs beam_etl/, sql_queries/, setup.py)
# Versions must match the environment the pipeline was tested in.
ARG PY_VERSION=312
FROM gcr.io/dataflow-templates-base/python${PY_VERSION}-template-launcher-base

ARG BEAM_VERSION=2.76.0
WORKDIR /template

COPY setup.py ./
COPY beam_etl/ beam_etl/
COPY sql_queries/ sql_queries/

# What the launcher runs, and how workers get the beam_etl package
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/beam_etl/pipeline.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/template/setup.py
# Lets "import beam_etl..." find /template/beam_etl, so sql_queries/ resolves to /template/sql_queries
ENV PYTHONPATH=/template

RUN pip install --no-cache-dir "apache-beam[gcp]==${BEAM_VERSION}"
