# Pipeline image: official Airflow + our Python dependencies.
# All versions are pinned. Never use "latest" or floating tags.
FROM apache/airflow:3.3.2-python3.11

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.11

COPY requirements.txt /requirements.txt

# Install our packages using Airflow's constraints file for this exact version,
# so nothing we add can upgrade or break Airflow's own dependencies.
# apache-airflow is listed explicitly so pip never swaps it for another version.
#
# Exception: pandas. Airflow's constraints pin pandas==3.0.5, but this project pins
# pandas==2.3.3 in requirements.txt (see PR). Airflow core does not depend on pandas (only some
# providers use it), so that one line is removed from a local copy of the constraints.
# Every other package stays constrained. curl -f makes the build fail if the download fails.
RUN curl -fsSL "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      | grep -vi '^pandas==' > /tmp/constraints.txt \
 && pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt \
    --constraint /tmp/constraints.txt

# Working directory for the project (see docker-compose.yml). Owned by group 0 and
# group-writable, so it works for any AIRFLOW_UID (containers run as "<uid>:0").
USER root
RUN mkdir -p /opt/project && chown airflow:0 /opt/project && chmod 775 /opt/project
USER airflow