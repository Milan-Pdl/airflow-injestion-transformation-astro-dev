FROM astrocrpublic.azurecr.io/runtime:3.2-5

# Copy the dbt project into the image
COPY dbt/ /usr/local/airflow/dbt/

# Copy the runtime environment file so Airflow DAGs can load AWS/Postgres settings
COPY nepal-simple-pipeline/.env /usr/local/airflow/.env

# Install dbt in an isolated virtual environment so its dependencies
# cannot conflict with Airflow's.
RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir "dbt-core==1.11.1" "dbt-postgres==1.10.2" && \
    deactivate

# The Astro runtime will install the project requirements automatically.
# Keep this image setup focused on dbt and the local airflow wiring.