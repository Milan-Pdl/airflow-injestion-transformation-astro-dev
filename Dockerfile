FROM astrocrpublic.azurecr.io/runtime:3.2-5

# Copy the dbt project into the image
COPY dbt/ /usr/local/airflow/dbt/

# Install dbt in an isolated virtual environment so its dependencies
# cannot conflict with Airflow's.
RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir "dbt-core==1.11.1" "dbt-postgres==1.10.2" && \
    deactivate