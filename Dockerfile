FROM astrocrpublic.azurecr.io/runtime:3.2-5

# Copy the dbt project into the image
COPY dbt/ /usr/local/airflow/dbt/

# Copy the runtime environment file so Airflow DAGs can load AWS/Postgres settings
COPY aws-simple-pipeline/.env /usr/local/airflow/.env

# Install dbt in an isolated virtual environment
RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir "dbt-core==1.11.1" "dbt-postgres==1.10.2" && \
    deactivate

# Switch to root to install OS system dependencies needed by Chromium
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    fontconfig \
    fonts-liberation \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Switch back to astro user to install the Chromium browser binary
USER astro
RUN playwright install chromium