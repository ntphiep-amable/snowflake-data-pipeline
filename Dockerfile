FROM quay.io/astronomer/astro-runtime:10.1.0

COPY include/astro_provider_snowflake-0.0.0-py3-none-any.whl /tmp

# Create the virtual environment
PYENV 3.8 snowpark requirements-snowpark.txt

# Install packages into the virtual environment
COPY requirements-snowpark.txt /tmp
RUN python3.8 -m pip install -r /tmp/requirements-snowpark.txt


RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && pip install --no-cache-dir dbt-postgres && deactivate


