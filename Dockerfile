FROM python:3.9.16-slim-buster as base

ENV POETRY_HOME=/etc/poetry \
    POETRY_VERSION=1.3.1
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

FROM base AS build

RUN apt-get update && \
    apt-get install -y -q build-essential \
    python3-dev  \
    libpq-dev \
    curl \
    librdkafka-dev \
    unixodbc \
    unixodbc-dev \
    openssl \
    libsasl2-dev

# For pyodbc
RUN curl -s -o microsoft.asc https://packages.microsoft.com/keys/microsoft.asc \
    && curl -s -o mssql-release.list https://packages.microsoft.com/config/debian/10/prod.list \
    && apt-get update -y \
    && apt-get install -y g++ unixodbc-dev

RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=${POETRY_HOME} python3 -
RUN poetry config virtualenvs.create false
RUN poetry config experimental.new-installer false

COPY poetry.lock pyproject.toml ./
RUN poetry lock --no-update
RUN poetry install --no-interaction --no-ansi --no-dev -vvv


FROM base as runtime

# For pyodbc
COPY --from=build microsoft.asc microsoft.asc
COPY --from=build mssql-release.list mssql-release.list

ENV ACCEPT_EULA=Y
RUN apt-get update -y && apt-get install -y gnupg2 \
    && apt-key add microsoft.asc && rm microsoft.asc \
    && mv mssql-release.list /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update -y \
    && apt-get install -y g++ unixodbc-dev \
    && apt-get install -y unixodbc-bin \
    && apt-get install -y msodbcsql17 libgssapi-krb5-2 libpq-dev \
    && apt-get install -y libaio1 wget unzip


WORKDIR /opt/oracle
# Installing Oracle instant client
RUN wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip && \
    unzip instantclient-basiclite-linuxx64.zip && rm -f instantclient-basiclite-linuxx64.zip && \
    cd /opt/oracle/instantclient* && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci && \
    echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf && ldconfig

RUN useradd --create-home --shell /bin/bash app
USER app

# non-interactive env vars https://bugs.launchpad.net/ubuntu/+source/ansible/+bug/1833013
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true
ENV UCF_FORCE_CONFOLD=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY . ./
COPY --from=build /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

ENTRYPOINT ["/bin/bash", "start.sh"]
