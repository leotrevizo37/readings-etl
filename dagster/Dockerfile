FROM python:3.10-slim-bookworm

ENV ACCEPT_EULA=Y \
    DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    PYTHONPATH=/opt/dagster/app/src

RUN mkdir -p "${DAGSTER_HOME}" /opt/dagster/app/projectA /opt/dagster/app/projectB /opt/dagster/app/projectC /opt/dagster/app/projectD

WORKDIR /opt/dagster/app

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       curl ca-certificates gnupg apt-transport-https \
       unixodbc unixodbc-dev gcc g++ \
       sudo \
       unzip \
       rsync \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
  && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/microsoft-prod.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends msodbcsql18 mssql-tools18 \
  && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/mssql-tools18/bin:${PATH}"

COPY dagster/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r /tmp/requirements.txt

RUN spark_jars_dir="$(python -c 'import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).resolve().parent / "jars")')" \
  && mkdir -p "${spark_jars_dir}" \
  && curl -fL --retry 3 --retry-delay 5 -o "${spark_jars_dir}/hadoop-aws-3.5.0.jar" \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
  && curl -fL --retry 3 --retry-delay 5 -o "${spark_jars_dir}/software.amazon.awssdk_bundle-2.35.4.jar" \
       https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
  && curl -fL --retry 3 --retry-delay 5 -o "${spark_jars_dir}/analyticsaccelerator-s3-1.3.1.jar" \
       https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar \
  && curl -fL --retry 3 --retry-delay 5 -o "${spark_jars_dir}/wildfly-openssl-2.2.5.Final.jar" \
       https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/2.2.5.Final/wildfly-openssl-2.2.5.Final.jar


COPY dagster/projectA /opt/dagster/app/projectA
COPY dagster/projectB /opt/dagster/app/projectB
COPY dagster/projectC /opt/dagster/app/projectC
COPY dagster/projectD /opt/dagster/app/projectD
