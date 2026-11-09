FROM apache/airflow:3.3.2
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
# The AWS SDK bundle that hadoop-aws 3.5.0 depends on (686 MB, fully shaded). Loaded from the driver
# classpath, not as a Spark package: as a package it breaks Spark's executor heartbeat (see spark.py).
ARG AWS_SDK_VERSION=2.35.4
ENV AWS_SDK_BUNDLE=/opt/spark-jars/aws-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN mkdir -p /opt/spark-jars \
  && url="https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar" \
  && curl -fsSL -o "$AWS_SDK_BUNDLE" "$url" \
  && echo "$(curl -fsSL "$url.sha1")  $AWS_SDK_BUNDLE" | sha1sum -c - \
  && chmod a+r "$AWS_SDK_BUNDLE"
# Spark's library cache (Delta, hadoop-aws), filled once at build time below.
# Writable by any user: containers run as AIRFLOW_UID, and Ivy writes small resolution files on each start.
ENV SPARK_IVY_DIR=/opt/spark-ivy
RUN mkdir -p "$SPARK_IVY_DIR" && chown airflow:0 "$SPARK_IVY_DIR"
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
# Start Spark once so Ivy downloads the libraries into the image, instead of in every new container
COPY --chown=airflow:0 src/transit/__init__.py src/transit/spark.py /tmp/warmup/transit/
RUN PYTHONPATH=/tmp/warmup python -c "from transit.spark import get_spark; get_spark('warmup').stop()" \
  && rm -rf /tmp/warmup \
  && chmod -R a+rwX "$SPARK_IVY_DIR"
