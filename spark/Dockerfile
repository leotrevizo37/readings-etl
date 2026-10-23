FROM apache/spark:4.2.0

USER root

RUN mkdir -p /opt/spark/jars-extra \
 && curl -L -o /opt/spark/jars-extra/hadoop-azure-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.5.0/hadoop-azure-3.5.0.jar \
 && curl -L -o /opt/spark/jars-extra/hadoop-azure-datalake-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.5.0/hadoop-azure-datalake-3.5.0.jar \
 && curl -L -o /opt/spark/jars-extra/azure-storage-7.0.1.jar \
      https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/7.0.1/azure-storage-7.0.1.jar \
 && curl -L -o /opt/spark/jars-extra/mssql-jdbc-12.6.1.jre11.jar \
      https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.6.1.jre11/mssql-jdbc-12.6.1.jre11.jar

RUN mkdir -p /opt/spark/jars \
 && curl -fL --retry 3 --retry-delay 5 -o /opt/spark/jars/hadoop-aws-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
 && curl -fL --retry 3 --retry-delay 5 -o /opt/spark/jars/software.amazon.awssdk_bundle-2.35.4.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
 && curl -fL --retry 3 --retry-delay 5 -o /opt/spark/jars/analyticsaccelerator-s3-1.3.1.jar \
      https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar \
 && curl -fL --retry 3 --retry-delay 5 -o /opt/spark/jars/wildfly-openssl-2.2.5.Final.jar \
      https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/2.2.5.Final/wildfly-openssl-2.2.5.Final.jar

ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars-extra/*

USER 185
