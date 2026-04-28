FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu

USER root

RUN mkdir -p /opt/spark/jars

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar

RUN curl -L -o /opt/spark/jars/aws-sdk-bundle-2.24.6.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar

RUN curl -L -o /opt/spark/jars/delta-spark_2.13-4.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar

RUN curl -L -o /opt/spark/jars/delta-storage-4.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar

RUN pip install minio 
RUN pip install delta-spark==4.0.0 --no-deps
RUN pip install python-dotenv

USER spark