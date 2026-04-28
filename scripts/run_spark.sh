set -a
source .env
set +a

SCRIPT=$1

docker run --rm \
  --network project-stocks\
  -v "${PROJECT_PATH}:/app" \
  my-spark:latest \
  bash -c "
    mkdir -p /tmp/ivy && \
    export PYTHONPATH=/app && \
    /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /app/delta_lake/$SCRIPT
  "