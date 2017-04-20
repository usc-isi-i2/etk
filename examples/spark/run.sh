ls /usr/lib/spark/bin
mkdir bin
cp /usr/lib/spark/bin/load-spark-env.sh bin
cp pyspark bin/
mkdir conf
grep -v PYSPARK_PYTHON /usr/lib/spark/conf/spark-env.sh > conf/spark-env.sh

export PYSPARK_PYTHON=./etk_env.zip/etk_env/bin/python
export PYSPARK_DRIVER_PYTHON=./etk_env/etk_env/bin/python
export DEFAULT_PYTHON=./etk_env.zip/etk_env/bin/python
./bin/pyspark \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./etk_env.zip/etk_env/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./etk_env.zip/etk_env/bin/python \
  --conf spark.executorEnv.DEFAULT_PYTHON=./etk_env.zip/etk_env/bin/python \
  --deploy-mode client \
  --executor-memory 5g --num-executors 5 --executor-cores 1 \
  --archives etk_env.zip \
  $@
