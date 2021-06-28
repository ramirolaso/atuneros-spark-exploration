# atuneros-spark-exploration

## Spark + Jupyter setup

Based on this article https://towardsdatascience.com/apache-spark-cluster-on-docker-ft-a-juyterlab-interface-418383c95445

You can also check the repository https://github.com/cluster-apps-on-docker/spark-standalone-cluster-on-docker
### Start the cluster

```bash
docker-compose up -d
```

### Checkout the master WebUI

```url
    http://127.0.0.1:8080
```

### Build your session

```code
from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()
```

### Do an action and check it

```url
http://localhost:4040/jobs/
```
