# atuneros-spark-exploration

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

spark = SparkSession.builder \
    .master("spark://127.0.0.1:7077") \ 
    .appName("awesome-app") \
    .getOrCreate()
```

### Do an action and check it

```url
http://localhost:4040/jobs/
```
