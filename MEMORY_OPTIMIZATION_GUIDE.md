# Memory Optimization Guide - NYC Taxi Trip Duration MLOps

## Overview

This guide documents the memory optimization strategies implemented to prevent OOM (Out of Memory) errors during data processing, particularly in the Gold layer (feature engineering).

---

## 🔥 Common OOM Causes Identified

### 1. **Multiple GroupBy + Joins in Feature Engineering**
**Location**: `src/data_transformation/feature_engineering.py:168-181`

**Problem**: Computing frequency features with 3 separate groupBy operations followed by 3 joins:
```python
pickup_counts = df.groupBy("PULocationID").count()
dropoff_counts = df.groupBy("DOLocationID").count()
route_counts = df.groupBy("route_id").count()
df = df.join(pickup_counts, ...).join(dropoff_counts, ...).join(route_counts, ...)
```

**Impact**: Each groupBy triggers a full shuffle, consuming memory. Multiple joins on large DataFrames multiply memory usage.

**Solution**: ✅ Broadcast small aggregated DataFrames and explicit memory cleanup between joins

### 2. **No Explicit Cache Management**
**Problem**: Intermediate DataFrames retained in memory throughout pipeline

**Solution**: ✅ Added `MemoryManager` class with explicit `unpersist()` and cache clearing

### 3. **Fixed Partition Counts**
**Problem**: Using fixed partitions (10-50) regardless of data size

**Solution**: ✅ Dynamic partition optimization based on DataFrame size

### 4. **Suboptimal Spark Configurations**
**Problem**: Default Spark settings not tuned for memory-constrained environments

**Solution**: ✅ Memory-optimized configurations in DAG and code

---

## ✅ Implemented Solutions

### 1. Memory Management Utilities

**File**: `src/utils/memory_utils.py`

#### `MemoryManager` Class
```python
from src.utils.memory_utils import MemoryManager

memory_manager = MemoryManager(spark)

# Clear cache explicitly
memory_manager.clear_cache()

# Optimize partitions dynamically
optimized_partitions = memory_manager.optimize_partitions(df)

# Repartition if needed
df = memory_manager.repartition_if_needed(df, target_partitions=50)
```

**Features**:
- Explicit cache clearing (`unpersist()` + `clearCache()` + Python GC)
- Dynamic partition optimization
- Memory statistics monitoring
- Checkpoint support for breaking lineage

#### Configuration Function
```python
from src.utils.memory_utils import configure_memory_optimized_spark

configure_memory_optimized_spark(spark)
```

**Applied Settings**:
```yaml
spark.memory.fraction: 0.6  # Down from 0.7
spark.memory.storageFraction: 0.3  # Up from 0.2
spark.sql.shuffle.partitions: 50  # Down from 200
spark.sql.autoBroadcastJoinThreshold: 10MB  # Optimized
spark.executor.memoryOverhead: 512m  # Added
```

---

### 2. Broadcast Joins for Small DataFrames

**Location**: `src/data_transformation/feature_engineering.py:168-220`

**Before** (OOM-prone):
```python
pickup_counts = df.groupBy("PULocationID").count()
df = df.join(pickup_counts, on="PULocationID", how="left")  # Large shuffle!
```

**After** (Memory-efficient):
```python
from pyspark.sql.functions import broadcast

pickup_counts = df.groupBy("PULocationID").count()
df = df.join(broadcast(pickup_counts), on="PULocationID", how="left")  # Broadcast!

# Explicit cleanup
pickup_counts.unpersist()
del pickup_counts
```

**Why it works**:
- Broadcast sends small DataFrame to all executors (< 10MB)
- Avoids shuffle on large DataFrame
- Explicit unpersist frees memory immediately

---

### 3. Memory-Optimized Transformations

**Preprocessing** (`src/data_transformation/preprocessing.py`):
```python
def _transform_data(self, df: DataFrame) -> DataFrame:
    # Clear cache before starting
    self.memory_manager.clear_cache()

    # Optimize partitions
    df = self.memory_manager.repartition_if_needed(df, target_partitions=50)

    # Apply transformations
    cleaned_df = self.cleaning_pipeline.run(df)

    # Clear intermediate DataFrame
    self.memory_manager.clear_cache(df)

    return cleaned_df
```

**Feature Engineering** (`src/data_transformation/feature_engineering.py`):
- Same pattern with additional broadcast joins
- Clears cache between aggregations

---

### 4. Optimized Spark Job Configurations

**Location**: `airflow/dags/nyc_taxi_etl_production_dag.py`

**Key Changes**:
```python
conf = {
    # Memory settings
    'spark.executor.memory': '2g',
    'spark.driver.memory': '2g',
    'spark.executor.memoryOverhead': '512m',  # NEW

    # Memory management
    'spark.memory.fraction': '0.6',  # Down from 0.7
    'spark.memory.storageFraction': '0.3',  # Up from 0.2
    'spark.cleaner.periodicGC.interval': '10min',  # NEW

    # Shuffle optimization
    'spark.sql.shuffle.partitions': '50',  # Down from 200

    # Broadcast optimization
    'spark.sql.autoBroadcastJoinThreshold': '10485760',  # 10MB

    # Disable Arrow (can cause OOM)
    'spark.sql.execution.arrow.pyspark.enabled': 'false',  # NEW

    # In-memory compression
    'spark.sql.inMemoryColumnarStorage.compressed': 'true',  # NEW
    'spark.sql.inMemoryColumnarStorage.batchSize': '5000',  # NEW (reduced)
}
```

---

### 5. Explicit Cache Clearing in Spark Jobs

**Location**: `spark_jobs/feature_engineering_job.py:73-75`

```python
for month in months:
    # Process month
    gold_path = feature_eng.process_and_save(year, [month])

    # Clear Spark cache after each month
    feature_eng.spark.catalog.clearCache()
    print(f"Cleared Spark cache after processing month {month:02d}")
```

**Also in**: `spark_jobs/data_preprocessing_job.py:73-75`

---

## 📊 Memory Optimization Checklist

When processing large datasets:

- [ ] **Reduce shuffle partitions** (50 instead of 200)
- [ ] **Use broadcast joins** for small DataFrames (< 10MB)
- [ ] **Clear cache explicitly** between transformations
- [ ] **Avoid count()** operations (triggers full scan)
- [ ] **Repartition before heavy operations** (50 partitions)
- [ ] **Disable Arrow** (`spark.sql.execution.arrow.pyspark.enabled: false`)
- [ ] **Add executor memory overhead** (512m)
- [ ] **Enable adaptive query execution** (`spark.sql.adaptive.enabled: true`)
- [ ] **Process month-by-month** (already implemented)
- [ ] **Use coalesce for writes** (avoids shuffle)

---

## 🛠️ Debugging OOM Errors

### 1. Monitor Memory Usage

**Spark UI**: http://localhost:8081
- Check "Executors" tab for memory usage
- Look for "Memory Spilled to Disk" (indicates pressure)

**Logs**:
```bash
# Check Spark job logs
docker logs <spark-worker-container> | grep -i "memory\|oom"

# Check Airflow task logs
airflow tasks logs nyc_taxi_etl_production2 data_transformation <date>
```

### 2. Identify Memory Bottlenecks

**Check DataFrame Size**:
```python
# Estimate DataFrame size (avoid on large data!)
df.cache()
df.count()
df.unpersist()

# Check partitions
print(f"Partitions: {df.rdd.getNumPartitions()}")
```

**Check for Wide Transformations**:
- groupBy, join, distinct, repartition
- These trigger shuffles and consume memory

### 3. Increase Memory (Last Resort)

**docker-compose.yml**:
```yaml
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=4g  # Increase from 2g
```

**DAG Configuration**:
```python
conf = {
    'spark.executor.memory': '4g',  # Increase from 2g
    'spark.driver.memory': '4g',
}
```

---

## 🎯 Best Practices

### 1. Process Data Incrementally
- ✅ Already implemented: Month-by-month processing
- Each month processed separately to isolate memory usage

### 2. Avoid Caching Unless Necessary
```python
# ❌ Avoid
df.cache()
df.count()
# ... long operations ...
df.unpersist()

# ✅ Prefer
# Don't cache unless DataFrame is reused multiple times
```

### 3. Use Broadcast for Small DataFrames
```python
# ❌ Avoid (large shuffle)
large_df.join(small_df, on="id")

# ✅ Prefer (broadcast)
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), on="id")
```

### 4. Minimize Shuffles
```python
# ❌ Multiple shuffles
df.groupBy("col1").count()  # Shuffle 1
df.groupBy("col2").count()  # Shuffle 2
df.groupBy("col3").count()  # Shuffle 3

# ✅ Single shuffle (if possible)
df.groupBy("col1", "col2", "col3").agg(...)
```

### 5. Clean Up Intermediate DataFrames
```python
# ✅ Always
intermediate_df = transform(df)
result_df = process(intermediate_df)

# Clean up
intermediate_df.unpersist()
del intermediate_df
```

---

## 🔧 Configuration Tuning

### Memory Fraction
```python
# Controls memory split: execution vs storage
spark.memory.fraction = 0.6  # 60% for Spark, 40% for system
spark.memory.storageFraction = 0.3  # Within Spark: 30% storage, 70% execution
```

### Shuffle Partitions
```python
# Rule of thumb: 50-100 partitions for monthly data
spark.sql.shuffle.partitions = 50

# Or calculate: ~128MB per partition
num_partitions = data_size_mb / 128
```

### Broadcast Threshold
```python
# Auto-broadcast DataFrames smaller than this
spark.sql.autoBroadcastJoinThreshold = 10485760  # 10MB

# For frequency features (pickup/dropoff counts), usually < 5MB
```

---

## 📈 Performance vs Memory Trade-offs

| Setting | Memory Impact | Performance Impact | Recommendation |
|---------|---------------|-------------------|----------------|
| Fewer shuffle partitions | ↓ Lower | ↑ Faster (less overhead) | 50 for monthly data |
| More shuffle partitions | ↑ Higher | ↓ Slower (coordination) | 200+ for full-year |
| Broadcast joins | ↓ Lower | ↑ Faster (no shuffle) | Always for < 10MB |
| Cache DataFrames | ↑ Higher | ↑ Faster (reuse) | Only if reused 2+ times |
| Disable Arrow | ↓ Lower | ↓ Slower (serialization) | Enable only if no OOM |
| Memory overhead | ↑ Higher | ↑ Stable (no OOM) | 512m recommended |

---

## 🚨 Emergency OOM Recovery

If you encounter OOM during production run:

### Step 1: Stop Failing Job
```bash
# In Airflow UI, mark task as failed
# Or kill Spark application
./spark-submit --kill <app-id>
```

### Step 2: Reduce Memory Pressure
1. **Process fewer months**: `--months 1,2,3` instead of all 12
2. **Reduce partitions**: Set `spark.sql.shuffle.partitions` to 30
3. **Increase memory**: Update `spark.executor.memory` to 4g

### Step 3: Clear Caches
```bash
# Connect to Spark master
docker exec -it <spark-master-container> bash

# Clear Spark cache
pyspark --conf spark.sql.clearCache=true
```

### Step 4: Restart with Tuned Settings
- Apply memory-optimized configurations
- Monitor Spark UI for memory usage
- Process incrementally (1 month at a time)

---

## 📝 Monitoring Recommendations

### 1. Spark UI Metrics
- **Storage Memory**: Should be < 80% of available
- **Execution Memory**: Spikes during shuffles are normal
- **Spilled Memory**: If > 0, indicates memory pressure

### 2. System Metrics
```bash
# Check Docker container memory
docker stats

# Check Spark worker memory
docker exec -it spark-worker free -h
```

### 3. Application Logs
```bash
# Watch for OOM indicators
grep -i "java.lang.OutOfMemoryError" airflow/logs/**/*.log
grep -i "Container killed" airflow/logs/**/*.log
```

---

## 🎓 Key Takeaways

1. **Broadcast small DataFrames** (< 10MB) to avoid shuffles
2. **Clear cache explicitly** between transformations
3. **Reduce shuffle partitions** (50 for monthly data)
4. **Disable Arrow** if encountering OOM
5. **Add memory overhead** (512m minimum)
6. **Process incrementally** (month-by-month)
7. **Monitor Spark UI** for memory usage patterns
8. **Avoid count()** operations on large DataFrames

---

## 🔗 Related Files

- **Memory utilities**: `src/utils/memory_utils.py`
- **Feature engineering**: `src/data_transformation/feature_engineering.py`
- **Preprocessing**: `src/data_transformation/preprocessing.py`
- **Base stage**: `src/data_transformation/base_stage.py`
- **Spark jobs**: `spark_jobs/feature_engineering_job.py`, `spark_jobs/data_preprocessing_job.py`
- **Airflow DAG**: `airflow/dags/nyc_taxi_etl_production_dag.py`

---

**Last Updated**: 2026-02-03
**Status**: ✅ Implemented and tested
