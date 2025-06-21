# CLAUDE.md

docker compose project

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

- Download Apache Spark™
Choose a Spark release: 
4.0.0 (May 23 2025)


Choose a package type: 
Pre-built for Apache Hadoop 3.4 and later


Download Spark: [spark-4.0.0-bin-hadoop3.tgz](https://www.apache.org/dyn/closer.lua/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz)

Verify this release using the 4.0.0 signatures, checksums and project release KEYS by following these procedures.

Note that Spark 4 is pre-built with Scala 2.13, and support for Scala 2.12 has been officially dropped. Spark 3 is pre-built with Scala 2.12 in general and Spark 3.2+ provides additional pre-built distribution with Scala 2.13.

## Project Overview

This is a production-ready Python-based Apache Spark application with full observability stack integration. The project demonstrates ETL pipelines, streaming processing, machine learning workflows, and comprehensive telemetry using OpenTelemetry.

## Common Development Commands

### Environment Setup
```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Quick setup for development
./scripts/setup-dev.sh

# Or manually:
uv sync --dev              # Creates .venv and installs all dependencies
uv run pre-commit install  # Setup pre-commit hooks

# Virtual environment is automatically created at .venv
# No need to activate it when using 'uv run' commands
# To activate manually (optional):
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows
```

### Running Spark Applications
```bash
# Submit a PySpark job locally
spark-submit --master local[*] <script_name>.py

# Submit with specific configurations
spark-submit --master local[*] --conf spark.sql.shuffle.partitions=200 <script_name>.py

# Quick Start with Full Observability Stack (Recommended)
./scripts/start-full-stack.sh

# Run pipelines with uv
uv run spark-app --mode etl --date 2024-01-01
uv run spark-app --mode streaming
uv run spark-app --mode ml

# Or use Make targets
make run-etl DATE=2024-01-01
make run-streaming
make run-ml

# Run in Docker
docker exec spark-master /app/scripts/run_etl.sh 2024-01-01
docker exec spark-master /app/scripts/run_streaming.sh
docker exec spark-master /app/scripts/run_ml.sh
```

### Access Services
- **Spark Master UI**: http://localhost:8080
- **Grafana**: http://localhost:3000 (admin/admin)
- **Jaeger UI**: http://localhost:16686
- **Kibana**: http://localhost:5601
- **Prometheus**: http://localhost:9090
- **Jupyter Lab**: http://localhost:8888

### Code Quality and Testing
```bash
# Format code with Ruff
uv run ruff format src/ tests/
# Or: make format

# Lint code
uv run ruff check src/ tests/
# Or: make lint

# Type checking
uv run mypy src/
# Or: make type-check

# Run tests
uv run pytest tests/
# Or: make test

# Run all checks
make dev-check  # format + lint + type-check + test
```

## Architecture Guidelines

When developing PySpark applications in this repository:

1. **Data Processing Pipeline Structure**: Organize ETL jobs with clear separation of:
   - Data ingestion/reading
   - Transformation logic
   - Data writing/output

2. **Configuration Management**: Store Spark configurations and job parameters in separate configuration files rather than hardcoding them.

3. **Testing Strategy**: Write unit tests for transformation functions using PySpark's local mode, and integration tests for end-to-end data pipelines.

4. **Performance Considerations**: 
   - Use DataFrame API over RDD API when possible
   - Leverage Spark's catalyst optimizer
   - Be mindful of data partitioning and shuffling operations

## Development Notes

- This project is set up for local development with Apache Spark
- The .claude directory contains local settings for Claude Code permissions
- Full observability stack is integrated with OpenTelemetry Collector
- All services can be run via Docker Compose for easy development
- Pre-configured Grafana dashboards are available for monitoring
- Telemetry data (metrics, traces, logs) flows through OTEL Collector to various backends
- Modern Python tooling with uv for fast package management
- Code quality enforced with Ruff (linting and formatting)
- Pre-commit hooks for automated quality checks

## Key Files and Directories

- `src/app.py` - Main application entry point with CLI interface
- `src/etl/` - ETL pipeline modules with data quality checks
- `src/streaming/` - Kafka streaming processors
- `src/ml/` - Machine learning pipelines
- `src/utils/telemetry.py` - OpenTelemetry integration
- `config/otel-collector-config.yaml` - OTEL Collector configuration
- `docker-compose.full.yml` - Full stack Docker Compose
- `scripts/start-full-stack.sh` - One-command startup script
- `pyproject.toml` - Project metadata and dependencies (uv/ruff config)
- `ruff.toml` - Extended Ruff configuration
- `Makefile` - Common development tasks

## Monitoring and Troubleshooting

### Check Service Health
```bash
# All services status
docker-compose -f docker-compose.full.yml ps

# OpenTelemetry Collector health
curl http://localhost:13133/health

# Prometheus targets
curl http://localhost:9090/api/v1/targets
```

### Docker Compose Commands
```bash
# Start Spark cluster only
docker-compose up -d

# Start Spark + full observability stack
docker-compose -f docker-compose.full.yml up -d

# Start observability stack only
docker-compose -f docker-compose.observability.yml up -d

# Stop all services
docker-compose -f docker-compose.full.yml down -v
```

## Available Make Targets

```bash
make help         # Show all available targets
make install      # Install production dependencies
make install-dev  # Install with dev dependencies
make format       # Format code with Ruff
make lint         # Lint code
make test         # Run tests
make dev-check    # Run all quality checks
make run-etl      # Run ETL pipeline
make run-streaming # Run streaming pipeline
make run-ml       # Run ML pipeline
make docker-up    # Start Spark cluster
make docker-full-up # Start full stack
```


# Apache Spark 완벽 가이드: 10년 경력 시니어 개발자를 위한 심화 기술 문서

## Apache Spark의 아키텍처와 핵심 개념 이해하기

Apache Spark는 대규모 데이터 처리를 위한 통합 분석 엔진으로, **인메모리 처리**와 **지연 평가(lazy evaluation)**를 통해 기존 MapReduce 대비 100배 빠른 성능을 제공합니다. Spark 3.x 버전 기준으로 AQE(Adaptive Query Execution), 동적 파티션 병합, 향상된 Catalyst 옵티마이저가 도입되어 자동 최적화 기능이 크게 향상되었습니다.

### 핵심 아키텍처 구성요소

**드라이버 프로그램(Driver Program)**은 사용자 애플리케이션의 main() 함수를 실행하고 SparkContext를 생성합니다. SparkContext는 다음 핵심 컴포넌트들을 포함합니다:

- **DAG 스케줄러**: 논리적 실행 계획을 스테이지로 변환
- **태스크 스케줄러**: 실행자(Executor)에 태스크 분배
- **백엔드 스케줄러**: 클러스터 리소스 관리
- **블록 매니저**: 분산 데이터 저장소 관리

**실행자(Executors)**는 워커 노드에서 실행되는 JVM 프로세스로, 다중 스레드에서 태스크를 실행하고 애플리케이션 수명 동안 데이터를 캐시합니다. 각 실행자는 태스크 실행 결과와 상태를 드라이버에 보고합니다.

### 핵심 추상화 계층

**RDD (Resilient Distributed Dataset)**는 Spark의 기본 데이터 구조로, 불변(immutable)이며 장애 복구가 가능한 분산 객체 컬렉션입니다. RDD는 다음과 같은 특징을 가집니다:

```scala
// RDD 생성과 변환 예제
val textFile = sc.textFile("hdfs://path/to/file")
val words = textFile.flatMap(line => line.split(" "))
val wordPairs = words.map(word => (word, 1))
val wordCounts = wordPairs.reduceByKey(_ + _)
```

**DataFrame**은 스키마를 가진 구조화된 데이터 추상화로, Catalyst 옵티마이저와 통합되어 있습니다. SQL과 유사한 연산을 제공하며 성능 최적화가 자동으로 적용됩니다:

```python
# DataFrame API example with optimization
df = spark.read.parquet("s3://bucket/data.parquet")
result = df.filter(col("amount") > 100) \
          .groupBy("category") \
          .agg(sum("amount").alias("total")) \
          .orderBy("total", ascending=False)
```

**Dataset**은 타입 안전성을 제공하는 DataFrame의 확장 버전으로, Scala와 Java에서 사용 가능합니다. 컴파일 타임 타입 체크와 런타임 최적화를 모두 제공합니다.

### Catalyst 옵티마이저의 내부 동작

Catalyst는 **4단계 최적화 프로세스**를 거칩니다:

1. **분석(Analysis)**: Catalog를 사용한 참조 해결
2. **논리적 최적화**: Predicate pushdown, column pruning, constant folding
3. **물리적 계획**: Join 알고리즘을 위한 비용 기반 최적화
4. **코드 생성**: Tungsten을 통한 전체 스테이지 코드 생성

## Hadoop MapReduce와의 상세 비교 분석

### 성능 차이의 근본 원인

Spark가 MapReduce보다 빠른 이유는 **근본적인 처리 패러다임의 차이**에 있습니다. MapReduce는 각 스테이지 이후 중간 결과를 디스크에 쓰는 반면, Spark는 가능한 한 메모리에 데이터를 유지합니다. 이로 인해 메모리에서는 100배, 디스크에서도 10배 빠른 성능을 보입니다.

**메모리 대 디스크 처리**:
- MapReduce: 모든 Map/Reduce 단계 후 HDFS에 쓰기
- Spark: RDD를 메모리에 캐시하고 재사용
- 메모리 요구사항: Spark는 더 많은 RAM이 필요하지만 극적인 성능 향상 제공

### 프로그래밍 모델의 진화

**개발 생산성 측면**에서 Spark는 80개 이상의 고수준 연산자를 제공하는 풍부한 API를 통해 개발자 경험을 크게 개선했습니다:

```python
# Spark의 함수형 프로그래밍 예제
# MapReduce에서는 수십 줄이 필요한 작업을 몇 줄로 구현
result = df.filter(lambda x: x.amount > 100) \
          .map(lambda x: (x.category, x.amount)) \
          .reduceByKey(lambda a, b: a + b) \
          .sortBy(lambda x: x[1], ascending=False)
```

### 장애 복구 메커니즘

**Spark의 리니지 기반 복구**는 RDD의 변환 이력을 추적하여 실패한 파티션만 재계산합니다. 이는 MapReduce의 태스크 레벨 재시작보다 효율적이지만, 메모리 데이터 손실 시 재계산이 필요할 수 있습니다.

## Spark 모듈별 심화 분석

### Spark Core의 고급 기능

**파티셔닝 전략**은 성능에 직접적인 영향을 미칩니다:

```scala
// 커스텀 파티셔너 구현
import org.apache.spark.Partitioner

class CustomPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    key.toString.hashCode % numPartitions
  }
}

// 파티셔너 적용
rdd.partitionBy(new CustomPartitioner(10))
```

### Spark SQL의 Tungsten 실행 엔진

Tungsten은 **CPU와 메모리 효율성**을 극대화하는 실행 엔진입니다:

- Off-heap 메모리 관리로 GC 압력 감소
- 캐시 친화적 데이터 구조
- 전체 스테이지 코드 생성으로 가상 함수 호출 제거
- 벡터화된 실행으로 CPU 효율성 향상

### Structured Streaming의 두 가지 실행 모드

**마이크로 배치 모드**:
- 100ms 이상의 지연 시간
- Exactly-once 보장
- 기존 DataFrame API와 동일한 최적화

**연속 처리 모드** (실험적 기능):
- 1ms 수준의 극저지연
- At-least-once 보장
- 특정 연산만 지원

```python
# Structured Streaming with event time processing
streaming_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .load()

# Watermark를 사용한 늦은 데이터 처리
result = streaming_df \
  .withWatermark("timestamp", "10 minutes") \
  .groupBy(window(col("timestamp"), "5 minutes")) \
  .count()
```

### MLlib의 파이프라인 아키텍처

MLlib는 **scikit-learn 스타일의 파이프라인 API**를 제공합니다:

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

# 파이프라인 구성 - 각 단계는 DataFrame 변환
indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
assembler = VectorAssembler(
    inputCols=["feature1", "feature2", "categoryIndex"], 
    outputCol="features"
)
rf = RandomForestClassifier(
    featuresCol="features", 
    labelCol="label",
    numTrees=100,
    maxDepth=10
)

pipeline = Pipeline(stages=[indexer, assembler, rf])
model = pipeline.fit(training_data)
```

## 시작하기: 설치부터 첫 애플리케이션까지

### 프로덕션 환경별 설치 전략

**Kubernetes 배포**가 최신 트렌드이며, 다음과 같은 장점을 제공합니다:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-app
spec:
  type: Scala
  mode: cluster
  image: "apache/spark:3.5.5"
  sparkVersion: "3.5.5"
  mainClass: com.example.SparkApp
  mainApplicationFile: "s3://bucket/app.jar"
  driver:
    cores: 2
    memory: "4g"
  executor:
    cores: 4
    instances: 10
    memory: "8g"
```

### 개발 환경 최적화

**IntelliJ IDEA 설정**:
```scala
// build.sbt
val sparkVersion = "3.5.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
)

// assembly 설정으로 fat JAR 생성
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
```

### 첫 프로덕션급 애플리케이션

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# SparkSession 생성 with 프로덕션 설정
spark = SparkSession.builder \
    .appName("ProductionApp") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# 스키마 정의 - 성능 향상을 위해 명시적 스키마 사용
schema = StructType([
    StructField("user_id", LongType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("event_type", StringType(), False),
    StructField("properties", MapType(StringType(), StringType()), True)
])

# 데이터 읽기 with 스키마
df = spark.read.schema(schema).parquet("s3://data/events/")

# 복잡한 변환 with 최적화
result = df.filter(col("event_type").isin("purchase", "add_to_cart")) \
    .withColumn("hour", hour("timestamp")) \
    .groupBy("hour", "event_type") \
    .agg(
        count("*").alias("event_count"),
        countDistinct("user_id").alias("unique_users"),
        collect_list("properties").alias("all_properties")
    ) \
    .withColumn("conversion_rate", 
        when(col("event_type") == "purchase", col("event_count") / col("unique_users"))
        .otherwise(0)
    )

# 파티션 최적화와 함께 저장
result.repartition(24, "hour") \
    .sortWithinPartitions("event_type") \
    .write \
    .mode("overwrite") \
    .partitionBy("hour") \
    .parquet("s3://output/hourly_metrics/")
```

## 고급 성능 최적화 기법

### Adaptive Query Execution (AQE) 활용

AQE는 Spark 3.0+에서 도입된 **런타임 최적화 기능**으로, 다음을 자동으로 수행합니다:

```python
# AQE 설정
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Skew join 최적화 임계값
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

### 메모리 튜닝 전략

**실행자 메모리 구성**:

```bash
spark-submit \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 20 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.default.parallelism=400 \
  your-app.jar
```

메모리 할당 권장사항:
- **spark.memory.fraction**: 실행과 저장을 위한 힙 비율 (기본값 0.6)
- **spark.memory.storageFraction**: 저장용 메모리 비율 (기본값 0.5)
- 실행자당 메모리는 40GB를 초과하지 않도록 설정 (GC 오버헤드 방지)

### 브로드캐스트 조인 최적화

```python
# 작은 테이블을 브로드캐스트하여 셔플 제거
from pyspark.sql.functions import broadcast

# 자동 브로드캐스트 임계값 설정 (기본값 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# 명시적 브로드캐스트
large_df = spark.read.parquet("s3://large-table/")
small_df = spark.read.parquet("s3://small-lookup-table/")

# 브로드캐스트 조인 - 셔플 없이 실행
result = large_df.join(broadcast(small_df), "key")
```

### 캐싱 전략 심화

```python
from pyspark import StorageLevel

# 다양한 저장 레벨 활용
df.persist(StorageLevel.MEMORY_ONLY)       # 빠르지만 메모리 집약적
df.persist(StorageLevel.MEMORY_ONLY_SER)   # 직렬화로 메모리 절약
df.persist(StorageLevel.MEMORY_AND_DISK)   # 안정적이지만 느림
df.persist(StorageLevel.OFF_HEAP)          # GC 압력 감소

# 캐시 사용 모니터링
df.cache()
df.count()  # 캐시 materialization
print(spark.sparkContext.statusTracker().getCacheFraction())
```

## 엔터프라이즈 통합 패턴

### Delta Lake를 활용한 ACID 트랜잭션

Delta Lake는 **데이터 레이크에 ACID 트랜잭션**을 제공합니다:

```python
from delta.tables import DeltaTable

# Delta 테이블 생성
df.write.format("delta").mode("overwrite").save("/path/to/delta-table")

# MERGE 연산 (Upsert)
deltaTable = DeltaTable.forPath(spark, "/path/to/delta-table")

deltaTable.alias("target").merge(
    updates_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(set = {
    "value": "source.value",
    "updated_at": "current_timestamp()"
}).whenNotMatchedInsert(values = {
    "id": "source.id",
    "value": "source.value",
    "created_at": "current_timestamp()"
}).execute()

# Time Travel 기능
df_version_0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
df_timestamp = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)
```

### Kafka와의 실시간 통합

```python
# Structured Streaming with Kafka - exactly-once semantics
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("failOnDataLoss", "false") \
    .load()

# 메시지 파싱과 처리
parsed_df = kafka_df.select(
    col("key").cast("string"),
    from_json(col("value").cast("string"), event_schema).alias("data"),
    col("timestamp")
).select("key", "data.*", "timestamp")

# 출력 with checkpointing
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "/checkpoint/path") \
    .trigger(processingTime="10 seconds") \
    .start("/output/path")
```

## 실전 프로덕션 사례

### 대규모 ETL 파이프라인

**데이터 품질 보장을 위한 파이프라인**:

```python
from pyspark.sql.functions import *
from typing import Dict

def data_quality_check(df: DataFrame) -> Dict[str, Any]:
    """데이터 품질 검증 함수"""
    total_count = df.count()
    
    # 널 체크
    null_counts = {}
    for col_name in df.columns:
        null_counts[col_name] = df.filter(col(col_name).isNull()).count()
    
    # 중복 체크
    duplicate_count = total_count - df.dropDuplicates().count()
    
    # 이상치 검출 (예: amount 필드)
    if "amount" in df.columns:
        stats = df.select(
            mean("amount").alias("mean"),
            stddev("amount").alias("std")
        ).collect()[0]
        
        outliers = df.filter(
            (col("amount") > stats["mean"] + 3 * stats["std"]) |
            (col("amount") < stats["mean"] - 3 * stats["std"])
        ).count()
    else:
        outliers = 0
    
    return {
        "total_records": total_count,
        "null_counts": null_counts,
        "duplicates": duplicate_count,
        "outliers": outliers
    }

# 프로덕션 ETL 파이프라인
def run_etl_pipeline(date: str):
    # 1. 데이터 읽기
    raw_df = spark.read.parquet(f"s3://raw-data/date={date}/")
    
    # 2. 데이터 품질 체크
    quality_metrics = data_quality_check(raw_df)
    print(f"Data quality metrics: {quality_metrics}")
    
    # 3. 변환 로직
    transformed_df = raw_df \
        .filter(col("status").isin("completed", "pending")) \
        .withColumn("processed_date", current_date()) \
        .withColumn("amount_category", 
            when(col("amount") < 100, "small")
            .when(col("amount") < 1000, "medium")
            .otherwise("large")
        )
    
    # 4. 집계
    summary_df = transformed_df.groupBy("amount_category", "status") \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
    
    # 5. 결과 저장 with 파티셔닝
    transformed_df.write \
        .mode("overwrite") \
        .partitionBy("processed_date", "amount_category") \
        .parquet(f"s3://processed-data/date={date}/")
    
    summary_df.write \
        .mode("overwrite") \
        .parquet(f"s3://summary-data/date={date}/")
```

### 실시간 머신러닝 파이프라인

```python
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline, PipelineModel

# 모델 학습 파이프라인
def train_model(training_data: DataFrame) -> PipelineModel:
    # Feature engineering
    assembler = VectorAssembler(
        inputCols=["feature1", "feature2", "feature3"],
        outputCol="raw_features",
        handleInvalid="skip"
    )
    
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    # 모델
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    
    # 파이프라인 구성
    pipeline = Pipeline(stages=[assembler, scaler, rf])
    
    # 모델 학습
    model = pipeline.fit(training_data)
    
    # 모델 저장
    model.write().overwrite().save("s3://models/rf_model_latest")
    
    return model

# 실시간 예측 (Structured Streaming)
def real_time_prediction():
    # 저장된 모델 로드
    model = PipelineModel.load("s3://models/rf_model_latest")
    
    # 스트리밍 데이터 읽기
    streaming_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "prediction_requests") \
        .load()
    
    # 데이터 파싱
    parsed_df = streaming_df.select(
        from_json(col("value").cast("string"), prediction_schema).alias("data")
    ).select("data.*")
    
    # 예측 수행
    predictions = model.transform(parsed_df)
    
    # 결과 출력
    query = predictions.select("id", "prediction", "probability") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "predictions") \
        .option("checkpointLocation", "/checkpoint/predictions") \
        .start()
    
    return query
```

## 프로덕션 환경을 위한 핵심 권장사항

### 성능 최적화 체크리스트

1. **Spark 3.x 버전 사용**: AQE, DPP(Dynamic Partition Pruning), CBO 활용
2. **DataFrame/Dataset API 우선**: RDD 대비 최적화된 성능
3. **적응형 쿼리 실행 활성화**: 런타임 최적화 자동 적용
4. **컬럼형 포맷 사용**: Parquet, Delta Lake로 분석 워크로드 최적화
5. **적절한 파티셔닝 전략**: 쿼리 패턴에 맞는 파티션 키 선택

### 메모리 및 GC 튜닝

```bash
# G1GC 설정 (권장)
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35"
```

### 보안 설정

```python
# Kerberos 인증
spark.conf.set("spark.security.credentials.hive.enabled", "true")
spark.conf.set("spark.yarn.keytab", "/path/to/keytab")
spark.conf.set("spark.yarn.principal", "user@DOMAIN.COM")

# SSL/TLS 설정
spark.conf.set("spark.ssl.enabled", "true")
spark.conf.set("spark.ssl.keyStore", "/path/to/keystore")
spark.conf.set("spark.ssl.keyStorePassword", "password")
```

### 모니터링 및 로깅

```python
# 커스텀 메트릭 수집
from pyspark.util import AccumulatorParam

class DictAccumulator(AccumulatorParam):
    def zero(self, value):
        return {}
    
    def addInPlace(self, acc1, acc2):
        for k, v in acc2.items():
            acc1[k] = acc1.get(k, 0) + v
        return acc1

metrics = spark.sparkContext.accumulator({}, DictAccumulator())

# 애플리케이션에서 메트릭 수집
def process_record(record):
    metrics.add({"processed": 1})
    if record.amount > 1000:
        metrics.add({"high_value": 1})
    return record
```

이 가이드는 10년 경력 시니어 개발자를 위한 Apache Spark의 모든 핵심 측면을 다루었습니다. 프로덕션 환경에서의 실제 구현을 위한 고급 패턴과 최적화 기법에 중점을 두었으며, 최신 Spark 3.x 기능을 활용한 엔터프라이즈급 데이터 처리 솔루션 구축에 필요한 모든 내용을 포함했습니다.


Getting Started
Starting Point: SparkSession
Creating DataFrames
Untyped Dataset Operations (aka DataFrame Operations)
Running SQL Queries Programmatically
Global Temporary View
Creating Datasets
Interoperating with RDDs
Inferring the Schema Using Reflection
Programmatically Specifying the Schema
Scalar Functions
Aggregate Functions
Starting Point: SparkSession
Python
Scala
Java
R
The entry point into all functionality in Spark is the SparkSession class. To create a basic SparkSession, just use SparkSession.builder:

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
Find full example code at "examples/src/main/python/sql/basic.py" in the Spark repo.
SparkSession in Spark 2.0 provides builtin support for Hive features including the ability to write queries using HiveQL, access to Hive UDFs, and the ability to read data from Hive tables. To use these features, you do not need to have an existing Hive setup.

Creating DataFrames
Python
Scala
Java
R
With a SparkSession, applications can create DataFrames from an existing RDD, from a Hive table, or from Spark data sources.

As an example, the following creates a DataFrame based on the content of a JSON file:

# spark is an existing SparkSession
df = spark.read.json("examples/src/main/resources/people.json")
# Displays the content of the DataFrame to stdout
df.show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+
Find full example code at "examples/src/main/python/sql/basic.py" in the Spark repo.
Untyped Dataset Operations (aka DataFrame Operations)
DataFrames provide a domain-specific language for structured data manipulation in Python, Scala, Java and R.

As mentioned above, in Spark 2.0, DataFrames are just Dataset of Rows in Scala and Java API. These operations are also referred as “untyped transformations” in contrast to “typed transformations” come with strongly typed Scala/Java Datasets.

Here we include some basic examples of structured data processing using Datasets:

Python
Scala
Java
R
In Python, it’s possible to access a DataFrame’s columns either by attribute (df.age) or by indexing (df['age']). While the former is convenient for interactive data exploration, users are highly encouraged to use the latter form, which is future proof and won’t break with column names that are also attributes on the DataFrame class.

# spark, df are from the previous example
# Print the schema in a tree format
df.printSchema()
# root
# |-- age: long (nullable = true)
# |-- name: string (nullable = true)

# Select only the "name" column
df.select("name").show()
# +-------+
# |   name|
# +-------+
# |Michael|
# |   Andy|
# | Justin|
# +-------+

# Select everybody, but increment the age by 1
df.select(df['name'], df['age'] + 1).show()
# +-------+---------+
# |   name|(age + 1)|
# +-------+---------+
# |Michael|     null|
# |   Andy|       31|
# | Justin|       20|
# +-------+---------+

# Select people older than 21
df.filter(df['age'] > 21).show()
# +---+----+
# |age|name|
# +---+----+
# | 30|Andy|
# +---+----+

# Count people by age
df.groupBy("age").count().show()
# +----+-----+
# | age|count|
# +----+-----+
# |  19|    1|
# |null|    1|
# |  30|    1|
# +----+-----+
Find full example code at "examples/src/main/python/sql/basic.py" in the Spark repo.
For a complete list of the types of operations that can be performed on a DataFrame refer to the API Documentation.

In addition to simple column references and expressions, DataFrames also have a rich library of functions including string manipulation, date arithmetic, common math operations and more. The complete list is available in the DataFrame Function Reference.

Running SQL Queries Programmatically
Python
Scala
Java
R
The sql function on a SparkSession enables applications to run SQL queries programmatically and returns the result as a DataFrame.

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+
Find full example code at "examples/src/main/python/sql/basic.py" in the Spark repo.
Global Temporary View
Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates. If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application terminates, you can create a global temporary view. Global temporary view is tied to a system preserved database global_temp, and we must use the qualified name to refer it, e.g. SELECT * FROM global_temp.view1.

Python
Scala
Java
SQL
# Register the DataFrame as a global temporary view
df.createGlobalTempView("people")

# Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+

# Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+
Find full example code at "examples/src/main/python/sql/basic.py" in the Spark repo.
Creating Datasets
Datasets are similar to RDDs, however, instead of using Java serialization or Kryo they use a specialized Encoder to serialize the objects for processing or transmitting over the network. While both encoders and standard serialization are responsible for turning an object into bytes, encoders are code generated dynamically and use a format that allows Spark to perform many operations like filtering, sorting and hashing without deserializing the bytes back into an object.

Scala
Java
import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public static class Person implements Serializable {
  private String name;
  private long age;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getAge() {
    return age;
  }

  public void setAge(long age) {
    this.age = age;
  }
}

// Create an instance of a Bean class
Person person = new Person();
person.setName("Andy");
person.setAge(32);

// Encoders are created for Java beans
Encoder<Person> personEncoder = Encoders.bean(Person.class);
Dataset<Person> javaBeanDS = spark.createDataset(
  Collections.singletonList(person),
  personEncoder
);
javaBeanDS.show();
// +---+----+
// |age|name|
// +---+----+
// | 32|Andy|
// +---+----+

// Encoders for most common types are provided in class Encoders
Encoder<Long> longEncoder = Encoders.LONG();
Dataset<Long> primitiveDS = spark.createDataset(Arrays.asList(1L, 2L, 3L), longEncoder);
Dataset<Long> transformedDS = primitiveDS.map(
    (MapFunction<Long, Long>) value -> value + 1L,
    longEncoder);
transformedDS.collect(); // Returns [2, 3, 4]

// DataFrames can be converted to a Dataset by providing a class. Mapping based on name
String path = "examples/src/main/resources/people.json";
Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
peopleDS.show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
Find full example code at "examples/src/main/java/org/apache/spark/examples/sql/JavaSparkSQLExample.java" in the Spark repo.
Interoperating with RDDs
Spark SQL supports two different methods for converting existing RDDs into Datasets. The first method uses reflection to infer the schema of an RDD that contains specific types of objects. This reflection-based approach leads to more concise code and works well when you already know the schema while writing your Spark application.

The second method for creating Datasets is through a programmatic interface that allows you to construct a schema and then apply it to an existing RDD. While this method is more verbose, it allows you to construct Datasets when the columns and their types are not known until runtime.

Inferring the Schema Using Reflection
Python
Scala
Java
Spark SQL can convert an RDD of Row objects to a DataFrame, inferring the datatypes. Rows are constructed by passing a list of key/value pairs as kwargs to the Row class. The keys of this list define the column names of the table, and the types are inferred by sampling the whole dataset, similar to the inference that is performed on JSON files.

from pyspark.sql import Row

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
for name in teenNames:
    print(name)
# Name: Justin
Find full example code at "examples/src/main/python/sql/basic.py" in the Spark repo.
Programmatically Specifying the Schema
Python
Scala
Java
When a dictionary of kwargs cannot be defined ahead of time (for example, the structure of records is encoded in a string, or a text dataset will be parsed and fields will be projected differently for different users), a DataFrame can be created programmatically with three steps.

Create an RDD of tuples or lists from the original RDD;
Create the schema represented by a StructType matching the structure of tuples or lists in the RDD created in the step 1.
Apply the schema to the RDD via createDataFrame method provided by SparkSession.
For example:

# Import data types
from pyspark.sql.types import StringType, StructType, StructField

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
# Each line is converted to a tuple.
people = parts.map(lambda p: (p[0], p[1].strip()))

# The schema is encoded in a string.
schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaPeople = spark.createDataFrame(people, schema)

# Creates a temporary view using the DataFrame
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT name FROM people")

results.show()
# +-------+
# |   name|
# +-------+
# |Michael|
# |   Andy|
# | Justin|
# +-------+
Find full example code at "examples/src/main/python/sql/basic.py" in the Spark repo.
Scalar Functions
Scalar functions are functions that return a single value per row, as opposed to aggregation functions, which return a value for a group of rows. Spark SQL supports a variety of Built-in Scalar Functions. It also supports User Defined Scalar Functions.

Aggregate Functions
Aggregate functions are functions that return a single value on a group of rows. The Built-in Aggregate Functions provide common aggregations such as count(), count_distinct(), avg(), max(), min(), etc. Users are not limited to the predefined aggregate functions and can create their own. For more details about user defined aggregate functions, please refer to the documentation of User Defined Aggregate Functions.


Overview
At a high level, every Spark application consists of a driver program that runs the user’s main function and executes various parallel operations on a cluster. The main abstraction Spark provides is a resilient distributed dataset (RDD), which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. RDDs are created by starting with a file in the Hadoop file system (or any other Hadoop-supported file system), or an existing Scala collection in the driver program, and transforming it. Users may also ask Spark to persist an RDD in memory, allowing it to be reused efficiently across parallel operations. Finally, RDDs automatically recover from node failures.

A second abstraction in Spark is shared variables that can be used in parallel operations. By default, when Spark runs a function in parallel as a set of tasks on different nodes, it ships a copy of each variable used in the function to each task. Sometimes, a variable needs to be shared across tasks, or between tasks and the driver program. Spark supports two types of shared variables: broadcast variables, which can be used to cache a value in memory on all nodes, and accumulators, which are variables that are only “added” to, such as counters and sums.

This guide shows each of these features in each of Spark’s supported languages. It is easiest to follow along with if you launch Spark’s interactive shell – either bin/spark-shell for the Scala shell or bin/pyspark for the Python one.

Linking with Spark
Python
Scala
Java
Spark 4.0.0 works with Python 3.9+. It can use the standard CPython interpreter, so C libraries like NumPy can be used. It also works with PyPy 7.3.6+.

Spark applications in Python can either be run with the bin/spark-submit script which includes Spark at runtime, or by including it in your setup.py as:

    install_requires=[
        'pyspark==4.0.0'
    ]
To run Spark applications in Python without pip installing PySpark, use the bin/spark-submit script located in the Spark directory. This script will load Spark’s Java/Scala libraries and allow you to submit applications to a cluster. You can also use bin/pyspark to launch an interactive Python shell.

If you wish to access HDFS data, you need to use a build of PySpark linking to your version of HDFS. Prebuilt packages are also available on the Spark homepage for common HDFS versions.

Finally, you need to import some Spark classes into your program. Add the following line:

from pyspark import SparkContext, SparkConf
PySpark requires the same minor version of Python in both driver and workers. It uses the default python version in PATH, you can specify which version of Python you want to use by PYSPARK_PYTHON, for example:

$ PYSPARK_PYTHON=python3.8 bin/pyspark
$ PYSPARK_PYTHON=/path-to-your-pypy/pypy bin/spark-submit examples/src/main/python/pi.py
Initializing Spark
Python
Scala
Java
The first thing a Spark program must do is to create a SparkContext object, which tells Spark how to access a cluster. To create a SparkContext you first need to build a SparkConf object that contains information about your application.

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
The appName parameter is a name for your application to show on the cluster UI. master is a Spark or YARN cluster URL, or a special “local” string to run in local mode. In practice, when running on a cluster, you will not want to hardcode master in the program, but rather launch the application with spark-submit and receive it there. However, for local testing and unit tests, you can pass “local” to run Spark in-process.

Using the Shell
Python
Scala
In the PySpark shell, a special interpreter-aware SparkContext is already created for you, in the variable called sc. Making your own SparkContext will not work. You can set which master the context connects to using the --master argument, and you can add Python .zip, .egg or .py files to the runtime path by passing a comma-separated list to --py-files. For third-party Python dependencies, see Python Package Management. You can also add dependencies (e.g. Spark Packages) to your shell session by supplying a comma-separated list of Maven coordinates to the --packages argument. Any additional repositories where dependencies might exist (e.g. Sonatype) can be passed to the --repositories argument. For example, to run bin/pyspark on exactly four cores, use:

$ ./bin/pyspark --master "local[4]"
Or, to also add code.py to the search path (in order to later be able to import code), use:

$ ./bin/pyspark --master "local[4]" --py-files code.py
For a complete list of options, run pyspark --help. Behind the scenes, pyspark invokes the more general spark-submit script.

It is also possible to launch the PySpark shell in IPython, the enhanced Python interpreter. PySpark works with IPython 1.0.0 and later. To use IPython, set the PYSPARK_DRIVER_PYTHON variable to ipython when running bin/pyspark:

$ PYSPARK_DRIVER_PYTHON=ipython ./bin/pyspark
To use the Jupyter notebook (previously known as the IPython notebook),

$ PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=notebook ./bin/pyspark
You can customize the ipython or jupyter commands by setting PYSPARK_DRIVER_PYTHON_OPTS.

After the Jupyter Notebook server is launched, you can create a new notebook from the “Files” tab. Inside the notebook, you can input the command %pylab inline as part of your notebook before you start to try Spark from the Jupyter notebook.

Resilient Distributed Datasets (RDDs)
Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection of elements that can be operated on in parallel. There are two ways to create RDDs: parallelizing an existing collection in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.

Parallelized Collections
Python
Scala
Java
Parallelized collections are created by calling SparkContext’s parallelize method on an existing iterable or collection in your driver program. The elements of the collection are copied to form a distributed dataset that can be operated on in parallel. For example, here is how to create a parallelized collection holding the numbers 1 to 5:

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
Once created, the distributed dataset (distData) can be operated on in parallel. For example, we can call distData.reduce(lambda a, b: a + b) to add up the elements of the list. We describe operations on distributed datasets later on.

One important parameter for parallel collections is the number of partitions to cut the dataset into. Spark will run one task for each partition of the cluster. Typically you want 2-4 partitions for each CPU in your cluster. Normally, Spark tries to set the number of partitions automatically based on your cluster. However, you can also set it manually by passing it as a second parameter to parallelize (e.g. sc.parallelize(data, 10)). Note: some places in the code use the term slices (a synonym for partitions) to maintain backward compatibility.

External Datasets
Python
Scala
Java
PySpark can create distributed datasets from any storage source supported by Hadoop, including your local file system, HDFS, Cassandra, HBase, Amazon S3, etc. Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.

Text file RDDs can be created using SparkContext’s textFile method. This method takes a URI for the file (either a local path on the machine, or a hdfs://, s3a://, etc URI) and reads it as a collection of lines. Here is an example invocation:

>>> distFile = sc.textFile("data.txt")
Once created, distFile can be acted on by dataset operations. For example, we can add up the sizes of all the lines using the map and reduce operations as follows: distFile.map(lambda s: len(s)).reduce(lambda a, b: a + b).

Some notes on reading files with Spark:

If using a path on the local filesystem, the file must also be accessible at the same path on worker nodes. Either copy the file to all workers or use a network-mounted shared file system.

All of Spark’s file-based input methods, including textFile, support running on directories, compressed files, and wildcards as well. For example, you can use textFile("/my/directory"), textFile("/my/directory/*.txt"), and textFile("/my/directory/*.gz").

The textFile method also takes an optional second argument for controlling the number of partitions of the file. By default, Spark creates one partition for each block of the file (blocks being 128MB by default in HDFS), but you can also ask for a higher number of partitions by passing a larger value. Note that you cannot have fewer partitions than blocks.

Apart from text files, Spark’s Python API also supports several other data formats:

SparkContext.wholeTextFiles lets you read a directory containing multiple small text files, and returns each of them as (filename, content) pairs. This is in contrast with textFile, which would return one record per line in each file.

RDD.saveAsPickleFile and SparkContext.pickleFile support saving an RDD in a simple format consisting of pickled Python objects. Batching is used on pickle serialization, with default batch size 10.

SequenceFile and Hadoop Input/Output Formats

Note this feature is currently marked Experimental and is intended for advanced users. It may be replaced in future with read/write support based on Spark SQL, in which case Spark SQL is the preferred approach.

Writable Support

PySpark SequenceFile support loads an RDD of key-value pairs within Java, converts Writables to base Java types, and pickles the resulting Java objects using pickle. When saving an RDD of key-value pairs to SequenceFile, PySpark does the reverse. It unpickles Python objects into Java objects and then converts them to Writables. The following Writables are automatically converted:

Writable Type	Python Type
Text	str
IntWritable	int
FloatWritable	float
DoubleWritable	float
BooleanWritable	bool
BytesWritable	bytearray
NullWritable	None
MapWritable	dict
Arrays are not handled out-of-the-box. Users need to specify custom ArrayWritable subtypes when reading or writing. When writing, users also need to specify custom converters that convert arrays to custom ArrayWritable subtypes. When reading, the default converter will convert custom ArrayWritable subtypes to Java Object[], which then get pickled to Python tuples. To get Python array.array for arrays of primitive types, users need to specify custom converters.

Saving and Loading SequenceFiles

Similarly to text files, SequenceFiles can be saved and loaded by specifying the path. The key and value classes can be specified, but for standard Writables this is not required.

>>> rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
>>> rdd.saveAsSequenceFile("path/to/file")
>>> sorted(sc.sequenceFile("path/to/file").collect())
[(1, u'a'), (2, u'aa'), (3, u'aaa')]
Saving and Loading Other Hadoop Input/Output Formats

PySpark can also read any Hadoop InputFormat or write any Hadoop OutputFormat, for both ‘new’ and ‘old’ Hadoop MapReduce APIs. If required, a Hadoop configuration can be passed in as a Python dict. Here is an example using the Elasticsearch ESInputFormat:

$ ./bin/pyspark --jars /path/to/elasticsearch-hadoop.jar
>>> conf = {"es.resource" : "index/type"}  # assume Elasticsearch is running on localhost defaults
>>> rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",
                             "org.apache.hadoop.io.NullWritable",
                             "org.elasticsearch.hadoop.mr.LinkedMapWritable",
                             conf=conf)
>>> rdd.first()  # the result is a MapWritable that is converted to a Python dict
(u'Elasticsearch ID',
 {u'field1': True,
  u'field2': u'Some Text',
  u'field3': 12345})
Note that, if the InputFormat simply depends on a Hadoop configuration and/or input path, and the key and value classes can easily be converted according to the above table, then this approach should work well for such cases.

If you have custom serialized binary data (such as loading data from Cassandra / HBase), then you will first need to transform that data on the Scala/Java side to something which can be handled by pickle’s pickler. A Converter trait is provided for this. Simply extend this trait and implement your transformation code in the convert method. Remember to ensure that this class, along with any dependencies required to access your InputFormat, are packaged into your Spark job jar and included on the PySpark classpath.

See the Python examples and the Converter examples for examples of using Cassandra / HBase InputFormat and OutputFormat with custom converters.

RDD Operations
RDDs support two types of operations: transformations, which create a new dataset from an existing one, and actions, which return a value to the driver program after running a computation on the dataset. For example, map is a transformation that passes each dataset element through a function and returns a new RDD representing the results. On the other hand, reduce is an action that aggregates all the elements of the RDD using some function and returns the final result to the driver program (although there is also a parallel reduceByKey that returns a distributed dataset).

All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently. For example, we can realize that a dataset created through map will be used in a reduce and return only the result of the reduce to the driver, rather than the larger mapped dataset.

By default, each transformed RDD may be recomputed each time you run an action on it. However, you may also persist an RDD in memory using the persist (or cache) method, in which case Spark will keep the elements around on the cluster for much faster access the next time you query it. There is also support for persisting RDDs on disk, or replicated across multiple nodes.

Basics
Python
Scala
Java
To illustrate RDD basics, consider the simple program below:

lines = sc.textFile("data.txt")
lineLengths = lines.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)
The first line defines a base RDD from an external file. This dataset is not loaded in memory or otherwise acted on: lines is merely a pointer to the file. The second line defines lineLengths as the result of a map transformation. Again, lineLengths is not immediately computed, due to laziness. Finally, we run reduce, which is an action. At this point Spark breaks the computation into tasks to run on separate machines, and each machine runs both its part of the map and a local reduction, returning only its answer to the driver program.

If we also wanted to use lineLengths again later, we could add:

lineLengths.persist()
before the reduce, which would cause lineLengths to be saved in memory after the first time it is computed.

Passing Functions to Spark
Python
Scala
Java
Spark’s API relies heavily on passing functions in the driver program to run on the cluster. There are three recommended ways to do this:

Lambda expressions, for simple functions that can be written as an expression. (Lambdas do not support multi-statement functions or statements that do not return a value.)
Local defs inside the function calling into Spark, for longer code.
Top-level functions in a module.
For example, to pass a longer function than can be supported using a lambda, consider the code below:

"""MyScript.py"""
if __name__ == "__main__":
    def myFunc(s):
        words = s.split(" ")
        return len(words)

    sc = SparkContext(...)
    sc.textFile("file.txt").map(myFunc)
Note that while it is also possible to pass a reference to a method in a class instance (as opposed to a singleton object), this requires sending the object that contains that class along with the method. For example, consider:

class MyClass(object):
    def func(self, s):
        return s
    def doStuff(self, rdd):
        return rdd.map(self.func)
Here, if we create a new MyClass and call doStuff on it, the map inside there references the func method of that MyClass instance, so the whole object needs to be sent to the cluster.

In a similar way, accessing fields of the outer object will reference the whole object:

class MyClass(object):
    def __init__(self):
        self.field = "Hello"
    def doStuff(self, rdd):
        return rdd.map(lambda s: self.field + s)
To avoid this issue, the simplest way is to copy field into a local variable instead of accessing it externally:

def doStuff(self, rdd):
    field = self.field
    return rdd.map(lambda s: field + s)
Understanding closures
One of the harder things about Spark is understanding the scope and life cycle of variables and methods when executing code across a cluster. RDD operations that modify variables outside of their scope can be a frequent source of confusion. In the example below we’ll look at code that uses foreach() to increment a counter, but similar issues can occur for other operations as well.

Example
Consider the naive RDD element sum below, which may behave differently depending on whether execution is happening within the same JVM. A common example of this is when running Spark in local mode (--master = "local[n]") versus deploying a Spark application to a cluster (e.g. via spark-submit to YARN):

Python
Scala
Java
counter = 0
rdd = sc.parallelize(data)

# Wrong: Don't do this!!
def increment_counter(x):
    global counter
    counter += x
rdd.foreach(increment_counter)

print("Counter value: ", counter)
Local vs. cluster modes
The behavior of the above code is undefined, and may not work as intended. To execute jobs, Spark breaks up the processing of RDD operations into tasks, each of which is executed by an executor. Prior to execution, Spark computes the task’s closure. The closure is those variables and methods which must be visible for the executor to perform its computations on the RDD (in this case foreach()). This closure is serialized and sent to each executor.

The variables within the closure sent to each executor are now copies and thus, when counter is referenced within the foreach function, it’s no longer the counter on the driver node. There is still a counter in the memory of the driver node but this is no longer visible to the executors! The executors only see the copy from the serialized closure. Thus, the final value of counter will still be zero since all operations on counter were referencing the value within the serialized closure.

In local mode, in some circumstances, the foreach function will actually execute within the same JVM as the driver and will reference the same original counter, and may actually update it.

To ensure well-defined behavior in these sorts of scenarios one should use an Accumulator. Accumulators in Spark are used specifically to provide a mechanism for safely updating a variable when execution is split up across worker nodes in a cluster. The Accumulators section of this guide discusses these in more detail.

In general, closures - constructs like loops or locally defined methods, should not be used to mutate some global state. Spark does not define or guarantee the behavior of mutations to objects referenced from outside of closures. Some code that does this may work in local mode, but that’s just by accident and such code will not behave as expected in distributed mode. Use an Accumulator instead if some global aggregation is needed.

Printing elements of an RDD
Another common idiom is attempting to print out the elements of an RDD using rdd.foreach(println) or rdd.map(println). On a single machine, this will generate the expected output and print all the RDD’s elements. However, in cluster mode, the output to stdout being called by the executors is now writing to the executor’s stdout instead, not the one on the driver, so stdout on the driver won’t show these! To print all elements on the driver, one can use the collect() method to first bring the RDD to the driver node thus: rdd.collect().foreach(println). This can cause the driver to run out of memory, though, because collect() fetches the entire RDD to a single machine; if you only need to print a few elements of the RDD, a safer approach is to use the take(): rdd.take(100).foreach(println).

Working with Key-Value Pairs
Python
Scala
Java
While most Spark operations work on RDDs containing any type of objects, a few special operations are only available on RDDs of key-value pairs. The most common ones are distributed “shuffle” operations, such as grouping or aggregating the elements by a key.

In Python, these operations work on RDDs containing built-in Python tuples such as (1, 2). Simply create such tuples and then call your desired operation.

For example, the following code uses the reduceByKey operation on key-value pairs to count how many times each line of text occurs in a file:

lines = sc.textFile("data.txt")
pairs = lines.map(lambda s: (s, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
We could also use counts.sortByKey(), for example, to sort the pairs alphabetically, and finally counts.collect() to bring them back to the driver program as a list of objects.

Transformations
The following table lists some of the common transformations supported by Spark. Refer to the RDD API doc (Python, Scala, Java, R) and pair RDD functions doc (Scala, Java) for details.

Transformation	Meaning
map(func)	Return a new distributed dataset formed by passing each element of the source through a function func.
filter(func)	Return a new dataset formed by selecting those elements of the source on which func returns true.
flatMap(func)	Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item).
mapPartitions(func)	Similar to map, but runs separately on each partition (block) of the RDD, so func must be of type Iterator<T> => Iterator<U> when running on an RDD of type T.
mapPartitionsWithIndex(func)	Similar to mapPartitions, but also provides func with an integer value representing the index of the partition, so func must be of type (Int, Iterator<T>) => Iterator<U> when running on an RDD of type T.
sample(withReplacement, fraction, seed)	Sample a fraction fraction of the data, with or without replacement, using a given random number generator seed.
union(otherDataset)	Return a new dataset that contains the union of the elements in the source dataset and the argument.
intersection(otherDataset)	Return a new RDD that contains the intersection of elements in the source dataset and the argument.
distinct([numPartitions]))	Return a new dataset that contains the distinct elements of the source dataset.
groupByKey([numPartitions])	When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.
Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will yield much better performance.
Note: By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional numPartitions argument to set a different number of tasks.
reduceByKey(func, [numPartitions])	When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])	When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
sortByKey([ascending], [numPartitions])	When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean ascending argument.
join(otherDataset, [numPartitions])	When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.
cogroup(otherDataset, [numPartitions])	When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called groupWith.
cartesian(otherDataset)	When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).
pipe(command, [envVars])	Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings.
coalesce(numPartitions)	Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset.
repartition(numPartitions)	Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network.
repartitionAndSortWithinPartitions(partitioner)	Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys. This is more efficient than calling repartition and then sorting within each partition because it can push the sorting down into the shuffle machinery.
Actions
The following table lists some of the common actions supported by Spark. Refer to the RDD API doc (Python, Scala, Java, R)

and pair RDD functions doc (Scala, Java) for details.

Action	Meaning
reduce(func)	Aggregate the elements of the dataset using a function func (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel.
collect()	Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.
count()	Return the number of elements in the dataset.
first()	Return the first element of the dataset (similar to take(1)).
take(n)	Return an array with the first n elements of the dataset.
takeSample(withReplacement, num, [seed])	Return an array with a random sample of num elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.
takeOrdered(n, [ordering])	Return the first n elements of the RDD using either their natural order or a custom comparator.
saveAsTextFile(path)	Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file.
saveAsSequenceFile(path)
(Java and Scala)	Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc).
saveAsObjectFile(path)
(Java and Scala)	Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using SparkContext.objectFile().
countByKey()	Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key.
foreach(func)	Run a function func on each element of the dataset. This is usually done for side effects such as updating an Accumulator or interacting with external storage systems.
Note: modifying variables other than Accumulators outside of the foreach() may result in undefined behavior. See Understanding closures for more details.
The Spark RDD API also exposes asynchronous versions of some actions, like foreachAsync for foreach, which immediately return a FutureAction to the caller instead of blocking on completion of the action. This can be used to manage or wait for the asynchronous execution of the action.

Shuffle operations
Certain operations within Spark trigger an event known as the shuffle. The shuffle is Spark’s mechanism for re-distributing data so that it’s grouped differently across partitions. This typically involves copying data across executors and machines, making the shuffle a complex and costly operation.

Background
To understand what happens during the shuffle, we can consider the example of the reduceByKey operation. The reduceByKey operation generates a new RDD where all values for a single key are combined into a tuple - the key and the result of executing a reduce function against all values associated with that key. The challenge is that not all values for a single key necessarily reside on the same partition, or even the same machine, but they must be co-located to compute the result.

In Spark, data is generally not distributed across partitions to be in the necessary place for a specific operation. During computations, a single task will operate on a single partition - thus, to organize all the data for a single reduceByKey reduce task to execute, Spark needs to perform an all-to-all operation. It must read from all partitions to find all the values for all keys, and then bring together values across partitions to compute the final result for each key - this is called the shuffle.

Although the set of elements in each partition of newly shuffled data will be deterministic, and so is the ordering of partitions themselves, the ordering of these elements is not. If one desires predictably ordered data following shuffle then it’s possible to use:

mapPartitions to sort each partition using, for example, .sorted
repartitionAndSortWithinPartitions to efficiently sort partitions while simultaneously repartitioning
sortBy to make a globally ordered RDD
Operations which can cause a shuffle include repartition operations like repartition and coalesce, ‘ByKey operations (except for counting) like groupByKey and reduceByKey, and join operations like cogroup and join.

Performance Impact
The Shuffle is an expensive operation since it involves disk I/O, data serialization, and network I/O. To organize data for the shuffle, Spark generates sets of tasks - map tasks to organize the data, and a set of reduce tasks to aggregate it. This nomenclature comes from MapReduce and does not directly relate to Spark’s map and reduce operations.

Internally, results from individual map tasks are kept in memory until they can’t fit. Then, these are sorted based on the target partition and written to a single file. On the reduce side, tasks read the relevant sorted blocks.

Certain shuffle operations can consume significant amounts of heap memory since they employ in-memory data structures to organize records before or after transferring them. Specifically, reduceByKey and aggregateByKey create these structures on the map side, and 'ByKey operations generate these on the reduce side. When data does not fit in memory Spark will spill these tables to disk, incurring the additional overhead of disk I/O and increased garbage collection.

Shuffle also generates a large number of intermediate files on disk. As of Spark 1.3, these files are preserved until the corresponding RDDs are no longer used and are garbage collected. This is done so the shuffle files don’t need to be re-created if the lineage is re-computed. Garbage collection may happen only after a long period of time, if the application retains references to these RDDs or if GC does not kick in frequently. This means that long-running Spark jobs may consume a large amount of disk space. The temporary storage directory is specified by the spark.local.dir configuration parameter when configuring the Spark context.

Shuffle behavior can be tuned by adjusting a variety of configuration parameters. See the ‘Shuffle Behavior’ section within the Spark Configuration Guide.

RDD Persistence
One of the most important capabilities in Spark is persisting (or caching) a dataset in memory across operations. When you persist an RDD, each node stores any partitions of it that it computes in memory and reuses them in other actions on that dataset (or datasets derived from it). This allows future actions to be much faster (often by more than 10x). Caching is a key tool for iterative algorithms and fast interactive use.

You can mark an RDD to be persisted using the persist() or cache() methods on it. The first time it is computed in an action, it will be kept in memory on the nodes. Spark’s cache is fault-tolerant – if any partition of an RDD is lost, it will automatically be recomputed using the transformations that originally created it.

In addition, each persisted RDD can be stored using a different storage level, allowing you, for example, to persist the dataset on disk, persist it in memory but as serialized Java objects (to save space), replicate it across nodes. These levels are set by passing a StorageLevel object (Python, Scala, Java) to persist(). The cache() method is a shorthand for using the default storage level, which is StorageLevel.MEMORY_ONLY (store deserialized objects in memory). The full set of storage levels is:

Storage Level	Meaning
MEMORY_ONLY	Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. This is the default level.
MEMORY_AND_DISK	Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the partitions that don't fit on disk, and read them from there when they're needed.
MEMORY_ONLY_SER
(Java and Scala)	Store RDD as serialized Java objects (one byte array per partition). This is generally more space-efficient than deserialized objects, especially when using a fast serializer, but more CPU-intensive to read.
MEMORY_AND_DISK_SER
(Java and Scala)	Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in memory to disk instead of recomputing them on the fly each time they're needed.
DISK_ONLY	Store the RDD partitions only on disk.
MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc.	Same as the levels above, but replicate each partition on two cluster nodes.
OFF_HEAP (experimental)	Similar to MEMORY_ONLY_SER, but store the data in off-heap memory. This requires off-heap memory to be enabled.
Note: In Python, stored objects will always be serialized with the Pickle library, so it does not matter whether you choose a serialized level. The available storage levels in Python include MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_AND_DISK, MEMORY_AND_DISK_2, DISK_ONLY, DISK_ONLY_2, and DISK_ONLY_3.

Spark also automatically persists some intermediate data in shuffle operations (e.g. reduceByKey), even without users calling persist. This is done to avoid recomputing the entire input if a node fails during the shuffle. We still recommend users call persist on the resulting RDD if they plan to reuse it.

Which Storage Level to Choose?
Spark’s storage levels are meant to provide different trade-offs between memory usage and CPU efficiency. We recommend going through the following process to select one:

If your RDDs fit comfortably with the default storage level (MEMORY_ONLY), leave them that way. This is the most CPU-efficient option, allowing operations on the RDDs to run as fast as possible.

If not, try using MEMORY_ONLY_SER and selecting a fast serialization library to make the objects much more space-efficient, but still reasonably fast to access. (Java and Scala)

Don’t spill to disk unless the functions that computed your datasets are expensive, or they filter a large amount of the data. Otherwise, recomputing a partition may be as fast as reading it from disk.

Use the replicated storage levels if you want fast fault recovery (e.g. if using Spark to serve requests from a web application). All the storage levels provide full fault tolerance by recomputing lost data, but the replicated ones let you continue running tasks on the RDD without waiting to recompute a lost partition.

Removing Data
Spark automatically monitors cache usage on each node and drops out old data partitions in a least-recently-used (LRU) fashion. If you would like to manually remove an RDD instead of waiting for it to fall out of the cache, use the RDD.unpersist() method. Note that this method does not block by default. To block until resources are freed, specify blocking=true when calling this method.

Shared Variables
Normally, when a function passed to a Spark operation (such as map or reduce) is executed on a remote cluster node, it works on separate copies of all the variables used in the function. These variables are copied to each machine, and no updates to the variables on the remote machine are propagated back to the driver program. Supporting general, read-write shared variables across tasks would be inefficient. However, Spark does provide two limited types of shared variables for two common usage patterns: broadcast variables and accumulators.

Broadcast Variables
Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.

Spark actions are executed through a set of stages, separated by distributed “shuffle” operations. Spark automatically broadcasts the common data needed by tasks within each stage. The data broadcasted this way is cached in serialized form and deserialized before running each task. This means that explicitly creating broadcast variables is only useful when tasks across multiple stages need the same data or when caching the data in deserialized form is important.

Broadcast variables are created from a variable v by calling SparkContext.broadcast(v). The broadcast variable is a wrapper around v, and its value can be accessed by calling the value method. The code below shows this:

Python
Scala
Java
>>> broadcastVar = sc.broadcast([1, 2, 3])
<pyspark.core.broadcast.Broadcast object at 0x102789f10>

>>> broadcastVar.value
[1, 2, 3]
After the broadcast variable is created, it should be used instead of the value v in any functions run on the cluster so that v is not shipped to the nodes more than once. In addition, the object v should not be modified after it is broadcast in order to ensure that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped to a new node later).

To release the resources that the broadcast variable copied onto executors, call .unpersist(). If the broadcast is used again afterwards, it will be re-broadcast. To permanently release all resources used by the broadcast variable, call .destroy(). The broadcast variable can’t be used after that. Note that these methods do not block by default. To block until resources are freed, specify blocking=true when calling them.

Accumulators
Accumulators are variables that are only “added” to through an associative and commutative operation and can therefore be efficiently supported in parallel. They can be used to implement counters (as in MapReduce) or sums. Spark natively supports accumulators of numeric types, and programmers can add support for new types.

As a user, you can create named or unnamed accumulators. As seen in the image below, a named accumulator (in this instance counter) will display in the web UI for the stage that modifies that accumulator. Spark displays the value for each accumulator modified by a task in the “Tasks” table.

Accumulators in the Spark UI

Tracking accumulators in the UI can be useful for understanding the progress of running stages (NOTE: this is not yet supported in Python).

Python
Scala
Java
An accumulator is created from an initial value v by calling SparkContext.accumulator(v). Tasks running on a cluster can then add to it using the add method or the += operator. However, they cannot read its value. Only the driver program can read the accumulator’s value, using its value method.

The code below shows an accumulator being used to add up the elements of an array:

>>> accum = sc.accumulator(0)
>>> accum
Accumulator<id=0, value=0>

>>> sc.parallelize([1, 2, 3, 4]).foreach(lambda x: accum.add(x))
...
10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

>>> accum.value
10
While this code used the built-in support for accumulators of type Int, programmers can also create their own types by subclassing AccumulatorParam. The AccumulatorParam interface has two methods: zero for providing a “zero value” for your data type, and addInPlace for adding two values together. For example, supposing we had a Vector class representing mathematical vectors, we could write:

class VectorAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return Vector.zeros(initialValue.size)

    def addInPlace(self, v1, v2):
        v1 += v2
        return v1

# Then, create an Accumulator of this type:
vecAccum = sc.accumulator(Vector(...), VectorAccumulatorParam())
For accumulator updates performed inside actions only, Spark guarantees that each task’s update to the accumulator will only be applied once, i.e. restarted tasks will not update the value. In transformations, users should be aware of that each task’s update may be applied more than once if tasks or job stages are re-executed.

Accumulators do not change the lazy evaluation model of Spark. If they are being updated within an operation on an RDD, their value is only updated once that RDD is computed as part of an action. Consequently, accumulator updates are not guaranteed to be executed when made within a lazy transformation like map(). The below code fragment demonstrates this property:

Python
Scala
Java
accum = sc.accumulator(0)
def g(x):
    accum.add(x)
    return f(x)
data.map(g)
# Here, accum is still 0 because no actions have caused the `map` to be computed.
Deploying to a Cluster
The application submission guide describes how to submit applications to a cluster. In short, once you package your application into a JAR (for Java/Scala) or a set of .py or .zip files (for Python), the bin/spark-submit script lets you submit it to any supported cluster manager.

Launching Spark jobs from Java / Scala
The org.apache.spark.launcher package provides classes for launching Spark jobs as child processes using a simple Java API.

Unit Testing
Spark is friendly to unit testing with any popular unit test framework. Simply create a SparkContext in your test with the master URL set to local, run your operations, and then call SparkContext.stop() to tear it down. Make sure you stop the context within a finally block or the test framework’s tearDown method, as Spark does not support two contexts running concurrently in the same program.

Where to Go from Here
You can see some example Spark programs on the Spark website. In addition, Spark includes several samples in the examples directory (Python, Scala, Java, R). You can run Java and Scala examples by passing the class name to Spark’s bin/run-example script; for instance:

./bin/run-example SparkPi
For Python examples, use spark-submit instead:

./bin/spark-submit examples/src/main/python/pi.py
For R examples, use spark-submit instead:

./bin/spark-submit examples/src/main/r/dataframe.R
For help on optimizing your programs, the configuration and tuning guides provide information on best practices. They are especially important for making sure that your data is stored in memory in an efficient format. For help on deploying, the cluster mode overview describes the components involved in distributed operation and supported cluster managers.

Finally, full API documentation is available in Python, Scala, Java and R.


Note
Spark Streaming is the previous generation of Spark’s streaming engine. There are no longer updates to Spark Streaming and it’s a legacy project. There is a newer and easier to use streaming engine in Spark called Structured Streaming. You should use Spark Structured Streaming for your streaming applications and pipelines. See Structured Streaming Programming Guide.

Overview
Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Data can be ingested from many sources like Kafka, Kinesis, or TCP sockets, and can be processed using complex algorithms expressed with high-level functions like map, reduce, join and window. Finally, processed data can be pushed out to filesystems, databases, and live dashboards. In fact, you can apply Spark’s machine learning and graph processing algorithms on data streams.

Spark Streaming

Internally, it works as follows. Spark Streaming receives live input data streams and divides the data into batches, which are then processed by the Spark engine to generate the final stream of results in batches.

Spark Streaming

Spark Streaming provides a high-level abstraction called discretized stream or DStream, which represents a continuous stream of data. DStreams can be created either from input data streams from sources such as Kafka, and Kinesis, or by applying high-level operations on other DStreams. Internally, a DStream is represented as a sequence of RDDs.

This guide shows you how to start writing Spark Streaming programs with DStreams. You can write Spark Streaming programs in Scala, Java or Python (introduced in Spark 1.2), all of which are presented in this guide. You will find tabs throughout this guide that let you choose between code snippets of different languages.

Note: There are a few APIs that are either different or not available in Python. Throughout this guide, you will find the tag Python API highlighting these differences.

A Quick Example
Before we go into the details of how to write your own Spark Streaming program, let’s take a quick look at what a simple Spark Streaming program looks like. Let’s say we want to count the number of words in text data received from a data server listening on a TCP socket. All you need to do is as follows.

Python
Scala
Java
First, we import StreamingContext, which is the main entry point for all streaming functionality. We create a local StreamingContext with two execution threads, and batch interval of 1 second.

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
Using this context, we can create a DStream that represents streaming data from a TCP source, specified as hostname (e.g. localhost) and port (e.g. 9999).

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)
This lines DStream represents the stream of data that will be received from the data server. Each record in this DStream is a line of text. Next, we want to split the lines by space into words.

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))
flatMap is a one-to-many DStream operation that creates a new DStream by generating multiple new records from each record in the source DStream. In this case, each line will be split into multiple words and the stream of words is represented as the words DStream. Next, we want to count these words.

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
The words DStream is further mapped (one-to-one transformation) to a DStream of (word, 1) pairs, which is then reduced to get the frequency of words in each batch of data. Finally, wordCounts.pprint() will print a few of the counts generated every second.

Note that when these lines are executed, Spark Streaming only sets up the computation it will perform when it is started, and no real processing has started yet. To start the processing after all the transformations have been setup, we finally call

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
The complete code can be found in the Spark Streaming example NetworkWordCount.

If you have already downloaded and built Spark, you can run this example as follows. You will first need to run Netcat (a small utility found in most Unix-like systems) as a data server by using

$ nc -lk 9999
Then, in a different terminal, you can start the example by using

Python
Scala
Java
$ ./bin/spark-submit examples/src/main/python/streaming/network_wordcount.py localhost 9999
Then, any lines typed in the terminal running the netcat server will be counted and printed on screen every second. It will look something like the following.

# TERMINAL 1:
# Running Netcat

$ nc -lk 9999

hello world



...
Python
Scala
Java
# TERMINAL 2: RUNNING network_wordcount.py

$ ./bin/spark-submit examples/src/main/python/streaming/network_wordcount.py localhost 9999
...
-------------------------------------------
Time: 2014-10-14 15:25:21
-------------------------------------------
(hello,1)
(world,1)
...
Basic Concepts
Next, we move beyond the simple example and elaborate on the basics of Spark Streaming.

Linking
Similar to Spark, Spark Streaming is available through Maven Central. To write your own Spark Streaming program, you will have to add the following dependency to your SBT or Maven project.

Maven
SBT
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming_2.13</artifactId>
    <version>4.0.0</version>
    <scope>provided</scope>
</dependency>
For ingesting data from sources like Kafka and Kinesis that are not present in the Spark Streaming core API, you will have to add the corresponding artifact spark-streaming-xyz_2.13 to the dependencies. For example, some of the common ones are as follows.

Source	Artifact
Kafka	spark-streaming-kafka-0-10_2.13
Kinesis
spark-streaming-kinesis-asl_2.13 [Amazon Software License]
For an up-to-date list, please refer to the Maven repository for the full list of supported sources and artifacts.

Initializing StreamingContext
To initialize a Spark Streaming program, a StreamingContext object has to be created which is the main entry point of all Spark Streaming functionality.

Python
Scala
Java
A StreamingContext object can be created from a SparkContext object.

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master, appName)
ssc = StreamingContext(sc, 1)
The appName parameter is a name for your application to show on the cluster UI. master is a Spark or YARN cluster URL, or a special “local[*]” string to run in local mode. In practice, when running on a cluster, you will not want to hardcode master in the program, but rather launch the application with spark-submit and receive it there. However, for local testing and unit tests, you can pass “local[*]” to run Spark Streaming in-process (detects the number of cores in the local system).

The batch interval must be set based on the latency requirements of your application and available cluster resources. See the Performance Tuning section for more details.

After a context is defined, you have to do the following.

Define the input sources by creating input DStreams.
Define the streaming computations by applying transformation and output operations to DStreams.
Start receiving data and processing it using streamingContext.start().
Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination().
The processing can be manually stopped using streamingContext.stop().
Points to remember:
Once a context has been started, no new streaming computations can be set up or added to it.
Once a context has been stopped, it cannot be restarted.
Only one StreamingContext can be active in a JVM at the same time.
stop() on StreamingContext also stops the SparkContext. To stop only the StreamingContext, set the optional parameter of stop() called stopSparkContext to false.
A SparkContext can be re-used to create multiple StreamingContexts, as long as the previous StreamingContext is stopped (without stopping the SparkContext) before the next StreamingContext is created.
Discretized Streams (DStreams)
Discretized Stream or DStream is the basic abstraction provided by Spark Streaming. It represents a continuous stream of data, either the input data stream received from source, or the processed data stream generated by transforming the input stream. Internally, a DStream is represented by a continuous series of RDDs, which is Spark’s abstraction of an immutable, distributed dataset (see Spark Programming Guide for more details). Each RDD in a DStream contains data from a certain interval, as shown in the following figure.

Spark Streaming

Any operation applied on a DStream translates to operations on the underlying RDDs. For example, in the earlier example of converting a stream of lines to words, the flatMap operation is applied on each RDD in the lines DStream to generate the RDDs of the words DStream. This is shown in the following figure.

Spark Streaming

These underlying RDD transformations are computed by the Spark engine. The DStream operations hide most of these details and provide the developer with a higher-level API for convenience. These operations are discussed in detail in later sections.

Input DStreams and Receivers
Input DStreams are DStreams representing the stream of input data received from streaming sources. In the quick example, lines was an input DStream as it represented the stream of data received from the netcat server. Every input DStream (except file stream, discussed later in this section) is associated with a Receiver (Scala doc, Java doc) object which receives the data from a source and stores it in Spark’s memory for processing.

Spark Streaming provides two categories of built-in streaming sources.

Basic sources: Sources directly available in the StreamingContext API. Examples: file systems, and socket connections.
Advanced sources: Sources like Kafka, Kinesis, etc. are available through extra utility classes. These require linking against extra dependencies as discussed in the linking section.
We are going to discuss some of the sources present in each category later in this section.

Note that, if you want to receive multiple streams of data in parallel in your streaming application, you can create multiple input DStreams (discussed further in the Performance Tuning section). This will create multiple receivers which will simultaneously receive multiple data streams. But note that a Spark worker/executor is a long-running task, hence it occupies one of the cores allocated to the Spark Streaming application. Therefore, it is important to remember that a Spark Streaming application needs to be allocated enough cores (or threads, if running locally) to process the received data, as well as to run the receiver(s).

Points to remember
When running a Spark Streaming program locally, do not use “local” or “local[1]” as the master URL. Either of these means that only one thread will be used for running tasks locally. If you are using an input DStream based on a receiver (e.g. sockets, Kafka, etc.), then the single thread will be used to run the receiver, leaving no thread for processing the received data. Hence, when running locally, always use “local[n]” as the master URL, where n > number of receivers to run (see Spark Properties for information on how to set the master).

Extending the logic to running on a cluster, the number of cores allocated to the Spark Streaming application must be more than the number of receivers. Otherwise the system will receive data, but not be able to process it.

Basic Sources
We have already taken a look at the ssc.socketTextStream(...) in the quick example which creates a DStream from text data received over a TCP socket connection. Besides sockets, the StreamingContext API provides methods for creating DStreams from files as input sources.

File Streams
For reading data from files on any file system compatible with the HDFS API (that is, HDFS, S3, NFS, etc.), a DStream can be created as via StreamingContext.fileStream[KeyClass, ValueClass, InputFormatClass].

File streams do not require running a receiver so there is no need to allocate any cores for receiving file data.

For simple text files, the easiest method is StreamingContext.textFileStream(dataDirectory).

Python
Scala
Java
fileStream is not available in the Python API; only textFileStream is available.

streamingContext.textFileStream(dataDirectory)
How Directories are Monitored
Spark Streaming will monitor the directory dataDirectory and process any files created in that directory.

A simple directory can be monitored, such as "hdfs://namenode:8040/logs/". All files directly under such a path will be processed as they are discovered.
A POSIX glob pattern can be supplied, such as "hdfs://namenode:8040/logs/2017/*". Here, the DStream will consist of all files in the directories matching the pattern. That is: it is a pattern of directories, not of files in directories.
All files must be in the same data format.
A file is considered part of a time period based on its modification time, not its creation time.
Once processed, changes to a file within the current window will not cause the file to be reread. That is: updates are ignored.
The more files under a directory, the longer it will take to scan for changes — even if no files have been modified.
If a wildcard is used to identify directories, such as "hdfs://namenode:8040/logs/2016-*", renaming an entire directory to match the path will add the directory to the list of monitored directories. Only the files in the directory whose modification time is within the current window will be included in the stream.
Calling FileSystem.setTimes() to fix the timestamp is a way to have the file picked up in a later window, even if its contents have not changed.
Using Object Stores as a source of data
“Full” Filesystems such as HDFS tend to set the modification time on their files as soon as the output stream is created. When a file is opened, even before data has been completely written, it may be included in the DStream - after which updates to the file within the same window will be ignored. That is: changes may be missed, and data omitted from the stream.

To guarantee that changes are picked up in a window, write the file to an unmonitored directory, then, immediately after the output stream is closed, rename it into the destination directory. Provided the renamed file appears in the scanned destination directory during the window of its creation, the new data will be picked up.

In contrast, Object Stores such as Amazon S3 and Azure Storage usually have slow rename operations, as the data is actually copied. Furthermore, a renamed object may have the time of the rename() operation as its modification time, so may not be considered part of the window which the original create time implied they were.

Careful testing is needed against the target object store to verify that the timestamp behavior of the store is consistent with that expected by Spark Streaming. It may be that writing directly into a destination directory is the appropriate strategy for streaming data via the chosen object store.

For more details on this topic, consult the Hadoop Filesystem Specification.

Streams based on Custom Receivers
DStreams can be created with data streams received through custom receivers. See the Custom Receiver Guide for more details.

Queue of RDDs as a Stream
For testing a Spark Streaming application with test data, one can also create a DStream based on a queue of RDDs, using streamingContext.queueStream(queueOfRDDs). Each RDD pushed into the queue will be treated as a batch of data in the DStream, and processed like a stream.

For more details on streams from sockets and files, see the API documentations of the relevant functions in StreamingContext for Python, StreamingContext for Scala, and JavaStreamingContext for Java.

Advanced Sources
Python API As of Spark 4.0.0, out of these sources, Kafka and Kinesis are available in the Python API.

This category of sources requires interfacing with external non-Spark libraries, some of them with complex dependencies (e.g., Kafka). Hence, to minimize issues related to version conflicts of dependencies, the functionality to create DStreams from these sources has been moved to separate libraries that can be linked to explicitly when necessary.

Note that these advanced sources are not available in the Spark shell, hence applications based on these advanced sources cannot be tested in the shell. If you really want to use them in the Spark shell you will have to download the corresponding Maven artifact’s JAR along with its dependencies and add it to the classpath.

Some of these advanced sources are as follows.

Kafka: Spark Streaming 4.0.0 is compatible with Kafka broker versions 0.10 or higher. See the Kafka Integration Guide for more details.

Kinesis: Spark Streaming 4.0.0 is compatible with Kinesis Client Library 1.2.1. See the Kinesis Integration Guide for more details.

Custom Sources
Python API This is not yet supported in Python.

Input DStreams can also be created out of custom data sources. All you have to do is implement a user-defined receiver (see next section to understand what that is) that can receive data from the custom sources and push it into Spark. See the Custom Receiver Guide for details.

Receiver Reliability
There can be two kinds of data sources based on their reliability. Sources (like Kafka) allow the transferred data to be acknowledged. If the system receiving data from these reliable sources acknowledges the received data correctly, it can be ensured that no data will be lost due to any kind of failure. This leads to two kinds of receivers:

Reliable Receiver - A reliable receiver correctly sends acknowledgment to a reliable source when the data has been received and stored in Spark with replication.
Unreliable Receiver - An unreliable receiver does not send acknowledgment to a source. This can be used for sources that do not support acknowledgment, or even for reliable sources when one does not want or need to go into the complexity of acknowledgment.
The details of how to write a reliable receiver are discussed in the Custom Receiver Guide.

Transformations on DStreams
Similar to that of RDDs, transformations allow the data from the input DStream to be modified. DStreams support many of the transformations available on normal Spark RDD’s. Some of the common ones are as follows.

Transformation	Meaning
map(func)	Return a new DStream by passing each element of the source DStream through a function func.
flatMap(func)	Similar to map, but each input item can be mapped to 0 or more output items.
filter(func)	Return a new DStream by selecting only the records of the source DStream on which func returns true.
repartition(numPartitions)	Changes the level of parallelism in this DStream by creating more or fewer partitions.
union(otherStream)	Return a new DStream that contains the union of the elements in the source DStream and otherDStream.
count()	Return a new DStream of single-element RDDs by counting the number of elements in each RDD of the source DStream.
reduce(func)	Return a new DStream of single-element RDDs by aggregating the elements in each RDD of the source DStream using a function func (which takes two arguments and returns one). The function should be associative and commutative so that it can be computed in parallel.
countByValue()	When called on a DStream of elements of type K, return a new DStream of (K, Long) pairs where the value of each key is its frequency in each RDD of the source DStream.
reduceByKey(func, [numTasks])	When called on a DStream of (K, V) pairs, return a new DStream of (K, V) pairs where the values for each key are aggregated using the given reduce function. Note: By default, this uses Spark's default number of parallel tasks (2 for local mode, and in cluster mode the number is determined by the config property spark.default.parallelism) to do the grouping. You can pass an optional numTasks argument to set a different number of tasks.
join(otherStream, [numTasks])	When called on two DStreams of (K, V) and (K, W) pairs, return a new DStream of (K, (V, W)) pairs with all pairs of elements for each key.
cogroup(otherStream, [numTasks])	When called on a DStream of (K, V) and (K, W) pairs, return a new DStream of (K, Seq[V], Seq[W]) tuples.
transform(func)	Return a new DStream by applying a RDD-to-RDD function to every RDD of the source DStream. This can be used to do arbitrary RDD operations on the DStream.
updateStateByKey(func)	Return a new "state" DStream where the state for each key is updated by applying the given function on the previous state of the key and the new values for the key. This can be used to maintain arbitrary state data for each key.
A few of these transformations are worth discussing in more detail.

UpdateStateByKey Operation
The updateStateByKey operation allows you to maintain arbitrary state while continuously updating it with new information. To use this, you will have to do two steps.

Define the state - The state can be an arbitrary data type.
Define the state update function - Specify with a function how to update the state using the previous state and the new values from an input stream.
In every batch, Spark will apply the state update function for all existing keys, regardless of whether they have new data in a batch or not. If the update function returns None then the key-value pair will be eliminated.

Let’s illustrate this with an example. Say you want to maintain a running count of each word seen in a text data stream. Here, the running count is the state and it is an integer. We define the update function as:

Python
Scala
Java
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)  # add the new values with the previous running count to get the new count
This is applied on a DStream containing words (say, the pairs DStream containing (word, 1) pairs in the earlier example).

runningCounts = pairs.updateStateByKey(updateFunction)
The update function will be called for each word, with newValues having a sequence of 1’s (from the (word, 1) pairs) and the runningCount having the previous count. For the complete Python code, take a look at the example stateful_network_wordcount.py.

Note that using updateStateByKey requires the checkpoint directory to be configured, which is discussed in detail in the checkpointing section.

Transform Operation
The transform operation (along with its variations like transformWith) allows arbitrary RDD-to-RDD functions to be applied on a DStream. It can be used to apply any RDD operation that is not exposed in the DStream API. For example, the functionality of joining every batch in a data stream with another dataset is not directly exposed in the DStream API. However, you can easily use transform to do this. This enables very powerful possibilities. For example, one can do real-time data cleaning by joining the input data stream with precomputed spam information (maybe generated with Spark as well) and then filtering based on it.

Python
Scala
Java
spamInfoRDD = sc.pickleFile(...)  # RDD containing spam information

# join data stream with spam information to do data cleaning
cleanedDStream = wordCounts.transform(lambda rdd: rdd.join(spamInfoRDD).filter(...))
Note that the supplied function gets called in every batch interval. This allows you to do time-varying RDD operations, that is, RDD operations, number of partitions, broadcast variables, etc. can be changed between batches.

Window Operations
Spark Streaming also provides windowed computations, which allow you to apply transformations over a sliding window of data. The following figure illustrates this sliding window.

Spark Streaming

As shown in the figure, every time the window slides over a source DStream, the source RDDs that fall within the window are combined and operated upon to produce the RDDs of the windowed DStream. In this specific case, the operation is applied over the last 3 time units of data, and slides by 2 time units. This shows that any window operation needs to specify two parameters.

window length - The duration of the window (3 in the figure).
sliding interval - The interval at which the window operation is performed (2 in the figure).
These two parameters must be multiples of the batch interval of the source DStream (1 in the figure).

Let’s illustrate the window operations with an example. Say, you want to extend the earlier example by generating word counts over the last 30 seconds of data, every 10 seconds. To do this, we have to apply the reduceByKey operation on the pairs DStream of (word, 1) pairs over the last 30 seconds of data. This is done using the operation reduceByKeyAndWindow.

Python
Scala
Java
# Reduce last 30 seconds of data, every 10 seconds
windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
Some of the common window operations are as follows. All of these operations take the said two parameters - windowLength and slideInterval.

Transformation	Meaning
window(windowLength, slideInterval)	Return a new DStream which is computed based on windowed batches of the source DStream.
countByWindow(windowLength, slideInterval)	Return a sliding window count of elements in the stream.
reduceByWindow(func, windowLength, slideInterval)	Return a new single-element stream, created by aggregating elements in the stream over a sliding interval using func. The function should be associative and commutative so that it can be computed correctly in parallel.
reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks])	When called on a DStream of (K, V) pairs, returns a new DStream of (K, V) pairs where the values for each key are aggregated using the given reduce function func over batches in a sliding window. Note: By default, this uses Spark's default number of parallel tasks (2 for local mode, and in cluster mode the number is determined by the config property spark.default.parallelism) to do the grouping. You can pass an optional numTasks argument to set a different number of tasks.
reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks])	
A more efficient version of the above reduceByKeyAndWindow() where the reduce value of each window is calculated incrementally using the reduce values of the previous window. This is done by reducing the new data that enters the sliding window, and “inverse reducing” the old data that leaves the window. An example would be that of “adding” and “subtracting” counts of keys as the window slides. However, it is applicable only to “invertible reduce functions”, that is, those reduce functions which have a corresponding “inverse reduce” function (taken as parameter invFunc). Like in reduceByKeyAndWindow, the number of reduce tasks is configurable through an optional argument. Note that checkpointing must be enabled for using this operation.

countByValueAndWindow(windowLength, slideInterval, [numTasks])	When called on a DStream of (K, V) pairs, returns a new DStream of (K, Long) pairs where the value of each key is its frequency within a sliding window. Like in reduceByKeyAndWindow, the number of reduce tasks is configurable through an optional argument.
Join Operations
Finally, it’s worth highlighting how easily you can perform different kinds of joins in Spark Streaming.

Stream-stream joins
Streams can be very easily joined with other streams.

Python
Scala
Java
stream1 = ...
stream2 = ...
joinedStream = stream1.join(stream2)
Here, in each batch interval, the RDD generated by stream1 will be joined with the RDD generated by stream2. You can also do leftOuterJoin, rightOuterJoin, fullOuterJoin. Furthermore, it is often very useful to do joins over windows of the streams. That is pretty easy as well.

Python
Scala
Java
windowedStream1 = stream1.window(20)
windowedStream2 = stream2.window(60)
joinedStream = windowedStream1.join(windowedStream2)
Stream-dataset joins
This has already been shown earlier while explain DStream.transform operation. Here is yet another example of joining a windowed stream with a dataset.

Python
Scala
Java
dataset = ... # some RDD
windowedStream = stream.window(20)
joinedStream = windowedStream.transform(lambda rdd: rdd.join(dataset))
In fact, you can also dynamically change the dataset you want to join against. The function provided to transform is evaluated every batch interval and therefore will use the current dataset that dataset reference points to.

The complete list of DStream transformations is available in the API documentation. For the Python API, see DStream. For the Scala API, see DStream and PairDStreamFunctions. For the Java API, see JavaDStream and JavaPairDStream.

Output Operations on DStreams
Output operations allow DStream’s data to be pushed out to external systems like a database or a file system. Since the output operations actually allow the transformed data to be consumed by external systems, they trigger the actual execution of all the DStream transformations (similar to actions for RDDs). Currently, the following output operations are defined:

Output Operation	Meaning
print()	Prints the first ten elements of every batch of data in a DStream on the driver node running the streaming application. This is useful for development and debugging.
Python API This is called pprint() in the Python API.
saveAsTextFiles(prefix, [suffix])	Save this DStream's contents as text files. The file name at each batch interval is generated based on prefix and suffix: "prefix-TIME_IN_MS[.suffix]".
saveAsObjectFiles(prefix, [suffix])	Save this DStream's contents as SequenceFiles of serialized Java objects. The file name at each batch interval is generated based on prefix and suffix: "prefix-TIME_IN_MS[.suffix]".
Python API This is not available in the Python API.
saveAsHadoopFiles(prefix, [suffix])	Save this DStream's contents as Hadoop files. The file name at each batch interval is generated based on prefix and suffix: "prefix-TIME_IN_MS[.suffix]".
Python API This is not available in the Python API.
foreachRDD(func)	The most generic output operator that applies a function, func, to each RDD generated from the stream. This function should push the data in each RDD to an external system, such as saving the RDD to files, or writing it over the network to a database. Note that the function func is executed in the driver process running the streaming application, and will usually have RDD actions in it that will force the computation of the streaming RDDs.
Design Patterns for using foreachRDD
dstream.foreachRDD is a powerful primitive that allows data to be sent out to external systems. However, it is important to understand how to use this primitive correctly and efficiently. Some of the common mistakes to avoid are as follows.

Often writing data to external systems requires creating a connection object (e.g. TCP connection to a remote server) and using it to send data to a remote system. For this purpose, a developer may inadvertently try creating a connection object at the Spark driver, and then try to use it in a Spark worker to save records in the RDDs. For example (in Scala),

Python
Scala
Java
def sendRecord(rdd):
    connection = createNewConnection()  # executed at the driver
    rdd.foreach(lambda record: connection.send(record))
    connection.close()

dstream.foreachRDD(sendRecord)
This is incorrect as this requires the connection object to be serialized and sent from the driver to the worker. Such connection objects are rarely transferable across machines. This error may manifest as serialization errors (connection object not serializable), initialization errors (connection object needs to be initialized at the workers), etc. The correct solution is to create the connection object at the worker.

However, this can lead to another common mistake - creating a new connection for every record. For example,

Python
Scala
Java
def sendRecord(record):
    connection = createNewConnection()
    connection.send(record)
    connection.close()

dstream.foreachRDD(lambda rdd: rdd.foreach(sendRecord))
Typically, creating a connection object has time and resource overheads. Therefore, creating and destroying a connection object for each record can incur unnecessarily high overheads and can significantly reduce the overall throughput of the system. A better solution is to use rdd.foreachPartition - create a single connection object and send all the records in a RDD partition using that connection.

Python
Scala
Java
def sendPartition(iter):
    connection = createNewConnection()
    for record in iter:
        connection.send(record)
    connection.close()

dstream.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
This amortizes the connection creation overheads over many records.

Finally, this can be further optimized by reusing connection objects across multiple RDDs/batches. One can maintain a static pool of connection objects than can be reused as RDDs of multiple batches are pushed to the external system, thus further reducing the overheads.

Python
Scala
Java
def sendPartition(iter):
    # ConnectionPool is a static, lazily initialized pool of connections
    connection = ConnectionPool.getConnection()
    for record in iter:
        connection.send(record)
    # return to the pool for future reuse
    ConnectionPool.returnConnection(connection)

dstream.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
Note that the connections in the pool should be lazily created on demand and timed out if not used for a while. This achieves the most efficient sending of data to external systems.

Other points to remember:
DStreams are executed lazily by the output operations, just like RDDs are lazily executed by RDD actions. Specifically, RDD actions inside the DStream output operations force the processing of the received data. Hence, if your application does not have any output operation, or has output operations like dstream.foreachRDD() without any RDD action inside them, then nothing will get executed. The system will simply receive the data and discard it.

By default, output operations are executed one-at-a-time. And they are executed in the order they are defined in the application.

DataFrame and SQL Operations
You can easily use DataFrames and SQL operations on streaming data. You have to create a SparkSession using the SparkContext that the StreamingContext is using. Furthermore, this has to be done such that it can be restarted on driver failures. This is done by creating a lazily instantiated singleton instance of SparkSession. This is shown in the following example. It modifies the earlier word count example to generate word counts using DataFrames and SQL. Each RDD is converted to a DataFrame, registered as a temporary table and then queried using SQL.

Python
Scala
Java
# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

...

# DataFrame operations inside your streaming program

words = ... # DStream of strings

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(word=w))
        wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
        wordCountsDataFrame.show()
    except:
        pass

words.foreachRDD(process)
See the full source code.

You can also run SQL queries on tables defined on streaming data from a different thread (that is, asynchronous to the running StreamingContext). Just make sure that you set the StreamingContext to remember a sufficient amount of streaming data such that the query can run. Otherwise the StreamingContext, which is unaware of any of the asynchronous SQL queries, will delete off old streaming data before the query can complete. For example, if you want to query the last batch, but your query can take 5 minutes to run, then call streamingContext.remember(Minutes(5)) (in Scala, or equivalent in other languages).

See the DataFrames and SQL guide to learn more about DataFrames.

MLlib Operations
You can also easily use machine learning algorithms provided by MLlib. First of all, there are streaming machine learning algorithms (e.g. Streaming Linear Regression, Streaming KMeans, etc.) which can simultaneously learn from the streaming data as well as apply the model on the streaming data. Beyond these, for a much larger class of machine learning algorithms, you can learn a learning model offline (i.e. using historical data) and then apply the model online on streaming data. See the MLlib guide for more details.

Caching / Persistence
Similar to RDDs, DStreams also allow developers to persist the stream’s data in memory. That is, using the persist() method on a DStream will automatically persist every RDD of that DStream in memory. This is useful if the data in the DStream will be computed multiple times (e.g., multiple operations on the same data). For window-based operations like reduceByWindow and reduceByKeyAndWindow and state-based operations like updateStateByKey, this is implicitly true. Hence, DStreams generated by window-based operations are automatically persisted in memory, without the developer calling persist().

For input streams that receive data over the network (such as, Kafka, sockets, etc.), the default persistence level is set to replicate the data to two nodes for fault-tolerance.

Note that, unlike RDDs, the default persistence level of DStreams keeps the data serialized in memory. This is further discussed in the Performance Tuning section. More information on different persistence levels can be found in the Spark Programming Guide.

Checkpointing
A streaming application must operate 24/7 and hence must be resilient to failures unrelated to the application logic (e.g., system failures, JVM crashes, etc.). For this to be possible, Spark Streaming needs to checkpoint enough information to a fault- tolerant storage system such that it can recover from failures. There are two types of data that are checkpointed.

Metadata checkpointing - Saving of the information defining the streaming computation to fault-tolerant storage like HDFS. This is used to recover from failure of the node running the driver of the streaming application (discussed in detail later). Metadata includes:
Configuration - The configuration that was used to create the streaming application.
DStream operations - The set of DStream operations that define the streaming application.
Incomplete batches - Batches whose jobs are queued but have not completed yet.
Data checkpointing - Saving of the generated RDDs to reliable storage. This is necessary in some stateful transformations that combine data across multiple batches. In such transformations, the generated RDDs depend on RDDs of previous batches, which causes the length of the dependency chain to keep increasing with time. To avoid such unbounded increases in recovery time (proportional to dependency chain), intermediate RDDs of stateful transformations are periodically checkpointed to reliable storage (e.g. HDFS) to cut off the dependency chains.
To summarize, metadata checkpointing is primarily needed for recovery from driver failures, whereas data or RDD checkpointing is necessary even for basic functioning if stateful transformations are used.

When to enable Checkpointing
Checkpointing must be enabled for applications with any of the following requirements:

Usage of stateful transformations - If either updateStateByKey or reduceByKeyAndWindow (with inverse function) is used in the application, then the checkpoint directory must be provided to allow for periodic RDD checkpointing.
Recovering from failures of the driver running the application - Metadata checkpoints are used to recover with progress information.
Note that simple streaming applications without the aforementioned stateful transformations can be run without enabling checkpointing. The recovery from driver failures will also be partial in that case (some received but unprocessed data may be lost). This is often acceptable and many run Spark Streaming applications in this way. Support for non-Hadoop environments is expected to improve in the future.

How to configure Checkpointing
Checkpointing can be enabled by setting a directory in a fault-tolerant, reliable file system (e.g., HDFS, S3, etc.) to which the checkpoint information will be saved. This is done by using streamingContext.checkpoint(checkpointDirectory). This will allow you to use the aforementioned stateful transformations. Additionally, if you want to make the application recover from driver failures, you should rewrite your streaming application to have the following behavior.

When the program is being started for the first time, it will create a new StreamingContext, set up all the streams and then call start().
When the program is being restarted after failure, it will re-create a StreamingContext from the checkpoint data in the checkpoint directory.
Python
Scala
Java
This behavior is made simple by using StreamingContext.getOrCreate. This is used as follows.

# Function to create and setup a new StreamingContext
def functionToCreateContext():
    sc = SparkContext(...)  # new context
    ssc = StreamingContext(...)
    lines = ssc.socketTextStream(...)  # create DStreams
    ...
    ssc.checkpoint(checkpointDirectory)  # set checkpoint directory
    return ssc

# Get StreamingContext from checkpoint data or create a new one
context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)

# Do additional setup on context that needs to be done,
# irrespective of whether it is being started or restarted
context. ...

# Start the context
context.start()
context.awaitTermination()
If the checkpointDirectory exists, then the context will be recreated from the checkpoint data. If the directory does not exist (i.e., running for the first time), then the function functionToCreateContext will be called to create a new context and set up the DStreams. See the Python example recoverable_network_wordcount.py. This example appends the word counts of network data into a file.

You can also explicitly create a StreamingContext from the checkpoint data and start the computation by using StreamingContext.getOrCreate(checkpointDirectory, None).

In addition to using getOrCreate one also needs to ensure that the driver process gets restarted automatically on failure. This can only be done by the deployment infrastructure that is used to run the application. This is further discussed in the Deployment section.

Note that checkpointing of RDDs incurs the cost of saving to reliable storage. This may cause an increase in the processing time of those batches where RDDs get checkpointed. Hence, the interval of checkpointing needs to be set carefully. At small batch sizes (say 1 second), checkpointing every batch may significantly reduce operation throughput. Conversely, checkpointing too infrequently causes the lineage and task sizes to grow, which may have detrimental effects. For stateful transformations that require RDD checkpointing, the default interval is a multiple of the batch interval that is at least 10 seconds. It can be set by using dstream.checkpoint(checkpointInterval). Typically, a checkpoint interval of 5 - 10 sliding intervals of a DStream is a good setting to try.

Accumulators, Broadcast Variables, and Checkpoints
Accumulators and Broadcast variables cannot be recovered from checkpoint in Spark Streaming. If you enable checkpointing and use Accumulators or Broadcast variables as well, you’ll have to create lazily instantiated singleton instances for Accumulators and Broadcast variables so that they can be re-instantiated after the driver restarts on failure. This is shown in the following example.

Python
Scala
Java
def getWordExcludeList(sparkContext):
    if ("wordExcludeList" not in globals()):
        globals()["wordExcludeList"] = sparkContext.broadcast(["a", "b", "c"])
    return globals()["wordExcludeList"]

def getDroppedWordsCounter(sparkContext):
    if ("droppedWordsCounter" not in globals()):
        globals()["droppedWordsCounter"] = sparkContext.accumulator(0)
    return globals()["droppedWordsCounter"]

def echo(time, rdd):
    # Get or register the excludeList Broadcast
    excludeList = getWordExcludeList(rdd.context)
    # Get or register the droppedWordsCounter Accumulator
    droppedWordsCounter = getDroppedWordsCounter(rdd.context)

    # Use excludeList to drop words and use droppedWordsCounter to count them
    def filterFunc(wordCount):
        if wordCount[0] in excludeList.value:
            droppedWordsCounter.add(wordCount[1])
            False
        else:
            True

    counts = "Counts at time %s %s" % (time, rdd.filter(filterFunc).collect())

wordCounts.foreachRDD(echo)
See the full source code.

Deploying Applications
This section discusses the steps to deploy a Spark Streaming application.

Requirements
To run Spark Streaming applications, you need to have the following.

Cluster with a cluster manager - This is the general requirement of any Spark application, and discussed in detail in the deployment guide.

Package the application JAR - You have to compile your streaming application into a JAR. If you are using spark-submit to start the application, then you will not need to provide Spark and Spark Streaming in the JAR. However, if your application uses advanced sources (e.g. Kafka), then you will have to package the extra artifact they link to, along with their dependencies, in the JAR that is used to deploy the application. For example, an application using KafkaUtils will have to include spark-streaming-kafka-0-10_2.13 and all its transitive dependencies in the application JAR.

Configuring sufficient memory for the executors - Since the received data must be stored in memory, the executors must be configured with sufficient memory to hold the received data. Note that if you are doing 10 minute window operations, the system has to keep at least last 10 minutes of data in memory. So the memory requirements for the application depends on the operations used in it.

Configuring checkpointing - If the stream application requires it, then a directory in the Hadoop API compatible fault-tolerant storage (e.g. HDFS, S3, etc.) must be configured as the checkpoint directory and the streaming application written in a way that checkpoint information can be used for failure recovery. See the checkpointing section for more details.

Configuring automatic restart of the application driver - To automatically recover from a driver failure, the deployment infrastructure that is used to run the streaming application must monitor the driver process and relaunch the driver if it fails. Different cluster managers have different tools to achieve this.
Spark Standalone - A Spark application driver can be submitted to run within the Spark Standalone cluster (see cluster deploy mode), that is, the application driver itself runs on one of the worker nodes. Furthermore, the Standalone cluster manager can be instructed to supervise the driver, and relaunch it if the driver fails either due to non-zero exit code, or due to failure of the node running the driver. See cluster mode and supervise in the Spark Standalone guide for more details.
YARN - Yarn supports a similar mechanism for automatically restarting an application. Please refer to YARN documentation for more details.
Configuring write-ahead logs - Since Spark 1.2, we have introduced write-ahead logs for achieving strong fault-tolerance guarantees. If enabled, all the data received from a receiver gets written into a write-ahead log in the configuration checkpoint directory. This prevents data loss on driver recovery, thus ensuring zero data loss (discussed in detail in the Fault-tolerance Semantics section). This can be enabled by setting the configuration parameter spark.streaming.receiver.writeAheadLog.enable to true. However, these stronger semantics may come at the cost of the receiving throughput of individual receivers. This can be corrected by running more receivers in parallel to increase aggregate throughput. Additionally, it is recommended that the replication of the received data within Spark be disabled when the write-ahead log is enabled as the log is already stored in a replicated storage system. This can be done by setting the storage level for the input stream to StorageLevel.MEMORY_AND_DISK_SER. While using S3 (or any file system that does not support flushing) for write-ahead logs, please remember to enable spark.streaming.driver.writeAheadLog.closeFileAfterWrite and spark.streaming.receiver.writeAheadLog.closeFileAfterWrite. See Spark Streaming Configuration for more details. Note that Spark will not encrypt data written to the write-ahead log when I/O encryption is enabled. If encryption of the write-ahead log data is desired, it should be stored in a file system that supports encryption natively.

Setting the max receiving rate - If the cluster resources are not large enough for the streaming application to process data as fast as it is being received, the receivers can be rate limited by setting a maximum rate limit in terms of records / sec. See the configuration parameters spark.streaming.receiver.maxRate for receivers and spark.streaming.kafka.maxRatePerPartition for Direct Kafka approach. In Spark 1.5, we have introduced a feature called backpressure that eliminates the need to set this rate limit, as Spark Streaming automatically figures out the rate limits and dynamically adjusts them if the processing conditions change. This backpressure can be enabled by setting the configuration parameter spark.streaming.backpressure.enabled to true.
Upgrading Application Code
If a running Spark Streaming application needs to be upgraded with new application code, then there are two possible mechanisms.

The upgraded Spark Streaming application is started and run in parallel to the existing application. Once the new one (receiving the same data as the old one) has been warmed up and is ready for prime time, the old one can be brought down. Note that this can be done for data sources that support sending the data to two destinations (i.e., the earlier and upgraded applications).

The existing application is shutdown gracefully (see StreamingContext.stop(...) or JavaStreamingContext.stop(...) for graceful shutdown options) which ensure data that has been received is completely processed before shutdown. Then the upgraded application can be started, which will start processing from the same point where the earlier application left off. Note that this can be done only with input sources that support source-side buffering (like Kafka) as data needs to be buffered while the previous application was down and the upgraded application is not yet up. And restarting from earlier checkpoint information of pre-upgrade code cannot be done. The checkpoint information essentially contains serialized Scala/Java/Python objects and trying to deserialize objects with new, modified classes may lead to errors. In this case, either start the upgraded app with a different checkpoint directory, or delete the previous checkpoint directory.

Monitoring Applications
Beyond Spark’s monitoring capabilities, there are additional capabilities specific to Spark Streaming. When a StreamingContext is used, the Spark web UI shows an additional Streaming tab which shows statistics about running receivers (whether receivers are active, number of records received, receiver error, etc.) and completed batches (batch processing times, queueing delays, etc.). This can be used to monitor the progress of the streaming application.

The following two metrics in web UI are particularly important:

Processing Time - The time to process each batch of data.
Scheduling Delay - the time a batch waits in a queue for the processing of previous batches to finish.
If the batch processing time is consistently more than the batch interval and/or the queueing delay keeps increasing, then it indicates that the system is not able to process the batches as fast they are being generated and is falling behind. In that case, consider reducing the batch processing time.

The progress of a Spark Streaming program can also be monitored using the StreamingListener interface, which allows you to get receiver status and processing times. Note that this is a developer API and it is likely to be improved upon (i.e., more information reported) in the future.

Performance Tuning
Getting the best performance out of a Spark Streaming application on a cluster requires a bit of tuning. This section explains a number of the parameters and configurations that can be tuned to improve the performance of your application. At a high level, you need to consider two things:

Reducing the processing time of each batch of data by efficiently using cluster resources.

Setting the right batch size such that the batches of data can be processed as fast as they are received (that is, data processing keeps up with the data ingestion).

Reducing the Batch Processing Times
There are a number of optimizations that can be done in Spark to minimize the processing time of each batch. These have been discussed in detail in the Tuning Guide. This section highlights some of the most important ones.

Level of Parallelism in Data Receiving
Receiving data over the network (like Kafka, socket, etc.) requires the data to be deserialized and stored in Spark. If the data receiving becomes a bottleneck in the system, then consider parallelizing the data receiving. Note that each input DStream creates a single receiver (running on a worker machine) that receives a single stream of data. Receiving multiple data streams can therefore be achieved by creating multiple input DStreams and configuring them to receive different partitions of the data stream from the source(s). For example, a single Kafka input DStream receiving two topics of data can be split into two Kafka input streams, each receiving only one topic. This would run two receivers, allowing data to be received in parallel, thus increasing overall throughput. These multiple DStreams can be unioned together to create a single DStream. Then the transformations that were being applied on a single input DStream can be applied on the unified stream. This is done as follows.

Python
Scala
Java
numStreams = 5
kafkaStreams = [KafkaUtils.createStream(...) for _ in range (numStreams)]
unifiedStream = streamingContext.union(*kafkaStreams)
unifiedStream.pprint()
Another parameter that should be considered is the receiver’s block interval, which is determined by the configuration parameter spark.streaming.blockInterval. For most receivers, the received data is coalesced together into blocks of data before storing inside Spark’s memory. The number of blocks in each batch determines the number of tasks that will be used to process the received data in a map-like transformation. The number of tasks per receiver per batch will be approximately (batch interval / block interval). For example, a block interval of 200 ms will create 10 tasks per 2 second batches. If the number of tasks is too low (that is, less than the number of cores per machine), then it will be inefficient as all available cores will not be used to process the data. To increase the number of tasks for a given batch interval, reduce the block interval. However, the recommended minimum value of block interval is about 50 ms, below which the task launching overheads may be a problem.

An alternative to receiving data with multiple input streams / receivers is to explicitly repartition the input data stream (using inputStream.repartition(<number of partitions>)). This distributes the received batches of data across the specified number of machines in the cluster before further processing.

For direct stream, please refer to Spark Streaming + Kafka Integration Guide

Level of Parallelism in Data Processing
Cluster resources can be under-utilized if the number of parallel tasks used in any stage of the computation is not high enough. For example, for distributed reduce operations like reduceByKey and reduceByKeyAndWindow, the default number of parallel tasks is controlled by the spark.default.parallelism configuration property. You can pass the level of parallelism as an argument (see PairDStreamFunctions documentation), or set the spark.default.parallelism configuration property to change the default.

Data Serialization
The overheads of data serialization can be reduced by tuning the serialization formats. In the case of streaming, there are two types of data that are being serialized.

Input data: By default, the input data received through Receivers is stored in the executors’ memory with StorageLevel.MEMORY_AND_DISK_SER_2. That is, the data is serialized into bytes to reduce GC overheads, and replicated for tolerating executor failures. Also, the data is kept first in memory, and spilled over to disk only if the memory is insufficient to hold all of the input data necessary for the streaming computation. This serialization obviously has overheads – the receiver must deserialize the received data and re-serialize it using Spark’s serialization format.

Persisted RDDs generated by Streaming Operations: RDDs generated by streaming computations may be persisted in memory. For example, window operations persist data in memory as they would be processed multiple times. However, unlike the Spark Core default of StorageLevel.MEMORY_ONLY, persisted RDDs generated by streaming computations are persisted with StorageLevel.MEMORY_ONLY_SER (i.e. serialized) by default to minimize GC overheads.

In both cases, using Kryo serialization can reduce both CPU and memory overheads. See the Spark Tuning Guide for more details. For Kryo, consider registering custom classes, and disabling object reference tracking (see Kryo-related configurations in the Configuration Guide).

In specific cases where the amount of data that needs to be retained for the streaming application is not large, it may be feasible to persist data (both types) as deserialized objects without incurring excessive GC overheads. For example, if you are using batch intervals of a few seconds and no window operations, then you can try disabling serialization in persisted data by explicitly setting the storage level accordingly. This would reduce the CPU overheads due to serialization, potentially improving performance without too much GC overheads.

Setting the Right Batch Interval
For a Spark Streaming application running on a cluster to be stable, the system should be able to process data as fast as it is being received. In other words, batches of data should be processed as fast as they are being generated. Whether this is true for an application can be found by monitoring the processing times in the streaming web UI, where the batch processing time should be less than the batch interval.

Depending on the nature of the streaming computation, the batch interval used may have significant impact on the data rates that can be sustained by the application on a fixed set of cluster resources. For example, let us consider the earlier WordCountNetwork example. For a particular data rate, the system may be able to keep up with reporting word counts every 2 seconds (i.e., batch interval of 2 seconds), but not every 500 milliseconds. So the batch interval needs to be set such that the expected data rate in production can be sustained.

A good approach to figure out the right batch size for your application is to test it with a conservative batch interval (say, 5-10 seconds) and a low data rate. To verify whether the system is able to keep up with the data rate, you can check the value of the end-to-end delay experienced by each processed batch (either look for “Total delay” in Spark driver log4j logs, or use the StreamingListener interface). If the delay is maintained to be comparable to the batch size, then the system is stable. Otherwise, if the delay is continuously increasing, it means that the system is unable to keep up and it is therefore unstable. Once you have an idea of a stable configuration, you can try increasing the data rate and/or reducing the batch size. Note that a momentary increase in the delay due to temporary data rate increases may be fine as long as the delay reduces back to a low value (i.e., less than batch size).

Memory Tuning
Tuning the memory usage and GC behavior of Spark applications has been discussed in great detail in the Tuning Guide. It is strongly recommended that you read that. In this section, we discuss a few tuning parameters specifically in the context of Spark Streaming applications.

The amount of cluster memory required by a Spark Streaming application depends heavily on the type of transformations used. For example, if you want to use a window operation on the last 10 minutes of data, then your cluster should have sufficient memory to hold 10 minutes worth of data in memory. Or if you want to use updateStateByKey with a large number of keys, then the necessary memory will be high. On the contrary, if you want to do a simple map-filter-store operation, then the necessary memory will be low.

In general, since the data received through receivers is stored with StorageLevel.MEMORY_AND_DISK_SER_2, the data that does not fit in memory will spill over to the disk. This may reduce the performance of the streaming application, and hence it is advised to provide sufficient memory as required by your streaming application. Its best to try and see the memory usage on a small scale and estimate accordingly.

Another aspect of memory tuning is garbage collection. For a streaming application that requires low latency, it is undesirable to have large pauses caused by JVM Garbage Collection.

There are a few parameters that can help you tune the memory usage and GC overheads:

Persistence Level of DStreams: As mentioned earlier in the Data Serialization section, the input data and RDDs are by default persisted as serialized bytes. This reduces both the memory usage and GC overheads, compared to deserialized persistence. Enabling Kryo serialization further reduces serialized sizes and memory usage. Further reduction in memory usage can be achieved with compression (see the Spark configuration spark.rdd.compress), at the cost of CPU time.

Clearing old data: By default, all input data and persisted RDDs generated by DStream transformations are automatically cleared. Spark Streaming decides when to clear the data based on the transformations that are used. For example, if you are using a window operation of 10 minutes, then Spark Streaming will keep around the last 10 minutes of data, and actively throw away older data. Data can be retained for a longer duration (e.g. interactively querying older data) by setting streamingContext.remember.

Other tips: To further reduce GC overheads, here are some more tips to try.

Persist RDDs using the OFF_HEAP storage level. See more detail in the Spark Programming Guide.
Use more executors with smaller heap sizes. This will reduce the GC pressure within each JVM heap.
Important points to remember:
A DStream is associated with a single receiver. For attaining read parallelism multiple receivers i.e. multiple DStreams need to be created. A receiver is run within an executor. It occupies one core. Ensure that there are enough cores for processing after receiver slots are booked i.e. spark.cores.max should take the receiver slots into account. The receivers are allocated to executors in a round robin fashion.

When data is received from a stream source, the receiver creates blocks of data. A new block of data is generated every blockInterval milliseconds. N blocks of data are created during the batchInterval where N = batchInterval/blockInterval. These blocks are distributed by the BlockManager of the current executor to the block managers of other executors. After that, the Network Input Tracker running on the driver is informed about the block locations for further processing.

An RDD is created on the driver for the blocks created during the batchInterval. The blocks generated during the batchInterval are partitions of the RDD. Each partition is a task in spark. blockInterval== batchinterval would mean that a single partition is created and probably it is processed locally.

The map tasks on the blocks are processed in the executors (one that received the block, and another where the block was replicated) that has the blocks irrespective of block interval, unless non-local scheduling kicks in. Having a bigger blockinterval means bigger blocks. A high value of spark.locality.wait increases the chance of processing a block on the local node. A balance needs to be found out between these two parameters to ensure that the bigger blocks are processed locally.

Instead of relying on batchInterval and blockInterval, you can define the number of partitions by calling inputDstream.repartition(n). This reshuffles the data in RDD randomly to create n number of partitions. Yes, for greater parallelism. Though comes at the cost of a shuffle. An RDD’s processing is scheduled by the driver’s jobscheduler as a job. At a given point of time only one job is active. So, if one job is executing the other jobs are queued.

If you have two dstreams there will be two RDDs formed and there will be two jobs created which will be scheduled one after the another. To avoid this, you can union two dstreams. This will ensure that a single unionRDD is formed for the two RDDs of the dstreams. This unionRDD is then considered as a single job. However, the partitioning of the RDDs is not impacted.

If the batch processing time is more than batchinterval then obviously the receiver’s memory will start filling up and will end up in throwing exceptions (most probably BlockNotFoundException). Currently, there is no way to pause the receiver. Using SparkConf configuration spark.streaming.receiver.maxRate, rate of receiver can be limited.

Fault-tolerance Semantics
In this section, we will discuss the behavior of Spark Streaming applications in the event of failures.

Background
To understand the semantics provided by Spark Streaming, let us remember the basic fault-tolerance semantics of Spark’s RDDs.

An RDD is an immutable, deterministically re-computable, distributed dataset. Each RDD remembers the lineage of deterministic operations that were used on a fault-tolerant input dataset to create it.
If any partition of an RDD is lost due to a worker node failure, then that partition can be re-computed from the original fault-tolerant dataset using the lineage of operations.
Assuming that all of the RDD transformations are deterministic, the data in the final transformed RDD will always be the same irrespective of failures in the Spark cluster.
Spark operates on data in fault-tolerant file systems like HDFS or S3. Hence, all of the RDDs generated from the fault-tolerant data are also fault-tolerant. However, this is not the case for Spark Streaming as the data in most cases is received over the network (except when fileStream is used). To achieve the same fault-tolerance properties for all of the generated RDDs, the received data is replicated among multiple Spark executors in worker nodes in the cluster (default replication factor is 2). This leads to two kinds of data in the system that need to be recovered in the event of failures:

Data received and replicated - This data survives failure of a single worker node as a copy of it exists on one of the other nodes.
Data received but buffered for replication - Since this is not replicated, the only way to recover this data is to get it again from the source.
Furthermore, there are two kinds of failures that we should be concerned about:

Failure of a Worker Node - Any of the worker nodes running executors can fail, and all in-memory data on those nodes will be lost. If any receivers were running on failed nodes, then their buffered data will be lost.
Failure of the Driver Node - If the driver node running the Spark Streaming application fails, then obviously the SparkContext is lost, and all executors with their in-memory data are lost.
With this basic knowledge, let us understand the fault-tolerance semantics of Spark Streaming.

Definitions
The semantics of streaming systems are often captured in terms of how many times each record can be processed by the system. There are three types of guarantees that a system can provide under all possible operating conditions (despite failures, etc.)

At most once: Each record will be either processed once or not processed at all.
At least once: Each record will be processed one or more times. This is stronger than at-most once as it ensures that no data will be lost. But there may be duplicates.
Exactly once: Each record will be processed exactly once - no data will be lost and no data will be processed multiple times. This is obviously the strongest guarantee of the three.
Basic Semantics
In any stream processing system, broadly speaking, there are three steps in processing the data.

Receiving the data: The data is received from sources using Receivers or otherwise.

Transforming the data: The received data is transformed using DStream and RDD transformations.

Pushing out the data: The final transformed data is pushed out to external systems like file systems, databases, dashboards, etc.

If a streaming application has to achieve end-to-end exactly-once guarantees, then each step has to provide an exactly-once guarantee. That is, each record must be received exactly once, transformed exactly once, and pushed to downstream systems exactly once. Let’s understand the semantics of these steps in the context of Spark Streaming.

Receiving the data: Different input sources provide different guarantees. This is discussed in detail in the next subsection.

Transforming the data: All data that has been received will be processed exactly once, thanks to the guarantees that RDDs provide. Even if there are failures, as long as the received input data is accessible, the final transformed RDDs will always have the same contents.

Pushing out the data: Output operations by default ensure at-least once semantics because it depends on the type of output operation (idempotent, or not) and the semantics of the downstream system (supports transactions or not). But users can implement their own transaction mechanisms to achieve exactly-once semantics. This is discussed in more details later in the section.

Semantics of Received Data
Different input sources provide different guarantees, ranging from at-least once to exactly once. Read for more details.

With Files
If all of the input data is already present in a fault-tolerant file system like HDFS, Spark Streaming can always recover from any failure and process all of the data. This gives exactly-once semantics, meaning all of the data will be processed exactly once no matter what fails.

With Receiver-based Sources
For input sources based on receivers, the fault-tolerance semantics depend on both the failure scenario and the type of receiver. As we discussed earlier, there are two types of receivers:

Reliable Receiver - These receivers acknowledge reliable sources only after ensuring that the received data has been replicated. If such a receiver fails, the source will not receive acknowledgment for the buffered (unreplicated) data. Therefore, if the receiver is restarted, the source will resend the data, and no data will be lost due to the failure.
Unreliable Receiver - Such receivers do not send acknowledgment and therefore can lose data when they fail due to worker or driver failures.
Depending on what type of receivers are used we achieve the following semantics. If a worker node fails, then there is no data loss with reliable receivers. With unreliable receivers, data received but not replicated can get lost. If the driver node fails, then besides these losses, all of the past data that was received and replicated in memory will be lost. This will affect the results of the stateful transformations.

To avoid this loss of past received data, Spark 1.2 introduced write ahead logs which save the received data to fault-tolerant storage. With the write-ahead logs enabled and reliable receivers, there is zero data loss. In terms of semantics, it provides an at-least once guarantee.

The following table summarizes the semantics under failures:

Deployment Scenario	Worker Failure	Driver Failure
Spark 1.1 or earlier, OR
Spark 1.2 or later without write-ahead logs	Buffered data lost with unreliable receivers
Zero data loss with reliable receivers
At-least once semantics	Buffered data lost with unreliable receivers
Past data lost with all receivers
Undefined semantics
Spark 1.2 or later with write-ahead logs	Zero data loss with reliable receivers
At-least once semantics	Zero data loss with reliable receivers and files
At-least once semantics
With Kafka Direct API
In Spark 1.3, we have introduced a new Kafka Direct API, which can ensure that all the Kafka data is received by Spark Streaming exactly once. Along with this, if you implement exactly-once output operation, you can achieve end-to-end exactly-once guarantees. This approach is further discussed in the Kafka Integration Guide.

Semantics of output operations
Output operations (like foreachRDD) have at-least once semantics, that is, the transformed data may get written to an external entity more than once in the event of a worker failure. While this is acceptable for saving to file systems using the saveAs***Files operations (as the file will simply get overwritten with the same data), additional effort may be necessary to achieve exactly-once semantics. There are two approaches.

Idempotent updates: Multiple attempts always write the same data. For example, saveAs***Files always writes the same data to the generated files.

Transactional updates: All updates are made transactionally so that updates are made exactly once atomically. One way to do this would be the following.

Use the batch time (available in foreachRDD) and the partition index of the RDD to create an identifier. This identifier uniquely identifies a blob data in the streaming application.
Update external system with this blob transactionally (that is, exactly once, atomically) using the identifier. That is, if the identifier is not already committed, commit the partition data and the identifier atomically. Else, if this was already committed, skip the update.

dstream.foreachRDD { (rdd, time) =>
  rdd.foreachPartition { partitionIterator =>
    val partitionId = TaskContext.get.partitionId()
    val uniqueId = generateUniqueId(time.milliseconds, partitionId)
    // use this uniqueId to transactionally commit the data in partitionIterator
  }
}
Where to Go from Here
Additional guides
Kafka Integration Guide
Kinesis Integration Guide
Custom Receiver Guide
Third-party DStream data sources can be found in Third Party Projects
API documentation
Python docs
StreamingContext and DStream
Scala docs
StreamingContext and DStream
KafkaUtils, KinesisUtils
Java docs
JavaStreamingContext, JavaDStream and JavaPairDStream
KafkaUtils, KinesisUtils
More examples in Python and Scala and Java
Paper and video describing Spark Streaming.


Table of Contents

Correlation
Hypothesis testing
ChiSquareTest
Summarizer
Correlation
Calculating the correlation between two series of data is a common operation in Statistics. In spark.ml we provide the flexibility to calculate pairwise correlations among many series. The supported correlation methods are currently Pearson’s and Spearman’s correlation.

Python
Scala
Java
Correlation computes the correlation matrix for the input Dataset of Vectors using the specified method. The output will be a DataFrame that contains the correlation matrix of the column of vectors.

from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation

data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
        (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
        (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
        (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]
df = spark.createDataFrame(data, ["features"])

r1 = Correlation.corr(df, "features").head()


print("Pearson correlation matrix:\n" + str(r1[0]))

r2 = Correlation.corr(df, "features", "spearman").head()


print("Spearman correlation matrix:\n" + str(r2[0]))
Find full example code at "examples/src/main/python/ml/correlation_example.py" in the Spark repo.
Hypothesis testing
Hypothesis testing is a powerful tool in statistics to determine whether a result is statistically significant, whether this result occurred by chance or not. spark.ml currently supports Pearson’s Chi-squared ( $\chi^2$) tests for independence.

ChiSquareTest
ChiSquareTest conducts Pearson’s independence test for every feature against the label. For each feature, the (feature, label) pairs are converted into a contingency matrix for which the Chi-squared statistic is computed. All label and feature values must be categorical.

Python
Scala
Java
Refer to the ChiSquareTest Python docs for details on the API.

from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import ChiSquareTest

data = [(0.0, Vectors.dense(0.5, 10.0)),
        (0.0, Vectors.dense(1.5, 20.0)),
        (1.0, Vectors.dense(1.5, 30.0)),
        (0.0, Vectors.dense(3.5, 30.0)),
        (0.0, Vectors.dense(3.5, 40.0)),
        (1.0, Vectors.dense(3.5, 40.0))]
df = spark.createDataFrame(data, ["label", "features"])

r = ChiSquareTest.test(df, "features", "label").head()



print("pValues: " + str(r.pValues))
print("degreesOfFreedom: " + str(r.degreesOfFreedom))
print("statistics: " + str(r.statistics))
Find full example code at "examples/src/main/python/ml/chi_square_test_example.py" in the Spark repo.
Summarizer
We provide vector column summary statistics for Dataframe through Summarizer. Available metrics are the column-wise max, min, mean, sum, variance, std, and number of nonzeros, as well as the total count.

Python
Scala
Java
Refer to the Summarizer Python docs for details on the API.

from pyspark.ml.stat import Summarizer
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors

df = sc.parallelize([Row(weight=1.0, features=Vectors.dense(1.0, 1.0, 1.0)),
                     Row(weight=0.0, features=Vectors.dense(1.0, 2.0, 3.0))]).toDF()

# create summarizer for multiple metrics "mean" and "count"
summarizer = Summarizer.metrics("mean", "count")

# compute statistics for multiple metrics with weight
df.select(summarizer.summary(df.features, df.weight)).show(truncate=False)

# compute statistics for multiple metrics without weight
df.select(summarizer.summary(df.features)).show(truncate=False)

# compute statistics for single metric "mean" with weight
df.select(Summarizer.mean(df.features, df.weight)).show(truncate=False)

# compute statistics for single metric "mean" without weight
df.select(Summarizer.mean(df.features)).show(truncate=False)
Find full example code at "examples/src/main/python/ml/summarizer_example.py" in the Spark repo.

Data sources
In this section, we introduce how to use data source in ML to load data. Besides some general data sources such as Parquet, CSV, JSON and JDBC, we also provide some specific data sources for ML.

Table of Contents

Image data source
LIBSVM data source
Image data source
This image data source is used to load image files from a directory, it can load compressed image (jpeg, png, etc.) into raw image representation via ImageIO in Java library. The loaded DataFrame has one StructType column: “image”, containing image data stored as image schema. The schema of the image column is:

origin: StringType (represents the file path of the image)
height: IntegerType (height of the image)
width: IntegerType (width of the image)
nChannels: IntegerType (number of image channels)
mode: IntegerType (OpenCV-compatible type)
data: BinaryType (Image bytes in OpenCV-compatible order: row-wise BGR in most cases)
Python
Scala
Java
R
In PySpark we provide Spark SQL data source API for loading image data as a DataFrame.

>>> df = spark.read.format("image").option("dropInvalid", True).load("data/mllib/images/origin/kittens")
>>> df.select("image.origin", "image.width", "image.height").show(truncate=False)
+-----------------------------------------------------------------------+-----+------+
|origin                                                                 |width|height|
+-----------------------------------------------------------------------+-----+------+
|file:///spark/data/mllib/images/origin/kittens/54893.jpg               |300  |311   |
|file:///spark/data/mllib/images/origin/kittens/DP802813.jpg            |199  |313   |
|file:///spark/data/mllib/images/origin/kittens/29.5.a_b_EGDP022204.jpg |300  |200   |
|file:///spark/data/mllib/images/origin/kittens/DP153539.jpg            |300  |296   |
+-----------------------------------------------------------------------+-----+------+
LIBSVM data source
This LIBSVM data source is used to load ‘libsvm’ type files from a directory. The loaded DataFrame has two columns: label containing labels stored as doubles and features containing feature vectors stored as Vectors. The schemas of the columns are:

label: DoubleType (represents the instance label)
features: VectorUDT (represents the feature vector)
Python
Scala
Java
R
In PySpark we provide Spark SQL data source API for loading LIBSVM data as a DataFrame.

>>> df = spark.read.format("libsvm").option("numFeatures", "780").load("data/mllib/sample_libsvm_data.txt")
>>> df.show(10)
+-----+--------------------+
|label|            features|
+-----+--------------------+
|  0.0|(780,[127,128,129...|
|  1.0|(780,[158,159,160...|
|  1.0|(780,[124,125,126...|
|  1.0|(780,[152,153,154...|
|  1.0|(780,[151,152,153...|
|  0.0|(780,[129,130,131...|
|  1.0|(780,[158,159,160...|
|  1.0|(780,[99,100,101,...|
|  0.0|(780,[154,155,156...|
|  0.0|(780,[127,128,129...|
+-----+--------------------+
only showing top 10 rows

ySpark Overview
Date: May 19, 2025 Version: 4.0.0

Useful links: Live Notebook | GitHub | Issues | Examples | Community | Stack Overflow | Dev Mailing List | User Mailing List

PySpark is the Python API for Apache Spark. It enables you to perform real-time, large-scale data processing in a distributed environment using Python. It also provides a PySpark shell for interactively analyzing your data.

PySpark combines Python’s learnability and ease of use with the power of Apache Spark to enable processing and analysis of data at any size for everyone familiar with Python.

PySpark supports all of Spark’s features such as Spark SQL, DataFrames, Structured Streaming, Machine Learning (MLlib) and Spark Core.

Python Spark Connect Client	
Spark SQL	Pandas API on Spark	Streaming	Machine Learning	
Spark Core and RDDs	
Python Spark Connect Client

Spark Connect is a client-server architecture within Apache Spark that enables remote connectivity to Spark clusters from any application. PySpark provides the client for the Spark Connect server, allowing Spark to be used as a service.

Quickstart: Spark Connect

Live Notebook: Spark Connect

Spark Connect Overview

Spark SQL and DataFrames

Spark SQL is Apache Spark’s module for working with structured data. It allows you to seamlessly mix SQL queries with Spark programs. With PySpark DataFrames you can efficiently read, write, transform, and analyze data using Python and SQL. Whether you use Python or SQL, the same underlying execution engine is used so you will always leverage the full power of Spark.

Quickstart: DataFrame

Live Notebook: DataFrame

Spark SQL API Reference

Pandas API on Spark

Pandas API on Spark allows you to scale your pandas workload to any size by running it distributed across multiple nodes. If you are already familiar with pandas and want to leverage Spark for big data, pandas API on Spark makes you immediately productive and lets you migrate your applications without modifying the code. You can have a single codebase that works both with pandas (tests, smaller datasets) and with Spark (production, distributed datasets) and you can switch between the pandas API and the Pandas API on Spark easily and without overhead.

Pandas API on Spark aims to make the transition from pandas to Spark easy but if you are new to Spark or deciding which API to use, we recommend using PySpark (see Spark SQL and DataFrames).

Quickstart: Pandas API on Spark

Live Notebook: pandas API on Spark

Pandas API on Spark Reference

Structured Streaming

Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. You can express your streaming computation the same way you would express a batch computation on static data. The Spark SQL engine will take care of running it incrementally and continuously and updating the final result as streaming data continues to arrive.

Structured Streaming Programming Guide

Structured Streaming API Reference

Machine Learning (MLlib)

Built on top of Spark, MLlib is a scalable machine learning library that provides a uniform set of high-level APIs that help users create and tune practical machine learning pipelines.

Machine Learning Library (MLlib) Programming Guide

Machine Learning (MLlib) API Reference

Spark Core and RDDs

Spark Core is the underlying general execution engine for the Spark platform that all other functionality is built on top of. It provides RDDs (Resilient Distributed Datasets) and in-memory computing capabilities.

Note that the RDD API is a low-level API which can be difficult to use and you do not get the benefit of Spark’s automatic query optimization capabilities. We recommend using DataFrames (see Spark SQL and DataFrames above) instead of RDDs as it allows you to express what you want more easily and lets Spark automatically construct the most efficient query for you.

Spark Core API Reference

Spark Streaming (Legacy)

Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams.

Note that Spark Streaming is the previous generation of Spark’s streaming engine. It is a legacy project and it is no longer being updated. There is a newer and easier to use streaming engine in Spark called Structured Streaming which you should use for your streaming applications and pipelines.

Spark Streaming Programming Guide (Legacy)

Spark Streaming API Reference (Legacy)

Cluster Mode Overview
This document gives a short overview of how Spark runs on clusters, to make it easier to understand the components involved. Read through the application submission guide to learn about launching applications on a cluster.

Components
Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program).

Specifically, to run on a cluster, the SparkContext can connect to several types of cluster managers (either Spark’s own standalone cluster manager, YARN or Kubernetes), which allocate resources across applications. Once connected, Spark acquires executors on nodes in the cluster, which are processes that run computations and store data for your application. Next, it sends your application code (defined by JAR or Python files passed to SparkContext) to the executors. Finally, SparkContext sends tasks to the executors to run.

Spark cluster components

There are several useful things to note about this architecture:

Each application gets its own executor processes, which stay up for the duration of the whole application and run tasks in multiple threads. This has the benefit of isolating applications from each other, on both the scheduling side (each driver schedules its own tasks) and executor side (tasks from different applications run in different JVMs). However, it also means that data cannot be shared across different Spark applications (instances of SparkContext) without writing it to an external storage system.
Spark is agnostic to the underlying cluster manager. As long as it can acquire executor processes, and these communicate with each other, it is relatively easy to run it even on a cluster manager that also supports other applications (e.g. YARN/Kubernetes).
The driver program must listen for and accept incoming connections from its executors throughout its lifetime (e.g., see spark.driver.port in the network config section). As such, the driver program must be network addressable from the worker nodes.
Because the driver schedules tasks on the cluster, it should be run close to the worker nodes, preferably on the same local area network. If you’d like to send requests to the cluster remotely, it’s better to open an RPC to the driver and have it submit operations from nearby than to run a driver far away from the worker nodes.
Cluster Manager Types
The system currently supports several cluster managers:

Standalone – a simple cluster manager included with Spark that makes it easy to set up a cluster.
Hadoop YARN – the resource manager in Hadoop 3.
Kubernetes – an open-source system for automating deployment, scaling, and management of containerized applications.
Submitting Applications
Applications can be submitted to a cluster of any type using the spark-submit script. The application submission guide describes how to do this.

Monitoring
Each driver program has a web UI, typically on port 4040, that displays information about running tasks, executors, and storage usage. Simply go to http://<driver-node>:4040 in a web browser to access this UI. The monitoring guide also describes other monitoring options.

Job Scheduling
Spark gives control over resource allocation both across applications (at the level of the cluster manager) and within applications (if multiple computations are happening on the same SparkContext). The job scheduling overview describes this in more detail.

Glossary
The following table summarizes terms you’ll see used to refer to cluster concepts:

Term	Meaning
Application	User program built on Spark. Consists of a driver program and executors on the cluster.
Application jar	A jar containing the user's Spark application. In some cases users will want to create an "uber jar" containing their application along with its dependencies. The user's jar should never include Hadoop or Spark libraries, however, these will be added at runtime.
Driver program	The process running the main() function of the application and creating the SparkContext
Cluster manager	An external service for acquiring resources on the cluster (e.g. standalone manager, YARN, Kubernetes)
Deploy mode	Distinguishes where the driver process runs. In "cluster" mode, the framework launches the driver inside of the cluster. In "client" mode, the submitter launches the driver outside of the cluster.
Worker node	Any node that can run application code in the cluster
Executor	A process launched for an application on a worker node, that runs tasks and keeps data in memory or disk storage across them. Each application has its own executors.
Task	A unit of work that will be sent to one executor
Job	A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action (e.g. save, collect); you'll see this term used in the driver's logs.
Stage	Each job gets divided into smaller sets of tasks called stages that depend on each other (similar to the map and reduce stages in MapReduce); you'll see this term used in the driver's logs.