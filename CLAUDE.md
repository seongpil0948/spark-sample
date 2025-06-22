# CLAUDE.md

이 파일은 Claude Code (claude.ai/code)가 이 저장소의 코드를 작업할 때 필요한 가이드를 제공합니다.

## 프로젝트 개요

**Python 3.12 기반 Apache Spark 4.0 프로덕션 애플리케이션**으로, AWS S3와 완전 통합된 ETL 파이프라인, 스트리밍 처리, 머신러닝 워크플로우를 제공합니다. OpenTelemetry를 통한 포괄적인 텔레메트리를 갖춘 엔터프라이즈급 데이터 처리 솔루션입니다.

### 핵심 기술 스택
- **Apache Spark 4.0**: 차세대 통합 분석 엔진
- **Python 3.12**: 최신 Python 언어 기능 활용
- **AWS S3**: 기본 저장소 백엔드 (profile: toy-root)
- **Delta Lake 4.0**: ACID 트랜잭션 지원 데이터 레이크
- **Apache Kafka**: 실시간 스트리밍 데이터 파이프라인
- **Docker Compose**: 통합 개발 환경

## 빠른 시작

### 환경 설정
```bash
# uv 설치 (권장 패키지 매니저)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 가상환경 생성 및 의존성 설치
uv sync --dev
uv run pre-commit install

# AWS Profile 설정
export AWS_PROFILE=toy-root
```

### Spark 애플리케이션 실행

#### 로컬 실행 (S3 사용)
```bash
# ETL 파이프라인 - S3 기본 경로 사용
./scripts/run_etl.sh 2024-01-01

# 머신러닝 파이프라인
./scripts/run_ml.sh

# 스트리밍 파이프라인
./scripts/run_streaming.sh

# 샘플 데이터를 S3에 생성
python scripts/generate_sample_data.py --s3
```

#### Docker 환경
```bash
# 통합 Spark 클러스터 시작
./scripts/start-full-stack.sh

# 또는 수동 시작
docker-compose up -d

# Docker 내에서 실행
docker exec spark-master /app/scripts/run_etl.sh 2024-01-01
```

#### uv를 통한 실행
```bash
uv run spark-app --mode etl --date 2024-01-01
uv run spark-app --mode streaming
uv run spark-app --mode ml
```

#### Make 타겟
```bash
make run-etl DATE=2024-01-01
make run-streaming
make run-ml
make docker-up
make docker-full-up
```

### 서비스 접근
- **Spark Master UI**: http://localhost:8080
- **Spark Worker 1 UI**: http://localhost:8081
- **Spark Worker 2 UI**: http://localhost:8082
- **Spark History Server**: http://localhost:18080
- **Jupyter Lab**: http://localhost:8888
- **Kafka**: localhost:9092

### 코드 품질 및 테스트
```bash
# 코드 포맷팅 (Ruff)
uv run ruff format src/ tests/
make format

# 린팅
uv run ruff check src/ tests/
make lint

# 타입 체킹 (mypy)
uv run mypy src/
make type-check

# 테스트 실행
uv run pytest tests/
make test

# 전체 품질 검사
make check  # lint + type-check
```

## 아키텍처 가이드라인

### PySpark 애플리케이션 개발 원칙

1. **데이터 처리 파이프라인 구조**
   - 데이터 읽기/수집
   - 변환 로직
   - 데이터 쓰기/출력의 명확한 분리

2. **구성 관리**
   - Spark 설정과 작업 매개변수를 별도 구성 파일에 저장
   - 하드코딩 방지

3. **테스트 전략**
   - PySpark 로컬 모드를 사용한 변환 함수 단위 테스트
   - 엔드투엔드 데이터 파이프라인 통합 테스트

4. **성능 고려사항**
   - RDD API보다 DataFrame API 우선 사용
   - Spark Catalyst 옵티마이저 활용
   - 데이터 파티셔닝 및 셔플링 작업 최적화

## 개발 노트

- **Python 3.12**: 최신 언어 기능 및 성능 개선 활용
- **AWS S3 통합**: profile 기반 인증 (toy-root) 및 자동 설정
- **Docker Compose**: 통합 개발 환경 (Spark + Kafka + Jupyter)
- **현대적인 Python 도구**: uv를 통한 빠른 패키지 관리
- **코드 품질**: Ruff를 통한 린팅 및 포맷팅 강제
- **Pre-commit 훅**: 자동화된 품질 검사
- **기본 S3 경로**:
  - 입력: `s3://theshop-lake-dev/spark/input/`
  - 출력: `s3://theshop-lake-dev/spark/output/`

## 주요 파일 및 디렉토리

### 애플리케이션 코어
- `src/app.py` - CLI 인터페이스를 갖춘 메인 애플리케이션 진입점
- `src/etl/` - 데이터 품질 검사를 포함한 ETL 파이프라인 모듈
- `src/streaming/` - Kafka 스트리밍 프로세서
- `src/ml/` - 머신러닝 파이프라인
- `src/utils/spark_config.py` - Spark 설정 및 S3 통합

### 설정 및 인프라
- `docker-compose.yml` - Kafka와 Jupyter를 포함한 Spark 클러스터
- `scripts/start-full-stack.sh` - 원클릭 시작 스크립트
- `pyproject.toml` - 프로젝트 메타데이터 및 의존성 (uv/ruff 설정)
- `Dockerfile` - Python 3.12 및 Spark 4.0 프로덕션 이미지
- `.env.example` - 환경변수 템플릿

### 개발 도구
- `Makefile` - 공통 개발 작업
- `notebooks/` - Jupyter 예제 및 가이드
  - `quick_start.ipynb` - S3 빠른 시작 가이드
  - `s3_example.ipynb` - 상세한 S3 데이터 분석 예제
  - `README.md` - Jupyter 사용 가이드

## AWS S3 통합

### 설정
프로젝트는 AWS profile `toy-root`를 기본값으로 사용하여 S3와 자동 통합됩니다:

```bash
# AWS Profile 확인
aws sts get-caller-identity --profile toy-root

# S3 접근 테스트
aws s3 ls s3://theshop-lake-dev/ --profile toy-root
```

### S3 경로 구조
```
s3://theshop-lake-dev/spark/
├── input/
│   ├── date=2024-01-01/
│   ├── date=2024-01-02/
│   └── training/
│       ├── train/
│       └── test/
├── output/
│   ├── etl_results/
│   ├── user_summary/
│   ├── models/
│   └── streaming/
└── checkpoint/
    └── streaming/
```

### Jupyter에서 S3 사용
```python
# 자동 S3 설정으로 SparkSession 생성
from src.utils.spark_config import create_spark_session, get_spark_config
config = get_spark_config("batch")
spark = create_spark_session("JupyterS3", config)

# S3 데이터 읽기
df = spark.read.parquet("s3://theshop-lake-dev/spark/input/date=2024-01-01/")

# S3에 결과 저장
df.write.mode("overwrite").parquet("s3://theshop-lake-dev/spark/output/results/")
```

## 모니터링 및 문제 해결

### 서비스 상태 확인
```bash
# 모든 서비스 상태
docker-compose ps

# Kafka 토픽 확인
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Spark 애플리케이션 확인
curl http://localhost:8080/api/v1/applications
```

### Docker Compose 명령어
```bash
# Kafka와 Jupyter를 포함한 Spark 클러스터 시작
docker-compose up -d

# 모든 서비스 중지
docker-compose down -v

# 편의 스크립트로 시작
./scripts/start-full-stack.sh
```

### 문제 해결

#### S3 접근 오류
```bash
# AWS 자격증명 확인
aws sts get-caller-identity --profile toy-root

# Docker 컨테이너 내에서 AWS 설정 확인
docker exec spark-master env | grep AWS
```

#### Spark 성능 문제
```python
# 적응형 쿼리 실행 활성화 (기본값)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# 파티션 수 조정
df.repartition(200)

# 캐싱 활용
df.cache()
```

## 사용 가능한 Make 타겟

```bash
make help           # 모든 사용 가능한 타겟 표시
make install        # 프로덕션 의존성 설치
make install-dev    # 개발 의존성 포함 설치
make format         # Ruff로 코드 포맷팅
make lint           # 코드 린팅
make type-check     # 타입 검사
make test           # 테스트 실행
make check          # 모든 품질 검사 실행
make run-etl        # ETL 파이프라인 실행
make run-streaming  # 스트리밍 파이프라인 실행
make run-ml         # ML 파이프라인 실행
make docker-up      # Spark 클러스터 시작
make docker-full-up # 전체 스택 시작 (Spark + Kafka + Jupyter)
```

---

## Apache Spark 4.0 심화 가이드

### 핵심 아키텍처 개선사항

**Spark 4.0**은 성능과 사용성을 크게 향상시킨 차세대 분석 엔진입니다:

- **향상된 AQE (Adaptive Query Execution)**: 런타임 최적화 자동 적용
- **개선된 Catalyst 옵티마이저**: 지능적인 쿼리 계획 최적화
- **Python 3.12 지원**: 최신 Python 기능 및 성능 개선
- **Delta Lake 4.0 통합**: ACID 트랜잭션 및 시간 여행 기능

### S3 최적화 설정

프로젝트는 S3 성능을 위해 다음 설정을 자동 적용합니다:

```python
# S3A 파일시스템 최적화
"spark.hadoop.fs.s3a.fast.upload": "true"
"spark.hadoop.fs.s3a.multipart.size": "104857600"  # 100MB
"spark.hadoop.fs.s3a.connection.maximum": "100"
"spark.hadoop.fs.s3a.retry.limit": "7"
"spark.hadoop.fs.s3a.retry.interval": "500ms"

# 커밋 프로토콜 최적화
"spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"
"spark.speculation": "false"  # S3 쓰기 시 비활성화
```

### 성능 튜닝 팁

#### 메모리 최적화
```python
# 실행자 메모리 구성 권장사항
# - spark.memory.fraction: 0.8 (실행 + 저장용)
# - spark.memory.storageFraction: 0.3 (저장용 비율)
# - 실행자당 메모리: 40GB 이하 (GC 오버헤드 방지)

spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")
```

#### 파티션 전략
```python
# 읽기 최적화
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")

# 쓰기 최적화
df.coalesce(10).write.parquet(path)

# 적응형 파티션 병합 (자동 활성화)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

#### 브로드캐스트 조인
```python
# 자동 브로드캐스트 임계값
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# 명시적 브로드캐스트
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```

### Delta Lake 4.0 활용

```python
from delta.tables import DeltaTable

# Delta 테이블 생성
df.write.format("delta").mode("overwrite").save("s3://bucket/delta-table/")

# ACID 트랜잭션 (MERGE 연산)
deltaTable = DeltaTable.forPath(spark, "s3://bucket/delta-table/")

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

# 시간 여행 (Time Travel)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
df_yesterday = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)
```

### Structured Streaming 고급 패턴

```python
# Exactly-once 처리를 위한 Kafka 통합
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 워터마크를 사용한 늦은 데이터 처리
windowed_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes")) \
    .count()

# Delta Lake로 스트리밍 결과 저장
query = windowed_df.writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoint/") \
    .trigger(processingTime="30 seconds") \
    .start("s3://bucket/streaming-output/")
```

### 머신러닝 파이프라인

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier

# ML 파이프라인 구성
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
rf = RandomForestClassifier(featuresCol="scaled_features", labelCol="label")

pipeline = Pipeline(stages=[assembler, scaler, rf])
model = pipeline.fit(train_df)

# 모델 저장 (S3)
model.write().overwrite().save("s3://bucket/models/rf_model/")

# 실시간 예측
predictions = model.transform(streaming_df)
```

### 프로덕션 배포 고려사항

#### Kubernetes 배포
```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-etl-job
spec:
  type: Python
  mode: cluster
  image: "spark-production-app:latest"
  imagePullPolicy: Always
  mainApplicationFile: "local:///app/src/app.py"
  arguments:
    - "--mode"
    - "etl"
    - "--date"
    - "2024-01-01"
  sparkVersion: "4.0.0"
  driver:
    cores: 2
    memory: "4g"
    serviceAccount: spark
  executor:
    cores: 4
    instances: 10
    memory: "8g"
```

#### 보안 설정
```python
# Kerberos 인증 (YARN 환경)
spark.conf.set("spark.security.credentials.hive.enabled", "true")
spark.conf.set("spark.yarn.keytab", "/path/to/keytab")
spark.conf.set("spark.yarn.principal", "user@DOMAIN.COM")

# SSL/TLS 설정
spark.conf.set("spark.ssl.enabled", "true")
spark.conf.set("spark.ssl.keyStore", "/path/to/keystore")
```

#### 모니터링 통합
```python
# 커스텀 메트릭 수집
from pyspark.util import AccumulatorParam

class MetricsAccumulator(AccumulatorParam):
    def zero(self, value):
        return {}
    
    def addInPlace(self, acc1, acc2):
        for k, v in acc2.items():
            acc1[k] = acc1.get(k, 0) + v
        return acc1

metrics = spark.sparkContext.accumulator({}, MetricsAccumulator())

# 애플리케이션에서 메트릭 수집
def process_partition(iterator):
    count = 0
    for record in iterator:
        count += 1
        # 처리 로직
    metrics.add({"processed_records": count})
    
df.foreachPartition(process_partition)
```

이 가이드는 Python 3.12와 Spark 4.0을 활용한 엔터프라이즈급 데이터 처리 솔루션 구축을 위한 모든 필수 정보를 제공합니다. 프로덕션 환경에서의 실제 구현을 위한 고급 패턴과 최적화 기법에 중점을 두었습니다.