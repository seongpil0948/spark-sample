# Jupyter Notebooks for Spark S3 Examples

이 디렉토리에는 Jupyter Lab에서 Spark와 S3를 사용하는 예제 노트북들이 있습니다.

## 노트북 목록

### 📊 `s3_example.ipynb`
Spark와 S3를 사용한 데이터 분석의 전체 워크플로우를 보여주는 종합 예제입니다.

**주요 내용:**
- SparkSession 생성 및 S3 설정
- S3에서 데이터 읽기/쓰기
- 데이터 분석 및 변환
- SQL 쿼리 사용
- Pandas 연동 및 시각화
- 성능 최적화 팁

## 사용 방법

### 1. 환경 준비
```bash
# Docker Compose로 Jupyter 시작
docker-compose up -d jupyter

# 또는 전체 스택 시작
./scripts/start-full-stack.sh
```

### 2. Jupyter Lab 접속
브라우저에서 http://localhost:8888/lab 접속

### 3. S3 샘플 데이터 생성 (선택사항)
```bash
# S3에 샘플 데이터 업로드
python scripts/generate_sample_data.py --s3
```

### 4. 노트북 실행
- `notebooks/s3_example.ipynb` 파일 열기
- 셀을 순서대로 실행

## 필수 조건

### AWS 설정
1. **AWS Profile 설정**
   ```bash
   aws configure --profile toy-root
   ```

2. **환경변수 확인**
   ```bash
   echo $AWS_PROFILE  # toy-root
   ```

3. **S3 접근 권한**
   - 버킷: `theshop-lake-dev`
   - 읽기/쓰기 권한 필요

### Docker 환경변수
Docker Compose에서 자동으로 설정되는 환경변수들:
```yaml
environment:
  - AWS_PROFILE=toy-root
  - AWS_REGION=us-east-1
  - SPARK_MASTER=spark://spark-master:7077
```

## S3 경로 구조

**⚠️ 중요**: Jupyter와 Spark에서는 반드시 `s3a://` 프로토콜을 사용하세요 (`s3://` 아님)

```
s3a://theshop-lake-dev/
├── spark/
│   ├── input/
│   │   ├── date=2024-01-01/
│   │   ├── date=2024-01-02/
│   │   └── training/
│   │       ├── train/
│   │       └── test/
│   ├── output/
│   │   ├── user_summary/
│   │   ├── models/
│   │   └── streaming/
│   └── checkpoint/
│       └── streaming/
```

## 주요 기능

### 1. 데이터 읽기
```python
# 특정 날짜 데이터 (s3a:// 프로토콜 사용)
df = spark.read.parquet("s3a://theshop-lake-dev/spark/input/date=2024-01-01/")

# 여러 날짜 데이터
df = spark.read.parquet("s3a://theshop-lake-dev/spark/input/date=*/")

# 유틸리티 함수 사용 (권장)
from s3_utils import get_s3_input_path
df = spark.read.parquet(get_s3_input_path("2024-01-01"))
```

### 2. 데이터 분석
```python
# DataFrame API 사용
result = df.groupBy("event_type").count()

# SQL 사용
df.createOrReplaceTempView("events")
result = spark.sql("SELECT event_type, COUNT(*) FROM events GROUP BY event_type")
```

### 3. 데이터 저장
```python
# S3에 Parquet 저장 (s3a:// 프로토콜 사용)
df.write.mode("overwrite").parquet("s3a://theshop-lake-dev/spark/output/results/")

# 유틸리티 함수 사용 (권장)
from s3_utils import get_s3_output_path
df.write.mode("overwrite").parquet(get_s3_output_path("results"))
```

### 4. 시각화
```python
# Pandas로 변환 후 matplotlib 사용
pandas_df = spark_df.toPandas()
pandas_df.plot(kind='bar')
```

## 문제 해결

### 1. S3 접근 오류
```bash
# AWS 자격증명 확인
aws sts get-caller-identity --profile toy-root

# S3 접근 테스트
aws s3 ls s3://theshop-lake-dev/ --profile toy-root
```

### 2. Spark 연결 오류
- Spark Master UI 확인: http://localhost:8080
- Jupyter 로그 확인: `docker-compose logs jupyter`

### 3. 메모리 부족
```python
# 파티션 수 조정
df = df.repartition(200)

# 캐시 정리
spark.catalog.clearCache()
```

## 성능 팁

### 1. 파티션 최적화
```python
# 읽기 전 파티션 수 설정
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")

# 쓰기 시 파티션 조정
df.coalesce(1).write.parquet(path)
```

### 2. 캐싱 활용
```python
# 자주 사용하는 데이터 캐싱
df.cache()
df.count()  # 캐시 트리거
```

### 3. 컬럼 선택
```python
# 필요한 컬럼만 선택
df.select("user_id", "event_type", "amount")
```

## 추가 리소스

- [Spark SQL 가이드](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [S3A 설정 가이드](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)
- [PySpark API 문서](https://spark.apache.org/docs/latest/api/python/)