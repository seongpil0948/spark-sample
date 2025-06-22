"""
S3 Utility Functions for Jupyter Notebooks
S3 경로 변환 및 유틸리티 함수들
"""

import os
import re
from typing import Optional


def s3_to_s3a(path: str) -> str:
    """
    s3:// 경로를 s3a:// 경로로 변환
    Spark는 s3a 프로토콜을 사용해야 함
    
    Args:
        path: S3 경로 (s3:// 또는 s3a://)
    
    Returns:
        s3a:// 형식의 경로
    """
    if path.startswith("s3://"):
        return path.replace("s3://", "s3a://", 1)
    return path


def get_s3_input_path(date: Optional[str] = None) -> str:
    """
    S3 입력 경로 생성
    
    Args:
        date: 날짜 (YYYY-MM-DD 형식), None이면 모든 날짜
    
    Returns:
        S3A 입력 경로
    """
    base_path = "s3a://theshop-lake-dev/spark/input/"
    
    if date:
        return f"{base_path}date={date}/"
    else:
        return f"{base_path}date=*/"


def get_s3_output_path(subpath: str = "") -> str:
    """
    S3 출력 경로 생성
    
    Args:
        subpath: 서브 경로 (예: "user_summary", "test")
    
    Returns:
        S3A 출력 경로
    """
    base_path = "s3a://theshop-lake-dev/spark/output/"
    
    if subpath:
        return f"{base_path}{subpath}/"
    else:
        return base_path


def get_s3_checkpoint_path(app_name: str = "default") -> str:
    """
    S3 체크포인트 경로 생성
    
    Args:
        app_name: 애플리케이션 이름
    
    Returns:
        S3A 체크포인트 경로
    """
    return f"s3a://theshop-lake-dev/spark/checkpoint/{app_name}/"


def validate_s3_path(path: str) -> bool:
    """
    S3 경로 유효성 검사
    
    Args:
        path: S3 경로
    
    Returns:
        유효한 경로인지 여부
    """
    # s3:// 또는 s3a:// 로 시작하는지 확인
    if not (path.startswith("s3://") or path.startswith("s3a://")):
        return False
    
    # 버킷명과 키가 있는지 확인
    pattern = r"^s3a?://[a-z0-9][a-z0-9\-]*[a-z0-9]/.*"
    return bool(re.match(pattern, path))


def print_s3_paths():
    """
    자주 사용하는 S3 경로들을 출력
    """
    print("🗂️  주요 S3 경로들:")
    print(f"   📥 입력: {get_s3_input_path()}")
    print(f"   📤 출력: {get_s3_output_path()}")
    print(f"   💾 체크포인트: {get_s3_checkpoint_path()}")
    print()
    print("📅 날짜별 입력 경로 예시:")
    print(f"   {get_s3_input_path('2024-01-01')}")
    print(f"   {get_s3_input_path('2024-01-02')}")
    print()
    print("📂 출력 경로 예시:")
    print(f"   {get_s3_output_path('user_summary')}")
    print(f"   {get_s3_output_path('models')}")
    print(f"   {get_s3_output_path('test')}")


def setup_spark_for_s3():
    """
    Spark S3 설정을 위한 코드 예시 출력
    """
    print("⚙️  Spark S3 설정 코드:")
    print("""
from src.utils.spark_config import create_spark_session

# S3 최적화된 Spark 세션 생성
spark = create_spark_session("S3-Example")

# S3 경로 사용 예시
input_path = get_s3_input_path("2024-01-01")
output_path = get_s3_output_path("test")

# 데이터 읽기
df = spark.read.parquet(input_path)

# 데이터 쓰기
df.write.mode("overwrite").parquet(output_path)
""")


if __name__ == "__main__":
    print_s3_paths()
    setup_spark_for_s3()