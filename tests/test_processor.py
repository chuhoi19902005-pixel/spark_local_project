# =================================================================
# 1. 優先導入系統標準庫（必須放在最前面）
# =================================================================
import os
import sys
import platform  # 導入系統判斷模組
# =================================================================
# 2. 【終極環境淨化器】（必須在 import pytest 與 pyspark 之前執行）
# =================================================================
# 僅在偵測到 Windows 系統時，才執行 Windows 專屬的環境淨化
if platform.system() == "Windows":
    spark_temp = "C:\\SparkTemp"
    os.makedirs(spark_temp, exist_ok=True)
# 解決 Windows 使用者路徑 & 符號與暫存問題
    os.environ["TEMP"] = spark_temp
    os.environ["TMP"] = spark_temp
    os.environ["USERPROFILE"] = spark_temp
    os.environ["HOMEPATH"] = "\\SparkTemp"
    os.environ["HOMEDRIVE"] = "C:"
    os.environ["APPDATA"] = os.path.join(spark_temp, "AppData", "Roaming")
    os.environ["LOCALAPPDATA"] = os.path.join(spark_temp, "AppData", "Local")

# 強制 Spark Worker 使用虛擬環境的 Python，避免 Socket 超時
# 不論是 Windows 還是 Linux，都強制指定 Python 執行路徑
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
# =================================================================

# =================================================================
# 3. 安全導入測試與 Spark 套件
# =================================================================
import pytest
from pyspark.sql import SparkSession
from src.data_processor import clean_user_data  # 導入您的數據清理函數
import logging

logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def spark_session():
    """建立本地測試用的 SparkSession"""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-local-testing") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_clean_user_data(spark_session):
    """測試數據清理邏輯是否正確"""
    # 建立測試模擬數據
    """schema = ["id", "name", "age"]
    input_data = [
        (1, "alice", 25),
        (2, "bob", None),       # 測試空值是否會補 0
        (3, "charlie", -5)      # 測試負數年齡是否會被過濾
    ]
    input_df = spark_session.createDataFrame(input_data, schema)"""
    input_df = spark_session.read \
        .option("multiLine", True) \
        .json("tests/data.json")

  #  print("input1: ")
  #  print(input_df)
  #  print("input2: ")
  #  input_df.show()
  #  print("input3: ")
  #  print(input_df.collect())
    # 執行您的清理函數
    output_df = clean_user_data(input_df)
  #  print("output1: ")
  #  print(output_df)
  #  print("output2: ")
  #  output_df.show()
  #  print(output_df.collect())
  #  print("output3: ")
    results = output_df.collect()
  #  print(results)

    # ---------------- 驗證斷言 (Assertions) ----------------
    # 1. 驗證負數是否被過濾（預期只剩下 2 筆數據）
    assert len(results) == 2

    # 2. 驗證 Bob 的年齡是否被正確補 0
    bob_record = next(row for row in results if row.id == 2)
    assert bob_record.age == 0

    # 3. 驗證 Alice 的名字是否被正確轉為大寫
    alice_record = next(row for row in results if row.id == 1)
    assert alice_record.name == "ALICE"

"""@pytest.mark.parametrize("json_file, expected_count", [
      ("users_normal.json", 2),
      ("users_empty.json", 0),
      ("users_dirty.json", 5)
])
def test_clean_user_data(spark_session, json_file, expected_count):
      df = spark_session.read.json(f"data/{json_file}")
      result_df = clean_user_data(df)
      assert result_df.count() == expected_count"""


from pyspark.sql.types import StructType, StructField, StringType, LongType

expected_schema = StructType([
    StructField("age", LongType(), True),
    StructField("id", LongType(), True),
    StructField("name", StringType(), True)
])

# 測試 PyCharm GUI 推送
def test_schema(spark_session):
    df = spark_session.read \
        .option("multiLine", True) \
        .json("tests/data.json")
    # 驗證資料表的結構是否與預期完全一致
    logger.info("Staring to check data schema")
    assert df.schema == expected_schema
    logger.info("Completed checking data schema")
