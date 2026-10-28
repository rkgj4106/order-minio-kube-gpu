import pyarrow.parquet as pq

# 다운로드한 실제 파일명으로 변경
table = pq.ParquetFile("orders_152432_0e1b2e6d11f94aeea497474d5dabeae0.parquet").read()

print("레코드 수:", table.num_rows)
print("칼럼 수:", table.num_columns)
print("칼럼명:", table.column_names)

for row in table.slice(0, 5).to_pylist():
    print(row)