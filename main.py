from CompareTwoDatasets import CompareTwoDatasets
import pandas as pd
import numpy as np


# Test Case 1: Identical DataFrames (Perfect Match)
df1_identical = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'salary': [50000, 60000, 75000, 80000, 55000],
    'department': ['HR', 'IT', 'Finance', 'IT', 'HR'],
    'hire_date': pd.to_datetime(['2020-01-15', '2019-03-22', '2021-07-08', '2018-11-30', '2022-02-14'])
})

df2_identical = df1_identical.copy()

# Test Case 2: Schema Differences (Different Column Names)
df1_schema_diff = pd.DataFrame({
    'employee_id': [1, 2, 3, 4, 5],
    'full_name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Adams'],
    'annual_salary': [50000, 60000, 75000, 80000, 55000],
    'dept': ['HR', 'IT', 'Finance', 'IT', 'HR']
})

df2_schema_diff = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Adams'],
    'salary': [50000, 60000, 75000, 80000, 55000],
    'department': ['HR', 'IT', 'Finance', 'IT', 'HR']
})

# Test Case 3: Schema Differences (Different Data Types)
df1_dtype_diff = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'score': [85.5, 92.3, 78.7, 88.1, 95.2],  # float
    'active': [True, False, True, True, False],  # boolean
    'category': ['A', 'B', 'A', 'C', 'B']
})

df2_dtype_diff = pd.DataFrame({
    'id': ['1', '2', '3', '4', '5'],  # string instead of int
    # 'id': [1, 2, 3, 4, 5],
    'score': [85, 92, 78, 88, 95],  # int instead of float
    'active': ['Y', 'N', 'Y', 'Y', 'N'],  # string instead of boolean
    'category': ['A', 'B', 'A', 'C', 'B']
})

# Test Case 4: Row Count Differences
df1_row_count = pd.DataFrame({
    'product_id': [101, 102, 103, 104, 105],
    'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Webcam'],
    'price': [999.99, 25.99, 79.99, 299.99, 89.99],
    'in_stock': [True, True, False, True, True]
})

df2_row_count = pd.DataFrame({
    'product_id': [101, 102, 103],  # Missing 2 rows
    'product_name': ['Laptop', 'Mouse', 'Keyboard'],
    'price': [999.99, 25.99, 79.99],
    'in_stock': [True, True, False]
})

# Test Case 5: Value Mismatches (Same schema, different values)
df1_value_diff = pd.DataFrame({
    'order_id': [1001, 1002, 1003, 1004, 1005],
    'customer_id': [501, 502, 503, 504, 505],
    'order_total': [150.75, 89.50, 245.00, 67.25, 189.99],
    'status': ['shipped', 'pending', 'delivered', 'cancelled', 'shipped']
})

df2_value_diff = pd.DataFrame({
    'order_id': [1001, 1002, 1003, 1004, 1005],
    'customer_id': [501, 502, 503, 504, 505],
    'order_total': [150.75, 89.50, 250.00, 67.25, 189.99],  # 1003 has different value
    'status': ['shipped', 'pending', 'delivered', 'processing', 'shipped']  # 1004 has different status
})

# Test Case 6: Missing Values vs Non-missing Values
df1_nulls = pd.DataFrame({
    'user_id': [1, 2, 3, 4, 5],
    'email': ['alice@email.com', 'bob@email.com', None, 'diana@email.com', 'eve@email.com'],
    'phone': ['555-0101', None, '555-0103', '555-0104', None],
    'age': [25, 30, 35, None, 28]
})

df2_nulls = pd.DataFrame({
    'user_id': [1, 2, 3, 4, 5],
    'email': ['alice@email.com', 'bob@email.com', 'charlie@email.com', 'diana@email.com', 'eve@email.com'],
    'phone': ['555-0101', '555-0102', '555-0103', '555-0104', '555-0105'],
    'age': [25, 30, 35, 40, 28]
})

# Test Case 7: Duplicate Rows
df1_duplicates = pd.DataFrame({
    'transaction_id': [1, 2, 3, 4, 5, 6],
    'amount': [100.0, 250.0, 75.0, 100.0, 325.0, 150.0],
    'merchant': ['Store A', 'Store B', 'Store C', 'Store A', 'Store D', 'Store E'],
    'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-01', '2023-01-05', '2023-01-06'])
})

df2_duplicates = pd.DataFrame({
    'transaction_id': [1, 2, 3, 4, 5, 6, 4],  # Duplicate transaction_id 4
    'amount': [100.0, 250.0, 75.0, 100.0, 325.0, 150.0, 100.0],
    'merchant': ['Store A', 'Store B', 'Store C', 'Store A', 'Store D', 'Store E', 'Store A'],
    'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-01', '2023-01-05', '2023-01-06', '2023-01-01'])
})

# Test Case 8: Extra Columns
df1_extra_cols = pd.DataFrame({
    'student_id': [1001, 1002, 1003, 1004],
    'name': ['John Doe', 'Jane Smith', 'Mike Johnson', 'Sarah Wilson'],
    'grade': ['A', 'B+', 'A-', 'B']
})

df2_extra_cols = pd.DataFrame({
    'student_id': [1001, 1002, 1003, 1004],
    'name': ['John Doe', 'Jane Smith', 'Mike Johnson', 'Sarah Wilson'],
    'grade': ['A', 'B+', 'A-', 'B'],
    'attendance': [95, 87, 92, 89],  # Extra column
    'final_score': [91.5, 83.2, 89.7, 85.1]  # Extra column
})

# Test Case 9: Different Row Order (Same Data, Different Sequence)
df1_order = pd.DataFrame({
    'region': ['North', 'South', 'East', 'West', 'Central'],
    'sales': [1200000, 980000, 1450000, 1100000, 750000],
    'quarter': ['Q1', 'Q1', 'Q1', 'Q1', 'Q1']
})

df2_order = pd.DataFrame({
    'region': ['West', 'Central', 'North', 'East', 'South'],  # Different order
    'sales': [1100000, 750000, 1200000, 1450000, 980000],    # Corresponding reorder
    'quarter': ['Q1', 'Q1', 'Q1', 'Q1', 'Q1']
})

# Test Case 10: Complex Mixed Differences (Multiple issues)
df1_complex = pd.DataFrame({
    'account_id': [2001, 2002, 2003, 2004, 2005, 2006],
    'balance': [1500.50, 2300.75, 890.25, 4200.00, 156.80, 3750.90],
    'account_type': ['Checking', 'Savings', 'Checking', 'Investment', 'Checking', 'Savings'],
    'last_transaction': pd.to_datetime(['2023-06-15', '2023-06-14', '2023-06-13', '2023-06-12', '2023-06-11', '2023-06-10']),
    'is_active': [True, True, False, True, True, True]
})

df2_complex = pd.DataFrame({
    'account_num': [2001, 2002, 2003, 2004, 2007],  # Different column name + different/missing IDs
    'balance': [1500.50, 2350.75, 890.25, 4200.00, 890.45],  # Value differences
    'type': ['Checking', 'Savings', 'Checking', 'Investment', 'Checking'],  # Different column name
    'last_transaction': ['2023-06-15', '2023-06-14', '2023-06-13', '2023-06-12', '2023-06-09'],  # String instead of datetime
    'status': ['Active', 'Active', 'Inactive', 'Active', 'Active'],  # Different column name + different values
    'credit_limit': [5000, 0, 1000, 0, 2500]  # Extra column
})

# compare = CompareTwoDatasets(df1_identical, df2_identical, legacy_key="id", cloud_key="id") # checked
# compare = CompareTwoDatasets(df1_identical, df2_identical, join_columns="id") # checked
# compare = CompareTwoDatasets(df1_schema_diff, df2_schema_diff, legacy_key="employee_id", cloud_key="id") # checked
# compare = CompareTwoDatasets(df1_dtype_diff, df2_dtype_diff, legacy_key="id", cloud_key="id") # checked
# compare = CompareTwoDatasets(df2_row_count, df1_row_count, legacy_key="product_id", cloud_key="product_id") # checked
# compare = CompareTwoDatasets(df1_value_diff, df2_value_diff, legacy_key="order_id", cloud_key="order_id")  # checked
# compare = CompareTwoDatasets(df1_nulls, df2_nulls, legacy_key="user_id", cloud_key="user_id") # add checking for None and NaN
compare = CompareTwoDatasets(df1_duplicates, df2_duplicates, legacy_key="transaction_id", cloud_key="transaction_id") # checked
# compare = CompareTwoDatasets(df1_extra_cols, df2_extra_cols, legacy_key="student_id", cloud_key="student_id") # checked
# compare = CompareTwoDatasets(df1_order, df2_order, legacy_key="region", cloud_key="region") # checked
# compare = CompareTwoDatasets(df1_complex, df2_complex, legacy_key="account_id", cloud_key="account_num") # checked
compare.compare()

print(compare.report())

