# -Data-Comparison-Tool-at-SIX

Tool prepared for assesment review at SIX:
```
Assessment Overview

We are in the process of migrating from an on-premises data warehouse to a cloud-based one. As part of this migration, we need to ensure that data transformations and results in the new cloud environment match those from our legacy systems.

Your task is to build a tool that compares two datasets – typically two tables or dataframes – and identifies any discrepancies between them. These might include differences in row counts, values, schemas, or other relevant attributes.


Requirements

The tool should take two datasets as inputs (e.g. two Spark DataFrames or database tables) and produce a summary of:
Schema differences
Row count differences
Value mismatches
Any other insights you find meaningful
You can assume the datasets are already loaded and available in a usable format (e.g. DataFrames, CSVs, tables, etc.).
We recommend using PySpark, as it's representative of our current stack. However, feel free to use any language or tool you’re comfortable with (e.g. SQL, pandas, dbt, etc.) as long as you can explain your design decisions.
The tool should be reusable and reasonably structured (e.g. a script, class, notebook, or small package).
Bonus points for thoughtful logging, reporting, or testing, but not mandatory.
```

## The tool consists of a class file CompareTwoDatasets.py and main.py which runs the tool.
The main.py file consists of 10 samples of dataframe pairs which allows to perform 10 different comparison scenarios.
All necesary libraries for running this project are stored in requirements.txt. 
The Python version on which this tool was developed was Python 3.9.12.
