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

### Overview
The tool consists of a class file CompareTwoDatasets.py and main.py which runs the tool.
The main.py file consists of 10 samples dataframe pairs which allows to perform 10 different comparison scenarios.
All necesary libraries for running this project are stored in requirements.txt. 
The Python version on which this tool was developed was Python 3.9.12.

The CompareTwoDataframes tool allows to compare two dataframes and produce summary of:
- Schema differences
- Row count differences
- Value mismatches
as requested in assesment goals.

The tool is limited, performs only simple comparison on the data. To reach production grade stage additional development and optimisation needs to be performed. It lacks functionalities like sampling for big datasets, handling null and nan values, detailed value comparison e.g. floating-point tolerance or datetime formats. 

To run the tool: clone the repo -> run virtualenvironment with python 3.9.12 -> install reuirements from requirements.txt file -> run main.py

In main.py there are 10 case scenarios to run tool against. 

example:
```
from CompareTwoDatasets import CompareTwoDatasets

#initialization of the object CompareTwoDatasets
compare = CompareTwoDatasets(df1, df2, legacy_key="account_id", cloud_key="account_num") 
#or compare = CompareTwoDatasets(df1, df2, join_columns=["account_id", "account_num"]) 
#or compare = CompareTwoDatasets(df1, df2, join_columns="account_id") 

#running comparison
compare.compare()

#prints report of the comparison
print(compare.report())
```

The tool have been created in python using pandas as initial version as i know this stack better than i do know PySpark. After creating logic and structure in pandas i wanted to rewrite it using PySpark. Unfortunatelly due to limited time i have to pospone rewriting this tool for later.

Creating this code took approximately 3 MD of working afterhours.

P.S.: Sorry for not including comments in the code. I hope it wouldn't make it much harder to get throught. Will add them for the tech-talk.

