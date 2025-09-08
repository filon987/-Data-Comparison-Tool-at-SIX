import logging
import pandas as pd

from typing import List, Union, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')

class CompareTwoDatasets:
    """
    Tool that compare two datasets, created for migration purposes from legacy systems to cloud environments.

    Parameters:
    legacy_df: pd.DataFrame
        Dataframe from legacy system
    cloud_df: pd.DataFrame
        Dataframe from cloud system
    legacy_key: str (optional)
        Column or list of columns for joining dataframes on.
    cloud_key: str (optional)
        Column or list of columns for joining dataframes on.
    join_columns: list or str (optional)
        Column or list of columns for joining dataframes on.

    Either (legacy_key, cloud_key) or join_columns parameters required.

    TODO:
    -add parameters for names for legacy and cloud datasets for better end user readability
    """

    def __init__(self, legacy_df: pd.DataFrame, cloud_df: pd.DataFrame, legacy_key: Optional[str] = None, cloud_key: Optional[str] = None, join_columns: Optional[Union[list, str]] = None):
        if legacy_key is not None and cloud_key is not None:
            if not isinstance(legacy_key, str) or not isinstance(cloud_key, str):
                raise TypeError("Legacy_key and cloud_key must be instance of string")
            self.legacy_key = legacy_key
            self.cloud_key = cloud_key
            self.join_columns = [legacy_key, cloud_key]
        elif join_columns is not None:
            logging.info(isinstance(join_columns, list))
            if isinstance(join_columns, str) or isinstance(join_columns, list):
                self.join_columns = join_columns
            else:
                raise TypeError("join_columns must be instance of string or list")
        else:
            raise TypeError("Must provide either (Legacy_key: str, cloud_key: str) or join_columns: list or str")
        self.legacy_df = legacy_df
        self.cloud_df = cloud_df

    def compare(self):
        """
        Compares two dataframes.

        Runs operations like:
        -comparing schemas
        -comparing row counts
        -looking for value mismatches
        
        between two dataframes.
        """
        logging.info("Validating dataframes...")
        self.check_dataframe(self.legacy_df)
        self.check_dataframe(self.cloud_df)

        logging.info("Checking schemas...")
        self.schema_difference()
        logging.info("Checking row counts...")
        self.row_count_difference()
        logging.info("Checking value mismatches...")
        self.value_mismatches()

    def schema_difference(self):
        """
        Comparing schemas between two datasets.

        Checking matching and mismatching columns in two datasets.

        TODO:
        -check for None and NaN values
        """
        self.common_columns = set(self.legacy_df.columns) & set(self.cloud_df.columns)
        self.missing_from_legacy = set(self.cloud_df.columns) - set(self.legacy_df.columns)
        self.missing_from_cloud = set(self.legacy_df.columns) - set(self.cloud_df.columns)

        legacy_datatypes = self.legacy_df.dtypes
        cloud_datatypes = self.cloud_df.dtypes

        self.mismatched_types = {}
        self.mismatched_counter = 0
        for column in self.common_columns:
            if legacy_datatypes[column] != cloud_datatypes[column]:
                legacy_column = 'legacy_' + column
                cloud_column = 'cloud_' + column
                self.mismatched_types[column] = {legacy_column: str(legacy_datatypes[column]), cloud_column: str(cloud_datatypes[column])}
                self.mismatched_counter = self.mismatched_counter + 1

    def row_count_difference(self):
        """Checking row counts and row counts difference between two datasets"""
        self.row_count_legacy = len(self.legacy_df.index)
        self.row_count_cloud = len(self.cloud_df.index)

        self.row_count_difference_legacy = self.row_count_legacy - self.row_count_cloud
        self.row_count_difference_cloud = self.row_count_cloud - self.row_count_legacy

    def value_mismatches(self):
        """
        Checking mismatches and differences between in the values in dataframes.

        TODO:
        -checking date formats
        """
        if isinstance(self.join_columns, str):
            self.matched_id_datatypes = self.legacy_df[self.join_columns].dtype == self.cloud_df[self.join_columns].dtype
        elif isinstance(self.join_columns, list):
            self.matched_id_datatypes = self.legacy_df[self.join_columns[0]].dtype == self.cloud_df[self.join_columns[1]].dtype
        if self.matched_id_datatypes:
            if isinstance(self.join_columns, str):
                try:
                    self.merged_dataframe = self.legacy_df.merge(
                        self.cloud_df,
                        how="outer",
                        on=self.join_columns,
                        suffixes=("_legacy", "_cloud"),
                        indicator=True
                    )
                except KeyError as e:
                    logging.error(f"Column {self.join_columns} not found in one or both dataframes: {e}")
                    raise
                except ValueError as e:
                    logging.error(f"Invalid values during merge {e}")
                    raise
            elif isinstance(self.join_columns, list):
                try:
                    self.merged_dataframe = self.legacy_df.copy().merge(
                        self.cloud_df.copy(),
                        how="outer",
                        left_on=self.join_columns[0],
                        right_on=self.join_columns[1],
                        suffixes=("_legacy", "_cloud"),
                        indicator=True
                    )
                except KeyError as e:
                    logging.error(f"Either column {self.join_columns[0]} or {self.join_columns[1]} not found: {e}")
                    raise
                except ValueError as e:
                    logging.error(f"Invalid values during merge {e}")
                    raise

            try:
                self.legacy_unique = self.merged_dataframe[self.merged_dataframe["_merge"] == "left_only"].copy()
                self.cloud_unique = self.merged_dataframe[self.merged_dataframe["_merge"] == "right_only"].copy()

                self.merged_dataframe_common = self.merged_dataframe[self.merged_dataframe["_merge"] == "both"].copy()

                self.duplicates = self.merged_dataframe_common.duplicated()
            except AttributeError as e:
                logging.error(f"An exception occur during processing merged dataframe: \n{e}")
                raise

            for column in self.common_columns:
                if isinstance(self.join_columns, list):
                    if column != self.join_columns[0] and column != self.join_columns[1]:
                        legacy_values = self.merged_dataframe_common[column + "_legacy"]
                        cloud_values = self.merged_dataframe_common[column + "_cloud"]
                        mismatch = column + "_mismatch"
                        self.merged_dataframe_common[mismatch] = (legacy_values != cloud_values).astype(int)
                elif isinstance(self.join_columns, str):
                    if column != self.join_columns:
                        legacy_values = self.merged_dataframe_common[column + "_legacy"]
                        cloud_values = self.merged_dataframe_common[column + "_cloud"]
                        mismatch = column + "_mismatch"
                        self.merged_dataframe_common[mismatch] = (legacy_values != cloud_values).astype(int)
            self.mismatches_exists = any('_mismatch' in col for col in self.merged_dataframe_common.columns)
            if self.mismatches_exists:
                try:
                    mismatch_columns = [col for col in self.merged_dataframe_common.columns if '_mismatch' in col]
                    mismatch_sum = self.merged_dataframe_common[mismatch_columns].sum(axis=1)
                    self.merged_dataframe_common["mismatch_sum"] = mismatch_sum
                    self.mismatched_values = self.merged_dataframe_common["mismatch_sum"].sum()
                    self.mismatched_dataframe = self.merged_dataframe_common[self.merged_dataframe_common["mismatch_sum"] > 0]
                except ValueError as e:
                    logging.error(f"An exception occur during processing mismatched values in dataframe: \n{e}")
                    self.mismatched_values = 0
                    self.mismatched_dataframe = pd.DataFrame()
        else:
            logging.warning("Id columns are not the same datatypes!")

    def check_dataframe(self, dataframe: pd.DataFrame):
        """Checking datatype of a dataframe"""
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError(f"{dataframe} is not a valid pandas DataFrame.")
        
    def report(self):
        try:
            schema = self.get_schema_summary()
            row_count = self.get_row_count_summary()
            value_mismatches = self.get_value_mismatches_summary()
        except AttributeError as e:
            logging.error(f"An exception occur during gathering report data: \n{e}")
            raise RuntimeError("Call compare method before generating report")
        return f"""Report
--------------------------------------------------------
Comparison of two dataframes

Comparison performed on dataframes: Legacy and Cloud
--------------------------------------------------------

Schema differences: 
-------------------
{schema}

==================

Row count differences:
----------------------
{row_count}

==================

Value mismatches:
-----------------
{value_mismatches}"""        

    def get_schema_summary(self):
        base = f"""Missing columns for Legacy: {self.missing_from_legacy if bool(self.missing_from_legacy) else "No missing columns"}
Missing columns for Cloud: {self.missing_from_cloud if bool(self.missing_from_cloud) else "No missing columns"}
Common columns: {self.common_columns if bool(self.common_columns) else "No common columns"}
Join columns: {self.join_columns}"""
        if self.mismatched_counter > 0:
            base = base + "\nMismatching datatypes on " + str(self.mismatched_counter) + " columns"
        if self.mismatched_types:
            base = base + "\n" + str(pd.DataFrame().from_dict(self.mismatched_types, orient="index"))
        return base
    
    def get_row_count_summary(self):
        base = f"""Row count Legacy: {self.row_count_legacy} rows
Row count Cloud : {self.row_count_cloud} rows
Row count difference for legacy: {self.row_count_legacy - self.row_count_cloud}
Row count difference for cloud: {self.row_count_cloud - self.row_count_legacy}"""
        return base
    
    def get_value_mismatches_summary(self):
        if self.matched_id_datatypes:
            nl = '\n'
            base = ""
            if not self.legacy_unique.empty:
                base = base + f"{len(self.legacy_unique)} unique for Legacy: " + nl + str(self.legacy_unique.drop("_merge", axis=1)) + "\n"
            if not self.cloud_unique.empty:
                base = base + f"{len(self.cloud_unique)} unique for Cloud: " + nl + str(self.cloud_unique.drop("_merge", axis=1)) + "\n"
            if self.mismatches_exists:
                base = base + f"There are {self.mismatched_values} mismatched values\n"
                if not self.mismatched_dataframe.empty:
                    base = base + str(self.mismatched_dataframe)

            if not self.duplicates[self.duplicates == True].empty:
                self.merged_dataframe_common["duplicate"] = self.merged_dataframe_common.duplicated()
                legacy_df_duplicates = self.legacy_df.duplicated()
                cloud_df_duplicates = self.cloud_df.duplicated()
                detected_in = ""
                if legacy_df_duplicates.any() and cloud_df_duplicates.any():
                    detected_in = "both dataframes"
                elif legacy_df_duplicates.any():
                    detected_in = "legacy dataframe"
                elif cloud_df_duplicates.any():
                    detected_in = "cloud dataframe"
                else:
                    detected_in = "merged data"
                base = base + "Duplicates detected in " + detected_in +": " + str(self.duplicates.sum()) + "\n" + str(self.merged_dataframe_common[self.merged_dataframe_common["duplicate"] == True])
        else:
            base = "Unmatched datatypes of the keys. Value matching cannot be performed!"

        if base == "":
            base = "No columns has been checked for mismatching values!"
        return base
    
    def sample_mismatches(self):
        """
        TODO:
        """
        pass