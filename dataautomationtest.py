# Databricks notebook source
<<<<<<< HEAD
=======
try:
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.sql.types import NumericType
    import re
    import matplotlib.pyplot as plt
    import math
    from pyspark.sql import functions as F
except Exception as e:
    raise Exception(f'Error while importing libraries, error {e}')


>>>>>>> origin/testing
class dataTestAutomation:

    def __init__(self, source_type,  source_path = None, options=None, expected_schema = None, changeDataType = False):

        '''
        Args:
            source_type (str): Type of the data source (e.g., 'delta', 'parquet', 'csv', 'jdbc', 'mongo', etc.).
            source_path (str): Path or connection URL of the data source. Default is None.
            expected_schema: Expected schema for data validation. Default is None.
            options (dict, optional): Additional options specific to the data source. Default is None.
            changeDataType: Change the datatype of actual schema of dataframe to expected schema. Default is False.
        '''
        self.source_type = source_type
        self.source_path = source_path
        self.options = options
        self.expected_schema = expected_schema
        self.changeDataType = changeDataType
<<<<<<< HEAD


        self.dataframe = self.readData()
        print(self.dataframe)
        print(self.dataframe.schema)
        if self.expected_schema is not None:
            self.schemaResult = self.validateSchema()
            print(self.schemaResult)

            if self.changeDataType is True:
                self.dataframe = self.changeSchema()
                print(self.dataframe.schema)

        
        
        self.dataprofiling = self.dataProfiling()
        print(self.dataprofiling)


    def readData(self):

        """
=======
        self.main()

    def readData(self):

        '''
>>>>>>> origin/testing
        Read data from different sources with configurable parameters.
        
        Returns:
            pyspark.sql.DataFrame: DataFrame containing the data from the specified source.
<<<<<<< HEAD
        """    
        # Read data based on the source type
        reader = spark.read.format(self.source_type)
        if self.options is not None:
            reader = reader.options(**self.options)
        if self.source_path is not None:
            reader = reader.load(self.source_path)

        df = reader
        return df
    
=======
        ''' 
        try:
            # Read data based on the source type
            reader = spark.read.format(self.source_type)
            if self.options is not None:
                reader = reader.options(**self.options)
            else:
                reader = reader.options()
            if self.source_path is not None:
                reader = reader.load(self.source_path)
            else:
                reader = reader.load()

            df = reader
            
            return df
        except Exception as e:
            raise Exception(f"Error occured while loading the file, error: {str(e)}")

    def handleInvalidColumns(self):

        '''
        Validate the column names by removing "",'.' and invalid characters from the columns.

        Returns:
            pyspark.sql.DataFrame: Dataframe containing the validated column names.
        '''

        validName = self.dataframe
        for column in self.dataframe.columns:
            if not re.match(r'^[A-Za-z0-9_]+$', column):
                valid_column_name = re.sub(r'[^A-Za-z0-9_]+', '_', column)
                validName = validName.withColumnRenamed(column, valid_column_name)

        return validName
    

>>>>>>> origin/testing
    def validateSchema(self):

        """
        Validate the actual schema of a DataFrame against the expected schema.
        Identify mismatched datatypes, missing columns, or extra columns.
            
        Returns:
            dict: Validation result with details of mismatched datatypes, missing columns, and extra columns.
        """
<<<<<<< HEAD
        validation_result = {"datatype_mismatch": [], "missing_columns": [], "extra_columns": []}
        actual_schema = self.dataframe.schema
        for field in self.expected_schema.fields:
            if field.name in actual_schema.fieldNames():
                actual_field = actual_schema[field.name]
                if actual_field.dataType != field.dataType:
                    validation_result["datatype_mismatch"].append(field.name)
            else:
                validation_result["missing_columns"].append(field.name)

        # Check for extra columns
        for field_name in actual_schema.fieldNames():
            if field_name not in [field.name for field in self.expected_schema.fields]:
                validation_result["extra_columns"].append(field_name)

        return validation_result
    
    def changeSchema(self):

=======

        try:
            validation_result = {"DataType Mismatched": [], "Missing Columns": [], "Extra Columns": []}
            actual_schema = self.dataframe.schema
            for field in self.expected_schema.fields:
                actual_schema_fields = [data.lower() for data in actual_schema.fieldNames()]
                if field.name.lower() in actual_schema_fields:
                    actual_field = actual_schema[field.name]
                    if actual_field.dataType != field.dataType:     #Check for mismatch datatype
                        validation_result["DataType Mismatched"].append(field.name)     
                else:
                    validation_result["Missing Columns"].append(field.name)     #check for missing columns

            # Check for extra columns
            for field_name in actual_schema.fieldNames():
                if field_name not in [field.name for field in self.expected_schema.fields]:
                    validation_result["Extra Columns"].append(field_name)

            return validation_result
        
        except Exception as e:
            raise Exception(f"Error occured while validating schema, error: {str(e)}")

    

    def changeSchema(self):
>>>>>>> origin/testing
        """
        Change the current schema to the expected schema provided.
        
        Returns:
            pyspark.sql.DataFrame: DataFrame containing the datatype of the expected schema.
<<<<<<< HEAD

        """
        actual_schema = self.dataframe.schema
        schemaValidatedDF = self.dataframe
        for field in self.expected_schema.fields:
            if field.name in actual_schema.fieldNames():
                schemaValidatedDF = schemaValidatedDF.withColumn(field.name,col(field.name).cast(field.dataType))
        return schemaValidatedDF 
            

    def dataProfiling(self):
        
        recordCounts  = self.dataframe.count()

        nullCountsPercentage = {}

        nullCounts = {}

        emptyString = {}

        stasticalDescription = {}

        for columnNames in self.dataframe.columns:
            nullCounts[columnNames] = self.dataframe.select(col(columnNames)).filter(col(columnNames).isNull()).count()
            nullCountsPercentage[columnNames] = f'{((nullCounts[columnNames])/recordCounts)*100}%'
            emptyString[columnNames] = self.dataframe.filter(col(columnNames) == "").count()
            column_data_type = self.dataframe.schema[columnNames].dataType
            if isinstance(column_data_type, (IntegerType, DoubleType, FloatType, LongType)):
                statistics = self.dataframe.select(col(columnNames)).describe().toPandas().set_index('summary').to_dict()[columnNames]
                stasticalDescription[columnNames] = statistics

        resultOutcome = {'Total Number of rows':recordCounts,'nullCounts':nullCounts,'null count percentage':nullCountsPercentage,'empty_string':emptyString,'stats':stasticalDescription}

        return resultOutcome
    



# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
#file_path = "/FileStore/tables/sample1.json"
file_path = "/FileStore/tables/titanic.csv"
#options = {'header':'true','inferSchema':'false','multiline':'true'}
options = {'header':'true','inferSchema':'false'}
exp_schema = StructType()\
            .add('PassengerId',IntegerType(),True)\
            .add('Survived',IntegerType(),True)\
            .add('Pclass',IntegerType(),True)\
            .add('Name',StringType(),True)\
            .add('Sex',StringType(),True)\
            .add('Age',IntegerType(),True)\
            .add('SibSp',IntegerType(),True)\
            .add('Parch',IntegerType(),True)\
            .add('Ticket',StringType(),True)\
            .add('SKJDGAH', FloatType(),True)

            

a = dataTestAutomation(source_type="csv",source_path=file_path, options=options,expected_schema=exp_schema,changeDataType=True)
=======
        """
        try:
            actual_schema = self.dataframe.schema
            schema_validated_df = self.dataframe
            for field in self.expected_schema.fields:
                if field.name in actual_schema.fieldNames():
                    actual_field = actual_schema[field.name]
                    expected_field = field.dataType
                    if isinstance(expected_field, ArrayType):
                        schema_validated_df = self.convert_array_type(schema_validated_df, field.name, actual_field, expected_field)
                    elif isinstance(expected_field, MapType):
                        schema_validated_df = self.convert_map_type(schema_validated_df, field.name, actual_field, expected_field)
                    else:
                        schema_validated_df = schema_validated_df.withColumn(field.name, col(field.name).cast(expected_field))
            return schema_validated_df
        except Exception as e:
            raise Exception(f"Error occurred while changing the data type: {str(e)}")

    def convert_array_type(self,dataframe, column_name, actual_field, expected_field):
        """
            Converts the data type of an array column in a DataFrame.

            Args:
                self (object): The instance of the class.
                dataframe (DataFrame): The DataFrame containing the data.
                column_name (str): The name of the array column.
                actual_field (DataType): The actual data type of the array column.
                expected_field (DataType): The desired data type for the array column.

            Returns:
                DataFrame: The DataFrame with the converted array column.

            Raises:
                Exception: If the actual data type of the column is not supported.
        """
        if isinstance(actual_field.dataType, StringType):
            dataframe = dataframe.withColumn(column_name, from_json(col(column_name), expected_field))
        elif isinstance(actual_field.dataType, ArrayType) and isinstance(actual_field.elementType, StructType):
            dataframe = self.convert_struct_type(dataframe, column_name, actual_field.elementType, expected_field.elementType)
        else:
            raise Exception(f"Invalid data type for column {column_name}")
        return dataframe

    def convert_map_type(self,dataframe, column_name, actual_field, expected_field):
        """
            Converts the data type of a map column in a DataFrame.

            Args:
                self (object): The instance of the class.
                dataframe (DataFrame): The DataFrame containing the data.
                column_name (str): The name of the map column.
                actual_field (DataType): The actual data type of the map column.
                expected_field (DataType): The desired data type for the map column.

            Returns:
                DataFrame: The DataFrame with the converted map column.

            Raises:
                Exception: If the actual data type of the column is not supported.
        """
        if isinstance(actual_field.dataType, StringType):
            dataframe = dataframe.withColumn(column_name, from_json(col(column_name), expected_field))
        elif isinstance(actual_field.dataType, MapType) and isinstance(actual_field.valueType, StructType):
            dataframe = self.convert_struct_type(dataframe, column_name, actual_field.valueType, expected_field.valueType)
        else:
            raise Exception(f"Invalid data type for column {column_name}")
        return dataframe

    def convert_struct_type(self,dataframe, column_name, actual_field, expected_field):
        """
        Converts the data type of a struct column in a DataFrame.

        Args:
            self (object): The instance of the class.
            dataframe (DataFrame): The DataFrame containing the data.
            column_name (str): The name of the struct column.
            actual_field (StructType): The actual data type of the struct column.
            expected_field (StructType): The desired data type for the struct column.

        Returns:
            DataFrame: The DataFrame with the converted struct column.

        Raises:
            Exception: If the actual data type of the column is not supported.
        """
        for field in expected_field.fields:
            if field.name in actual_field.fieldNames():
                actual_nested_field = actual_field[field.name]
                expected_nested_field = field.dataType
                if isinstance(expected_nested_field, ArrayType):
                    dataframe = convert_array_type(dataframe, f"{column_name}.{field.name}", actual_nested_field, expected_nested_field)
                elif isinstance(expected_nested_field, MapType):
                    dataframe = convert_map_type(dataframe, f"{column_name}.{field.name}", actual_nested_field, expected_nested_field)
                else:
                    dataframe = dataframe.withColumn(f"{column_name}.{field.name}", col(f"{column_name}.{field.name}").cast(expected_nested_field))
        return dataframe


    
    def getCategoricalNumerical(self):

        '''
        Distinguish the categorical and numerical column, saves the column names in the list.
        Returns:
            List: Two different list containing the names of column classified into numerical and categorical columns
        '''

        categorical = []
        numerical_datatype = []


        for i in self.dataframe.columns:
            column_data_type = self.dataframe.schema[i].dataType
            if isinstance(column_data_type, (IntegerType, DoubleType, FloatType, LongType, DecimalType, NumericType, ShortType,ByteType)):
                numerical_datatype.append(i)
            elif isinstance(column_data_type,StringType):
                distinct_values = self.dataframe.agg(countDistinct(i)).collect()[0][0]
                if distinct_values < 12:
                    categorical.append(i)
        

        
        return categorical, numerical_datatype


    def dataProfiling(self):

        '''
            Checks the null counts, empty strings, stastical description and counts of the number of rows.

            Returns:
                dict: Data profiling results including total row count, null counts, null count percentage,
                  empty string counts, and statistical descriptions for each column.

        '''
        try:
            recordCounts  = self.dataframe.count()
            nullCountsPercentage = {}
            nullCounts = {}
            emptyString = {}
            stasticalDescription = {}
            distinctValue = {}
            for columnNames in self.dataframe.columns:
                column_data_type = self.dataframe.schema[columnNames].dataType
                if not isinstance(column_data_type,(ArrayType,MapType,StructType)):
                    nullCounts[columnNames] = self.dataframe.select(col(columnNames)).filter(col(columnNames).isNull()).count() #count of null value
                    nullCountsPercentage[columnNames] = f'{((nullCounts[columnNames])/recordCounts)*100}%'    #percentage of null values
                    emptyString[columnNames] = self.dataframe.filter(col(columnNames) == "").count()    #count of empty strings
            
            stasticalDescription = {colName: self.dataframe.select(col(colName)).describe().toPandas().set_index('summary').to_dict()[colName] for colName in self.numerical}
            

            distinctValue = {colName: self.dataframe.select(col(colName)).distinct() for colName in self.categorical}


            resultOutcome = {'Total Number of rows':recordCounts,'nullCounts':nullCounts,'null count percentage':nullCountsPercentage,'empty_string':emptyString,'stats':stasticalDescription, 'Distinct Values':distinctValue}
            return resultOutcome
        
        except Exception as e:
            raise Exception(f"Error occured while doing data profiling, error: {str(e)}")
            
    

    def run_range_validations(self, range_validations):

        '''
        range_validations(dict): Validation rules passed in dictionary.
        Format for the parameter range_validation = {
            'col1':(min_value,max_value),
            'col2':(None,max_value),
            'col3':(min_value,None),
            'col4':(None,None)
        }
        Returns:
            dict: Validation results indicating if the range validations passed or failed for each column.
        '''

        try:
            validation_results = {}

            for column, (min_value, max_value) in range_validations.items():
                if min_value is None and max_value is None:
                    validation_results[column] = True
                elif min_value is None:
                    validation_result = self.dataframe.filter(col(column) <= max_value).count() == self.dataframe.count()
                    validation_results[column] = validation_result
                elif max_value is None:
                    validation_result = self.dataframe.filter(col(column) >= min_value).count() == self.dataframe.count()
                    validation_results[column] = validation_result
                else:
                    validation_result = self.dataframe.filter((col(column) >= min_value) & (col(column) <= max_value)).count() == self.dataframe.count()
                    validation_results[column] = validation_result
            return validation_results
        
        except Exception as e:
            raise Exception(f"Error occured while validating range, error: {str(e)}")
    

    def validateColumnFormat(self, column_rules, display_rows = 20, showData = False):

        '''
        args:
            column_rules(dict): Dictionary with column name and format as regex for the validation.
            eg : {
                "Ticket": r'^\d{6}$',
                "Embarked":r'S'
            }
            Ticket and Embarked, the two keys are the column names, other two, values are the regular expression validation rules.
            display_rows(int): Limit for number of invalid rows to be displayed. Default value is 20.
        
        Returns:
            The two dataframes are printed as well.
            Dictionary containing the dataframe for invalid count and invalid record of specific column.
        '''

        columnValidationFormat = {}
        for column, rule in column_rules.items():
            df = self.dataframe.withColumn("validation_result", regexp_extract(col(column), rule, 0))
            invalid_records = df.filter(col("validation_result") == "")
            invalid_count = invalid_records.count()

            schema = StructType([StructField(f'Invalid Data Count - {column}', IntegerType(), nullable=False)])
            invalid_count_DF = spark.createDataFrame([(invalid_count,)],schema=schema)

            if invalid_count > 0:
                
                invalid_record_data = invalid_records
                if showData == True:
                    print(f'Invalid count for {column}')
                    invalid_count_DF.display(display_rows)
                    print(f'Invalid data for {column}')
                    display(invalid_record_data.limit(display_rows))
                    columnValidationFormat[column] = {invalid_count_DF, invalid_record_data}
                elif showData == False:
                    print(f'Invalid count for {column}')
                    invalid_count_DF.display(display_rows)
                    columnValidationFormat[column] = {invalid_count_DF}
            else:
                print(f'Invalid count for {column}')
                invalid_count_DF.display(display_rows)
                columnValidationFormat[column] = {invalid_count_DF}

                
            

        return columnValidationFormat

    def duplicateValues(self):

        '''
        Counts the number of duplicate values in the dataframe.

        Returns:
            Dict: Dictionary containing the dataframes for the duplicate values and duplicate values count.
        '''
        
        duplicatesCount = {}
        duplicate_rows_df = self.dataframe.groupBy(self.dataframe.columns).count().where(col("count") > 1)
        duplicatesValuesCount = spark.createDataFrame([Row(count=duplicate_rows_df.count())], schema=['Duplicates Count'])
        if duplicate_rows_df.count() > 1:
            duplicatesValues = duplicate_rows_df.drop('count')
            duplicatesCount = {'values': duplicatesValues, 'count': duplicatesValuesCount}
        else:
            duplicatesCount = {'count': duplicatesValuesCount}

        return duplicatesCount
    
       
    
    def tabularReport(self):

        """
        Generate a report summarizing the data validation results.

        Returns:
            str: Report containing information about overall evaluation of data.

        """

        try:  
            print("Raw Data:\n\n")
            display(self.dataframe.limit(5))
            print("\n\n Total Number of Records\n\n")
            row = Row('Total Number of rows')(self.dataprofiling['Total Number of rows'])
            self.totalCount = spark.createDataFrame([row])
            self.totalCount.display()

            if self.expected_schema is None:
                    pass
            else:
                print("\n\n Schema Validation:\n\n")
                schemaValidationData = [(key,value) for key,value in self.schemaResult.items()]
                self.schemaValidationReport = spark.createDataFrame(schemaValidationData,['Validation Name','Result'])
                self.schemaValidationReport.display()
            print("\n\n Null Counts and Percentage:\n\n")
            nullCountsData = [(column, count )for column, count in self.dataprofiling['nullCounts'].items()]
            self.nullCounts = spark.createDataFrame(nullCountsData,schema=['Column','Count'])
            #self.nullCounts.display()
            # print("\n\n Null Counts Percentage:\n\n")
            nullCountsPercentageData = [(column, count )for column, count in self.dataprofiling['null count percentage'].items()]
            self.nullCountsPercentage = spark.createDataFrame(nullCountsPercentageData,schema=['Column','Percentage'])
            #self.nullCountsPercentage.display()
            self.nullCountJoin = self.nullCounts.join(self.nullCountsPercentage,'Column','inner')
            self.nullCountJoin.display()
            print("\n\n Empty String:\n\n")
            emptyStringData = [(column, count) for column, count in self.dataprofiling['empty_string'].items()]
            self.emptyStringData = spark.createDataFrame(emptyStringData,schema=['Column','Count'])
            self.emptyStringData.display()

            print("\n\n Statistics:\n\n")
            data = [(k, v['count'], v['mean'], v['stddev'], v['min'], v['max']) for k, v in self.dataprofiling['stats'].items()]
            schema = StructType([
                StructField("column", StringType(), nullable=False),
                StructField("count", StringType(), nullable=False),
                StructField("mean", StringType(), nullable=False),
                StructField("stddev", StringType(), nullable=False),
                StructField("min", StringType(), nullable=False),
                StructField("max", StringType(), nullable=False)
            ])
            self.statistics = spark.createDataFrame(data,schema)
            self.statistics.display()

            print("\n\n Distinct Values:\n\n")
            self.distinctValueData = [data.display() for column, data in self.dataprofiling['Distinct Values'].items()]

            print("\n\n Duplicates:\n\n")
            duplicates = self.duplicates
            self.duplicateValues = []
            for key, value in duplicates.items():
                self.duplicateValues.append(value)
                value.display()
            
            if self.expected_schema is None:
                self.dfList = [self.totalCount, self.statistics,self.emptyStringData,self.nullCounts,self.nullCountsPercentage,self.distinctValueData]
            else:
                self.dfList = [self.totalCount, self.schemaValidationReport, self.statistics,self.emptyStringData,self.nullCounts,self.nullCountsPercentage,self.distinctValueData]

        
        except Exception as e:
            raise Exception(f"Error occured while generating report, error: {str(e)}")

    def trendAnalysis(self, date_column, trend_columns):

        """
        Performs trend analysis on a DataFrame.

        Args:
            self (object): The instance of the class containing the DataFrame.
            date_column (str): The name of the column containing the date values.
            trend_columns (list): A list of column names to perform trend analysis on.

        Returns:
            tuple: A tuple containing two DataFrames:
                - grouped_data: DataFrame with aggregated data grouped by date and other columns.
                - ordered_df: DataFrame with descriptive statistics of trend_columns ordered by date.
        """
        self.date_column = date_column
        df = self.dataframe.withColumn(self.date_column, f.to_date(f.col(self.date_column), 'yyyy-MM-dd'))
        
        for column in trend_columns:
            if not isinstance(df.schema[column].dataType, (int, float)):
                df = df.withColumn(column,df[column].cast('int'))
        
        group_columns = [f.col(column) for column in df.columns if column != self.date_column]
        agg_columns = [f.col(column).alias(column) for column in trend_columns]
        grouped_data = df.groupBy(self.date_column, *group_columns).agg(*agg_columns)
        selected_columns = [f.col(column) for column in trend_columns]
        selected_columns.append(f.col(self.date_column))
        selected_df = df.select(*selected_columns)
        groupby = selected_df.groupBy(self.date_column)
        agg_exprs = [f.min(column).alias("min_" + column) for column in trend_columns] + \
                    [f.max(column).alias("max_" + column) for column in trend_columns] + \
                    [f.avg(column).alias("avg_" + column) for column in trend_columns] + \
                    [f.stddev(column).alias("stddev_" + column) for column in trend_columns]
        description_df = groupby.agg(*agg_exprs)
        ordered_df = description_df.orderBy(self.date_column)
        return grouped_data, ordered_df
    
    def plotTrendAnalysis(self,dataframe,trend_columns):
        """
            Plots trend analysis for specified columns in a DataFrame.

            Args:
                self (object): The instance of the class.
                dataframe (DataFrame): The DataFrame containing the data.
                trend_columns (list): A list of column names to plot the trend analysis for.

            Returns:
                None
        """
        pandas_df = dataframe.toPandas()
        pandas_df.set_index(self.date_column, inplace=True)
        
        num_trend_columns = len(trend_columns)
        num_rows = math.ceil(math.sqrt(num_trend_columns))
        num_cols = math.ceil(num_trend_columns / num_rows)
        
        fig, axes = plt.subplots(num_rows, num_cols, figsize=(12, 10))
        for i, column in enumerate(trend_columns):
            if num_rows == 1 and num_cols == 1:
                ax = axes
            elif num_rows == 1 or num_cols == 1:
                ax = axes[i]
            else:
                ax = axes[i // num_cols, i % num_cols]

            pandas_df[column].plot(ax=ax, legend = False)
            ax.set_title(column)
            ax.set_ylabel('Count')
            ax.tick_params(axis='x', rotation=90)

        fig.tight_layout()
        plt.show()
          
        
    def main(self):

        self.dataframe = self.readData()
        self.dataframe = self.handleInvalidColumns()
        if self.expected_schema is not None:
            self.schemaResult = self.validateSchema()
            if self.changeDataType is True:
                self.dataframe = self.changeSchema()
        self.categorical, self.numerical = self.getCategoricalNumerical() 
        self.dataprofiling = self.dataProfiling()
        self.duplicates = self.duplicateValues()
        self.report = self.tabularReport()


   

>>>>>>> origin/testing


# COMMAND ----------

<<<<<<< HEAD
df = spark.read.format("csv").options(**options).load(file_path)

# COMMAND ----------

df.show()

# COMMAND ----------


=======
try:
    import pandas as pd
    from pandas import ExcelFile
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
except Exception as e:
    raise Exception(f'Error import libraries, error {str(e)}')

class readSchema:

    def __init__(self,file_path,fieldNameColumn, dataTypeColumn,sheet = None, relatedDataTypeColumn = None ,descriptionColumn = None):
      
        """
        Initialize the class instance.

        Args:
            file_path (str): The path to the file.
            sheet (str, optional): The name of the sheet (if using an Excel file). Defaults to None.
            fieldNameColumn (str, optional): The column name for the field names.
            dataTypeColumn (str, optional): The column name for the data types. 
            relatedDataTypeColumn (str, optional): The column name for the related data types. Defaults to None.
            descriptionColumn (str, optional): The column name for the descriptions. Defaults to None.
        """

        self.spark_df = None
        self.schema = None
        self.sheet = sheet
        self.file_path = file_path
        self.fieldNameColumn = fieldNameColumn
        self.dataTypeColumn = dataTypeColumn
        self.relatedDataTypeColumn = relatedDataTypeColumn
        self.descriptionColumn = descriptionColumn



    def readFile(self, skip_rows = None, start_column = None, usecols = None):
      
        """
        Read the file and create a Spark DataFrame.

        Raises:
            ValueError: If the file format is unsupported.
        """
        try:
            file_extension = self.file_path.split('.')[-1].lower()
            if file_extension.lower() == 'json':
        
                    df = pd.read_json(self.file_path)


            elif file_extension.lower() == 'csv':
                df = pd.read_csv(self.file_path)
            elif file_extension.lower() == 'xlsx':
                if self.sheet is not None:
                    df = pd.ExcelFile(self.file_path, engine = 'openpyxl',skiprows = skip_rows,index_col = start_column, usecols = usecols)
                    df = df.parse(self.sheet)
                else:
                    df = pd.read_excel(self.file_path,skiprows = skip_rows,index_col = start_column, usecols = usecols)



            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
        
        except Exception as e:
            raise Exception(f'Error loading {file_extension} file, error {str(e)}')
        
        try:
            self.spark_df = spark.createDataFrame(df)
        except Exception as e:
            raise Exception(f'Error creating Spark Dataframe, error {str(e)}')

    def convertDFtoList(self):
    
        """
        Convert the Spark DataFrame to a list of dictionaries.

        This method collects the data from the Spark DataFrame and converts each row into a dictionary.

        Note:
            This method assumes that the `spark_df` attribute is already set with a valid Spark DataFrame.

        Returns:
            None
        """
        try:
            data = self.spark_df.select(*self.spark_df.columns).collect()
            self.dict_list = [row.asDict() for row in data]
            return self.dict_list
        except Exception as e:
            raise Exception(f'Error occurred while converting DataFrame to list of dictionaries: {str(e)}')


    def structureFormat(self):
    
        """
        Modify the structure format of related data types in the dictionary list.

        This method checks for specific characters and formats the related data types accordingly.

        Note:
            This method assumes that the `dict_list` attribute is already set with a list of dictionaries. The dict_list is created by convertDFtoList() method.
            It also assumes that the `relatedDataTypeColumn` attribute is set with the column name for related data types which is defined by the user during the initialization of the class.

        Raises:
            Exception: If an error occurs during the modification process.
        """
        try:
            for items in self.dict_list:
                if items[self.relatedDataTypeColumn] is not None:
                    if '{' in items[self.relatedDataTypeColumn] or '}' in items[self.relatedDataTypeColumn]:
                        if '"' not in items[self.relatedDataTypeColumn]:
                            string = items[self.relatedDataTypeColumn].strip().replace('\n', '').replace(' ', '')
                            string = string.strip('{}')
                            pairs = string.split(',')
                            result = {}
                            for pair in pairs:
                                key, value = pair.split(':')
                                result[key] = value
                            items[self.relatedDataTypeColumn] = result
        except Exception as e:
            raise Exception(f'Error occurred while processing related data for item: {str(e)}')

    def get_data_type(self,type_string):
        """
        Get the corresponding Spark data type based on the input type string.

        This method uses a mapping dictionary to associate the input type string with the appropriate Spark data type.

        Args:
            type_string (str): The input type string.

        Returns:
            DataType: The corresponding Spark data type.


        """
        try:
            self.type_mapping = {
                'string': StringType(),
                'str':StringType(),
                'boolean': BooleanType(),
                'integer': IntegerType(),
                'int': IntegerType(),
                'long': LongType(),
                'float': FloatType(),
                'double': DoubleType(),
                'timestamp': TimestampType(),
                'date': DateType()
            }
            return self.type_mapping.get(type_string, StringType())
        except Exception as e:
            raise Exception(f'Error occurred while getting data type for {type_string}: {str(e)}')

    def get_field_data_type(self):
        """
        Get the field data types based on the provided column information.

        This method iterates over the `dict_list` attribute and retrieves the field name, field type, and related data type (if applicable) for each column. It uses the `get_data_type` method to get the corresponding Spark data type.

        Returns:
            list: A list of StructField objects representing the fields with their data types.

        Note:
            This method assumes that the necessary attributes (`dict_list`, `fieldNameColumn`, `dataTypeColumn`, `descriptionColumn`, and `relatedDataTypeColumn`) are properly set.

        """
        fields = []
        try:
            for item in self.dict_list:
                field_name = item[self.fieldNameColumn]
                field_type = item[self.dataTypeColumn]
                if self.descriptionColumn is not None:
                    field_description = item[self.descriptionColumn]

                if field_type in ['array', 'object']:
                    if self.relatedDataTypeColumn is not None:
                        related_data = item[self.relatedDataTypeColumn]
                        if isinstance(related_data, dict):
                            array_fields = []
                            for subfield_name, subfield_type in related_data.items():
                                subfield_data_type = self.get_data_type(subfield_type.lower())
                                array_fields.append(StructField(subfield_name, subfield_data_type, True))
                            field_data_type = ArrayType(StructType(array_fields))
                        else:
                            field_data_type = StringType()
                    else:
                        field_data_type = StringType()
                elif field_type in ['map']:
                    if self.relatedDataTypeColumn is not None:
                        related_data = item[self.relatedDataTypeColumn]
                        if isinstance(related_data, dict):
                            array_fields = []
                            for subfield_name, subfield_type in related_data.items():
                                subfield_data_type = self.get_data_type(subfield_type.lower)
                                array_fields.append(StructField(subfield_name, subfield_data_type, True))
                            field_data_type = MapType(StructType(array_fields))
                        else:
                            field_data_type = MapType(StringType(),StringType())
                    else:
                        field_data_type = MapType(StringType(),StringType())

                else:
                    field_data_type = self.get_data_type(field_type.lower())
                if self.descriptionColumn is not None:
                    field = StructField(field_name, field_data_type, nullable=True, metadata={'description': field_description})
                else:
                    field = StructField(field_name, field_data_type, nullable=True, metadata={'description': ''})

                fields.append(field)
        except Exception as e:
            raise Exception(f'Error occurred while getting field data types: {str(e)}')
        
        return fields


    def spark_schema(self):
        """
        Generate the Spark schema based on the field data types.

        This method calls the `get_field_data_type` method to retrieve the field data types and constructs the Spark schema using the `StructType` class.

        Returns:
            StructType: The Spark schema generated from the field data types.

        Note:
            This method assumes that the `get_field_data_type` method has been properly implemented.

        """
        fields = self.get_field_data_type()
        self.schema = StructType(fields)
        return self.schema

    def createSchema(self):
        """
        Create the schema for the Spark DataFrame.

        This method performs the following steps:
        1. Converts the Spark DataFrame to a list of dictionaries using the `convertDFtoList` method.
        2. If the `relatedDataTypeColumn` is provided, it formats the related data types using the `strctureFormat` method.
        3. Generates the Spark schema using the `spark_schema` method.
        4. Returns the generated schema.

        Returns:
            StructType: The schema generated for the Spark DataFrame.

        Note:
            This method assumes that the `convertDFtoList` and `spark_schema` methods have been properly implemented.

        """
        self.convertDFtoList()
        if self.relatedDataTypeColumn is not None:
            print(self.relatedDataTypeColumn)
            self.structureFormat()
        self.schema = self.spark_schema()
        return self.schema


# COMMAND ----------

filePathSchema = '/dbfs/FileStore/schema.xlsx'
schema_reader = readSchema(file_path=filePathSchema,fieldNameColumn='Field',dataTypeColumn='Type',descriptionColumn='Description')
#schema_reader = readSchema(filePathSchema, fieldNameColumn = 'Field',dataTypeColumn='Type')
schema_reader.readFile(skip_rows=4, usecols="C:E")
loaded_schema = schema_reader.createSchema()
loaded_schema

# COMMAND ----------

file_path = 'https://healthdata.gov/resource/g62h-syeh.json'
spark.sparkContext.addFile(file_path)
path = "file://"+SparkFiles.get("g62h-syeh.json")
options = {'header':'true','inferSchema':'true','multiline':'true'}
datatest = dataTestAutomation(source_type="json",source_path=path,options=options)

# COMMAND ----------

range_validation_format = {
    "deaths_covid":(0,None),
    "deaths_covid_coverage":(0,1000)
}
range_val = datatest.run_range_validations(range_validation_format)
print('\n\n Range Validations Results: \n\n')
for key, value in range_val.items():
    print(f'{key}  :  {value}')
print('\n\n')
column_rules = {
    "date": r'\d{4}-\d{2}-\d{2} \d{2}-\d{2}-\d{2}'
}
invalid_data = datatest.validateColumnFormat(column_rules=column_rules, showData=False)

# COMMAND ----------

trend_columns = ['deaths_covid', 'deaths_covid_coverage', 'hospital_onset_covid', 'hospital_onset_covid_coverage', 'icu_patients_confirmed_influenza']
date_col = 'date'
grouped_data, trendDF = datatest.trendAnalysis(date_col,trend_columns)

# COMMAND ----------

trendDF.display()

# COMMAND ----------

datatest.plotTrendAnalysis(grouped_data,trend_columns)
>>>>>>> origin/testing
