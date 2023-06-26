# Databricks notebook source
try:
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.sql.types import NumericType
    import re
except Exception as e:
    raise Exception(f'Error while importing libraries, error {e}')


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
        self.main()

    def readData(self):

        '''
        Read data from different sources with configurable parameters.
        
        Returns:
            pyspark.sql.DataFrame: DataFrame containing the data from the specified source.
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
    

    def validateSchema(self):

        """
        Validate the actual schema of a DataFrame against the expected schema.
        Identify mismatched datatypes, missing columns, or extra columns.
            
        Returns:
            dict: Validation result with details of mismatched datatypes, missing columns, and extra columns.
        """

        try:
            validation_result = {"DataType Mismatched": [], "Missing Columns": [], "Extra Columns": []}
            actual_schema = self.dataframe.schema
            for field in self.expected_schema.fields:
                if field.name in actual_schema.fieldNames():
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

        """
        Change the current schema to the expected schema provided.
        
        Returns:
            pyspark.sql.DataFrame: DataFrame containing the datatype of the expected schema.

        """
        try:
            actual_schema = self.dataframe.schema
            schemaValidatedDF = self.dataframe
            for field in self.expected_schema.fields:
                if field.name in actual_schema.fieldNames():
                    schemaValidatedDF = schemaValidatedDF.withColumn(field.name,col(field.name).cast(field.dataType)) #cast the data type of the schema
            return schemaValidatedDF 
        
        except Exception as e:
            raise Exception(f"Error occured while changing the data type, error: {str(e)}")

    
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
            else:
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
            
            if invalid_count > 0:
                schema = StructType([StructField(f'Invalid Data Count - {column}', IntegerType(), nullable=False)])
                invalid_count_DF = spark.createDataFrame([(invalid_count,)],schema=schema)
                invalid_record_data = invalid_records
                if showData == True:
                    print(f'Invalid count for {column}')
                    invalid_count_DF.display(display_rows)
                    print(f'Invalid data for {column}')
                    invalid_record_data.display(display_rows)
                elif showData == False:
                    print(f'Invalid count for {column}')
                    invalid_count_DF.display(display_rows)

            columnValidationFormat[column] = {invalid_count_DF, invalid_record_data}

        return columnValidationFormat

    def duplicateValues(self):

        '''
        Counts the number of duplicate values in the dataframe.

        Returns:
            Dict: Dictionary containing the dataframes for the duplicate values and duplicate values count.
        '''
        
        duplicatesCount = {}
        duplicates = self.dataframe.groupBy(self.dataframe.columns).count().filter(col('count')>1)
        duplicatesValuesCount = spark.createDataFrame([Row(count=duplicates.count())], schema=['Duplicates Count'])
        if duplicates.count() > 1:
            duplicatesValues = duplicates.drop('count')
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
            self.dataframe.display(5)
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
            print("\n\n Null Counts:\n\n")
            nullCountsData = [(column, count )for column, count in self.dataprofiling['nullCounts'].items()]
            self.nullCounts = spark.createDataFrame(nullCountsData,schema=['Column','Count'])
            self.nullCounts.display()
            print("\n\n Null Counts Percentage:\n\n")
            nullCountsPercentageData = [(column, count )for column, count in self.dataprofiling['null count percentage'].items()]
            self.nullCountsPercentage = spark.createDataFrame(nullCountsPercentageData,schema=['Column','Percentage'])
            self.nullCountsPercentage.display()
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






    # def generatePDF(self,output_file="DataReport.pdf", range_validation = None):

    #     '''
    #     Generates the report in the PDF format
    #     args:
    #         range_validation: Add range validation in report if the parameter is not None. Default value is None.
    #         output_file(str): Name for the output file. Default value if DataReport.pdf
        
    #     '''
    #     try:
    #         from reportlab.lib.pagesizes import letter
    #         from reportlab.platypus import SimpleDocTemplate, Table
    #     except Exception as e:
    #         raise Exception(f"Error while loading the library, error: {str(e)}")
            
    #     try:

    #         for i, df in enumerate(self.dfList):
    #             pdf_filename = f"temp_{i+1}.pdf"
    #             df.write.csv(pdf_filename)
    #             pdf_merger.append(pdf_filename)
            
    #         pdf_merger.write(output_file)
    #         pdf_merger.close()

    #     except Exception as e:
    #         raise Exception(f"Error occured while generating pdf, error: {str(e)}")
        
    #     try:
    #         for i in range(len(dfToSave)):
    #             temp_filename = f"temp_{i+1}.pdf"
    #             os.remove(temp_filename)
    #     except Exception as e:
    #         raise Exception(f"Could not remove the files from os, error: {str(e)}")
          
        
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
            .add('Ticket',StringType(),True)

a = dataTestAutomation(source_type="csv",source_path=file_path, options=options,expected_schema=exp_schema,changeDataType=True)
range_validation_format = {
    "Age":(20,60)
}
a.run_range_validations(range_validation_format)
column_rules = {
    "Ticket": r'^\d{6}$',
    "Embarked":r'S'
}
invalid_data = a.validateColumnFormat(column_rules=column_rules)

# COMMAND ----------

#Using snowflakes 

options = {
  "sfUrl": "ac30669.ap-south-1.aws.snowflakecomputing.com",
  "sfUser": "rabin",
  "sfPassword": "Admin123",
  "sfDatabase": "NEWDB",
  "sfSchema": "NEWSCHEMA",
  "sfWarehouse": "WAREHOUSE01",
  "dbtable":"CARDETAILS"
}
a = dataTestAutomation(source_type="snowflake", options=options)

# COMMAND ----------

exp_schema = StructType()\
            .add('PassengerId',StringType(),True)\
            .add('Survived',IntegerType(),True)\
            .add('Pclass',IntegerType(),True)\
            .add('Name',StringType(),True)\
            .add('Sex',StringType(),True)\
            .add('Age',IntegerType(),True)\
            .add('SibSp',IntegerType(),True)\
            .add('Parch',IntegerType(),True)\
            .add('Ticket',StringType(),True)\
            .add('Fare',FloatType(),True)\
            .add('Cabin',StringType(),True)\
            .add('Embarked',StringType(),True)

file_path = "/FileStore/tables/titanic.csv"
#options = {'header':'true','inferSchema':'false','multiline':'true'}
options = {'header':'true','inferSchema':'false'}
df1 = spark.read.format("csv").options(**options).schema(exp_schema).load(file_path)

# COMMAND ----------

#Using snowflakes 
options = {
  "sfUrl": "zu11856.central-india.azure.snowflakecomputing.com",
  "sfUser": "AIRBYTE_USER",
  "sfPassword": "password",
  "sfDatabase": "AIRBYTE_DATABASE",
  "sfSchema": "AIRBYTE_SCHEMA",
  "sfWarehouse": "COMPUTE_WH",
  "dbtable":"AIR_QUALITY_DATA___2015_2020_"
}

#df2 = spark.read.format("snowflake").options(**options).load()
a = dataTestAutomation(source_type="snowflake", options=options)

# COMMAND ----------

file_path = "/FileStore/tables/titanic.csv"
#options = {'header':'true','inferSchema':'false','multiline':'true'}
options = {'header':'true','inferSchema':'false'}

df = spark.read.format("csv").options(**options).load(file_path)

# COMMAND ----------

df.show(10)

# COMMAND ----------


