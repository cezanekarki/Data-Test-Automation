# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import NumericType

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

        """
        Read data from different sources with configurable parameters.
        
        Returns:
            pyspark.sql.DataFrame: DataFrame containing the data from the specified source.
        """    
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
                column_data_type = self.dataframe.schema[columnNames].dataType
                if isinstance(column_data_type, (IntegerType, DoubleType, FloatType, LongType, DecimalType, NumericType, ShortType,ByteType)):
                    statistics = self.dataframe.select(col(columnNames)).describe().toPandas().set_index('summary').to_dict()[columnNames]
                    stasticalDescription[columnNames] = statistics  #Statistical Summary of the data
                else:
                    distinctValues = self.dataframe.select(col(columnNames)).distinct()
                    distinctValuesCount = distinctValues.count()
                    if distinctValuesCount <= 12:
                        distinctValue[columnNames] = distinctValues   #Distinct values and it's count

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
    

    def validateColumnFormat(self, column_rules, display_rows = 20):

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
                invalid_count_DF = spark.createDataFrame([invalid_count],schema=[f'Invalid Data Count - {column}'])
                invalid_records.display(20)
            columnValidationFormat[column] = [invalid_count_DF, invalid_records]
        return columnValidationFormat

    def duplicateValues(self):
        
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
            
            self.dfList = [self.totalCount, self.schemaValidationReport, self.statistics,self.emptyStringData,self.nullCounts,self.nullCountsPercentage,self.distinctValueData]

        
        except Exception as e:
            raise Exception(f"Error occured while generating report, error: {str(e)}")



    def generatePDF(self,output_file="DataReport.pdf", range_validation = None):

        '''
        Generates the report in the PDF format
        args:
            range_validation: Add range validation in report if the parameter is not None. Default value is None.
            output_file(str): Name for the output file. Default value if DataReport.pdf
        
        '''
        try:
            from PyPDF2 import PdfMerger
            import os
            pdf_merger = PdfMerger()
        except Exception as e:
            raise Exception(f"Error while loading the library, error: {str(e)}")
            
        try:

            for i, df in enumerate(self.dfList):
                pdf_filename = f"temp_{i+1}.pdf"
                df.write.csv(pdf_filename)
                pdf_merger.append(pdf_filename)
            
            pdf_merger.write(output_file)
            pdf_merger.close()

        except Exception as e:
            raise Exception(f"Error occured while generating pdf, error: {str(e)}")
        
        try:
            for i in range(len(dfToSave)):
                temp_filename = f"temp_{i+1}.pdf"
                os.remove(temp_filename)
        except Exception as e:
            raise Exception(f"Could not remove the files from os, error: {str(e)}")
        
    def main(self):
        try:

            self.dataframe = self.readData()
            if self.expected_schema is not None:
                self.schemaResult = self.validateSchema()
                if self.changeDataType is True:
                    self.dataframe = self.changeSchema()
            self.dataprofiling = self.dataProfiling()
            self.duplicates = self.duplicateValues()
            self.report = self.tabularReport()

        
        except Exception as e:
            raise Exception(f"Error occured, error: {str(e)}")


    



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
range_validation_format = {
    "Age":(20,60)
}
a.run_range_validations(range_validation_format)

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
a = dataTestAutomation(source_type="snowflake",source_path=file_path, options=options)

# COMMAND ----------

file_path = "/FileStore/tables/titanic.csv"
#options = {'header':'true','inferSchema':'false','multiline':'true'}
options = {'header':'true','inferSchema':'false'}
df1 = spark.read.format("csv").options(**options).load(file_path)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract
column_rules = {
    "Ticket": r'^\d{6}$',
    "Embarked":r'S'
}


for column, rule in column_rules.items():
    df = df1.withColumn("validation_result", regexp_extract(col(column), rule, 0))
    invalid_records = df.filter(col("validation_result") == "")
    invalid_count = invalid_records.count()
    
    if invalid_count > 0:
        print(f"Invalid records in column '{column}': {invalid_count}")
        invalid_records.display()
    

# COMMAND ----------


