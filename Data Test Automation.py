# Databricks notebook source
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


        self.dataframe = self.readData()
        #print(self.dataframe)
        #print(self.dataframe.schema)
        if self.expected_schema is not None:
            self.schemaResult = self.validateSchema()
            #print(self.schemaResult)

            if self.changeDataType is True:
                self.dataframe = self.changeSchema()
                #print(self.dataframe.schema)

        
        
        self.dataprofiling = self.dataProfiling()
        #print(self.dataprofiling)


    def readData(self):

        """
        Read data from different sources with configurable parameters.
        
        Returns:
            pyspark.sql.DataFrame: DataFrame containing the data from the specified source.
        """    
        # Read data based on the source type
        reader = spark.read.format(self.source_type)
        if self.options is not None:
            reader = reader.options(**self.options)
        if self.source_path is not None:
            reader = reader.load(self.source_path)

        df = reader
        return df
    
    def validateSchema(self):

        """
        Validate the actual schema of a DataFrame against the expected schema.
        Identify mismatched datatypes, missing columns, or extra columns.
            
        Returns:
            dict: Validation result with details of mismatched datatypes, missing columns, and extra columns.
        """
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

        """
        Change the current schema to the expected schema provided.
        
        Returns:
            pyspark.sql.DataFrame: DataFrame containing the datatype of the expected schema.

        """
        actual_schema = self.dataframe.schema
        schemaValidatedDF = self.dataframe
        for field in self.expected_schema.fields:
            if field.name in actual_schema.fieldNames():
                schemaValidatedDF = schemaValidatedDF.withColumn(field.name,col(field.name).cast(field.dataType))
        return schemaValidatedDF 
            

    def dataProfiling(self):

        '''
            Checks the null counts, empty strings, stastical description and counts of the number of rows.

            Returns:
                dict: Data profiling results including total row count, null counts, null count percentage,
                  empty string counts, and statistical descriptions for each column.

        '''
        
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
        print(validation_results)
        return validation_results


    def generateReport(self):

        """
        Generate a report summarizing the data validation results.

        Returns:
            str: Report containing information about the total row count, null counts, null count percentage,
                 empty string counts, and statistical descriptions for each column.

        """

        report = f"Data Report:\n\n"
        report += f"Total Number of Rows: {self.dataprofiling['Total Number of rows']}\n\n"
        report += f"Null Counts:\n"
        for column, count in self.dataprofiling['nullCounts'].items():
            report += f"  - {column}: {count}\n"
        report += "\n"
        report += f"Null Count Percentage:\n"
        for column, percentage in self.dataprofiling['null count percentage'].items():
            report += f"  - {column}: {percentage}\n"
        report += "\n"
        report += f"Empty String Counts:\n"
        for column, count in self.dataprofiling['empty_string'].items():
            report += f"  - {column}: {count}\n"
        report += "\n"
        report += f"Statistics:\n"
        for column, stats in self.dataprofiling['stats'].items():
            report += f"  - {column}:\n"
            for stat, value in stats.items():
                report += f"    - {stat}: {value}\n"
        
        return report
    

    



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


# COMMAND ----------


