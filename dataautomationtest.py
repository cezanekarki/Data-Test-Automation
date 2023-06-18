# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
        self.report = self.main()
        print(self.report)

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
                if isinstance(column_data_type, (IntegerType, DoubleType, FloatType, LongType, DecimalType,NumericType,ShortType,ByteType)):
                    statistics = self.dataframe.select(col(columnNames)).describe().toPandas().set_index('summary').to_dict()[columnNames]
                    stasticalDescription[columnNames] = statistics  #Statistical Summary of the data
                else:
                    distinctValues = self.dataframe.select(col(columnNames)).distinct()
                    distinctValuesCount = distinctValues.count()
                    if distinctValuesCount <= 12:
                        distinctValue[columnNames] = {'Distinct Values':distinctValues, 'Count':distinctValuesCount}    #Distinct values and it's count

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


    def generateReport(self):

        """
        Generate a report summarizing the data validation results.

        Returns:
            str: Report containing information about overall evaluation of data.

        """
        try:
            report = f"Data Report:\n\n"
            report += f"Total Number of Rows: {self.dataprofiling['Total Number of rows']}\n\n"

            if self.expected_schema is None:
                pass
            else:
                report += f"Schema Validation Report:\n"
                for key,value in self.schemaResult.items():
                    report += f"    - {key}:{value}\n"
            report += "\n"
            report += "\n"
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
            report += f"Statistics:\n\n"
            for column, stats in self.dataprofiling['stats'].items():
                report += f"  - {column}:\n"
                for stat, value in stats.items():
                    report += f"    - {stat}: {value}\n"
            report += "\n"
            report += "\n"
            report += f"Distinct Values:\n"
            for column, data in self.dataprofiling['Distinct Values'].items():
                report += f"  - Column : "
                report += f" {data['Distinct Values'].toPandas().to_string(index=False)}\n"
                report += f"     -Count:\n  "
                report += f"        {data['Count']}\n"
        
            return report
        
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
            from fpdf import FPDF

            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            pdf.multi_cell(0, 10, txt=self.report)
            pdf.output(output_file)

        except Exception as e:
            raise Exception(f"Error occured while generating pdf, error: {str(e)}")
        
    def main(self):
        try:

            self.dataframe = self.readData()
            if self.expected_schema is not None:
                self.schemaResult = self.validateSchema()
                if self.changeDataType is True:
                    self.dataframe = self.changeSchema()
            self.dataprofiling = self.dataProfiling()
            self.report = self.generateReport()
            return self.report
        
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

range_validation_format = {
    "weight":(1800,5500)
}
a.run_range_validations(range_validation_format)

# COMMAND ----------

from pyspark.sql.types import NumericType

# COMMAND ----------

a.generatePDF("DOCS.pdf")

# COMMAND ----------


