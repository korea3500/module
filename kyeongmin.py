import glob
import pandas as pd
import os
import io
import pyspark
from pyspark.sql import SparkSession
import pyspark


spark = SparkSession\
        .builder\
        .appName('temp_module')\
        .config('spark.some.config.option', 'some-value')\
        .getOrCreate()

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)


 
def read_vcf(path, engine):

    #define default engine 
    if engine == "" :
        engine = "pandas"


    with open(path, 'r') as f:
        lines = [l for l in f if not l.startswith('##')]

    if engine == 'pandas' :
        return pd.read_csv(
            io.StringIO(''.join(lines)),
            dtype={'#CHROM': str, 'POS': int, 'ID': str, 'REF': str, 'ALT': str,
                'QUAL': str, 'FILTER': str, 'INFO': str},
            sep='\t', engine = 'python'
        ).rename(columns={'#CHROM': 'CHROM'})

    elif engine == 'spark' :
        ### Create json file using spark
        # sparkContext로 객체 생성

        spark = SparkSession\
                .builder\
                .appName('Python Spark SQL basic example')\
                .config('spark.some.config.option', 'some-value')\
                .getOrCreate()

        sc = spark.sparkContext


        output_path = "./"
        input_df = spark.read.text(path)

        dtype={'#CHROM': str, 'POS': int, 'ID': str, 'REF': str, 'ALT': str,
                        'QUAL': str, 'FILTER': str, 'INFO': str},


        input_df = input_df.filter(~input_df.value.startswith("##"))
        # input_df = input_df.filter(~input_df.value.startswith("#"))

        input_df.coalesce(1).write.text(output_path) # header 정보 유지


    else :
        print("engine must be pandas or spark!")
        raise "engine must be pandas or spark!"

def createDirectory(directory): 
    try: 
        if not os.path.exists(directory): 
            os.makedirs(directory) 
    except OSError: 
        print("Error: Failed to create the directory.")
