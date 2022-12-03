import yaml 
from yaml.loader import SafeLoader
from pyspark.sql import SparkSession
import os 

PATH_TO_CONFIG = "/app/conf/config.yaml"

def load_config(path):
    file = open(str(os.getcwd()) + PATH_TO_CONFIG, 'r')
    config_data = yaml.load(file, Loader=SafeLoader)
    return config_data 

def create_spark_session(app_name):
    spark = SparkSession.builder \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()      
    
    return spark
