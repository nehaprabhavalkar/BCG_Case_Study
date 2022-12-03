import yaml 
from yaml.loader import SafeLoader
from pyspark.sql import SparkSession


def load_config(path):
    file = open(str(os.getcwd()) + '/app/conf/config.yaml', 'r')
    config_data = yaml.load(file, Loader=SafeLoader)
    return config_data 

def create_spark_session(app_name):
    spark = SparkSession.builder \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()      
    
    return spark
