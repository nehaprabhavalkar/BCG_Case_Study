from app.code import analyze_data 
from app.utils.helper import create_spark_session, load_config

jobs = {
    'analyze_data': analyze_data.process
}

def run(params):
    print("Params:", params)
    app_name = params['job_name']
    config_path = params['path']

    spark = create_spark_session(app_name)
    config = load_config(config_path)

    process_function = jobs[app_name]
    process_function(spark=spark, config=config)

    #print("Neha")
