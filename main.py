from app.run import run 
import sys 
import ast 

# spark-submit main.py "{'job_name': 'analyze_data', 'path': ''}"

if __name__ == "__main__":
    str_params = sys.argv[1]
    params = ast.literal_eval(str_params)

    module_run = run(params)