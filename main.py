from app.run import run 
import sys 
import ast 

# spark-submit --master yarn --deploy-mode client main.py "{'job_name': 'analyze_data', 'path': 'app/conf/config.yaml'}"

if __name__ == "__main__":
    str_params = sys.argv[1]
    params = ast.literal_eval(str_params)

    module_run = run(params)

    # print("Neha")
    # print("Neha")
    # print("Neha")
    # print("Neha")
    # print("Neha")
    # print("Neha")
