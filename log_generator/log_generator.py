import random
from datetime import datetime, timedelta
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
output_folder = os.path.join(base_dir, "..", "data", "raw_logs", "generated_logs")

os.makedirs(output_folder, exist_ok=True)


log_levels = ["INFO", "WARNING", "ERROR"]
servers = ['SERVER_1', 'SERVER_2','SERVER_3']


def generate_log(time_stamp):
    
    server = random.choice(servers)
    
    cpu = random.randint(20,95)
    memory = round(random.uniform(1.0, 4.0),2)   # in gb
    response_time = random.randint(100, 1000)    # in ms
    requests = random.randint(50,500)
    
    if cpu > 85 and response_time > 700:
        level = "ERROR"
    elif cpu > 65:
        level = "WARNING"
    else:
        level = "INFO"
        
        
    log = f"{time_stamp} | {server} | {level} | Response Time = {response_time}ms | CPU={cpu}% | Memory={memory}GB | Requests={requests}"
    return log

start_time = datetime.now()

files = 5
logs_per_file = 250000

for file_no in range(files):
    file_path = os.path.join(
        output_folder,
        f"system_logs_{file_no + 1}.log"
    )
    
    with open(file_path, "w") as file:
        for i in range(logs_per_file):
            current_time = start_time + timedelta(seconds=i)
            file.write(generate_log(current_time) + "\n")
        
print("log file has been successfully generated")