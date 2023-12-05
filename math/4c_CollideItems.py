import subprocess
import csv

testResultDir = "4c.csv";

def run_with_bucket_size(bucket_size, fp_len):
    compile_command = ["g++", f"-D MY_BUCKET_SIZE={bucket_size}", f"-D MY_FP_LEN={fp_len}", "4b_4c_CollideItems.cpp", "-o", "4b_4c_CollideItems"]
    subprocess.run(compile_command, check=True)
    
    result = subprocess.run(["./4b_4c_CollideItems"], capture_output=True, text=True).stdout.strip()

    return result

bucket_sizes = [4, 8, 12, 16]

with open(testResultDir, "w", newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["bucket_size", "Experimental", "Upper bound"]) 

    for size in bucket_sizes:
        result = run_with_bucket_size(size, 16)
        csvwriter.writerow([size] + result.split(',')) 

print("Results saved to "+testResultDir)
