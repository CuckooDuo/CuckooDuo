import subprocess
import csv

testResultDir = "result.csv";

def run_with_bucket_size(bucket_size):
    compile_command = ["g++", f"-D MY_BUCKET_SIZE={bucket_size}", "main.cpp", "-o", "main"]
    subprocess.run(compile_command, check=True)
    
    result = subprocess.run(["./main"], capture_output=True, text=True).stdout.strip()
    
    return result

bucket_sizes = [4, 8, 12, 16]

with open(testResultDir, "w", newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["bucket_size", "cuckoo", "MapEmbed", "RACE", "TEA", "cuckoo_SingleHash"])

    for size in bucket_sizes:
        result = run_with_bucket_size(size)
        csvwriter.writerow([size] + result.split(','))

print("Results saved to"+testResultDir)
