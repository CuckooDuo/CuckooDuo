import subprocess
import csv

def run_6d():
    compile_command = ["g++", "6d_BFSfailure.cpp", "-o", "6d_BFSfailure"]
    subprocess.run(compile_command, check=True)
    
    result = subprocess.run(["./6d_BFSfailure"], capture_output=True, text=True).stdout.strip()
    
    return result

run_6d()
print("Results saved to 6d.csv")
