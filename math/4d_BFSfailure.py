import subprocess
import csv

def run_4d():
    compile_command = ["g++", "4d_BFSfailure.cpp", "-o", "4d_BFSfailure"]
    subprocess.run(compile_command, check=True)
    
    result = subprocess.run(["./4d_BFSfailure"], capture_output=True, text=True).stdout.strip()
    
    return result

run_4d()
print("Results saved to 4d.csv")
