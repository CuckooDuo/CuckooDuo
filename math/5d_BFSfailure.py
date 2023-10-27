import subprocess
import csv

def run_5d():
    compile_command = ["g++", "5d_BFSfailure.cpp", "-o", "5d_BFSfailure"]
    subprocess.run(compile_command, check=True)
    
    result = subprocess.run(["./5d_BFSfailure"], capture_output=True, text=True).stdout.strip()
    
    return result

run_5d()
print("Results saved to 5d.csv")
