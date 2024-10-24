import subprocess
import csv

def run_4a():
    compile_command = ["g++", "4a_SingleCollide.cpp", "-o", "4a_SingleCollide"]
    subprocess.run(compile_command, check=True)
    
    result = subprocess.run(["./4a_SingleCollide"], capture_output=True, text=True).stdout.strip()
    
    return result

run_4a()
print("Results saved to 4a.csv")
