import subprocess
import csv

def run_5a():
    compile_command = ["g++", "5a_SingleCollide.cpp", "-o", "5a_SingleCollide"]
    subprocess.run(compile_command, check=True)
    
    result = subprocess.run(["./5a_SingleCollide"], capture_output=True, text=True).stdout.strip()
    
    return result

run_5a()
print("Results saved to 5a.csv")
