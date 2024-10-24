import subprocess
import csv

def run_6a():
    compile_command = ["g++", "6a_SingleCollide.cpp", "-o", "6a_SingleCollide"]
    subprocess.run(compile_command, check=True)
    
    result = subprocess.run(["./6a_SingleCollide"], capture_output=True, text=True).stdout.strip()
    
    return result

run_6a()
print("Results saved to 6a.csv")
