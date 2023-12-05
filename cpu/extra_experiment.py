import argparse
import subprocess
import csv
import pandas as pd
import os

testResultDir = "result.csv";

def make5d(table_size):
    compile_command = ["g++", f"-D TEST_SLOTS={table_size}", "main4originCuckooCompare.cpp", "-o", "main4originCuckooCompare"]
    subprocess.run(compile_command, check=True)
    result = subprocess.run(["./main4originCuckooCompare"], capture_output=True, text=True).stdout.strip()
    return result

def make5e():
    compile_command = ["g++", f"main4originCuckooCompare.cpp", "-o", "main4originCuckooCompare"]
    subprocess.run(compile_command, check=True)
    result = subprocess.run(["./main4originCuckooCompare average_moved_items"], capture_output=True, text=True).stdout.strip()
    return result

def make5f(bucket_size, bfs_len):
    compile_command = ["g++", f"-D MY_BUCKET_SIZE={bucket_size}", "-D MY_BFSLEN={bfs_len}", "main4originCuckooCompare.cpp", "-o", "main4originCuckooCompare"]
    subprocess.run(compile_command, check=True)
    result = subprocess.run(["./main4originCuckooCompare"], capture_output=True, text=True).stdout.strip()
    return result

def make5i(sig_len):
    compile_command = ["g++", f"-D MY_SIGLEN={sig_len}", "main4originCuckooCompare.cpp", "-o", "main4originCuckooCompare"]
    subprocess.run(compile_command, check=True)
    result = subprocess.run(["./main4originCuckooCompare average_adjustments"], capture_output=True, text=True).stdout.strip()
    return result

parser = argparse.ArgumentParser(description='Call particular function based on the argument.')
parser.add_argument('-fig', type=str, help='Function to be called.')
args = parser.parse_args()

if args.fig == '5d':
    table_size = [5000000, 10000000, 15000000, 20000000, 25000000, 30000000]
    with open(testResultDir, "w", newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["table_size", "BFS(ours)", "DFS(cuckoo)"])
        for size in table_size:
            result = make5d(size)
            result = result.split(',')
            csvwriter.writerow([size] + [result[0], result[4]])
    print("Results saved to "+testResultDir)
elif args.fig == '5e':
    make5e()
elif args.fig == '5f':
    bucket_size = [4, 8, 16, 32]
    bfs_len = [1, 2, 3]
    with open(testResultDir, "w", newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["bucket_size", "L=1", "L=2", "L=3"])
        for size in bucket_size:
            row_result = []
            for l in bfs_len:
                result = make5f(size, l)
                result = result.split(',')
                row_result = row_result + [result[0]]
            csvwriter.writerow([size] + row_result)
    print("Results saved to "+testResultDir)
elif args.fig == '5i':
    file_path = './result.csv'
    if os.path.exists(file_path):
        os.remove(file_path)
    sig_len = [1, 2, 4]
    for l in sig_len:
        make5i(l)
        df = pd.read_csv('output.csv')
        col = df.iloc[:, 1]
        try:
            df_result = pd.read_csv('result.csv')
            df_result = pd.concat([df_result, col], axis=1)
        except FileNotFoundError:
            df_result = col.to_frame()
        df_result.to_csv('result.csv', index=False)
    new_row = pd.DataFrame([["load_factor", "f=8", "f=16", "f=32"]], columns=df_result.columns)
    df_result = pd.concat([new_row, df_result], ignore_index=True)
    df_result.to_csv('result.csv', index=False)