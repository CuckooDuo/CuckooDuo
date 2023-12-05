import subprocess
import csv

testResultDir = "4c.csv";

# 定义一个函数来编译并运行 main.cpp
def run_with_bucket_size(bucket_size, fp_len):
    # 使用 subprocess 编译 main.cpp
    compile_command = ["g++", f"-D MY_BUCKET_SIZE={bucket_size}", f"-D MY_FP_LEN={fp_len}", "4b_4c_CollideItems.cpp", "-o", "4b_4c_CollideItems"]
    subprocess.run(compile_command, check=True)
    
    # 运行编译后的 main 程序并捕获输出
    result = subprocess.run(["./4b_4c_CollideItems"], capture_output=True, text=True).stdout.strip()
    
    # 返回捕获的输出
    return result

# 定义要测试的 bucket sizes
bucket_sizes = [4, 8, 12, 16]

# 创建一个 CSV 文件并写入数据
with open(testResultDir, "w", newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["bucket_size", "Experimental", "Upper bound"])  # 写入标题行

    for size in bucket_sizes:
        result = run_with_bucket_size(size, 16)
        csvwriter.writerow([size] + result.split(','))  # 写入 bucket_size 和对应的结果

print("Results saved to "+testResultDir)
