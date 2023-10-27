import argparse
import subprocess
import csv
import re

def change_parameters(file_path, **kwargs):
    with open(file_path, 'r') as f:
        content = f.readlines()

    for key, value in kwargs.items():
        for idx, line in enumerate(content):
            if line.strip().startswith(f"#define {key} "):
                parts = line.split()
                if len(parts) >= 3:
                    # Keeping the comment if it exists
                    comment = " ".join(parts[3:])
                    content[idx] = f"#define {key} {value} {comment}\n"
                else:
                    content[idx] = f"#define {key} {value}\n"

                break  # Break the inner loop once a match is found for this line


    with open(file_path, 'w') as f:
        f.writelines(content)

def sync_output_run_commands():
    try:
        # Run the make command
        subprocess.check_call(["make"])
        print("Make command executed successfully!")
        
        # Run ./main and capture its output while also printing it in real-time
        process = subprocess.Popen(["./main"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output_lines = []
        while True:
            line = process.stdout.readline()
            if line:
                print(line.decode('utf-8').strip())  # Print in real-time
                output_lines.append(line.decode('utf-8'))
            if process.poll() is not None:
                break
        
        print("./main executed successfully!")
        return "".join(output_lines)
    except subprocess.CalledProcessError:
        print("Error occurred while executing commands.")
        return ""

def parse_output_with_ansi(output):
    # Remove ANSI escape codes
    ansi_escape = re.compile(r'\x1B[@-_][0-?]*[ -/]*[@-~]')
    cleaned_output = ansi_escape.sub('', output)
    
    lines = cleaned_output.splitlines()
    results = {}
    current_entry_num = None
    for line in lines:
        if "entry num:" in line:
            current_entry_num = int(line.split(":")[1].strip())
        if "load factor:" in line and current_entry_num:
            results[current_entry_num] = float(line.split(":")[1].strip())
    return results

def write_to_csv(data, csv_path):
    with open(csv_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Writing the header
        first_item = next(iter(data.items()))
        header = ["N,M"] + sorted(first_item[1].keys())
        writer.writerow(header)
        # Writing the data
        for NM, values in data.items():
            row = [NM] + [values[k] for k in sorted(values.keys())]
            writer.writerow(row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Change parameters in a C++ header file.")
    parser = argparse.ArgumentParser(description="Change parameters in a C++ header file.")
    parser.add_argument("-KEY_LEN", type=str, help="Set the value for KEY_LEN")
    parser.add_argument("-VAL_LEN", type=str, help="Set the value for VAL_LEN")
    parser.add_argument("-emptyIndex", type=str, help="Set the value for emptyIndex")
    parser.add_argument("-indexTypeNotFound", type=str, help="Set the value for indexTypeNotFound")
    parser.add_argument("-N", type=str, help="Set the value for N")
    parser.add_argument("-M", type=str, help="Set the value for M")
    parser.add_argument("-L", type=str, help="Set the value for L")
    parser.add_argument("-SIG_LEN", type=str, help="Set the value for SIG_LEN")
    parser.add_argument("-TCAM_SIZE", type=str, help="Set the value for TCAM_SIZE")
    parser.add_argument("-TABLE1", type=str, help="Set the value for TABLE1")
    parser.add_argument("-TABLE2", type=str, help="Set the value for TABLE2")
    parser.add_argument("-rebuildError", type=str, help="Set the value for rebuildError")
    parser.add_argument("-rebuildOK", type=str, help="Set the value for rebuildOK")
    parser.add_argument("-rebuildNeedkick", type=str, help="Set the value for rebuildNeedkick")
    parser.add_argument("-rebuildNeedTwokick", type=str, help="Set the value for rebuildNeedTwokick")
    
    args = parser.parse_args()
    parameters = vars(args)
    filtered_parameters = {k: v for k, v in parameters.items() if v is not None}
    csv_data = {}

    to_change = "cuckoo_group_bfs_twohash.h"
    print(f"to_change: {to_change}")

    for SIG_LEN in [2]:
        filtered_parameters['SIG_LEN'] = SIG_LEN
        print(filtered_parameters)
        change_parameters(to_change, **filtered_parameters)

        output = sync_output_run_commands()
        parsed_output = parse_output_with_ansi(output)

        csv_data[f"{filtered_parameters['SIG_LEN']}"] = parsed_output
        
    print(csv_data)
    # Writing the results to a CSV file
    write_to_csv(csv_data, "results.csv")
