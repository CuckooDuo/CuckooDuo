import argparse
import subprocess
import csv
import re
import pandas as pd
import sys

def modify_cpp_file(file_path, revert=False):
    """Modify or revert the bfs_test_twohash.cpp file based on the revert flag."""
    with open(file_path, 'r') as f:
        lines = f.readlines()

    new_lines = []
    for line in lines:
        if revert:
            # Revert changes: Comment out the 'if (cuckoo.tcam_num > old_cache)' line
            if 'if (cuckoo.tcam_num > old_cache)' in line:
                new_lines.append('//' + line)
            # Revert changes: Uncomment the 'if (insert_success_count ...' line
            elif 'if (insert_success_count % (test_num/100) == 0)' in line and '//' in line:
                new_lines.append(line.replace('//', ''))
            else:
                new_lines.append(line)
        else:
            # Apply changes: Comment out the 'if (insert_success_count ...' line
            if 'if (insert_success_count % (test_num/100) == 0)' in line:
                new_lines.append('//' + line)
            # Apply changes: Uncomment the 'if (cuckoo.tcam_num > old_cache)' line
            elif 'if (cuckoo.tcam_num > old_cache)' in line and '//' in line:
                new_lines.append(line.replace('//', ''))
            else:
                new_lines.append(line)

    with open(file_path, 'w') as f:
        f.writelines(new_lines)

def change_cpp_includes(file_path, library_to_include, revert=False):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    new_lines = []
    for line in lines:
        if library_to_include in line:
            if revert:
                # Ensure the line is not already commented
                if not line.strip().startswith('//'):
                    new_lines.append('//' + line.strip() + '\n')
                else:
                    new_lines.append(line)
            else:
                new_lines.append(line.replace('//', '').strip() + '\n')
        else:
            new_lines.append(line)

    with open(file_path, 'w') as f:
        f.writelines(new_lines)

def execute_with_header(header_file, output_file):
    """Change the header file and execute ./main with redirection."""
    change_cpp_includes("bfs_test_twohash.cpp", header_file)
    change_cpp_includes("bfs_test_twohash.cpp", '#define run_6e')
    subprocess.check_call(["make"])
    print("Make command executed successfully!")

    subprocess.run(["./main"], stdout=open(output_file, "w"))
    change_cpp_includes("bfs_test_twohash.cpp", header_file, revert=True)
    change_cpp_includes("bfs_test_twohash.cpp", '#define run_6e', revert=True)

def extract_values_from_file(filename):
    """Extract inserted and cache values from a given file."""
    with open(filename, "r") as file:
        lines = file.readlines()
    
    values = []
    for line in lines:
        if 'cache=' in line:
            inserted_value = re.search(r'inserted=(\d+)', line)
            cache_value = re.search(r'cache=(\d+)', line)
            if inserted_value and cache_value:
                values.append((int(inserted_value.group(1)), int(cache_value.group(1))))
    return values

def save_to_csv(basic_values, dual_values, csv_filename):
    """Save the extracted values to a CSV file."""
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = ['onehash_inserted', 'onehash_cache', 'twohash_inserted', 'twohash_cache']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for idx in range(max(len(basic_values), len(dual_values))):
            row = {}
            if idx < len(basic_values):
                row['onehash_inserted'] = basic_values[idx][0]
                row['onehash_cache'] = basic_values[idx][1]
            if idx < len(dual_values):
                row['twohash_inserted'] = dual_values[idx][0]
                row['twohash_cache'] = dual_values[idx][1]
            writer.writerow(row)

def main():
    # Modify the bfs_test_twohash.cpp file
    modify_cpp_file("bfs_test_twohash.cpp", revert=False)
    
    # Execute with cuckoo_group_bfs_twohash.h header and redirect to dual.txt
    execute_with_header("cuckoo_group_bfs_twohash.h", "dual.txt")
    
    # Execute with cuckoo_group_bfs.h header and redirect to basic.txt
    execute_with_header("cuckoo_group_bfs.h", "basic.txt")

    # Revert changes to the bfs_test_twohash.cpp file
    modify_cpp_file("bfs_test_twohash.cpp", revert=True)
    
    basic_values = extract_values_from_file("basic.txt")
    dual_values = extract_values_from_file("dual.txt")
    save_to_csv(basic_values, dual_values, "extracted_values.csv")

    

# Call the main function to execute the program
main()

