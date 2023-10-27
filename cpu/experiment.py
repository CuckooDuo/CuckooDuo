import argparse
import subprocess
import csv
import re
import pandas as pd
import sys

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


def change_parameters(header_file_path, cpp_file_path, **kwargs):
    # Update the header file with parameters
    with open(header_file_path, 'r') as f:
        content = f.readlines()

    for key, value in kwargs.items():
        if key == 'lib': # Exclude non-parameter arguments
            continue
        for idx, line in enumerate(content):
            if line.strip().startswith(f"#define {key} "):
                parts = line.split()
                if len(parts) >= 3:
                    comment = " ".join(parts[3:])
                    content[idx] = f"#define {key} {value} {comment}\n"
                else:
                    content[idx] = f"#define {key} {value}\n"
                break

    with open(header_file_path, 'w') as f:
        f.writelines(content)

    # Update the cpp file with library includes
    library_name = kwargs['lib']
    change_cpp_includes(cpp_file_path, library_name)



def sync_output_run_commands(verbose=True):
    try:
        # Run the make command
        subprocess.check_call(["make"])
        print("Make command executed successfully!")
        
        # Run ./main and capture its output
        process = subprocess.Popen(["./main"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output_lines = []
        while True:
            line = process.stdout.readline()
            if line:
                if verbose:
                    print(line.decode('utf-8').strip())  # Print in real-time only if verbose is True
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

def write_to_csv(data, csv_path, Xaixs):
    with open(csv_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Writing the header
        first_item = next(iter(data.items()))
        header = [Xaixs] + sorted(first_item[1].keys())
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

    parser.add_argument('-lib', '--libraries', nargs='+', help='List of libraries to include. e.g. cuckoo_group_dfs_random')
    parser.add_argument("-fig", type=str, help="to generate the data of 6{abcdefg}")


    args = parser.parse_args()
    parameters = vars(args)
    fig = parameters['fig']
    parameters.pop('fig')

    filtered_parameters = {k: v for k, v in parameters.items() if v is not None}
    header_file_path = cpp_file_path = None

    
    if '6a' in fig:
        header_file_path = "cuckoo_group_bfs_twohash.h"  # Replace with your header file path
        cpp_file_path = "bfs_test_twohash.cpp"  # Replace with your cpp file path
        print(f'header:{header_file_path}\n\
                cpp:{cpp_file_path}')
    
        csv_data = {}
        for N,M in zip([4,8,12,16], [3,6,9,12]):
            filtered_parameters['N'] = N
            filtered_parameters['M'] = M
            filtered_parameters['L'] = N
            filtered_parameters['maxNL'] = N
            filtered_parameters['lib'] = header_file_path

            print(filtered_parameters)
            change_parameters(header_file_path, cpp_file_path, **filtered_parameters)
    
            output = sync_output_run_commands(verbose=False)
            parsed_output = parse_output_with_ansi(output)
            print(parsed_output)

            NM_key = f"{N},{M}"
            csv_data[NM_key] = parsed_output
            
        print(csv_data)
        # Writing the results to a CSV file
        write_to_csv(csv_data, "results.csv", "table_size")

        filtered_parameters['N'] = filtered_parameters['L'] = filtered_parameters['maxNL'] = 8
        filtered_parameters['M'] = 6
        change_parameters(header_file_path, cpp_file_path, **filtered_parameters)

    if '6b' in fig:
        header_file_path = "cuckoo_group_bfs_twohash.h"  # Replace with your header file path
        cpp_file_path = "bfs_test_twohash.cpp"  # Replace with your cpp file path
        print(f'header:{header_file_path}\n\
                cpp:{cpp_file_path}')
    
        csv_data = {}
        for sig in [1,2,4]:
            filtered_parameters['SIG_LEN'] = sig
            filtered_parameters['lib'] = header_file_path

            print(filtered_parameters)
            change_parameters(header_file_path, cpp_file_path, **filtered_parameters)
    
            output = sync_output_run_commands()
            parsed_output = parse_output_with_ansi(output)
            print(parsed_output)

            f_key = f"{sig}"
            csv_data[f_key] = parsed_output
            
        print(csv_data)
        # Writing the results to a CSV file
        write_to_csv(csv_data, "results.csv", "table_size")

        filtered_parameters['SIG_LEN'] = 2
        change_parameters(header_file_path, cpp_file_path, **filtered_parameters)

    if '6c' in fig:
        import pandas as pd
        data = {}
        for f in [8,16,24,32]:
            data[f] = []
        for f in [8,16,24,32]:
            for d in [4,8,12,16]:
                data[f].append(2*d / 2**f)
        
        data = pd.DataFrame(data)
        print(data)
        data_dict = data.to_dict(orient='records')

        csv_data = {}
        for i in range(len(data_dict)):
            csv_data[i]=data_dict[i]
        print(csv_data)

        write_to_csv(csv_data, "results.csv", "bucket_size")
        filtered_parameters['lib'] = None

    if '6d' in fig:
        header_file_path = "cuckoo_group_bfs_twohash.h"  # Replace with your header file path
        cpp_file_path = "bfs_test_twohash.cpp"  # Replace with your cpp file path
    
        csv_data = {}
        for header_file_path in ['cuckoo_group_bfs_twohash.h', 'cuckoo_group_dfs.h', 'cuckoo_group_dfs_random.h']:
            filtered_parameters['lib'] = header_file_path

            print(filtered_parameters)
            change_parameters(header_file_path, cpp_file_path, **filtered_parameters)
    
            output = sync_output_run_commands(verbose=False)
            parsed_output = parse_output_with_ansi(output)
            print(parsed_output)

            header_key = f"{header_file_path}"
            csv_data[header_key] = parsed_output

            change_cpp_includes(cpp_file_path, filtered_parameters['lib'],revert=True)
            
        print(csv_data)
        # Writing the results to a CSV file
        write_to_csv(csv_data, "results.csv", "table_size")
        filtered_parameters['lib'] = None

    if '6e' in fig:
        header_file_path = "cuckoo_group_bfs_twohash.h"  # Replace with your header file path
        cpp_file_path = "bfs_test_twohash.cpp"  # Replace with your cpp file path
    
        csv_data = {}
        import os

        for header_file_path, file in zip(['cuckoo_group_bfs_twohash_move_less.h', 'cuckoo_group_dfs.h', 'cuckoo_group_dfs_random.h']\
                                        ,['bfs','dfs','random']):
            filtered_parameters['lib'] = header_file_path

            print(filtered_parameters)
            change_parameters(header_file_path, cpp_file_path, **filtered_parameters)
            change_cpp_includes(cpp_file_path, '#define run_6e')
    
            output = sync_output_run_commands(verbose=False)
            parsed_output = parse_output_with_ansi(output)
            print(parsed_output)

            header_key = f"{header_file_path}"
            csv_data[header_key] = parsed_output

            change_cpp_includes(cpp_file_path, filtered_parameters['lib'],revert=True)
            change_cpp_includes(cpp_file_path, '#define run_6e',revert=True)
            sys.exit()

            #change cuckoo.csv
            folder_path = './'
            old_file_name = 'cuckoo.csv'
            new_file_name = f'cuckoo_{file}.csv'
            
            old_file_path = os.path.join(folder_path, old_file_name)
            new_file_path = os.path.join(folder_path, new_file_name)
            
            try:
                os.rename(old_file_path, new_file_path)
            except OSError as e:
                print(f'error: {e}')

        #calculate the data
        def merge_csv_files_v2(random_file, dfs_file, bfs_file):
            cuckoo_random = pd.read_csv(random_file)
            cuckoo_dfs = pd.read_csv(dfs_file)
            cuckoo_bfs = pd.read_csv(bfs_file)
            
            merged_df = pd.DataFrame({
                'load_factor': [i/100 for i in range(101)]
            })
            
            merged_df['sum_move_num_random'] = merged_df['load_factor'].map(cuckoo_random.set_index('load_factor')['sum_move_num'])
            merged_df['sum_move_num_dfs'] = merged_df['load_factor'].map(cuckoo_dfs.set_index('load_factor')['sum_move_num'])
            merged_df['sum_move_num_bfs'] = merged_df['load_factor'].map(cuckoo_bfs.set_index('load_factor')['sum_move_num'])
            
            return merged_df
        
        # merge csv
        merged_output = merge_csv_files_v2("cuckoo_random.csv", "cuckoo_dfs.csv", "cuckoo_bfs.csv")
        output_path_v2 = "draw6e.csv"
        merged_output.to_csv(output_path_v2, index=False)
        
        # generate csv
        datafile = u'draw6e.csv'
        data = pd.read_csv(datafile)
        
        # Given insertion amount
        insertion_amount = 300000
        
        # Calculating the difference in move numbers for consecutive load factors
        data['random_diff'] = data['sum_move_num_random'].diff()
        data['dfs_diff'] = data['sum_move_num_dfs'].diff()
        data['bfs_diff'] = data['sum_move_num_bfs'].diff()
        
        # Calculating the average move number for each load factor
        # Note: The first entry will be NaN because there is no previous point to compare with
        data['bfs_avg_move'] = (data['bfs_diff']) / insertion_amount
        data['dfs_avg_move'] = (data['dfs_diff']) / insertion_amount
        data['dfsRandom_avg_move'] = (data['random_diff']) / insertion_amount
        
        # Filter the data to only keep rows where load_factor is in the specified range
        desired_load_factors = [i/100 for i in range(72, 100, 3)]
        filtered_data = data[data['load_factor'].isin(desired_load_factors)]
        filtered_data[['load_factor','bfs_avg_move','dfs_avg_move','dfsRandom_avg_move']].to_csv('results.csv', index=False)
        filtered_parameters['lib'] = None

    if '6g' in fig:
        header_file_path = "cuckoo_group_bfs_twohash.h"  # Replace with your header file path
        cpp_file_path = "bfs_test_twohash.cpp"  # Replace with your cpp file path
    
        csv_data = {}
        for header_file_path in ['cuckoo_group_bfs_twohash.h', 'cuckoo_group_bfs.h']:
            filtered_parameters['lib'] = header_file_path

            print(filtered_parameters)
            change_parameters(header_file_path, cpp_file_path, **filtered_parameters)
    
            output = sync_output_run_commands(verbose=False)
            parsed_output = parse_output_with_ansi(output)
            print(parsed_output)

            header_key = f"{header_file_path}"
            csv_data[header_key] = parsed_output

            change_cpp_includes(cpp_file_path, filtered_parameters['lib'],revert=True)
            
        print(csv_data)
        # Writing the results to a CSV file
        write_to_csv(csv_data, "results.csv", "table_size")
        filtered_parameters['lib'] = None

    

    if filtered_parameters['lib'] is not None:
        change_cpp_includes(cpp_file_path, filtered_parameters['lib'],revert=True)