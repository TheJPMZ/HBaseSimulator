"""
This is a simulator for HBase.
Made by: Cayetano Molina 20211, Jose Monzon 20309, Mario de Leon 19019
"""
import os
import json
import re
import datetime
import calendar
import tempfile
import names
import random

columnas = []


def i_get_table(table_name):
    file_path = os.path.join("database", table_name + ".json")

    try:
        with open(file_path, "r") as file:
            dic = json.load(file)
    except FileNotFoundError:
        print("Table not found, use 'list' to see the list of tables")
        return False

    return dic, file_path

    

def i_toggle(table_name, status):
    if not (memes := i_get_table(table_name)):
        return

    dic, file_path = memes

    if dic["info"]["_status"] == status:
        print(f"Table already {status}")
    else:
        dic["info"]["_status"] = status
        with open(file_path, "w") as file:
            json.dump(dic, file, indent=2)
        print(f"Table {table_name} {status}")


def listing():
    print("List of tables:")
    for filename in os.listdir("database"):
        print(f"- {filename[:-5]}")


def create(table_name, *args):
    dic = {
        "info": {"_status": "enabled"},
        "data": {}
    }

    for arg in args:
        dic["info"][arg] = {"VERSIONS": 1}

    file_path = os.path.join("database", table_name + ".json")

    with open(file_path, "w") as file:
        json.dump(dic, file)

def drop(table_name):
    
    tables = os.listdir("database")
    if table_name+".json" in tables:
        if is_enabled(table_name):
            print(f"- {table_name} is still enable, please disable it before dropping")
        else:
            os.remove("database/" + table_name + ".json")
    else:
        print(f"{table_name} was not found")
        
def dropall(regex):
    
    
    tables = os.listdir("database")
    if regex:
        r = re.compile(regex)
        matched_tables = list(filter(r.match, tables))
        if matched_tables:
            for i in matched_tables:
                print(i[:-5])
            print(f"drop the above {len(matched_tables)} (y/n)?")
            option = input(">>> ")
            if option== "y":
                for i in matched_tables:
                    os.remove("database/" + i + ".json")
                print(f"{len(matched_tables)} were successfully dropped\n")
            else:
                print("no tables were dropped\n")
        else:
            print("No tables found with the regex provided\n")
    else:
        for i in tables:
                print(i[:-5])
        print(f"drop the above {len(matched_tables)} (y/n)?")
        option = input(">>> ")
        if option== "y":
            for i in tables:
                os.remove("database/" + i + ".json")
            print(f"{len(tables)} were successfully dropped\n")
        else:
            print("no tables were dropped\n")

def get(table_name, *args):
    print(table_name, *args)
    print(args)
    row_name = args[0]
    if not (file_info := i_get_table(table_name)):
        return
    
    if is_enabled(table_name)==False:
        print(f"- {table_name} is disabled, no actions allowed on this table")
        return
    
    dic = file_info[0]
    data = dic["data"]
    if row_name in data.keys():
        print("\n\tCOLUMNS\t\t\tCELL")
        for col_fam, cols in data[row_name].items():
            for key, val in cols.items():
                if val[0]:
                    print(f"{col_fam}:{key}\t\tTimestamp {val[-1][1]}, \t value={val[-1][0]}")
                else:
                    print(f"{col_fam}:{key}\t\tTimestamp=null, \t value=null")
    else:
        print("Row not found")
    print(" ")

def put(table_name, *args):
    
 
    print(f"put {table_name}, {args}")
    if len(args)!= 3:
        print(f"Invalid arguments")
        return   
    row, column, value = args
    col_fam = column.split(":")[0]
    col = column.split(":")[1]
    
    date = datetime.datetime.utcnow()
    utc_time = calendar.timegm(date.utctimetuple())
    
    try:
        value = float(value)
    except:
        value = value

        

    
    if not (file_info := i_get_table(table_name)):
        return
    
    if is_enabled(table_name)==False:
        print(f"- {table_name} is disabled, no actions allowed on this table")
        return

    dic, file_path = file_info
    data=dic["data"]
    info = dic["info"]
    
    
        

    
    if row in data.keys():
        if col_fam in data[row].keys():
            col_fam_versions = dic["info"][col_fam]["VERSIONS"]

            if col in data[row][col_fam].keys():
                if len(dic["data"][row][col_fam][col]) < col_fam_versions:
                    dic["data"][row][col_fam][col].append([value, utc_time])
                else:
                    dic["data"][row][col_fam][col].append([value, utc_time])
                    dic["data"][row][col_fam][col].pop(0)
                print(f"Value added successfully to row {row}\n")
                with open(file_path, "w") as file:
                    json.dump(dic, file, indent=2)
                
            else:
                dic["data"][row][col_fam][col] = [[value, utc_time]]
                print(f"Column created {col}")
                print(f"Value added successfully to row {row}\n")
                with open(file_path, "w") as file:
                    json.dump(dic, file, indent=2)
        else:
            if col_fam in info.keys():
                col_fam_versions = dic["info"][col_fam]["VERSIONS"]
                datos = {
                            col: [[value, utc_time]]
                        }
                
                dic["data"][row][col_fam] = datos
                with open(file_path, "w") as file:
                    json.dump(dic, file, indent=2)
                    
                print(f"Value added successfully to row {row}\n")
                    
                
            else:
                print("Column family not found")
        
    else:
        if col_fam in info.keys():
            datos = {col_fam:
                        {
                            col: [[value, utc_time]]
                        }
                    }
            dic["data"][row] = datos
            with open(file_path, "w") as file:
                json.dump(dic, file, indent=2)
            print("New row created with values given")
        else:
            print("Column family not found")
        
def alter(table_name, *args):
    vars = list(args)
    print(vars)
    vars = list(filter(lambda x: x!="=>", vars))[1:]
    col_name, prop, value = vars
    
    try:
        value = int(value)
    except:
        value = value
    
    if not (file_info := i_get_table(table_name)):
        return
    
        
    if is_enabled(table_name)==False:
        print(f"- {table_name} is disabled, no actions allowed on this table")
        return
    
    dic, file_path = file_info
    info = dic["info"]
    file = open(file_path, "w")
    if col_name in info.keys():
        dic["info"][col_name][prop] = value
        with open(file_path, "w") as file:
            json.dump(dic, file, indent=2)
            
        print(f"{prop} successfully changed in {col_name}")
        
    else:
        print("Column family not found")
    
def domany(table_name):
    inp = " "
    ops = []
    if not (i_get_table(table_name)):
        return
    while inp:
        inp = input(">>>>>> ")
        arguments = inp.split(" ")
        if inp:
            ops.append(arguments)
    print(" ")
    for i in ops:
        put(table_name, *i)
        print(" ")
    
def disable(table_name):
    i_toggle(table_name, "disabled")


def enable(table_name):
    i_toggle(table_name, "enabled")


def is_enabled(table_name):
    if not (memes := i_get_table(table_name)):
        return

    dic, file_path = memes

    if dic["info"]["_status"] == "disabled":
        print("Table is disabled")
        return False
    print("Table is enabled")
    return True

def describe(table_name):
    if not (memes := i_get_table(table_name)):
        return

    dic, file_path = memes

    print("Table: " + table_name)
    print("Status: " + dic["info"]["_status"])
    print("Columns:")
    for key, value in dic["info"].items():
        if key != "_status":
            print(f"\t{key} => {value}".replace(":", "->"))
            
def scan(table_name):
    if not (memes := i_get_table(table_name)):
        return

    dic, file_path = memes

    print(f"All rows in table {table_name}:")
    for row_key in dic["data"].keys():
        row = dic["data"][row_key]
        print(f"Row: {row_key}")
        for column_key in row.keys():
            print(f"\tColumn: {column_key} => Value: {row[column_key]}")



def count(table_name):
    if not (memes := i_get_table(table_name)):
        return
    dic, file_path = memes
    count = len(dic["data"])
    print(f"Table {table_name} has {count} rows")


def truncate(table_name):
    disable(table_name)
    if not (memes := i_get_table(table_name)):
        return
    dic, file_path = memes
    dic["data"] = {}
    with open(file_path, "w") as file:
        json.dump(dic, file, indent=2)
    print(f"{table_name} has been truncated.")


def retruncate(table_name):
    disable(table_name)
    if not (memes := i_get_table(table_name)):
        return
    if is_enabled(table_name)==False:
        print(f"- {table_name} is disabled, no operations allowed on this table")
        return
    
    dic, file_path = memes
    with open(file_path, "r") as file:
        table_data = json.load(file)
    # Temp file
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        json.dump(table_data, temp_file, indent=2)
        temp_file.flush()
        temp_file_path = temp_file.name
    # Delete table json
    new_table_name = table_name
    os.remove(file_path)
    # Recreate table
    create(new_table_name, *dic["info"].keys())
    # Copy the data back from the temp file
    with open(temp_file_path, "r") as temp_file:
        temp_data = json.load(temp_file)
    with open(file_path, "w") as file:
        json.dump(temp_data, file, indent=2)
    # Delete temp file
    os.remove(temp_file_path)
    enable(new_table_name)
    print(f"{table_name} has been truncated.")


# delete <table_name> <Robert> <personal:pet> <timestamp>
def delete(table_name, *args):
    num_args = len(args)
    if num_args != 3:
        print("Invalid arguments. Usage: delete <table_name> <row_key> <column_family>:<column_qualifier> <timestamp>")
    else:
        if not (memes := i_get_table(table_name)):
            return
        
        if is_enabled(table_name)==False:
            print(f"- {table_name} is disabled, no actions allowed on this table")
            return
        
        dic, file_path = memes
        row_key = args[0]
        column_family, column_qualifier = args[1].split(":")
        timestamp = args[2]
        if row_key not in dic["data"]:
            print("Row key not found")
            return
        if column_family not in dic["data"][row_key]:
            print("Column family not found")
            return
        if column_qualifier not in dic["data"][row_key][column_family]:
            print("Column qualifier not found")
            return
        for idx, cell in enumerate(dic["data"][row_key][column_family][column_qualifier]):
            
            if cell:
                if cell[1] == int(timestamp):
                    del dic["data"][row_key][column_family][column_qualifier][idx]
        with open(file_path, "w") as file:
            json.dump(dic, file, indent=2)
            print("Deleted successfully")
        


# deleteall <table_name> <Geoffrey>
def deleteall(*args):
    if len(args) != 2:
        print("Invalid arguments. Usage: deleteall <table_name> <row_key>")
    else:
        table_name, row_key = args[:2]
        if not (memes := i_get_table(table_name)):
            return
        
        if is_enabled(table_name)==False:
            print(f"- {table_name} is disabled, no actions allowed on this table")
            return
        
        dic, file_path = memes
        if row_key not in dic["data"]:
            print(f"Row key '{row_key}' not found in table '{table_name}'")
            return
        del dic["data"][row_key]
        with open(file_path, "w") as file:
            json.dump(dic, file, indent=2)
        print(
            f"All cells in row '{row_key}' of table '{table_name}' have been deleted.")

all_helps = json.load(open("help.json", "r"))
list_of_commands = list(all_helps.keys())
command_dict = {
    "create": create,
    "list": listing,
    "disable": disable,
    "enable": enable,
    "is_enabled": is_enabled,
    "describe": describe,
    "drop": drop,
    "dropall": dropall,
    "get": get,
    "put": put,
    "alter": alter,
    "delete": delete,
    "deleteall": deleteall,
    "scan": scan,
    "count": count,
    "truncate": truncate,
    "retruncate": retruncate,
    "domany": domany

    # Para agregar una funcion solamente se agrega el nombre de la funcion y el nombre del comando
}


def show_help(command=None):
    print("--------------------")
    if command:
        print("Help for command: " + f"{command}".upper())
        print("- Description: " + all_helps[command]["desc"])
        print(f"- Usage: {command} " + all_helps[command]["args"])
    else:
        print("For more detail, type '<command>?'")
        print("List of commands:")
        for command, help in all_helps.items():
            print(f"\t{command}: {help['desc']}")
    print("--------------------")


def main():
    print("==== Welcome to HBase Simulator ====")
    print("Type 'help or ?' for a list of commands")
    print("To get more information about a command, type '<command>?'")
    print("To exit, type 'exit' or press Ctrl+C")

    running = True
    while running:

        user_input = input(">>> ")

        command_list = user_input.split(" ")

        command, *args = command_list

        if command in "help?":
            show_help()
        elif command == "exit":
            running = False
        elif command[-1] == "?":
            if command[:-1] in list_of_commands:
                show_help(command[:-1])
            else:
                print("Command not found")
        elif command in list_of_commands:
            try:
                command_dict[command](*args)
            except TypeError:
                print(f"Invalid arguments, type '{command}?' for more information ")
        else:
            print(command)
            print("Command not found, type 'help' for a list of commands")


if __name__ == "__main__":
    #for i in range(1000):
    #    pet_name = ['dog', 'cat']
    #    departamento = ['sales', 'hr', 'marketing', 'engineering']
    #    name = names.get_first_name()
    #    put(*f"employees2 {name} personal:age {random.randint(20,60)}".split(" "))
    #    put(*f"employees2 {name} personal:pet {random.choice(pet_name)}".split(" "))
    #    put(*f"employees2 {name} professional:department {random.choice(departamento)}".split(" "))
    #    put(*f"employees2 {name} professional:salary {random.randint(20000, 100000)}".split(" "))
        
    main()
