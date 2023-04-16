"""
This is a simulator for HBase.
Made by: Cayetano Molina 20211, Jose Monzon 20309, Mario de Leon TODO
"""
import os
import json


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
            json.dump(dic, file)
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


all_helps = json.load(open("help.json", "r"))
list_of_commands = list(all_helps.keys())
command_dict = {
    "create": create,
    "list": listing,
    "disable": disable,
    "enable": enable,
    "is_enabled": is_enabled,
    "describe": describe,

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
            print("Command not found, type 'help' for a list of commands")


if __name__ == "__main__":
    main()
