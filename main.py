"""
This is a simulator for HBase.
Made by: Cayetano Molina 20211, Jose Monzon 20309, Mario de Leon TODO
"""
import os
import json


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

all_helps = json.load(open("help.json", "r"))
list_of_commands = list(all_helps.keys())
command_dict = {
    "create": create,
    "list": listing,
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

    print("Welcome to HBase Simulator")
    print("Type 'help or ?' for a list of commands")
    print("To get more information about a command, type '<command>?'")
    print("To exit, type 'exit' or 'quit'")

    running = True
    while running:

        user_input = input(">>> ")

        command_list = user_input.split(" ")

        command, *args = command_list

        if command == "help" or command == "?":
            show_help()
        elif command == "exit" or command == "quit":
            running = False
        elif command[-1] == "?":
            if command[:-1] in list_of_commands:
                show_help(command[:-1])
            else:
                print("Command not found")
        elif command in list_of_commands:
            command_dict[command](*args)




        else:
            print("Command not found, type 'help' for a list of commands")

if __name__ == "__main__":
    main()