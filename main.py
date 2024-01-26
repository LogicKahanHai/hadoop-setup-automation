import subprocess as sp
import json
import errors
import file_handling as fh
from modules.hadoop_master import HadoopMasterSetup
from modules.hadoop_setup import HadoopEnvSetup
from modules.set_env import SetEnv


# TODO: Change the start to while loop with a menu with the places I am exiting the program.
# TODO: Clean up the Print statements to properly guide the user.
# TODO: SSH password-less login setup if possible, or at least after that part.
# DONE: Master configuration.


class Main:
    def __init__(self) -> None:
        self.db = fh.FileHandling()

    def initial_setup(self):
        self.set_env = SetEnv(self.db)
        print("Checking if Java is Installed on the system...")
        self.set_env.check_java()
        print("System environment setup complete.")
        self.set_env.add_hadoop()
        print("Hadoop installation location added.")
        print("Editing the Hadoop environment files...")
        self.hadoop_setup = HadoopEnvSetup(self.db)
        self.hadoop_setup.edit_hadoop_env()
        self.hadoop_setup.edit_mapred_env()
        self.hadoop_setup.edit_yarn_env()
        print("Hadoop environment files edited.")

    def hadoop_master_setup(self):
        print("Please enter the Public IPv4 DNS of this machine: ", end="")
        master_dns = input()
        print(
            "Please enter the path to the directory where you want to store the NameNode data: ",
            end="",
        )
        name_dir = input()
        print(
            "Please enter the Public IPv4 DNS of the Resource Manager machine: ", end=""
        )
        resource_manager_dns = input()
        hadoop_master_setup = HadoopMasterSetup(
            main.db, master_dns, name_dir, resource_manager_dns
        )
        hadoop_master_setup.edit_core_site()
        hadoop_master_setup.edit_hdfs_site()
        hadoop_master_setup.edit_mapred_site()
        hadoop_master_setup.edit_yarn_site()
        print("Hadoop configuration files edited on the Master System.")


if __name__ == "__main__":
    main = Main()
    print("Welcome to the Hadoop Cluster Setup Script.")
    print("============================================")
    while True:
        print("\nIs this the Master Node? (y/n): ", end="")
        is_master = input()
        switcher = {"y": True, "n": False}
        is_master = switcher.get(is_master, "Invalid choice.")
        if is_master == "Invalid choice.":
            print("Invalid choice. Please try again.")
            continue
        elif not is_master:
            print("Please run this script on the Master Node ONLY.")
            print("Exiting program...")
            exit(0)

        print("\n\n")
        print("============================================")
        print("Please select an option from the menu below:")
        print(
            "If you are running this script for the first time, please select option 1, otherwise select the appropriate step."
        )
        print("1. Initial Setup")
        print("2. Hadoop Master Setup")
        print("3. Hadoop Slave Setup (After SSH password-less login is setup)")
        print("4. Exit")
        print("Enter your choice: ", end="")
        choice = input()
        if choice == "1":
            print("\n\n")
            print("============================================")
            print("Initial Setup Started...")
            main.initial_setup()
            print("Initial Setup Complete.")
            print("============================================")
            print("\n\n")
        elif choice == "2":
            print("\n\n")
            print("============================================")
            print("Hadoop Master Setup Started...")
            main.hadoop_master_setup()
            print("Hadoop Master Setup Complete.")
            print("============================================")
            print("\n\n")
        elif choice == "3":
            print("Hadoop Slave Setup")
        elif choice == "4":
            print("Exiting program...")
            exit(0)
        else:
            print("Invalid choice. Please try again.")
