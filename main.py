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
        print("The first step is to check if Java is installed...")
        self.set_env.check_java()
        print("System environment setup complete.")
        print("The next step is to add the Hadoop installation location...")
        self.set_env.add_hadoop()
        print("Hadoop installation location added.")
        print("The next step is to edit the Hadoop environment files...")
        self.hadoop_setup = HadoopEnvSetup(self.db)
        self.hadoop_setup.edit_hadoop_env()
        self.hadoop_setup.edit_mapred_env()
        self.hadoop_setup.edit_yarn_env()
        print("Hadoop environment files edited.")

        print("The next step is to edit the Hadoop configuration files...")

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
    print("Hello! Welcome to Logic's Hadoop Cluster Setup Wizard. :)")
    print(
        "Are you running this program on an AWS EC2 instance running Ubuntu? (y/n): ",
        end="",
    )
    is_aws_ubuntu = input()
    if is_aws_ubuntu == "n":
        print(
            "Please run this program on an AWS EC2 instance running Ubuntu. The Developer is working hard to test and provide a universal solution... Till then, please use an AWS EC2 instance running Ubuntu."
        )
        print("Exiting program...")
        exit(0)
    print(
        "This program will guide you through the setup process and also do all the tedious work for you."
    )
    print(
        "Please make sure you meet the following basic requirements before you begin:"
    )
    print(
        "1. Java is installed on your system and is available in your .bashrc file (or the equivalent in your OS)."
    )
    print("2. You have downloaded and extracted Hadoop on your system.")
    print("\n\nDo you meet the above requirements? (y/n): ", end="")
    meets_requirements = input()
    if meets_requirements == "n":
        print("Please meet the above requirements and then run this program again.")
        print("Exiting program...")
        exit(0)
    main.initial_setup()
    print("Initial setup complete.")
    print("\n\nIs this the MASTER machine? (y/n): ", end="")
    is_master = input()
    if is_master == "y":
        main.hadoop_master_setup()
        print("Hadoop configuration on the Master System complete.")
    else:
        print("Please run this program on the MASTER machine ONLY.")
        print("Exiting program...")
        exit(0)
    print(
        "\n\nHave you setup SSH password-less login with the worker machines? (y/n): ",
        end="",
    )
    ssh_setup = input()
    if ssh_setup == "n":
        print("Please setup SSH password-less login with the worker machines first.")
        print("Exiting program...")
        exit(0)
