import subprocess as sp
import json
import errors
import file_handling as fh
from modules.hadoop_setup import HadoopSetup
from modules.set_env import SetEnv


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
        self.hadoop_setup = HadoopSetup(self.db)
        self.hadoop_setup.edit_hadoop_env()
        self.hadoop_setup.edit_mapred_env()
        self.hadoop_setup.edit_yarn_env()
        print("Hadoop environment files edited.")


if __name__ == "__main__":
    main = Main()
    main.initial_setup()
    print("Initial setup complete.")
    print("\n\nIs this the MASTER machine? (y/n): ", end="")
    is_master = input()
    if is_master == "y":
        print("Please enter the Public IPv4 DNS of this machine: ", end="")
        master_dns = input()
        main.hadoop_setup.edit_core_site(master_dns)
    else:
        print("Please run this program on the MASTER machine ONLY.")
        print("Exiting program...")
