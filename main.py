import subprocess as sp
import json
import errors
import file_handling as fh
from modules.hadoop_setup import HadoopSetup
from modules.set_env import SetEnv


class Main:
    def __init__(self) -> None:
        self.db = fh.FileHandling()
        self.set_env = SetEnv(self.db)
        self.hadoop_setup = HadoopSetup(self.db)

    def run(self):
        print("The first step is to check if Java is installed...")
        self.set_env.check_java()
        print("System environment setup complete.")
        print("The next step is to add the Hadoop installation location...")
        self.set_env.add_hadoop()
        print("Hadoop installation location added.")
        print("The next step is to edit the Hadoop environment files...")
        self.hadoop_setup.edit_hadoop_env()
        self.hadoop_setup.edit_mapred_env()
        self.hadoop_setup.edit_yarn_env()
        print("Hadoop environment files edited.")
