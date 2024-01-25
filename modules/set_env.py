import errors
import subprocess as sp

from file_handling import FileHandling
from const.constants import Constants


class SetEnv:
    def __init__(self, db: FileHandling) -> None:
        print("Setting up environment...")
        self.db = db

    def check_java(self):
        print("Checking if Java is installed...")
        try:
            which_java = str(sp.getoutput(["which java"]))
            if not which_java:
                raise errors.JavaNotInstalledError()
        except Exception as e:
            if e is not errors.JavaNotInstalledError:
                raise e

        self.db.write_property(Constants.which_java(), which_java)
        print("Java is installed. Getting JAVA_HOME...")
        java_home = str(sp.getoutput(["echo $JAVA_HOME"]))
        if java_home:
            self.db.write_property(Constants.java_home(), java_home)
            print("JAVA_HOME is set. Skipping updating .bashrc...")
        else:
            print("JAVA_HOME is not set. Updating .bashrc...")
            self.update_bashrc()

        print("Java is installed and JAVA_HOME is set.")

    def update_bashrc(self):
        print("Updating .bashrc...")
        bashrc = open("~/.bashrc", "a")
        which_java = self.db.read_property(Constants.which_java())
        java_home = sp.getoutput(["readlink", "-f", which_java])
        java_home = java_home.replace("/bin/java", "")
        self.db.write_property(Constants.java_home(), java_home)
        bashrc.write("\nexport JAVA_HOME=" + java_home)
        bashrc.write("\nexport PATH=$PATH:$JAVA_HOME/bin")
        bashrc.close()
        sp.getoutput(["source", "~/.bashrc"])
        print(".bashrc updated...")

    def add_hadoop(self):
        print(
            "Please enter the path to your Hadoop installation: (It should look something like `/path/to/hadoop-x.y.z`)"
        )
        print("\nExample: /home/user/hadoop-3.3.0")
        print("Enter path to Hadoop installation (Press ENTER after pasting): ", end="")

        hadoop_home = input()
        self.db.write_property(Constants.hadoop_home(), hadoop_home)
