from const.constants import Constants
import subprocess as sp
import errors
import paramiko


class HadoopSlaveSetup:
    def __init__(self, db, ip_addr, ipv4_dns, data_dir, hadoop_dir) -> None:
        self.db = db
        self.ip_addr = ip_addr
        self.ipv4_dns = ipv4_dns
        self.data_dir = data_dir
        self.hadoop_dir = hadoop_dir

    def setup_slave(self):
        print("Adding Worker details...")
        self.db.write_property(self.ip_addr, {})
        self.db.write_slave_property(self.ip_addr, Constants.ipv4_dns(), self.ipv4_dns)
        self.db.write_slave_property(self.ip_addr, Constants.data_dir(), self.data_dir)
        self.db.write_slave_property(
            self.ip_addr, Constants.hadoop_home(), self.hadoop_dir
        )
        print("Added Worker details.")

    def check_java_slave(self):
        print("Checking if Java is installed on Worker...")
        try:
            which_java = str(sp.getoutput(["ssh " + self.ipv4_dns + " which java"]))
            if not which_java:
                raise errors.JavaNotInstalledError()
        except Exception as e:
            if e is not errors.JavaNotInstalledError:
                raise e
        java_home = sp.getoutput(["ssh " + self.ipv4_dns + " echo $JAVA_HOME"])
        print(f"{java_home} is java_home")
        if java_home:
            self.db.write_slave_property(self.ip_addr, Constants.java_home(), java_home)
            print("Java is installed on Worker and JAVA_HOME is set.")
        else:
            print("Java is not added to PATH on this worker. Adding to PATH...")
            self.add_java_slave()
            print("Java is installed on Worker and JAVA_HOME is set.")

    def add_java_slave(self):
        print("Editing .bashrc on Worker...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.ipv4_dns, username="ubuntu")
        stdin, stdout, stderr = ssh.exec_command("echo $HOME")
        if stderr.readlines():
            raise Exception("Error getting $HOME on Worker.")
        home = stdout.readlines()[0].rstrip("\n")
        stdin, stdout, stderr = ssh.exec_command("which java")
        if stderr.readlines():
            raise Exception("Error getting `which java` on Worker.")
        which_java = stdout.readlines()[0].rstrip("\n")
        stdin, stdout, stderr = ssh.exec_command("readlink -f " + which_java)
        if stderr.readlines():
            raise Exception("Error getting `readlink -f " + which_java + "` on Worker.")
        java_home = stdout.readlines()[0].rstrip("\n")
        java_home = java_home.rstrip("/bin/java")
        stdin, stdout, stderr = ssh.exec_command(
            "echo '\nexport JAVA_HOME=" + java_home + "' >> " + home + "/.bashrc"
        )
        if stderr.readlines():
            raise Exception(
                "Error editing .bashrc on Worker. Please check the SSH connection."
            )
        stdin, stdout, stderr = ssh.exec_command(
            "echo '\nexport PATH=$PATH:$JAVA_HOME/bin' >> " + home + "/.bashrc"
        )
        if stderr.readlines():
            raise Exception(
                "Error editing .bashrc on Worker. Please check the SSH connection."
            )
        ssh.close()
        print("Edited .bashrc on Worker.")

    def scp_core_site(self):
        print("Copying core-site.xml to worker...")
        sp.getoutput(
            [
                "scp "
                + self.hadoop_dir
                + "/etc/hadoop/core-site.xml "
                + self.ipv4_dns
                + ":"
                + self.hadoop_dir
                + "/etc/hadoop/"
            ]
        )
        print("Copied core-site.xml to worker.")
