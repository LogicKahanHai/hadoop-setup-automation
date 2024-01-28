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
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.ipv4_dns, username="ubuntu")

        # TODO: Fix this, JAVA_HOME is already set on the worker but still gives empty output

        stdin, stdout, stderr = ssh.exec_command(
            "cat $HOME/.bashrc | grep '$JAVA_HOME'"
        )
        if stderr.readlines():
            ssh.close()
            raise Exception("Error getting $JAVA_HOME on Worker.")
        print("Got JAVA_HOME on Worker.")
        stdin, stdout, stderr = ssh.exec_command("cat $HOME/.bashrc | grep 'JAVA_HOME'")
        java_home = stdout.readlines()[0].rstrip("\n").split("=")[1]
        print(f"{java_home} is java_home")
        if java_home:
            self.db.write_slave_property(self.ip_addr, Constants.java_home(), java_home)
            print("Java is installed on Worker and JAVA_HOME is set already.")
            ssh.close()
        else:
            ssh.close()
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
        self.db.write_slave_property(self.ip_addr, Constants.java_home(), java_home)
        print("Edited .bashrc on Worker.")

    def setup_slave_env(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.ipv4_dns, username="ubuntu")
        print("Setting up hadoop-env.sh on Worker...")
        stdin, stdout, stderr = ssh.exec_command(
            "echo '\nexport JAVA_HOME="
            + self.db.read_slave_property(self.ip_addr, Constants.java_home())
            + "' >> "
            + self.hadoop_dir
            + "/etc/hadoop/hadoop-env.sh"
        )
        if stderr.readlines():
            raise Exception(
                "Error editing hadoop-env.sh on Worker. Please check the SSH connection."
            )
        print("Set up hadoop-env.sh on Worker.")
        print("Setting up yarn-env.sh on Worker...")
        stdin, stdout, stderr = ssh.exec_command(
            "echo '\nexport JAVA_HOME="
            + self.db.read_slave_property(self.ip_addr, Constants.java_home())
            + "' >> "
            + self.hadoop_dir
            + "/etc/hadoop/yarn-env.sh"
        )
        if stderr.readlines():
            raise Exception(
                "Error editing yarn-env.sh on Worker. Please check the SSH connection."
            )
        print("Set up yarn-env.sh on Worker.")
        print("Setting up mapred-env.sh on Worker...")
        stdin, stdout, stderr = ssh.exec_command(
            "echo '\nexport JAVA_HOME="
            + self.db.read_slave_property(self.ip_addr, Constants.java_home())
            + "' >> "
            + self.hadoop_dir
            + "/etc/hadoop/mapred-env.sh"
        )
        if stderr.readlines():
            raise Exception(
                "Error editing mapred-env.sh on Worker. Please check the SSH connection."
            )
        print("Set up mapred-env.sh on Worker.")
        ssh.close()
        print("Set up environment on Worker.")

    def scp_core_site(self):
        print("Copying core-site.xml to worker...")
        sp.getoutput(
            [
                "scp "
                + self.db.read_property(Constants.hadoop_home())
                + "/etc/hadoop/core-site.xml ubuntu@"
                + self.ipv4_dns
                + ":"
                + self.hadoop_dir
                + "/etc/hadoop/"
            ]
        )
        print("Copied core-site.xml to worker.")

    def add_hdfs_site(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.ipv4_dns, username="ubuntu")
        print("Adding hdfs-site.xml to worker...")
        stdin, stdout, stderr = ssh.exec_command(
            "cat " + self.hadoop_dir + "/etc/hadoop/hdfs-site.xml"
        )
        if stderr.readlines():
            raise Exception(
                "Error editing hdfs-site.xml on Worker. Please check the SSH connection."
            )
        final_hdfs_site = ""
        for line in stdout.readlines():
            if "</configuration>" in line:
                final_hdfs_site += (
                    "\t<property>\n\t\t<name>dfs.datanode.data.dir</name>\n\t\t<value>"
                    + self.db.read_slave_property(self.ip_addr, Constants.data_dir())
                    + "</value>\n\t</property>\n"
                )
            final_hdfs_site += line
        stdin, stdout, stderr = ssh.exec_command(
            "rm " + self.hadoop_dir + "/etc/hadoop/hdfs-site.xml"
        )
        if stderr.readlines():
            raise Exception(
                "Error editing hdfs-site.xml on Worker. Please check the SSH connection."
            )
        stdin, stdout, stderr = ssh.exec_command(
            "echo '"
            + final_hdfs_site
            + "' >> "
            + self.hadoop_dir
            + "/etc/hadoop/hdfs-site.xml"
        )
        if stderr.readlines():
            raise Exception(
                "Error editing hdfs-site.xml on Worker. Please check the SSH connection."
            )
        print("Added hdfs-site.xml to worker.")
        ssh.close()

    def scp_mapred_site(self):
        print("Copying mapred-site.xml to worker...")
        sp.getoutput(
            [
                "scp "
                + self.db.read_property(Constants.hadoop_home())
                + "/etc/hadoop/mapred-site.xml ubuntu@"
                + self.ipv4_dns
                + ":"
                + self.hadoop_dir
                + "/etc/hadoop/"
            ]
        )
        print("Copied mapred-site.xml to worker.")

    def scp_yarn_site(self):
        print("Copying yarn-site.xml to worker...")
        sp.getoutput(
            [
                "scp "
                + self.db.read_property(Constants.hadoop_home())
                + "/etc/hadoop/yarn-site.xml ubuntu@"
                + self.ipv4_dns
                + ":"
                + self.hadoop_dir
                + "/etc/hadoop/"
            ]
        )
        print("Copied yarn-site.xml to worker.")

    def edit_workers_file_master(self):
        sp.getoutput(
            [
                "echo '"
                + self.ipv4_dns
                + "' >> "
                + self.db.read_property(Constants.hadoop_home())
                + "/etc/hadoop/workers"
            ]
        )
