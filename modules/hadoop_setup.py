from const.constants import Constants
from file_handling import FileHandling


class HadoopSetup:
    def __init__(self, db: FileHandling) -> None:
        self.db = db
        self.hadoop_home = self.db.read_property(Constants.hadoop_home)

    def edit_hadoop_env(self):
        print("Editing hadoop-env.sh Adding JAVA_HOME...")
        hadoop_env = open(self.hadoop_home + "/etc/hadoop/hadoop-env.sh", "a")
        hadoop_env.write(
            "\nexport JAVA_HOME=" + self.db.read_property(Constants.java_home)
        )
        hadoop_env.close()
        print("Added JAVA_HOME to hadoop-env.sh ...")

    def edit_mapred_env(self):
        print("Editing mapred-env.sh Adding JAVA_HOME...")
        mapred_env = open(self.hadoop_home + "/etc/hadoop/mapred-env.sh", "a")
        mapred_env.write(
            "\nexport JAVA_HOME=" + self.db.read_property(Constants.java_home)
        )
        mapred_env.close()
        print("Added JAVA_HOME to mapred-env.sh ...")

    def edit_yarn_env(self):
        print("Editing yarn-env.sh Adding JAVA_HOME...")
        yarn_env = open(self.hadoop_home + "/etc/hadoop/yarn-env.sh", "a")
        yarn_env.write(
            "\nexport JAVA_HOME=" + self.db.read_property(Constants.java_home)
        )
        yarn_env.close()
        print("Added JAVA_HOME to yarn-env.sh ...")
