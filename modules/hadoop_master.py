from const.constants import Constants
from file_handling import FileHandling


class HadoopMasterSetup:
    def __init__(
        self,
        db: FileHandling,
        master_dns: str,
        name_dir: str,
        resource_manager_dns: str,
    ) -> None:
        self.db = db
        self.hadoop_home = self.db.read_property(Constants.hadoop_home())
        self.db.write_property(Constants.master_dns(), master_dns)
        self.db.write_property(Constants.name_dir(), name_dir)
        self.db.write_property(Constants.resource_manager_dns(), resource_manager_dns)

    def edit_core_site(self):
        master_dns = self.db.read_property(Constants.master_dns())
        print("Editing core-site.xml...")
        with open(self.hadoop_home + "/etc/hadoop/core-site.xml", "r") as core_site:
            lines = core_site.readlines()
        with open(self.hadoop_home + "/etc/hadoop/core-site.xml", "w") as core_site:
            for line in lines:
                if "<configuration>" in line:
                    core_site.write(line)
                    core_site.write(
                        f"\t<property>\n\t\t<name>fs.default.name</name>\n\t\t<value>hdfs://{master_dns}:50070</value>\n\t</property>\n"
                    )
                elif "</configuration>" in line:
                    core_site.write(line)
                else:
                    core_site.write(line)
        print("Edited core-site.xml ...")

    def edit_hdfs_site(self):
        name_dir = self.db.read_property(Constants.name_dir())
        print("Editing hdfs-site.xml...")
        with open(self.hadoop_home + "/etc/hadoop/hdfs-site.xml", "r") as hdfs_site:
            lines = hdfs_site.readlines()
        with open(self.hadoop_home + "/etc/hadoop/hdfs-site.xml", "w") as hdfs_site:
            for line in lines:
                if "<configuration>" in line:
                    hdfs_site.write(line)
                    hdfs_site.write(
                        f"\t<property>\n\t\t<name>dfs.namenode.name.dir</name>\n\t\t<value>{name_dir}</value>\n\t</property>\n"
                    )
                elif "</configuration>" in line:
                    hdfs_site.write(line)
                else:
                    hdfs_site.write(line)
        print("Edited hdfs-site.xml ...")

    def edit_mapred_site(self):
        print("Editing mapred-site.xml...")
        with open(self.hadoop_home + "/etc/hadoop/mapred-site.xml", "r") as mapred_site:
            lines = mapred_site.readlines()
        with open(self.hadoop_home + "/etc/hadoop/mapred-site.xml", "w") as mapred_site:
            for line in lines:
                if "<configuration>" in line:
                    mapred_site.write(line)
                    mapred_site.write(
                        "\t<property>\n\t\t<name>mapreduce.framework.name</name>\n\t\t<value>yarn</value>\n\t</property>\n"
                    )
                elif "</configuration>" in line:
                    mapred_site.write(line)
                else:
                    mapred_site.write(line)
        print("Edited mapred-site.xml ...")

    def edit_yarn_site(self):
        print("Editing yarn-site.xml...")
        resource_manager_dns = self.db.read_property(Constants.resource_manager_dns())
        with open(self.hadoop_home + "/etc/hadoop/yarn-site.xml", "r") as yarn_site:
            lines = yarn_site.readlines()
        with open(self.hadoop_home + "/etc/hadoop/yarn-site.xml", "w") as yarn_site:
            for line in lines:
                if "<configuration>" in line:
                    yarn_site.write(line)
                    yarn_site.write(
                        f"\t<property>\n\t\t<name>yarn.resourcemanager.hostname</name>\n\t\t<value>{resource_manager_dns}</value>\n\t</property>\n"
                    )
                    yarn_site.write(
                        f"\t<property>\n\t\t<name>yarn.resourcemanager.address</name>\n\t\t<value>{resource_manager_dns}:8032</value>\n\t</property>\n"
                    )
                elif "</configuration>" in line:
                    yarn_site.write(line)
                else:
                    yarn_site.write(line)
        print("Edited yarn-site.xml ...")
