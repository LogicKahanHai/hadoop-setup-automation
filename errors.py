class JavaNotInstalledError(Exception):
    def __init__(
        self,
        message="Java has not been installed correctly.\nPlease install Java first according to your system and make sure `which java` command is working.",
    ) -> None:
        self.message = message
        super().__init__(self.message)


class CannotStoreProperties(Exception):
    def __init__(self) -> None:
        self.message = "There has been some error creating a local DB to store the different values to complete this setup.\nPlease make sure this folder (where you have cloned this repo) can have new files created in it."
        super().__init__(self.message)


class JavaHomeSetRestart(Exception):
    def __init__(self) -> None:
        self.message = (
            "JAVA_HOME is set. Please restart your terminal and run the program again."
        )
        super().__init__(self.message)
