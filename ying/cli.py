import fire
from ying.plog import run_command


class ProgressLog(object):
    def plog(self, cmd):
        run_command(cmd)


class Command(object):

    def __init__(self):
        self.progress = ProgressLog()

    def plog(self, cmd):
        self.progress.plog(cmd)


def main():
    fire.Fire(Command)


if __name__ == "__main__":
    main()
