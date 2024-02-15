import fire
from ying.plog import run_command


class ProgressLog(object):
    def plog(self, cmd, mode):
        run_command(cmd, mode)


class Command(object):

    def __init__(self):
        self.progress = ProgressLog()

    def plog(self, cmd, mode='r'):
        self.progress.plog(cmd, mode)


def main():
    fire.Fire(Command)


if __name__ == "__main__":
    main()
