import eventlet

from glance.domain import async

class Runner(async.TaskRunnerInterface):
    def __init__(self):
        super(Runner, self).__init__()

    def add_task(self, queue):

    def run_task(self, queue):

    def run(self):
        pass

    def kill(self):
        pass
