__version__ = "0.1.0-rc9"

from .workflow import Workflow
from .job import Job


class JTracker(object):
    def __init__(self,  workflow_yaml_file=None, workflow_yaml_string=None):
        self._workflow = Workflow(workflow_yaml_file=workflow_yaml_file, workflow_yaml_string=workflow_yaml_string)

    @property
    def workflow(self):
        return self._workflow

    def validate_jobjson(self, jobjson):
        pass

    def get_execution_plan(self, jobjson):
        job = Job(self.workflow, jobjson)
        return job.job_with_task_execution_plan
