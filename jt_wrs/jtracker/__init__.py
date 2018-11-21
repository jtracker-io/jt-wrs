from ..__init__ import __version__
from .workflow import Workflow
from .job import Job


class JTracker(object):
    def __init__(self,  workflow_yaml_file=None, workflow_yaml_string=None):
        self._workflow = Workflow(workflow_yaml_file=workflow_yaml_file, workflow_yaml_string=workflow_yaml_string)

    @property
    def workflow(self):
        return self._workflow

    def validate_job_json(self, job_json):
        pass

    def get_execution_plan(self, job_json):
        job = Job(self.workflow, job_json)
        return job.job_with_task_execution_plan
