from . import wrs
from .exceptions import OwnerNameNotFound, AMSNotAvailable

__version__ = '0.2.0a14'


def get_all_workflows():
    return wrs.get_workflows() or ('No workflow found', 404)


def get_workflows(owner_name=None):
    try:
        workflows = wrs.get_workflows(owner_name)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500
    return workflows or ('No workflow found', 404)


def get_workflow_by_id_and_version(workflow_id, workflow_version=None):
    return wrs.get_workflow_by_id_and_version(workflow_id, workflow_version) or ('No workflow found', 404)


def get_workflow_by_id(workflow_id):
    return get_workflow_by_id_and_version(workflow_id) or ('No workflow found', 404)


def get_workflow(owner_name, workflow_name):
    try:
        workflow = wrs.get_workflow(owner_name, workflow_name)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflow or ('No workflow found', 404)


def get_workflow_ver(owner_name, workflow_name, workflow_version):
    try:
        workflow = wrs.get_workflow(owner_name, workflow_name, workflow_version)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflow or ('No workflow found', 404)


def register_workflow(owner_name, workflow_entry=None):
    try:
        return wrs.register_workflow(owner_name, workflow_entry)
    except Exception as err:
        return 'Failed registering workflow: %s' % str(err), 400


def delete_workflow(owner_name, workflow_name, worklow_version=None):
    pass


def delete_workflow1(owner_name, workflow_name):
    delete_workflow(owner_name, workflow_name)


def release_workflow(owner_name, workflow_name, workflow_version):
    pass


def get_job_json_template(owner_name, workflow_name, workflow_version, job_json):
    pass


def get_execution_plan(owner_name, workflow_name, workflow_version, job_json):
    try:
        return wrs.get_execution_plan(owner_name, workflow_name, workflow_version, job_json) \
               or ('JobJSON invalid', 400)
    except NotImplementedError as err:
        return str(err), 501


def download_workflowfile(owner_name, workflow_name, workflow_version):
    workflowfile = wrs.get_workflowfile(owner_name, workflow_name, workflow_version)
    return workflowfile or ('No workflowfile found', 404)

def download_workflow_package(owner_name, workflow_name, workflow_version):
    workflow_package = wrs.get_workflow_package(owner_name, workflow_name, workflow_version)
    return workflow_package or ('No workflow package found', 404)

