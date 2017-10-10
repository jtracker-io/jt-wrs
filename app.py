#!/usr/bin/env python3
import connexion
from connexion import NoContent
import datetime
import logging
import jt_wrs
from jt_wrs.exceptions import OwnerNameNotFound, AMSNotAvailable


def get_all_workflows():
    return get_workflows() or ('No workflow found', 404)


def get_workflows(owner_name=None):
    try:
        workflows = jt_wrs.get_workflows(owner_name)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500
    return workflows or ('No workflow found', 404)


def get_workflow_by_id_and_version(workflow_id, workflow_version=None):
    return jt_wrs.get_workflow_by_id_and_version(workflow_id, workflow_version) or ('No workflow found', 404)


def get_workflow_by_id(workflow_id):
    return get_workflow_by_id_and_version(workflow_id) or ('No workflow found', 404)


def get_workflow(owner_name, workflow_name):
    try:
        workflow = jt_wrs.get_workflow(owner_name, workflow_name)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflow or ('No workflow found', 404)


def get_workflow_ver(owner_name, workflow_name, workflow_version):
    try:
        workflow = jt_wrs.get_workflow(owner_name, workflow_name, workflow_version)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflow or ('No workflow found', 404)


def register_workflow(owner_name, workflow_entry=None):
    try:
        return jt_wrs.register_workflow(owner_name, workflow_entry)
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
        return jt_wrs.get_execution_plan(owner_name, workflow_name, workflow_version, job_json) \
               or ('JobJSON invalid', 400)
    except NotImplementedError as err:
        return str(err), 501


def download_workflowfile(owner_name, workflow_name, workflow_version):
    workflowfile = jt_wrs.get_workflowfile(owner_name, workflow_name, workflow_version)
    return workflowfile or ('No workflowfile found', 404)

def download_workflow_package(owner_name, workflow_name, workflow_version):
    workflow_package = jt_wrs.get_workflow_package(owner_name, workflow_name, workflow_version)
    return workflow_package or ('No workflow package found', 404)


logging.basicConfig(level=logging.INFO)
app = connexion.App(__name__)
app.add_api('swagger.yaml', base_path='/api/jt-wrs/v0.1')
# set the WSGI application callable to allow using uWSGI:
# uwsgi --http :8080 -w app
application = app.app

if __name__ == '__main__':
    # run our standalone gevent server
    app.run(port=12015, server='gevent')
