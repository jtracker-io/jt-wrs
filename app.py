#!/usr/bin/env python3
import connexion
import datetime
import logging
import jt_wrs
from jt_wrs.exceptions import AccountNameNotFound, AMSNotAvailable
from connexion import NoContent


def get_workflows(account_name):
    try:
        workflows = jt_wrs.get_workflows(account_name)
    except AccountNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflows or ('No workflow found', 404)


def get_workflow(account_name, workflow_name):
    try:
        workflow = jt_wrs.get_workflow(account_name, workflow_name)
    except AccountNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflow or ('No workflow found', 404)


def get_workflow_ver(account_name, workflow_name, workflow_version):
    try:
        workflow = jt_wrs.get_workflow(account_name, workflow_name, workflow_version)
    except AccountNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflow or ('No workflow found', 404)


def register_workflow(account_name, account_type='org'):
    exists = jt_wrs.get_account(account_name)
    if exists:
        return NoContent, 409
    else:
        return jt_wrs.create_account(account_name, account_type)


def release_workflow(account_name, workflow_name, workflow_version):
    pass


def validate_job_json(account_name, workflow_name, workflow_version):
    pass


def download_workflow(account_name, workflow_name, workflow_version):
    pass


logging.basicConfig(level=logging.INFO)
app = connexion.App(__name__)
app.add_api('swagger.yaml', base_path='/api/jt-wrs/v0.1')
# set the WSGI application callable to allow using uWSGI:
# uwsgi --http :8080 -w app
application = app.app

if __name__ == '__main__':
    # run our standalone gevent server
    app.run(port=1207, server='gevent')
