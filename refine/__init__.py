# based on work by David Huynh (@dfhuynh)
import os.path, time
import requests

try:
    import urlparse
except ImportError:
    from urllib import parse as urlparse


class Refine:

    def __init__(self, server='http://127.0.0.1:3333'):
        self.server = server[0, -1] if server.endswith('/') else server

    def new_project(self, file_path, options=None):
        file_name = os.path.split(file_path)[-1]
        project_name = (
            options['project_name'] if options is not None and
            'project_name' in options else file_name
        )
        data = {
            'project-name': project_name
        }
        files = {
            'project_file': open(file_path)
        }

        response = requests.post(
            self.server + '/command/core/create-project-from-upload',
            data, files=files
        )
        if response.ok:
            project_id = urlparse.parse_qs(
                response.url.split('?')[-1]
            )['project'][0]
            return RefineProject(self.server, project_id, project_name)

        # TODO: better error reporting
        return None


class RefineProject:
    def __init__(self, server, id, project_name):
        self.server = server
        self.id = id
        self.project_name = project_name

    def wait_until_idle(self, polling_delay=0.5):
        data = {'project': self.id}
        while True:
            response = requests.post(
                self.server + '/command/core/get-processes', data
            )
            response_json = response.json()
            condition = (
                'processes' in response_json and
                len(response_json['processes']) > 0
            )
            if condition:
                time.sleep(polling_delay)
            else:
                return

    def apply_operations(self, file_path, wait=True):
        fd = open(file_path)
        operations_json = fd.read()

        data = {
            'operations': operations_json,
            'project': self.id
        }
        response = requests.post(
            self.server + '/command/core/apply-operations', data
        )
        response_json = response.json()
        if response_json['code'] == 'error':
            raise Exception(response_json['message'])
        elif response_json['code'] == 'pending':
            if wait:
                self.wait_until_idle()
            return 'ok'

        return response_json['code']  # can be 'ok' or 'pending'

    def export_rows(self, format='csv'):
        data = {
            'engine': '{"facets":[],"mode":"row-based"}',
            'project': self.id,
            'format': format
        }
        response = requests.post(
            self.server + '/command/core/export-rows', data
        )
        return response.content

    def delete_project(self):
        data = {
            'project': self.id
        }
        response = requests.post(
            self.server + '/command/core/delete-project', data
        )
        response_json = response.json()
        return 'code' in response_json and response_json['code'] == 'ok'
