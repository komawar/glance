#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import glance.exception
from glance.openstack.common import importutils
from glance.openstack.common import timeutils
from glance.openstack.common import uuidutils


class Task(object):
    def __init__(self, task_id, status, message, request, parameters,
                 result, created_at, updated_at, started_at, stopped_at,
                 runner):
        if status not in ('pending', 'success', 'failure', 'inprogress'):
            raise glance.exception.InvalidTaskStatus(status)
        self.task_id = task_id
        self.request = request
        self.parameters = parameters
        self.result = result
        self._status = status
        self.message = message
        self.created_at = created_at
        self.updated_at = updated_at
        self.started_at = started_at  # time of transition to inprogress
        self.stopped_at = stopped_at  # time of transition to success/failure
        self._runner = runner

    @property
    def status(self):
        return self._status

    def start(self):
        self._status = 'inprogress'
        self.started_at = timeutils.utcnow()
        self._runner.run(self)

    def kill(self, message=None):
        self._status = 'failure'
        self.message = message or _('Task was forced to quit.')
        self._runner.kill(self)

    def complete(self, result):
        self._status = 'success'
        self.result = result
        self.stopped_at = timeutils.utcnow()

    def fail(self, message):
        self._status = 'failure'
        self.message = message
        self.stopped_at = timeutils.utcnow()


class TaskFactory(object):
    def new_task(self, task_id=None, request=None, parameters=None):
        if not request:
            raise TypeError('new_task() takes at least one argument (\'request\')')

        task_id = task_id or uuidutils.generate_uuid() 
        status = 'pending'
        message = None
        parameters = parameters or {}
        result = None
        created_at = timeutils.utcnow()
        updated_at = created_at
        started_at = None
        stopped_at = None

        return Task(task_id, status, message, request, parameters,
                    result, created_at, updated_at, started_at, stopped_at)


class TaskRunnerInterface(object):
    def __init__(self, task):
        if not async_processor:
            importutils.import_object(CONF.async_processor_class)
        self.async_processor = async_processor
        self.task = task

    def run(self):
        self.async_processor.run()

    def kill(self, task):
        self.async_processor.kill()
