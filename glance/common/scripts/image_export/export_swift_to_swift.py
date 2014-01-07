# Copyright 2013 OpenStack LLC.
# All Rights Reserved.
#
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
import re

from oslo.config import cfg
from glance.openstack.common import uuidutils

from glance.common import exception
from glance.openstack.common import log as logging

try:
    import swiftclient
except ImportError:
    pass


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class ExportScript(object):
    def __init__(self, gateway, context):
        self.gateway = gateway
        self.context = context
        self.image_repo = self.gateway.get_repo(self.context)

    def _set_task_processing(self, task_repo, task_id):
        task = self.get_task(task_repo, task_id)
        if task is None:
            #TODO(nikhil): handle it in a better way
            LOG.error(_("Task corresponding to task_id %s was not found "
                        "while attempting to execute it.") % task_id)
            return

        LOG.info(_("Executor is beginning processing on "
                   "Task %s") % task_id)
        task.begin_processing()
        task_repo.save(task)
        return task

    def execute(self, task_id):
        task_repo = self.gateway.get_task_repo(self.context)
        task = self._set_task_processing(task_repo, task_id)

        if task is None:
            return

        self.validate_task_input(task)

    def get_task(self, task_repo, task_id):
        task = None
        try:
            task = task_repo.get(task_id)
        except exception.NotFound as e:
            msg = _('Task not found for task_id %s') % task_id
            LOG.exception(msg)

        return task

    def validate_task_input(self, task):

        for key in ["image_uuid", "receiving_swift_container"]:
            if key not in task.input:
                msg = _("Task %(task_id)s input has missing key "
                        "%(key)s") % {'task_id': task.task_id, 'key': key}
                raise exception.Invalid(msg)

        image_id = task.input["image_uuid"]

        if not uuidutils.is_uuid_like(image_id):
            msg = _("The specified image id %(image_id)s for task %(task_id)s"
                    " is not a uuid.") % {'task_id': task.task_id,
                                          'image_id': image_id}
            raise exception.Invalid(msg)

        container = task.input["receiving_swift_container"]
        if str(container) == '' or re.search(r'[/?.]', str(container)):
            msg = _("Invalid value for receiving_swift_container for "
                    "task %(task_id)s. "
                    "Given value is ''.") % {'task_id': task.task_id}
            raise exception.Invalid(msg)
