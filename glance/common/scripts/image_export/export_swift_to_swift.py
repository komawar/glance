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
from glance.common.scripts import utils as script_utils
from glance.openstack.common import log as logging


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class ExportScript(object):
    def __init__(self, gateway, context, swift_store=None):
        self.gateway = gateway
        self.context = context
        self.image_repo = self.gateway.get_repo(self.context)
        self.swift_store = swift_store or script_utils.SwiftStore()
        self.task_repo = self.gateway.get_task_repo(self.context)

    def _set_task_processing(self, task_id):
        task = self._get_task(task_id)

        LOG.info(_("Executor is beginning processing on "
                   "Task %s") % task_id)
        task.begin_processing()
        self.task_repo.save(task)
        return task

    def execute(self, task_id):
        try:
            task = self._set_task_processing(task_id)

            self.validate_task_input(task)

            self.transfer_image_data(task.input['image_uuid'],
                                     task.input['receiving_swift_container'])

            #TODO: is this what we want?
            export_location = (str(task.input['receiving_swift_container'])
                               + '/' + str(task.input['image_uuid']))
            result = {'export_location': export_location}
            self._save_task(task_id, result, status='success')
        # NOTE(ameade): Always use unicode for logging and saving tasks. The
        # task won't set it's status otherwise.
        except exception.TaskNotFound as e:
            msg = _('Task %s could not be found during execution') % task_id
            LOG.exception(msg)
        except exception.Duplicate as e:
            LOG.exception(unicode(e))
            self._save_task(task_id, unicode(e), status='failure')
        except exception.NotFound as e:
            LOG.exception(unicode(e))
            self._save_task(task_id, unicode(e), status='failure')
        except exception.Invalid as e:
            LOG.info(unicode(e))
            self._save_task(task_id, unicode(e), status='failure')
        except Exception as e:
            msg = _('Unknown error occurred during execution')
            LOG.exception(msg)
            self._save_task(task_id, msg, status='failure')

    def _save_task(self, task_id, info, status='failure'):
        task = self._get_task(task_id)
        if status == 'failure':
            task.fail(info)
        elif status == 'success':
            task.succeed(info)

        self.task_repo.save(task)

    def _get_task(self, task_id):
        try:
            return self.task_repo.get(task_id)
        except exception.NotFound as e:
            msg = _('Task not found for task_id %s') % task_id
            LOG.exception(msg)
            raise exception.TaskNotFound(str(e), task_id=task_id)

    def validate_task_input(self, task):

        for key in ["image_uuid", "receiving_swift_container"]:
            if key not in task.input:
                msg = _("Task '%(task_id)s' input has missing key "
                        "'%(key)s'") % {'task_id': task.task_id, 'key': key}
                raise exception.Invalid(msg)

        image_id = task.input["image_uuid"]

        if not uuidutils.is_uuid_like(image_id):
            msg = _("The specified image id '%(image_id)s' for task "
                    "'%(task_id)s' is not a uuid.") % {'task_id': task.task_id,
                                                       'image_id': image_id}
            raise exception.Invalid(msg)

        container = task.input["receiving_swift_container"]
        if container == '' or re.search(r'[/?.]', container):
            msg = _("Invalid value of receiving_swift_container for "
                    "task '%(task_id)s' Given value is"
                    " '%(container)s'") % {'task_id': task.task_id,
                                           'container': container}
            raise exception.Invalid(msg)

    def transfer_image_data(self, image_id, swift_container):
        image = self.image_repo.get(image_id)
        image_size = image.size
        image_data_iter = image.get_data()
        self.upload_image_data(image_id, image_data_iter,
                               image_size, swift_container)

    def upload_image_data(self, image_id, data_iter,
                          image_size, swift_container):
        auth_token = getattr(self.context, 'auth_tok', None)
        self.swift_store.add(image_id, data_iter, image_size, auth_token,
                             swift_container)
