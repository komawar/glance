# Copyright 2013 OpenStack Foundation
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

import eventlet

from oslo.config import cfg
from glance import async

from glance.common import exception
from glance.openstack.common import log as logging
from glance.openstack.common.gettextutils import _
from glance.common.scripts.image_import import main as image_import_script


LOG = logging.getLogger(__name__)

task_type_opts = [
    cfg.ListOpt('valid_tasks_type',
                default=[
                    'import',
                ],
                help=_('List of tasks types supported by the deployment')),
]

CONF = cfg.CONF
CONF.register_opts(task_type_opts)


class TaskEventletExecutor(async.TaskExecutorInterface):

    def load_script(self, task_type):
        script = None
        if task_type == 'import':
            script = image_import_script.ImageImporter()
        return script

    def run(self, task_id, task_status, task_type, task_input):
        if task_type not in CONF.valid_tasks_type:
            raise exception.InvalidTaskType(type=task_type)

        msg = _("Running task '%(task_id)s'") % {'task_id': task_id}
        LOG.info(msg)
        script = self.load_script(task_type)
        if script:
            eventlet.spawn_n(script.execute,
                             self.context,
                             task_id)
        else:
            msg = (_("Error loading script for task type %(type)s'") %
                   {'type': task_type})
            LOG.warn(msg)
