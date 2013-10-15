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

from glance.domain import async
from glance.openstack.common import log as logging, importutils
from glance.openstack.common.gettextutils import _


LOG = logging.getLogger(__name__)


class TaskEventletExecutor(async.TaskExecutorInterface):

    def load_script(self):
        script_class = 'glance.common.scripts.' \
                       'import_filesystem_to_filesystem.ImportScript'
        script = importutils.import_object(script_class,
                                           self.gateway,
                                           self.context)
        return script

    def run(self, task_id):
        if task['type'] == 'import':
            return import_executor.TaskImportExecutor(context, gateway)

        raise exception.InvalidTaskType(type=task['type'])

        msg = _("Running task '%(task_id)s'") % {'task_id': task_id}
        LOG.info(msg)
        script = self.load_script()
        eventlet.spawn_n(script.execute, task_id)
