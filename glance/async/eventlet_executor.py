# Copyright 2014 OpenStack Foundation
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
eventlet.monkey_patch(os=True)
from oslo.config import cfg

import glance.async
import glance.common.scripts as scripts
from glance.openstack.common import gettextutils
import glance.openstack.common.log as logging


_LI = gettextutils._LI
LOG = logging.getLogger(__name__)
CONF = cfg.CONF
CONF.import_opt('eventlet_executor_pool_size', 'glance.common.config',
                group='task')

MAX_EXECUTOR_THREADS = CONF.task.eventlet_executor_pool_size
THREAD_POOL = eventlet.GreenPool(size=MAX_EXECUTOR_THREADS)


class TaskExecutor(glance.async.TaskExecutor):
    def _run(self, task_id, task_type):
        LOG.info(_LI('Eventlet executor picked up the execution of task ID '
                     '%(task_id)s of task type '
                     '%(task_type)s') % {'task_id': task_id,
                                         'task_type': task_type})

        THREAD_POOL.spawn_n(scripts.run_task,
                            task_id,
                            task_type,
                            self.context,
                            self.task_repo,
                            self.image_repo,
                            self.image_factory)
