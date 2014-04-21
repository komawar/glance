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


import glance.openstack.common.log as logging


LOG = logging.getLogger(__name__)


class TaskExecutor(object):

    def __init__(self, context, task_repo, image_repo, image_factory):
        self.context = context
        self.task_repo = task_repo
        self.image_repo = image_repo
        self.image_factory = image_factory

    def begin_processing(self, task_id):
        task = self.task_repo.get(task_id)
        task.begin_processing()
        self.task_repo.save(task)

        # start running
        self._run(task_id, task.type)

    def _run(self, task_id, task_type):
        task = self.task_repo.get(task_id)
        list_of_executors = 'eventlet'
        msg = _("This executor is not implemented. Please use one from the "
                "list - %s") % list_of_executors
        LOG.error(msg)
        task.fail(_("Internal error occurred while trying to process task."))
        self.task_repo.save(task)
