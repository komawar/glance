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

class TasksController(object):
    def __init__(self, gateway):
        self.gateway = gateway

    def index(self, req):
        task_repo = self.gateway.get_task_repo(req.context)
        return task_repo.list()

    def get(self, req, task_id):
        task_repo = self.gateway.get_task_repo(req.context)
        return task_repo.get(task_id)

    def kill(self, req, task_id):
        task_repo = self.gateway.get_task_repo(req.context)
        task = task_repo.get(task_id)
        task.kill()
        task_repo.save(task)
        return task
