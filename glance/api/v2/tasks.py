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

from oslo.config import cfg

from glance.common import utils
from glance.common import wsgi
import glance.db
import glance.gateway
import glance.openstack.common.log as logging


LOG = logging.getLogger(__name__)

CONF = cfg.CONF


class TasksController(object):
    def __init__(self, gateway, db_api=None):
        self.db_api = db_api or glance.db.get_api()
        self.db_api.setup_db_env()
        self.gateway = glance.gateway.Gateway(self.db_api)

    def index(self, req):
        task_repo = self.gateway.get_task_repo(req.context)
        return task_repo.list()

    def list(self, req):
        task_repo = self.gateway.get_task_repo(req.context)
        return task_repo.list()

    @utils.mutating
    def create(self, req):
        task_repo = self.gateway.get_task_repo(req.context)
        return task_repo.list()

    @utils.mutating
    def update(self, req, task_id):
        task_repo = self.gateway.get_task_repo(req.context)
        return task_repo.get(task_id)

    @utils.mutating
    def delete(self, req, task_id):
        task_repo = self.gateway.get_task_repo(req.context)
        task = task_repo.get(task_id)
        task.kill()
        task_repo.save(task)
        return task

def create_resource():
    """Tasks resource factory method"""
    controller = TasksController()
    return wsgi.Resource(controller)
