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

from oslo.config import cfg

import glance.api.policy
import glance.db
import glance.gateway
import glance.notifier
import glance.store
from glance.openstack.common import log as logging, importutils


LOG = logging.getLogger(__name__)

task_type_opts = [
    cfg.StrOpt('import_task_script',
               default='glance.common.scripts.image_import.'
                       'import_filesystem_to_filesystem.ImportScript',
               help=_('Import task executor script to load'))
]

CONF = cfg.CONF
CONF.register_opts(task_type_opts)


class ImageImporter(object):
    def __init__(self, db_api=None, policy_enforcer=None, notifier=None,
                 store_api=None):
        self.db_api = db_api or glance.db.get_api()
        self.db_api.setup_db_env()
        self.policy = policy_enforcer or glance.api.policy.Enforcer()
        self.notifier = notifier or glance.notifier.Notifier()
        self.store_api = store_api or glance.store
        self.gateway = glance.gateway.Gateway(self.db_api, self.store_api,
                                              self.notifier, self.policy)

    def load_script(self, context):
            script_class = CONF.import_task_script
            script = importutils.import_object(script_class,
                                               self.gateway,
                                               context)
            return script

    def execute(self, context, task_id):
        LOG.msg(_("Loaded main module for Image Import Script"))
        script = self.load_script(context)
        script.execute(task_id)
