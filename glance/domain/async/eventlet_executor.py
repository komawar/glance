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

from glance.common import exception as exc
from glance.domain import async
from glance.openstack.common import timeutils
from glance.store import swift as swift_store


LOG = logging.getLogger(__name__)

async_worker_opts = [ 
    cfg.BoolOpt('do_image_conversion',
               default=False,
               help=_('Image conversion')),
    cfg.BoolOpt('do_test_image_is_bootable',
               default=False,
               help=_('Bootable test')),
    cfg.StrOpt('transfer_data_scheme',
               default='glance_worker',
               help=_('Transfer scheme')),
    cfg.ListOpt('allowed_input_image_formats',
               default=['qcow', 'vhd', 'ami'],
               help=_('Input image format')),
]

CONF = cfg.CONF
CONF.register_opts(async_worker_opts)

class Runner(async.TaskRunnerInterface):
    def __init__(self):
        super(TaskRunnerInterface, self).__init__(task)

    def execute_import(self):
        if CONF.do_image_conversion:
            self.convert_image()
        if CONF.do_test_image_is_bootable:
            self.test_iage_bootable()
        if CONF.transfer_data_scheme = 'temp_url':
            #Note(nikhil): with the current version of tasks, we do not support temp
            #url scheme
            pass

        else:
            data = swift_store.get(source)
            image.set_data(data)
            repo.save()

    def run(self):
        if task.properties.get('type') == 'import':
            try:
                self.add_import_flow()
                source = task.properties['input'].get('import_from')
                self.validate_source_string(source)
                img_format = task.properties['input'].get('import_from_format')
                self.validate_input_from_format(img_format)
                t = eventlet.spawn_n(excute_import)
            except Exception as e:
                raise e

    def convert_image():
        pass

    def test_iage_bootable():
        pass

    def validate_source_string(source):
        pass

    def validate_input_from_format(img_format):
        if img_format not in CONF.allowed_input_image_formats:
            raise exc.InvalidImageFormat()

    t.run()
