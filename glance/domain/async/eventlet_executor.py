import eventlet

from glance.domain import async
from glance.openstack.common import timeutils
from glance.store import swift as swift_store

class Runner(async.TaskRunnerInterface):
    def __init__(self):
        super(TaskRunnerInterface, self).__init__(task)

    def execute_import(self):
        if CONF.do_image_conversion:
            pass
        if CONF.do_test_image_is_bootable:
            pass
        print "foo"
        if CONF.transfer_data_scheme = 'temp_url':
            pass

        else:
            swift_store.get(source)
            swift_store.add()

    def run(self):
        if task.properties.get('type') == 'import':
            source = task.properties['input'].get('import_from')
            self.validate_source_string(source)
            img_format = task.properties['input'].get('import_from_format')
            self.validate_input_from_format(img_format)
            t = eventlet.spawn_n(excute_import)

        t.run()
