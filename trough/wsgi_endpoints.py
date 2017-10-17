import trough
from flask.views import View
segment_manager = Flask(__name__)

class SegmentManager:
    def __init__(self, *args, **kwargs):
        self.controller = trough.sync.get_controller(server_mode=False)
        self.controller.check_config()
    def provision_writable_segment():
        '''Provisions Writes. Will respond with a JSON object which describes segment metadata, including:
        - write url
        - segment size on disk
        - schema ID used to provision segment
or respond with a 500 including error description.'''
        # TODO: this needs to do schema selection via a GET variable.
        segment_name = request.get_data(as_text=True)
        return self.controller.provision_writable_segment(segment_name)

    def promote_writable_segment():
        '''Promotes segments to HDFS, will respond with a JSON object which describes:
        - hdfs path
        - segment size on disk
        - whether or not an upstream segment will be overwritten

This endpoint will toggle a value on the write lock record, which will be consulted so that a segment cannot be promoted while a promotion is in progress. The current journal will be committed, and then the promotion will commence, and this URL will return its JSON document at that point. During promotion, the segment will be put into write-ahead mode, and put back into journal mode after promotion.'''
        segment_name = request.get_data(as_text=True)
        return ujson.dumps(self.promote_writable_segment_upstream(segment_name)) # TODO

    def list_schemas():
        '''Schema API Endpoint. list schema names'''
        return ujson.dumps(self.controller.list_schemas()) # TODO

    def get_schema(self):
        '''Schema API Endpoint.
Get request responds with schema listing for schema with 'name'.
Post request creates/updates schema with 'name'.'''
        if request.type == 'POST':
            return self.controller.upsert_schema(name=name, request.get_data(as_text=True)) # TODO
        else:
            return self.controller.get_schema(name=name) # TODO

segment_manager.add_url_rule('/',              view_func=SegmentManager.as_view('show_users'))
segment_manager.add_url_rule('/write',         view_func=SegmentManager.as_view('show_users'))
segment_manager.add_url_rule('/promote',       view_func=SegmentManager.as_view('provision_writable_segment'))
segment_manager.add_url_rule('/schema',        view_func=SegmentManager.as_view('list_schemas'))
segment_manager.add_url_rule("/schema/<name>", view_func=SegmentManager.as_view('get_schema'))

# other wsgi endpoints as class based views as above here.