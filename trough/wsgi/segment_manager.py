import trough
from flask.views import MethodView
from flask import Flask, request
import logging

app = Flask(__name__)

controller = trough.sync.get_controller(server_mode=True)
controller.check_config()

@app.route('/', methods=['POST'])
@app.route('/write', methods=['POST'])
def provision_writable_segment():
    '''Provisions Writes. Will respond with a JSON object which describes segment metadata, including:
    - write url
    - segment size on disk
    - schema ID used to provision segment
or respond with a 500 including error description.'''
    schema = request.args.get('schema')
    # TODO: this needs to do schema selection via a GET variable.
    segment_name = request.get_data(as_text=True)
    logging.info('provisioning writable segment %r (schema=%r)', segment_name, schema)
    write_url = controller.provision_writable_segment(segment_name)
    return write_url

@app.route('/promote', methods=['POST'])
def promote_writable_segment():
    '''Promotes segments to HDFS, will respond with a JSON object which describes:
    - hdfs path
    - segment size on disk
    - whether or not an upstream segment will be overwritten

This endpoint will toggle a value on the write lock record, which will be consulted so that a segment cannot be promoted while a promotion is in progress. The current journal will be committed, and then the promotion will commence, and this URL will return its JSON document at that point. During promotion, the segment will be put into write-ahead mode, and put back into journal mode after promotion.'''
    segment_name = request.get_data(as_text=True)
    return ujson.dumps(controller.promote_writable_segment_upstream(segment_name)) # TODO

@app.route('/schema', methods=['GET'])
def list_schemas():
    '''Schema API Endpoint. list schema names'''
    return ujson.dumps(controller.list_schemas()) # TODO

@app.route('/schema/<name>', methods=['GET'])
def get_schema():
    '''Schema API Endpoint.
Get request responds with schema listing for schema with 'name'.
Post request creates/updates schema with 'name'.'''
    return controller.get_schema(name=name) # TODO

@app.route('/schema/<name>', methods=['POST'])
def set_schema():
    '''Schema API Endpoint.
Get request responds with schema listing for schema with 'name'.
Post request creates/updates schema with 'name'.'''
    return controller.set_schema(name=name, schema=request.get_data(as_text=True)) # TODO
