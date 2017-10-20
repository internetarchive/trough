import trough
import flask
import logging
import ujson

def make_app(controller):
    controller.check_config()
    app = flask.Flask(__name__)

    @app.route('/', methods=['POST'])
    def simple_provision_writable_segment():
        ''' deprecated api '''
        segment_id = flask.request.get_data(as_text=True)
        logging.info('provisioning writable segment %r', segment_id)
        result_dict = controller.provision_writable_segment(segment_id)
        return flask.Response(result_dict.get('write_url'), mimetype='text/plain')

    @app.route('/provision', methods=['POST'])
    def provision_writable_segment():
        '''Provisions Writes. Will respond with a JSON object which describes segment metadata, including:
        - write url
        - segment size on disk
        - schema ID used to provision segment
    or respond with a 500 including error description.'''
        segment_id = flask.request.json['segment']
        schema = flask.request.json.get('schema')
        logging.info('provisioning writable segment %r (schema=%r)', segment_id, schema)
        # {'write_url': write_url, 'size': None, 'schema': schema}
        result_dict = controller.provision_writable_segment(segment_id, schema=schema)
        result_json = ujson.dumps(result_dict)
        return flask.Response(result_json, mimetype='application/json')

    @app.route('/promote', methods=['POST'])
    def promote_writable_segment():
        '''Promotes segments to HDFS, will respond with a JSON object which describes:
        - hdfs path
        - segment size on disk
        - whether or not an upstream segment will be overwritten

    This endpoint will toggle a value on the write lock record, which will be consulted so that a segment cannot be promoted while a promotion is in progress. The current journal will be committed, and then the promotion will commence, and this URL will return its JSON document at that point. During promotion, the segment will be put into write-ahead mode, and put back into journal mode after promotion.'''
        segment_id = flask.request.get_data(as_text=True)
        return ujson.dumps(controller.promote_writable_segment_upstream(segment_id)) # TODO

    @app.route('/schema', methods=['GET'])
    def list_schemas():
        '''Schema API Endpoint. list schema names'''
        result_json = ujson.dumps(controller.list_schemas())
        return flask.Response(result_json, mimetype='application/json')

    @app.route('/schema/<id>', methods=['GET'])
    def get_schema(id):
        '''Schema API Endpoint.
    Get request responds with schema listing for schema with 'id'.
    Post request creates/updates schema with 'id'.'''
        schema = controller.get_schema(id=id)
        if not schema:
            flask.abort(404)

        best_match = flask.request.accept_mimetypes.best_match(
                ['application/sql', 'application/json'])

        if best_match == 'application/json':
            return flask.Response(ujson.dumps(schema), mimetype='application/json')
        else:
            return flask.Response(schema.sql, mimetype='application/sql')

    @app.route('/schema/<id>', methods=['POST'])
    def set_schema(id):
        '''Schema API Endpoint.
    Get request responds with schema listing for schema with 'id'.
    Post request creates/updates schema with 'id'.'''
        schema, created = controller.set_schema(id=id, schema=flask.request.get_data(as_text=True))

        best_match = flask.request.accept_mimetypes.best_match(
                ['application/sql', 'application/json'])

        if best_match == 'application/json':
            return flask.Response(ujson.dumps(schema), status=201 if created else 200, mimetype='application/json')
        else:
            return flask.Response(schema.sql, status=201 if created else 200, mimetype='application/sql')

    return app

local = make_app(trough.sync.get_controller(server_mode=False))
server = make_app(trough.sync.get_controller(server_mode=True))

