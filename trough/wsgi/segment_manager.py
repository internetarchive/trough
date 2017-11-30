import trough
import flask
import logging
import ujson
import sqlite3

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
        schema_id = flask.request.json.get('schema', 'default')
        logging.info('provisioning writable segment %r (schema_id=%r)', segment_id, schema_id)
        # {'write_url': write_url, 'size': None, 'schema': schema}
        result_dict = controller.provision_writable_segment(segment_id, schema_id=schema_id)
        result_json = ujson.dumps(result_dict)
        return flask.Response(result_json, mimetype='application/json')

    @app.route('/promote', methods=['POST'])
    def promote_writable_segment():
        '''Promotes segments to HDFS, will respond with a JSON object which describes:
        - hdfs path
        - segment size on disk
        - whether or not an upstream segment will be overwritten

    This endpoint will toggle a value on the write lock record, which will be consulted so that a segment cannot be promoted while a promotion is in progress. The current journal will be committed, and after promotion completes, this URL will return its JSON document.'''
        post_json = ujson.loads(flask.request.get_data())
        segment_id = post_json['segment']
        result_dict = controller.promote_writable_segment_upstream(segment_id)
        result_json = ujson.dumps(result_dict)
        return flask.Response(result_json, mimetype='application/json')

    @app.route('/schema', methods=['GET'])
    def list_schemas():
        '''Schema API Endpoint, lists schema names'''
        result_json = ujson.dumps(controller.list_schemas())
        return flask.Response(result_json, mimetype='application/json')

    @app.route('/schema/<id>', methods=['GET'])
    def get_schema(id):
        '''Schema API Endpoint, returns schema json'''
        schema = controller.get_schema(id=id)
        if not schema:
            flask.abort(404)
        return flask.Response(ujson.dumps(schema), mimetype='application/json')

    @app.route('/schema/<id>/sql', methods=['GET'])
    def get_schema_sql(id):
        '''Schema API Endpoint, returns schema sql'''
        schema = controller.get_schema(id=id)
        if not schema:
            flask.abort(404)
        return flask.Response(schema.sql, mimetype='application/sql')

    @app.route('/schema/<id>', methods=['PUT'])
    def put_schema(id):
        '''Schema API Endpoint, creates or updates schema from json input'''
        try:
            schema_dict = ujson.loads(flask.request.get_data(as_text=True))
        except:
            return flask.Response(
                    status=400, mimetype='text/plain',
                    response='input could not be parsed as json')
        if set(schema_dict.keys()) != {'id','sql'}:
            return flask.Response(status=400, mimetype='text/plain', response=(
                "input json has keys %r (should be {'id', 'sql'})" % set(schema_dict.keys())))
        if schema_dict.get('id') != id:
            return flask.Response(
                    status=400, mimetype='text/plain',
                    response='id in json %r does not match id in url %r' % (
                        schema_dict.get('id'), id))

        try:
            schema, created = controller.set_schema(id=id, sql=schema_dict['sql'])
        except sqlite3.OperationalError as e:
            return flask.Response(
                    status=400, mimetype='text/plain',
                    response='schema sql failed validation: %s' % e)

        return flask.Response(status=201 if created else 204)

    @app.route('/schema/<id>/sql', methods=['PUT'])
    def put_schema_sql(id):
        '''Schema API Endpoint, creates or updates schema from sql input'''
        sql = flask.request.get_data(as_text=True)
        try:
            schema, created = controller.set_schema(id=id, sql=sql)
        except sqlite3.OperationalError as e:
            return flask.Response(
                    status=400, mimetype='text/plain',
                    response='schema sql failed validation: %s' % e)

        return flask.Response(status=201 if created else 204)

    return app

local = make_app(trough.sync.get_controller(server_mode=False))
server = make_app(trough.sync.get_controller(server_mode=True))

