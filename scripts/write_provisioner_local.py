import trough
import logging
#from trough.settings import settings

# wsgi entrypoint
def application(env, start_response):
    # TODO: master_mode approach may be wrong below. May want something in `settings`
    try:
        controller = trough.sync.get_controller(server_mode=False)
        controller.check_config()
        segment_name = env.get('wsgi.input').read()
        segment_name = segment_name.decode('UTF-8')
        controller.provision_writable_segment(segment_name)
        start_response('200 OK', [('Content-Type','application/json')])
        return b"OK"
    except Exception as e:
        logging.error('500 Server Error due to exception', exc_info=True)
        start_response('500 Server Error', [('Content-Type', 'text/plain')])
        return [('500 Server Error: %s' % str(e)).encode('utf-8')]
