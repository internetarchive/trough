import tornado.web

# setup an web application
# export it as 'app' so it can be used by the same read and write scripts.
#asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
#loop = asyncio.get_event_loop()
app = tornado.web.Application()
