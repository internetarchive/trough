from . import settings, read, write, sync

# monkey-patch log level TRACE
import logging
TRACE = logging.DEBUG // 2
def _logging_trace(msg, *args, **kwargs):
    logging.root.trace(msg, *args, **kwargs)
def _logger_trace(self, msg, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, msg, args, **kwargs)
logging.trace = _logging_trace
logging.Logger.trace = _logger_trace
logging.addLevelName(TRACE, 'TRACE')

# monkey-patch log level TRACE
NOTICE = (logging.INFO + logging.WARNING) // 2
def _logging_notice(msg, *args, **kwargs):
    logging.root.notice(msg, *args, **kwargs)
def _logger_notice(self, msg, *args, **kwargs):
    if self.isEnabledFor(NOTICE):
        self._log(NOTICE, msg, args, **kwargs)
logging.notice = _logging_notice
logging.Logger.notice = _logger_notice
logging.addLevelName(NOTICE, 'NOTICE')

