import re


__all__ = (
    'JOBKEY_SEPARATOR', 'JOBKEY_PATTERN',
    'META_STATE', 'META_STATE_FINISHED',
    'META_CLOSE_REASON', 'META_CLOSE_REASON_FINISHED',
    'META', 'META_KEY', 'META_ITEMS', 'META_SPIDER',
)


JOBKEY_SEPARATOR = '/'
JOBKEY_PATTERN = re.compile('\d+{sep}\d+{sep}\d+'.format(sep=JOBKEY_SEPARATOR))


# ============
#     meta
# ============
META = 'meta'

META_KEY = 'key'
META_ITEMS = 'items'
META_SPIDER = 'spider'

META_STATE = 'state'
META_STATE_FINISHED = 'finished'

META_CLOSE_REASON = 'close_reason'
META_CLOSE_REASON_FINISHED = 'finished'
