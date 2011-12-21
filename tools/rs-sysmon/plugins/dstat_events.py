### Author: curino@mit.edu

class dstat_plugin(dstat):
    """
    EVENTS COMMENT.
    """

    def __init__(self):
        self.name = 'events'
        self.nick = ('comment',)
        self.vars = ('text',)
        self.type = 's'
        self.width = 12
        self.scale = 0

    def extract(self):
	f = open('/tmp/dstat/events', 'r')
        self.val['text'] = f.readline().rstrip() 
	f.close()

    def check(self): 
        if not os.access('/tmp/dstat_events', os.R_OK):
            raise Exception, 'Cannot access /tmp/dstat_events'

# vim:ts=4:sw=4:et
