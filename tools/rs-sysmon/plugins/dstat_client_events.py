### Author: curino@mit.edu

class dstat_plugin(dstat):
    """
    EVENTS COMMENT.
    """

    def __init__(self):
        self.name = 'events'
        self.type = 's'
        self.width = 12
        self.scale = 0

    def vars(self):
        ret = []
        for name in glob.glob('/tmp/dstat/sla/client[0-9]*'):
            ret.append(os.path.basename(name))
        ret.sort()
        return ret

    def nick(self):
        return [name.lower() for name in self.vars]

    def extract(self):
	for csla in self.vars: 
		f = open('/tmp/dstat/sla/'+csla+'/events', 'r')
 	      	self.val[csla] = f.readline().rstrip() 
 		f.close()

    def check(self): 
        for csla in glob.glob('/tmp/dstat/sla/client[0-9]*'):
            if not os.access(csla+'/events', os.R_OK):
                raise Exception, 'Cannot access events %s information' % os.path.basename(csla)

# vim:ts=4:sw=4:et
