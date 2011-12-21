### Author: curino@mit.edu

class dstat_plugin(dstat):
    """
    THROUGHPUT.
    """

    def __init__(self):
        self.name = 'throughput'
        self.type = 'p'
        self.width = 4
        self.scale = 34

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
		f = open('/tmp/dstat/sla/'+csla+'/throughput', 'r')
		st = f.readline().rstrip()
		if(st == ""):
		   self.val[csla] = 0
		else:
 	      	   self.val[csla] = float(st)
 		f.close()

    def check(self): 
        for csla in glob.glob('/tmp/dstat/sla/client[0-9]*'):
            if not os.access(csla+'/throughput', os.R_OK):
                raise Exception, 'Cannot access throughput %s information' % os.path.basename(csla)


# vim:ts=4:sw=4:et
