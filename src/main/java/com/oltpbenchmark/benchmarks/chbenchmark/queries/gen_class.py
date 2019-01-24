def split_string(s, n=40):
    ss = []
    for i in xrange(0, len(s), n):
        ss.append('"{0}"'.format(s[i:i+n]))
    return ("\n" + " " * 30 + "+ ").join(ss)

template = '''package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q{1} extends GenericQuery {{

	@Override
	protected SQLStmt getStmtSQL() {{
		return new SQLStmt(
"{0}"
                                  );
	}}
}}
'''

if __name__ == '__main__':
	for q in xrange(2, 23):
		with open("q{0}.txt".format(q), 'r') as f:
		    with open("Q{0}.java".format(q), 'w') as cf:
		        cf.write(template.format(f.read().replace("\n", '"\n + "'), q))

