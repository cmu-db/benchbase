template = '''else if (nextTransaction.getProcedureClass().equals(Q{0}.class)) {{
        	Q{0} proc = (Q{0}) this.getProcedure(Q{0}.class);
			proc.run(conn, gen, this);
		}}'''

with open("worker.txt", 'w') as f:
    with open("import.txt", "w") as fi:

        for x in xrange(1, 22):
            print >> f, template.format(x).zfill(2),
            print >> fi, "import com.oltpbenchmark.benchmarks.tpch.queries.Q{0};".format(x).zfill(2)

