template = '''else if (nextTransaction.getProcedureClass().equals(Q{0}.class)) {{
        	Q{0} proc = (Q{0}) this.getProcedure(Q{0}.class);
			proc.run(conn, gen, terminalWarehouseID, numWarehouses,
					terminalDistrictLowerID, terminalDistrictUpperID, this);
		}}'''

with open("worker.txt", 'w') as f:
    with open("import.txt", "w") as fi:

        for x in xrange(2, 23):
            print >> f, template.format(x),
            print >> fi, "import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q{0};".format(x)

