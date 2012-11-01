for num, pre in enumerate(hxs.select('//pre[span[contains(text(), "select")]]')):
	with open("q{0}.txt".format(num + 1), "w") as q:
		print >>q, " ".join(pre.select(".//text()").extract())
    
