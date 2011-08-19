#!/usr/bin/python

"""Stupid Plot

A stupid Python wrapper around GnuPlot. It automates some of the tasks involved
in creating GnuPlot output from Python data.

To use it, call the gnuplotTable() function with your data (2D list) and output
file. It will do its best attempt to plot it in EPS format. To customize stuff, use
the gnuplotOptions dictionary. You will likely need to know a bit about
GnuPlot and read the source to this module to understand what is going on.
"""

import math
import os
import tempfile


# A few "standard" label types
# 12.345
EXPONENTIAL_LABEL = "%.0t{/Symbol \264}10^{%T}" # 1*10^1
EXPONENTIAL_LABEL1 = "%.1t{/Symbol \264}10^{%T}" # 1*10^1

def histogram( data, numBuckets, minTruncate=None, maxTruncate=None ):
	'''Pass in a list of data values and the number of buckets. It will return a list of
	coordinates that will draw a nice histogram in GnuPlot with lines.'''

	data = list( data )
	data.sort()
	
	minValue = data[0]
	maxValue = data[-1]
	
	if maxTruncate != None:
		maxValue = min( maxTruncate, maxValue )
	if minTruncate != None:
		assert minTruncate < maxValue
		minValue = max( minTruncate, minValue )
	
	interval = float(maxValue-minValue)/numBuckets
	
	buckets = []
	
	bucketCount = 0
	bucketLimit = minValue + interval
	
	for value in data:
		while value > bucketLimit and len(buckets) < numBuckets-1:
			#~ print bucketLimit, bucketCount
			buckets.append( bucketCount )
			bucketLimit = minValue + interval*(len( buckets ) + 1)
			bucketCount = 0
		
		if value > maxValue:
			break
		
		bucketCount += 1
	buckets.append( bucketCount )
	
	assert abs( bucketLimit - maxValue ) < 0.00001
	
	# Draw the line for gnuplot
	total = float( len( data ) )
	
	out = []
	
	# Start at zero
	if buckets[0] != 0:
		out.append( (minValue, 0) )
	
	previous = minValue
	for i, bucketCount in enumerate( buckets ):
		next_value = minValue + interval*(i + 1)
		
		out.append( (previous, bucketCount/total*100.0) )
		out.append( (next_value, bucketCount/total*100.0) )
		
		previous = next_value
	
	# End at zero
	if buckets[-1] != 0:
		out.append( (previous, 0) )
	
	assert abs( out[-1][0] - maxValue ) < 0.00001
	
	return out

# T statistic for 0.05 confidence (95%)
# Computed with Excel: =TINV(0.05, x)
# TODO: Import python code for computing this
T_TABLE = [
    None,
    12.706204733987,
    4.30265272954454,
    3.18244630488688,
    2.77644510504380,
    2.57058183469754,
    2.44691184643268,
    2.36462425094932,
    2.30600413329912,
    2.26215715817358,
    2.22813884242587,
    2.20098515872184,
    2.17881282716507,
    2.16036865224854,
    2.14478668128208,
    2.13144953567595,
    2.11990528516258,
    2.10981555859266,
    2.10092203686118,
    2.09302404985486,
    2.08596344129554,
    2.07961383708272,
    2.07387305831561,
    2.06865759861054,
    2.06389854731807,
    2.05953853565859,
    2.05552941848069,
    2.05183049297067,
    2.04840711466289,
    2.04522961110855,
    2.04227244936679,
    2.03951343844151,
    2.03693333440703,
    2.03451528722141,
    2.03224449783959,
    2.03010791544831,
    2.02809398678268,
    2.02619244736580,
    2.02439414671557,
    2.02269090124204,
    2.02107536985045,
    2.01954094826419,
    2.01808167886218,
    2.01669217343735,
    2.01536754676655,
    2.01410335926697,
    2.01289556732150,
    2.01174048010300,
    2.01063472192628,
    2.00957519932024,
    2.00855907214326,
    2.00758372817470,
    2.00664676070409,
    2.00574594871316,
    2.00487927499539,
    2.00404476937785,
    2.00324070420509,
    2.00246544390452,
    2.00171746800345,
    2.00099536118020,
    2.00029780432954,
    1.99962356652375,
    1.99897149776650,
    1.99834052244951,
    1.99772963343394,
    1.99713788668814,
    1.99656439642122,
    1.99600833066037,
    1.99546890722492,
    1.99494539005665,
    1.99443708586968,
    1.99394334108840,
    1.99346353904453,
    1.99299709740838,
    1.99254346583172,
    1.99210212378202,
    1.99167257855056,
    1.99125436341783,
    1.99084703596246,
    1.99045017650031,
    1.99006338664240,
    1.98968628796136,
    1.98931852075642,
    1.98895974290966,
    1.98860962882431,
    1.98826786843964,
    1.98793416631522,
    1.98760824077917,
    1.98728982313560,
    1.98697865692559,
    1.98667449723875,
    1.98637711007022,
    1.98608627172049,
    1.98580176823426,
    1.98552339487556,
    1.98525095563659,
    1.98498426277744,
    1.98472313639477,
    1.98446740401708,
    1.98421690022499,
    1.98397146629437]


def stats(r):
    """Returns statistics about a sequence of numbers.
    
    Returns (average, median, standard deviation, min, max, 95% confidence interval)"""

    total = sum(r)
    average = total/float(len(r))
    sum_deviation_squared = sum([(i-average)**2 for i in r])
    standard_deviation = math.sqrt(sum_deviation_squared/(len(r)-1 or 1))
    s = list(r)
    s.sort()
    median = s[len(s)/2]
    minimum = s[0]
    maximum = s[-1]
    # z value for 95% confidence interval is ~1.96
    # This isn't correct for sampled data, since we don't *know* the standard deviation
    # See: http://davidmlane.com/hyperstat/
    # confidence_95 = 1.959963984540051 * standard_deviation / math.sqrt(len(r))
    # We must estimate both using the t distribution:
    # http://davidmlane.com/hyperstat/B7483.html
    # s_m = s / sqrt(N)
    s_m = standard_deviation / math.sqrt(len(r))
    # Degrees of freedom = n-1
    # t = t(degrees_of_freedom, 0.05)
    # confidence = +/- t * s_m
    confidence_95 = T_TABLE[len(r)-1] * s_m
    return average, median, standard_deviation, minimum, maximum, confidence_95


def hackDottedStyle( epsFile ):
	'''GNUPlot's default dotted line styles suck. This will hack the given EPS so
	the lines are more easily distinguishable.'''
	
	f = file( epsFile )
	inLines = f.readlines()
	f.close()

	output = file( epsFile, 'w' )
	for line in inLines:
		if line.startswith( '/LT1 {' ):
			output.write( '/LT1 { PL [8 dl1 4 dl2] LC1 DL } def\n' )
		elif line.startswith( '/LT2 {' ):
			output.write( '/LT2 { PL [4 dl1 3 dl2] LC2 DL } def\n' )
		elif line.startswith( '/LT3 {' ):
			output.write( '/LT3 { PL [2 dl1 2 dl2] LC3 DL } def\n' )
		elif line.startswith( '/LT4 {' ):
			output.write( '/LT4 { PL [5 dl1 2 dl2 1 dl1 2 dl2 1 dl1 2 dl2 1 dl1 2 dl2] LC4 DL } def\n' )
		elif line.startswith( '/LT5 {' ):
			output.write( '/LT5 { PL [5 dl1 2 dl2 5 dl1 8 dl2] LC5 DL } def\n' )
		elif line.startswith( '/LT6 {' ):
			output.write( '/LT6 { PL [2 dl1 2 dl2 2 dl1 2 dl2 2 dl1 8 dl2] LC6 DL } def\n' )
		elif line.startswith( '/LT7 {' ):
			output.write( '/LT7 { PL [8 dl1 5 dl2 1 dl1 5 dl2] LC7 DL } def\n' )
		else:
			output.write( line )
	output.close()

def hackBarChartColor( epsFile, numBars, skip=0 ):
	'''Creates a legible greyscale bar chart EPS from a color one.'''
	
	f = file( epsFile )
	inLines = f.readlines()
	f.close()
	
	colors = []
	# Create even colors from black to white
	color = 0.0
	step = 1.0
	if numBars > 1: step = 1.0 / (numBars-1)
	while len( colors ) < numBars:
		colors.append( color )
		color = len( colors ) * step
	
	output = file( epsFile, 'w' )
	for line in inLines:
		if line.startswith( '/LT' ) and line[3].isdigit():
			barNumber = int( line[3] )
			if barNumber + skip < len( colors ):
				color = colors[barNumber+skip]
				output.write( '/LT%d { PL [] %f %f %f DL } def\n' % ( barNumber, color, color, color ) )
				# Skip the rest of the processing
				continue
				
		output.write( line )
	output.close()


# The default plot formatting options; overridden by user code
DEFAULT_OPTIONS = {
    "gnuplot": "gnuplot",
    "title" : "",
    "xrange": "[]",
    "yrange": "[]",
    "ylabel": "Y Axis Values",
    "grid": "",
    "xformat": r"%0.1s%c",
    "yformat": r"%0.1s%c",
    "linewidth": 4,
    "fontsize": 16,
    "color": True,
    "dashed": False,
    "plottype": "lines", # Changed the default "plottype"
    "plottypes": {}, # Overrides the default "plottype"
    "key": "",
    "size": "",
    "boxstuff": "",
    "pointsize": "",
    "colors": {},
    "xtics": "",
    "ytics": "",
}


# NOTE: If you pass "plottype": 'barchart' this script tries to plot a nice bar chart
def gnuplotTable( table, outputFile, gnuplotOptions={} ):
	"""table - 2D list of data. The first row is taken as headers.
	outputFile - The output of GnuPlot goes here (in .eps format).
	gnuplotOptions - A dict used to customize the behaviour of gnuplot."""
	
	# Make table into tables, unless it already is
	tables = table
	if not isinstance( tables[0][0], list ) and not isinstance( tables[0][0], tuple ):
		tables = ( tables, )
	
	# For the tables to work, the first column must have the same header, not necissarily the same values
	for table in tables:
		assert( table[0][0] == tables[0][0][0] )
	
	# Copy the user supplied options into our options dictionary
	options = dict(DEFAULT_OPTIONS)
	options["xlabel"] = tables[0][0][0]
	for key,value in gnuplotOptions.items():
		options[key] = value
	
	# Add "set key " to the front of the key option, if it was specified
	if options["key"] != "":
		if options["key"] is False:
			options["key"] = "set nokey"
		else:
			options["key"] = "set key " + options["key"]
	# Add "set size " to the front of the size option, if it was specified
	if options["size"] != "": options["size"] = "set size " + options["size"]
	# Add "set pointsize " to the front of the size option, if it was specified
	if options["pointsize"] != "": options["pointsize"] = "set pointsize " + str( options["pointsize"] )
	if options["xtics"] != "": options["xtics"] = "set xtics " + str(options["xtics"])
	if options["ytics"] != "": options["ytics"] = "set ytics " + str(options["ytics"])

	color = "color"
	solid = "set terminal postscript solid"
	if not options["color"] and options['plottype'] != 'barchart':
		color = "monochrome"
		solid = ""
	if options["color"] and options["dashed"]:
		solid = ""

	boxWidth = None
	if options['plottype'] == 'barchart':
		options['barchart'] = True
		options['xformat'] = "%s"
		xtics = [ None ]
		
		# Number of X values = number of rows in the tables
		numXValues = len( tables[0] ) - 1
		# Number of boxes = number of columns in the tables
		numBoxes = 0
		for table in tables:
			numBoxes += (len( table[0] ) - 1)
			
			for i, row in enumerate( table ):
				#~ print i, row
				if i == 0: continue
					
				if i < len( xtics ):
					assert( row[0] == xtics[i] )
				else:
					xtics.append( row[0] )
					
				row[0] = i
		
		# This box width is sufficient to leave one empty box between clusters
		boxWidth = 1.0 / (1 + numBoxes)
		
		options['xrange'] = "[%f:%f]" % (1 - (boxWidth*numBoxes)/2, numXValues + (boxWidth*numBoxes)/2)
		options['grid'] = 'noxtics ytics linewidth 2.0'
		options['plottype'] = 'boxes'

		xticString = "( "
		for i, x in enumerate( xtics ):
			if i == 0: continue
			
			xticString += '"%s" %d, ' % ( x, i )
		xticString = xticString[:-2] + ")"
		
		#set xtics rotate %s\n
		options['boxstuff'] = 'set tics scale 0\nset xtics %s\nset boxwidth %f\nset style fill solid border -1' % ( xticString, boxWidth )

	
	# Gnuplot output
	scriptfile = """
set title "%s"
set xlabel "%s"
set ylabel "%s"
set grid %s

# Set the axes to engineering notation
set format x '%s'
set format y '%s'

set xrange %s
set yrange %s

set terminal postscript "Helvetica" %d
set terminal postscript %s # color or monochrome
%s # Use solid or dotted lines
set terminal postscript eps enhanced
set output "%s"

%s
%s
%s
%s
%s
%s
""" % ( options["title"], options["xlabel"], options["ylabel"],
	 options["grid"], options["xformat"], options["yformat"],
	options["xrange"], options["yrange"], options["fontsize"],
	color, solid, outputFile, options["key"], options["size"], options["boxstuff"],
	options["pointsize"], options["xtics"], options["ytics"])

	if 'calculated' in options:
		scriptfile += 'f(x) = %s\n' % options['calculated']
	
	tempDataFiles = []
	
	plotLines = []
	linecount = 1
	for table in tables:
		data = tempfile.NamedTemporaryFile()
		# Skip the headers in the data files. On occasion they confuse GnuPlot
		for line in table[1:]:
			data.write( "\t".join( [str(i) for i in line] ) )
			data.write( "\n" )
		data.flush()
		tempDataFiles.append( data )
	
		headings = table[0]
		
		for i,heading in enumerate( headings[1:] ):
			# Skip this column if it is an error bar column. Error bar columns
			# are expressed in the ORIGINAL table columns: That is, column 1
			# there = column 0 here (we skip the x-axis column
			if "errorbars" in options and (i in options["errorbars"] or i-1 in options["errorbars"]): continue
			
			plottype = options["plottype"]
			if i in options["plottypes"]: plottype = options["plottypes"][i]
			
			color = ""
			if linecount in options["colors"]:
				color = "linecolor %s" % (options["colors"][linecount])

			if boxWidth:
				offset = (-(numBoxes-1)/2.0 + i) * boxWidth
				#~ print "numBoxes =", numBoxes, "boxWidth =", boxWidth, "i =",i, "offset =", offset
				plotLines.append( ' "%s" using ($1+%f):%d title "%s" with %s' % ( data.name, offset, i+2, heading, plottype ) )
			else:
				plotLines.append( ' "%s" using 1:%d title "%s" with %s linetype %d linewidth %d %s' % ( data.name, i+2, heading, plottype, linecount, options["linewidth"], color ) )
			
				if "errorbars" in options and i+1 in options["errorbars"]:
					# Set the linetype so it looks the same as the line we drew
					# Set pointsize to 0 because otherwise gnuplot puts a cross or other point at the midpoint
					if options["color"] and color == "":
						color = "linecolor %d" % linecount
					plotLines.append( '"%s" using 1:%d:%d:%d notitle with yerrorbars linetype 1 linewidth %f %s pointsize 0' % ( data.name, i+2, i+3, i+4, options["linewidth"] * 0.25, color ) )
			linecount += 1
	
	if 'calculated' in options:
		plotLines.insert( 0, 'f(x) with lines' )
	
	scriptfile += "plot " + ", ".join( plotLines )
	
	script = tempfile.NamedTemporaryFile()
	script.write( scriptfile )
	script.flush()
	
	#~ import shutil
	#~ shutil.copyfile( data.name, "data.txt" )
	#~ shutil.copyfile( script.name, "script.txt" )
	
	code = os.spawnlp( os.P_WAIT, options["gnuplot"], options["gnuplot"], script.name )
	assert( code == 0 )
	
	script.close()
	for data in tempDataFiles:
		data.close()
	
	if not options['color'] or options['dashed']:
		if 'barchart' in options and options['barchart']:
			hackBarChartColor( outputFile, numBoxes )
		else:
			hackDottedStyle( outputFile )
