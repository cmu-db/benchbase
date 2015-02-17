#!/usr/bin/env perl

use strict;
use warnings;

my $YEAR=`date +%Y`;
chomp($YEAR);

my $COPYRIGHT = <<END;
/******************************************************************************
 *  Copyright $YEAR by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

END

die("ERROR: Missing target directory\n") unless ($#ARGV >= 0);
my $TARGET_DIR = $ARGV[0];
die("ERROR: Invalid target directory $TARGET_DIR\n") unless (-d $TARGET_DIR);

foreach my $file (`find $TARGET_DIR -name "*.java" -type f`) {
    chomp($file);
    my $line = `head -n 20 $file`;
    unless ($line =~ m/Copyright [\d]{4,4} .*?/) {
        open(IN, "<$file") or die;
        undef $/;
        my $contents = <IN>;
        close(IN);
        $contents =~ s/^\/\*{20,}.*GNU General Public License.*\*{20,}\///s;
        $contents =~ s/^\/\*{20,}\n \*.*H-Store Project.* \*{75}\///s;
        $contents = $COPYRIGHT.$contents;
        open(OUT, ">$file") or die;
        print OUT $contents;
        close(OUT);
        print "Updated $file\n";
    } ## UNLESS
} # FOREACH
# print `svn status $TARGET_DIR`;
exit;
