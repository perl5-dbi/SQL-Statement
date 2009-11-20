#!/usr/bin/perl -w
#
# exercise an sql bug

use strict;
use DBI;
use File::Temp qw(tempdir);
use Data::Dumper;
use Data::Peek;

my $f_dir = tempdir(CLEANUP => 1);

my $dbh = DBI->connect("dbi:CSV:f_dir=$f_dir");

$dbh->do('CREATE TABLE foo (a VARCHAR(32), b VARCHAR(32), c VARCHAR(32))');
$dbh->do('CREATE TABLE bar (a VARCHAR(32), d VARCHAR(32), e VARCHAR(32))');
$dbh->do("INSERT INTO foo (a,b) VALUES ('one', 'foo-1b')");
$dbh->do("INSERT INTO bar (a,d) VALUES ('one', 'bar-1d')");

my $ref = $dbh->selectrow_hashref('SELECT * FROM foo JOIN bar USING (a)');
DDumper($ref);
$ref = $dbh->selectrow_hashref('select * from foo join bar using (a)');
DDumper($ref);
