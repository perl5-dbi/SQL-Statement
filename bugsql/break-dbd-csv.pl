#!/usr/pkg/bin/perl

use strict;
use warnings;

use DBI;
#use lib 'blib/lib';

my $dbh = DBI->connect( "dbi:CSV:", undef, undef, { f_dir => ".", } );

print "Using DBD::CSV-$DBD::CSV::VERSION, SQL::Statement-$SQL::Statement::VERSION\n";
$dbh->do(q{create table "foo.csv" (c_foo integer)});

for (qw( foo.csv foo csv ))
{
    open my $fh, "<", $_ or next;
    local $/;
    print "File $_:\n", <$fh>;
}

unlink qw( foo.csv foo csv );

