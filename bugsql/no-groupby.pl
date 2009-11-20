#!/usr/pkg/bin/perl

use strict;
use warnings;

use Data::Peek;
use DBI;

open my $fh, ">", "foo.csv";
print $fh "c_foo,foo,bar\n";
for ( 1 .. 40000 )
{
    print $fh join ",", $_, ( "a" .. "f" )[ int rand 6 ], int rand 10, "\n";
}
close $fh;

my $dbh = DBI->connect(
    "dbi:CSV:",
    undef, undef,
    {
       f_dir    => ".",
       f_ext    => ".csv/r",
       f_schema => "undef",

       RaiseError => 1,
       PrintError => 1,
    }
);

my ( $foo, $cnt, $x1, $x2 );
my $sth = $dbh->prepare(
    qq;
       select   foo, count (*)
       from     foo;
    );
$sth->execute;
DDumper $sth->{NAME_lc};
my $ary_ref = $sth->fetchall_arrayref();
DDumper $ary_ref;
