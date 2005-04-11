#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More tests => 2;
use lib qw' ./ ./t ';
use SQLtest;

# Test RaiseError for prepare errors
#
$parser = new_parser();
my $foo=0;
sub test {$foo = 9;}
do_("CREATE FUNCTION test");
ok(do_("CALL test"),'call function');
ok(9==$foo,'call function');
