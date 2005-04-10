#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More tests => 4;
use lib qw' ./ ./t ';
use SQLtest;

# Test RaiseError for prepare errors
#
$parser = new_parser();
$parser->{PrintError}=0;
ok(parse("Junk"),'Parse RaiseError=0 (default)');
$parser->{RaiseError}=1;
ok(!parse("Junk"),'Parse RaiseError=1');

# Test RaiseError for execute errors
#
$parser = new_parser();
$parser->{PrintError}=0;
my $sql = 'SELECT * FROM nonexistant';
ok(do_($sql),'Execute RaiseError=0 (default)');
$parser->{RaiseError}=1;
ok(!do_($sql),'Execute RaiseError=1');
