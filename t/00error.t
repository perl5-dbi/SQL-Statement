#!/usr/bin/perl -w
$| = 1;
use strict;
use Test::More tests => 6;
use lib qw' ./ ./t ';
use SQLtest;

# Test RaiseError for prepare errors
#
$parser = new_parser();
$parser->{PrintError} = 0;
ok( parse("Junk"), 'Parse RaiseError=0 (default)' );
$parser->{RaiseError} = 1;
ok( !parse("Junk"), 'Parse RaiseError=1' );

# Test RaiseError for execute errors
#
$parser = new_parser();
$parser->{PrintError} = 0;
do_("SELECT UPPER('a')");
ok( !defined( $SQLtest::stmt->errstr() ), '$stmt->errstr with no error' );
my $sql = 'SELECT * FROM nonexistant';
ok( do_($sql), 'Execute RaiseError=0 (default)' );
$parser->{RaiseError} = 1;
ok( !do_($sql),                          'Execute RaiseError=1' );
ok( defined( $SQLtest::stmt->errstr() ), '$stmt->errstr with error' );
