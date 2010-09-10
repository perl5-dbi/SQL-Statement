#!/usr/bin/perl -w
use strict;
use warnings;
use lib qw(t);

use Test::More;
use TestLib qw(connect prove_reqs show_reqs);

my ( $required, $recommended ) = prove_reqs();
show_reqs( $required, $recommended );
my @test_dbds = ( 'SQL::Statement', grep { /^dbd:/i } keys %{$recommended} );

foreach my $test_dbd (@test_dbds)
{
    my $dbh;
    diag("Running tests for $test_dbd");

    # Test RaiseError for prepare errors
    #
    $dbh = connect(
                    $test_dbd,
                    {
                       PrintError => 0,
                       RaiseError => 0,
                    }
                  );
    eval { $dbh->prepare("Junk"); };
    ok( !$@, 'Parse "Junk" RaiseError=0 (default)' ) or diag($@);
    eval { $dbh->do("SELECT UPPER('a')"); };
    ok( !$@, 'Execute function succeeded' ) or diag($@);
    ok( !$dbh->errstr(), 'Execute function no errstr' ) or diag($dbh->errstr());
    eval { $dbh->do( "SELECT * FROM nonexistant" ); };
    ok( !$@, 'Execute RaiseError=0' ) or diag($@);

    $dbh = connect(
                    $test_dbd,
                    {
                       PrintError => 0,
                       RaiseError => 1,
                    }
                  );
    eval { $dbh->prepare("Junk"); };
    ok( $@, 'Parse "Junk" RaiseError=1' );
    {
	local $TODO = "Bug in DBD::DBM 0.39 (DBI before 1.614)" if( $test_dbd eq "DBD::DBM" and DBD::DBM->VERSION() lt "0.40" );
	eval { $dbh->do( "SELECT * FROM nonexistant" ); };
	ok( $@, 'Execute RaiseError=1' );
	ok( $dbh->errstr(), 'Execute "SELECT * FROM nonexistant" has errstr' );
    }
}

done_testing();
