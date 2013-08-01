package DBI::Test::Case::SQL::Statement::error;

use strict;
use warnings;
#use lib qw(t);

#use Test::More;
#use TestLib qw(connect prove_reqs show_reqs);

use Test::More;
use DBI::Test;

#my ( $required, $recommended ) = prove_reqs();
#show_reqs( $required, $recommended );
#my @test_dbds = ( 'SQL::Statement', grep { /^dbd:/i } keys %{$recommended} );

#foreach my $test_dbd (@test_dbds)
sub run_test
{
    my @DB_CREDS = @{$_[1]};

    # note("Running tests for $test_dbd");

    # Test RaiseError for prepare errors
    #
    $DB_CREDS[3]->{PrintError} = 0;
    $DB_CREDS[3]->{RaiseError} = 0;
    my $dbh = connect_ok(@DB_CREDS);

    eval { $dbh->prepare("Junk"); };
    ok( !$@, 'Parse "Junk" RaiseError=0 (default)' ) or diag($@);
    eval { $dbh->do("SELECT UPPER('a')"); };
    ok( !$@, 'Execute function succeeded' ) or diag($@);
    ok( !$dbh->errstr(), 'Execute function no errstr' ) or diag($dbh->errstr());
    eval { $dbh->do( "SELECT * FROM nonexistant" ); };
    ok( !$@, 'Execute RaiseError=0' ) or diag($@);

    $DB_CREDS[3]->{RaiseError} = 1;
    $dbh = connect_ok(@DB_CREDS);
    eval { $dbh->prepare("Junk"); };
    ok( $@, 'Parse "Junk" RaiseError=1' );
    {
	eval { $dbh->do( "SELECT * FROM nonexistant" ); };
	ok( $@, 'Execute RaiseError=1' );
	ok( $dbh->errstr(), 'Execute "SELECT * FROM nonexistant" has errstr' );
    }

    done_testing();
}

1;
