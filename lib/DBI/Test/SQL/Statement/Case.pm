package DBI::Test::SQL::Statement::Case;

use strict;
use warnings;

use Carp qw(carp);

use DBI::Mock ();

sub filter_drivers
{
    my ($self, @test_dbds) = @_;
    my @supported_dbds = grep { "DBD::$_"->isa("DBI::DBD::SqlEngine") || $_ eq 'NullP' }
	map { eval "require DBD::$_;" and "$_" } @test_dbds;
    return @supported_dbds;
}

sub requires_extended { 0 }

sub supported_variant
{
    my ( $self, $test_case, $cfg_pfx, $test_confs, $dsn_pfx, $dsn_cred, $options ) = @_;

    # allow DBI::DBD::SqlEngine based for DBI
    if( ( -f $INC{'DBI.pm'} and !scalar(@$test_confs)) or grep { $_->{cat_abbrev} eq "z" } @$test_confs )
    {
	$dsn_cred or return;
	$dsn_cred->[0] or return;
	(my $driver = $dsn_cred->[0]) =~ s/^dbi:(\w*?)(?:\((.*?)\))?:.*/DBD::$1/i;
	# my $drh = $DBI::installed_drh{$driver} || $class->install_driver($driver)
	#   or die "panic: $class->install_driver($driver) failed";    
	eval "require $driver;";
	$@ and return carp $@;
	$driver->isa("DBD::File") and return 1;
    }

    # allow DBD::NullP for DBI::Mock
    if( ($INC{'DBI.pm'} eq "mocked" and !scalar(@$test_confs)) or grep { $_->{cat_abbrev} eq "m" } @$test_confs )
    {
	$dsn_cred or return 1;
	$dsn_cred->[0] eq 'dbi:NullP:' and return 1;
    }

    return;
}

1;
