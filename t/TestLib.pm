package TestLib;

use strict;
use warnings;
use vars qw(@ISA @EXPORT @EXPORT_OK);

use Exporter;
use File::Spec;
use Cwd;
use File::Path;

@ISA       = qw(Exporter);
@EXPORT_OK = qw(test_dir prove_reqs show_reqs connect default_recommended);

my $test_dsn  = delete $ENV{DBI_DSN};
my $test_user = delete $ENV{DBI_USER};
my $test_pass = delete $ENV{DBI_PASS};

my $test_dir;
END { defined($test_dir) and rmtree $test_dir }

sub test_dir
{
    unless ( defined($test_dir) )
    {
        $test_dir = File::Spec->rel2abs( File::Spec->curdir() );
        $test_dir = File::Spec->catdir( $test_dir, "test_output_" . $$ );
        $test_dir = VMS::Filespec::unixify($test_dir) if ( $^O eq 'VMS' );
        rmtree $test_dir;
        mkpath $test_dir;
    }

    return $test_dir;
}

sub check_mod
{
    my ( $module, $version ) = @_;
    my $mod_path = $module;
    $mod_path =~ s|::|/|g;
    $mod_path .= '.pm';
    eval { require $mod_path };
    $@                             and return ( 0, $@ );
    $version le $module->VERSION() and return ( 1, $module->VERSION() );
    return (
             0,
             sprintf(
                      "%s->VERSION() of %s doesn't satisfy requirement of %s",
                      $module, $module->VERSION, $version
                    )
           );
}

my %defaultRecommended = (
              'DBI'       => '1.612',
	      'DBD::File' => '0.39',
              'DBD::CSV'  => '0.30',
              'DBD::DBM'  => '0.05',
            );

sub default_recommended
{
    return %defaultRecommended;
}

sub prove_reqs
{
    my %requirements;
    my %recommends;

    {
        my %req = ( 'SQL::Statement' => '1.32', );
        my %missing;
        while ( my ( $m, $v ) = each %req )
        {
            my ( $ok, $msg ) = check_mod( $m, $v );
            $ok and $requirements{$m} = $msg;
            $ok or $missing{$m} = $msg;
        }

        if (%missing)
        {
            my $missingMsg =
                "YOU ARE MISSING REQUIRED MODULES: [ "
              . join( ", ", keys %missing ) . " ]:\n"
              . join( "\n", values(%missing) );

            if ( $INC{'Test/More.pm'} )
            {
                Test::More::BAIL_OUT $missingMsg;
            }
            else
            {
                print STDERR "\n\n$missingMsg\n\n";
                exit 0;
            }
        }
    }
    {
        my %req =
          $_[0]
          ? %{ $_[0] }
          : %defaultRecommended;
        while ( my ( $m, $v ) = each %req )
        {
            my ( $ok, $msg ) = check_mod( $m, $v );
	#    if ( !$ok and $INC{'Test/More.pm'} )
	#    {
	#	Test::More::diag($msg);
	#    }
            $ok and $recommends{$m} = $msg;
        }
    }

    return ( \%requirements, \%recommends );
}

sub show_reqs
{
    my @proved_reqs = @_;

    if ( $INC{'Test/More.pm'} )
    {
        Test::More::diag("Using required:") if ( $proved_reqs[0] );
        Test::More::diag( "  $_: " . $proved_reqs[0]->{$_} ) for sort keys %{ $proved_reqs[0] };
        Test::More::diag("Using recommended:") if ( $proved_reqs[1] );
        Test::More::diag( "  $_: " . $proved_reqs[1]->{$_} ) for sort keys %{ $proved_reqs[1] };
    }
    else
    {
        print("# Using required:\n") if ( $proved_reqs[0] );
        print( "#   $_: " . $proved_reqs[0]->{$_} . "\n" ) for sort keys %{ $proved_reqs[0] };
        print("# Using recommended:\n") if ( $proved_reqs[1] );
        print( "#   $_: " . $proved_reqs[1]->{$_} . "\n" ) for sort keys %{ $proved_reqs[1] };
    }
}

sub connect
{
    my $type = shift;
    defined($type)
      and $type =~ m/^dbi:/i
      and return TestLib::DBD->new( $type, @_ );
    defined($type)
      and $type =~ s/^dbd::/dbi:/i
      and return TestLib::DBD->new( "$type:", @_ );
    return TestLib::Direct->new(@_);
}

package TestLib::Direct;

sub new
{
    my ( $class, $flags ) = @_;
    $flags ||= {};
    my $parser = SQL::Parser->new( 'ANSI', $flags );
    my %instance = ( parser => $parser, );
    my $self = bless( \%instance, $class );
    return $self;
}

sub prepare
{
    my ( $self, $sql ) = @_;
    my $stmt = SQL::Statement->new( $sql, $self->{parser} );
    $self->{stmt} = $stmt;
    return $self;
}

sub execute
{
    my $self = shift;
    return $self->{stmt}->execute(@_);
}

sub do
{
    my $self = shift;
    my $sql  = shift;
    return $self->prepare($sql)->execute(@_);
}

sub selectrow_array
{
    my $self  = shift;
    $self->do(@_);
    my $result = $self->{sth}->fetchrow_arrayref();
    return wantarray ? @$result : $result->[0];
}

sub fetch_row
{
    my $self = $_[0];
    return $self->{stmt}->fetch_row();
}

sub fetch_rows
{
    my $self = $_[0];
    return $self->{stmt}->fetch_rows();
}

sub errstr
{
    defined( $_[0]->{stmt} ) and return $_[0]->{stmt}->errstr();
    return $_[0]->{parser}->errstr();
}

sub finish
{
    delete $_[0]->{stmt};
}

package TestLib::DBD;

sub new
{
    my ( $class, $dsn, $attrs ) = @_;
    $attrs ||= {};
    my $dbh = DBI->connect( $dsn, undef, undef, $attrs );
    my %instance = ( dbh => $dbh, );
    my $self = bless( \%instance, $class );
    return $self;
}

sub prepare
{
    my ( $self, $sql, $attr ) = @_;
    my $sth = $self->{dbh}->prepare($sql, $attr);
    $self->{sth} = $sth;
    return $self;
}

sub execute
{
    my $self = shift;
    return $self->{sth}->execute(@_);
}

sub do
{
    my $self  = shift;
    my $sql   = shift;
    my $attrs = shift;
    return $self->prepare($sql, $attrs)->execute(@_);
}

sub selectrow_array
{
    my $self  = shift;
    $self->do(@_);
    my $result = $self->{sth}->fetchrow_arrayref();
    return wantarray ? @$result : $result->[0];
}

sub fetch_row
{
    my $self = $_[0];
    return $self->{sth}->fetch();
}

sub fetch_rows
{
    my $self = $_[0];
    return $self->{sth}->fetchall_arrayref();
}

sub fetchall_hashref
{
    my $self = shift;
    return $self->{sth}->fetchall_hashref(@_);
}

sub errstr
{
    defined( $_[0]->{sth} ) and return $_[0]->{sth}->errstr();
    return $_[0]->{dbh}->errstr();
}

sub finish
{
    delete $_[0]->{sth};
}

1;
