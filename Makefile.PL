# -*- perl -*-

use 5.008;
use strict;
use warnings;
use ExtUtils::MakeMaker;

eval { require SQL::Statement; };

unless ($@)
{
    my $sqlstmtver = $SQL::Statement::VERSION;
    $sqlstmtver =~ s/_[0-9]+$//;
    if ( $sqlstmtver <= 1.20 )
    {
        my $warning = sprintf( <<'END_OF_WARNING', $SQL::Statement::VERSION );

WARNING! You seem to have an older version of SQL::Statement already installed (%s <= 1.20).
This new version introduces a number of features that will impact operation of SQL::Statement and of DBD drivers for CSV, AnyData, and Excel.

Changes include (1.003):
  * There are *many* new SQL features including table joins
  * BLOBS, Empty strings, and NULLs behave differently
  * Some applications may run a bit slower than previously

Changes include (1.22):
  * behavior for unquoted identifiers modified to lower case them
  * IN and BETWEEN operators are supported native

See the Changes for details.

END_OF_WARNING

        my $cpan_smoker = grep { $_ =~ m/(?:CR_SMOKER|CPAN_REPORTER|AUTOMATED_TESTING)/ } keys %ENV;
        if ( $cpan_smoker || defined( $ENV{SQL_STATEMENT_WARN_UPDATE} ) )
        {
            warn $warning;
        }
        else
        {
            $warning .= <<'END_OF_WORKAROUND';
Use

$ env SQL_STATEMENT_WARN_UPDATE=sure perl Makefile.PL

to build and install anyway.

END_OF_WORKAROUND
            Carp::croak($warning);
        }
    }
}

WriteMakefile1(
    MIN_PERL_VERSION => '5.008',
    META_MERGE       => { resources => { repository => 'https://svn.perl.org/modules/SQL-Statement/', }, },
    NAME             => 'SQL::Statement',
    VERSION_FROM     => 'lib/SQL/Statement.pm',
    dist             => {
              'SUFFIX'       => ".gz",
              'DIST_DEFAULT' => 'manifest tardist',
              'COMPRESS'     => "gzip -9vf"
            },
    PREREQ_PM => {
                   'Carp'         => '0',
                   'Clone'        => '0.30',
                   'Data::Dumper' => '0',
                   'Params::Util' => '1.00',
                   'Scalar::Util' => '1.0',
                 },
    LICENSE        => 'perl',
    BUILD_REQUIRES => {
        'Test::Simple' => '0.86',
        # means: DBI >= 1.609
        'DBD::File' => '0.37',
                      },
    AUTHOR => 'Jeff Zucker <jeff@vpservices.com>, Jens Rehsack <rehsack@cpan.org>',
              );

sub WriteMakefile1
{    #Written by Alexandr Ciornii, version 0.21. Added by eumm-upgrade.
    my %params       = @_;
    my $eumm_version = $ExtUtils::MakeMaker::VERSION;
    $eumm_version = eval $eumm_version;
    die "EXTRA_META is deprecated" if ( exists( $params{EXTRA_META} ) );
    die "License not specified"    if ( !exists( $params{LICENSE} ) );
    if ( $params{BUILD_REQUIRES} and ( $eumm_version < 6.5503 ) )
    {
        #EUMM 6.5502 has problems with BUILD_REQUIRES
        $params{PREREQ_PM} = { %{ $params{PREREQ_PM} || {} }, %{ $params{BUILD_REQUIRES} } };
        delete $params{BUILD_REQUIRES};
    }
    delete $params{CONFIGURE_REQUIRES} if ( $eumm_version < 6.52 );
    delete $params{MIN_PERL_VERSION}   if ( $eumm_version < 6.48 );
    delete $params{META_MERGE}         if ( $eumm_version < 6.46 );
    delete $params{META_ADD}           if ( $eumm_version < 6.46 );
    delete $params{LICENSE}            if ( $eumm_version < 6.31 );
    delete $params{AUTHOR}             if ( $] < 5.005 );
    delete $params{ABSTRACT_FROM}      if ( $] < 5.005 );
    delete $params{BINARY_LOCATION}    if ( $] < 5.005 );

    WriteMakefile(%params);
}
