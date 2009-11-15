# -*- perl -*-

use strict;
require ExtUtils::MakeMaker;

eval {
    require SQL::Statement;
    require version;
};

unless($@)
{
    if ( version->parse($SQL::Statement::VERSION) <= version->parse('1.20') )
    {
        my $warning = <<'END_OF_WARNING';

WARNING! You seem to have an older version of SQL::Statement already installed.
This new version introduces a number of features that will impact operation of SQL::Statement and of DBD drivers for CSV, AnyData, and Excel.

Changes include (1.003):
  * There are *many* new SQL features including table joins
  * BLOBS, Empty strings, and NULLs behave differently
  * Some applications may run a bit slower than previously

Changes include (1.22):
  * behavior for unquoted identifiers modified to lower case them
  * IN and BETWEEN operators are supported native

See the Changes for details.

Use

$ env SQL_STATEMENT_WARN_UPDATE=sure perl Makefile.PL

to build and install anyway.

END_OF_WARNING
        if ( defined( $ENV{SQL_STATEMENT_WARN_UPDATE} ) )
        {
            warn $warning;
        }
        else
        {
            Carp::croak($warning);
        }
    }
}

my %opts = (
             'NAME'         => 'SQL::Statement',
             'VERSION_FROM' => './lib/SQL/Statement.pm',
             'dist'         => {
                         'SUFFIX'       => ".gz",
                         'DIST_DEFAULT' => 'manifest tardist',
                         'COMPRESS'     => "gzip -9vf"
                       },
             'PREREQ_PM' => {
                              'Carp'         => '0',
                              'Clone'        => '0.30',
                              'Data::Dumper' => '0',
                              'Params::Util' => '1.00',
                              'Scalar::Util' => '1.0',
                              'Test::Simple' => '0.86',
                            },
             'CONFIGURE_REQUIRES' => { 'version' => '0', },
             LICENSE              => 'perl',
           );
if ( $ExtUtils::MakeMaker::VERSION >= 5.43 )
{
    $opts{'AUTHOR'} = 'Jeff Zucker <jeff@vpservices.com>, Jens Rehsack <rehsack@cpan.org>';
}
ExtUtils::MakeMaker::WriteMakefile(%opts);
