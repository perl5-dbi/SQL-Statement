# -*- perl -*-

use strict;
require ExtUtils::MakeMaker;

eval {
  require SQL::Statement;
  if ($SQL::Statement::VERSION < 1.002) {
    print <<END_OF_WARNING;

WARNING! You seem to have an older version of SQL::Statement already installed.  This new version introduces a number of features that will impact operation of SQL::Statement and of DBD drivers for CSV, AnyData, and Excel.  Changes include:

  1. There are *many* new SQL features including table joins
  2. BLOBS, Empty strings, and NULLs behave differently
  3. Some applications may run a bit slower than previously

See the README for details.

END_OF_WARNING
print "Press enter to continue ... ";
my $x = <STDIN>;
last if $x;
  }
};

my %opts =
    ('NAME'         => 'SQL::Statement',
     'VERSION_FROM' => './lib/SQL/Statement.pm',
     'dist'         => { 'SUFFIX'       => ".gz",
	 'DIST_DEFAULT' => 'all tardist',
	 'COMPRESS'     => "gzip -9vf" },
     'PREREQ_PM'    => {
         'Scalar::Util' => '1.0',
         'Params::Util' => '0.35',
         'Clone'        => '0.30',
     },
     );
if ($ExtUtils::MakeMaker::VERSION >= 5.43) {
    $opts{'AUTHOR'} = 'Jeff Zucker (jeff@vpservices.com)';
}
ExtUtils::MakeMaker::WriteMakefile(%opts);
