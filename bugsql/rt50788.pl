#!/pro/bin/perl

use strict;
use warnings;

use DBI;

my $tbl = "rt50788";
open my $fh, ">", $tbl or die "$tbl: $!\n";
print $fh <<EOT;
column1|column2
H|19
H|21
KK|1
KK|2
KK|13
MMM|25
EOT
close $fh or die "$tbl: $!\n";

my $dbh = DBI->connect ("dbi:CSV:", undef, undef, {
   f_dir               => ".",
#   f_ext               => ".csv/r",
#   f_schema            => undef,

   csv_sep_char        => "|",
   csv_quote_char      => undef,

   RaiseError          => 1,
   PrintError          => 1,
   }) or die "Cannot connect: $DBI::errstr\n";

print STDERR "udate test with:", ( map { "\n  $_-".eval "\$${_}::VERSION" }
   qw( DBI DBD::File DBD::CSV SQL::Statement Text::CSV_XS )), "\n";

my $ssv = $SQL::Statement::VERSION;

my $sth = $dbh->prepare ("update $tbl set column1 = ? where column1 = ?");
my $cnt = $sth->execute ("XXXX", "KK");
print STDERR "$ssv\t- $cnt rows updated though DBI\t";
$sth->finish;

$dbh->disconnect;

open $fh, "<", $tbl or die "$tbl: $!\n";
$cnt = 0;
while (<$fh>) { m/^XXXX/ and $cnt++ };
close $fh;

unlink $fh;

print STDERR "$cnt records actually updated\n";
