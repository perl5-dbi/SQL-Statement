#!/usr/bin/perl -w

# From Cosimo Streppone <cosimoATcpan.org>

$|=1;
use strict;
use Test::More;
eval { require DBD::File; };
if ($@) {
        plan skip_all => "No DBD::File available";
}
else {
    plan tests => 1;
}
use lib  qw( ../lib );
use SQL::Statement; printf "SQL::Statement v.%s\n", $SQL::Statement::VERSION;
use DBI;
use vars qw($dbh $sth $DEBUG);
$dbh = DBI->connect('dbi:File(RaiseError=1):') or die "Can't create dbi:File connection";

# Create a test table
$dbh->do("CREATE TEMP TABLE allcols ( f1 char(10), f2 char(10) )");
$sth = $dbh->prepare("INSERT INTO allcols (f1,f2) VALUES (?,?)") or die "Can't prepare insert sth";

$sth->execute('abc', 'def');
my $allcols_before = @{$sth->{f_stmt}->{all_cols}};
# diag('@all_cols before is '.$allcols_before);

$sth->execute('abc', 'def') for 1 .. 100;
my $allcols_after = @{$sth->{f_stmt}->{all_cols}};
# diag('@all_cols after  is '.$allcols_after);

ok( $allcols_before == $allcols_after, '->{all_cols} structure does not grow beyond control');

$sth->finish();
$dbh->disconnect();

