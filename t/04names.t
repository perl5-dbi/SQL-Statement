#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More;
use lib  qw( ../lib );
if ($@) {
        plan skip_all => "No DBD::File available";
}
else {
    plan tests => 2;
}
use SQL::Statement; printf "SQL::Statement v.%s\n", $SQL::Statement::VERSION;
use DBI;
use vars qw($dbh $sth $DEBUG);
$DEBUG = 0;
$dbh = DBI->connect('dbi:File(RaiseError=1):');
$dbh->do($_) for <DATA>;

$sth = $dbh->prepare("SELECT * FROM Prof");
ok( 'PID PNAME' eq (join' ',cols($sth)),'Column Names: select list = *');

$sth = $dbh->prepare("SELECT pname,pID FROM Prof");
ok( 'pname pID' eq (join' ',cols($sth)),'Column Names: select list = named');

sub cols {
    my($sth)=@_;
    $sth->execute;
    my $str='';
    while (my $r=$sth->fetch) {
        $str .= sprintf "%s^",join('~',map { defined $_ ? $_ : 'undef' } @$r);
    }
    return @{$sth->{NAME}};
}
__END__
CREATE TEMP TABLE Prof (pid INT, pname VARCHAR(30))
INSERT INTO Prof VALUES (1,'Sue')
INSERT INTO Prof VALUES (2,'Bob')
INSERT INTO Prof VALUES (3,'Tom')
