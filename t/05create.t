#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More;
eval { require DBI; require DBD::File; };
if ($@) {
        plan skip_all => "No DBI or DBD::File available";
}
else {
    plan tests => 5;
}
use lib  qw( ../lib );
use SQL::Statement; printf "SQL::Statement v.%s\n", $SQL::Statement::VERSION;
use vars qw($dbh $sth $DEBUG);
$dbh = DBI->connect('dbi:File(RaiseError=1):');

########################################
# CREATE TABLE AS IMPORT($AoA);
########################################
my $aoa = [['c1','c2'],[1,9],[2,8] ];
$dbh->do("CREATE TEMP TABLE aoa AS IMPORT(?)",{},$aoa);
$sth = $dbh->prepare("SELECT * FROM aoa");
ok( '1~9^2~8^' eq query2str($sth),'CREATE TABLE AS IMPORT($AoA)' );

########################################
# CREATE TABLE AS IMPORT($AoH);
########################################
my $aoh = [{c1=>1,c2=>9},{c1=>2,c2=>8}];
$dbh->do("CREATE TEMP TABLE aoh AS IMPORT(?)",{},$aoh);
$sth = $dbh->prepare("SELECT C1,c2 FROM aOh");
ok( '1~9^2~8^' eq query2str($sth),'CREATE TABLE AS IMPORT($AoH)' );

########################################
# CREATE TABLE AS IMPORT($internal_sth);
########################################
$sth = $dbh->prepare("SELECT * FROM aoh");
$sth->execute;
$dbh->do("CREATE TEMP TABLE aoi AS IMPORT(?)",{},$sth);
$sth = $dbh->prepare("SELECT * FROM aoi");
$sth->execute;
ok( '1~9^2~8^' eq query2str($sth),'CREATE TABLE AS IMPORT($internal_sth)' );

########################################
# CREATE TABLE AS IMPORT($external_sth);
########################################
eval { require DBD::XBase };
SKIP: {
   skip('No XBase installed',1) if $@;
   ok(external_sth(),'CREATE TABLE AS IMPORT($external_sth)');
};

sub external_sth {
    my $xb_dbh = DBI->connect('dbi:XBase:./');
    unlink 'xb' if -e 'xb';
    $xb_dbh->do($_) for split /\n/,<<"";
        CREATE TABLE xb (id INTEGER, xphrase VARCHAR(30))
        INSERT INTO xb VALUES(1,'foo')

    $xb_dbh->do($_) for split /\n/,<<"";
        CREATE TABLE xb2 (c1 INTEGER, c2 VARCHAR(30))
        INSERT INTO xb2 VALUES(2,'bar')

    my $xb_sth = $xb_dbh->prepare('SELECT * FROM xb');
    $xb_sth->execute;
    my $xb2_sth = $xb_dbh->prepare('SELECT * FROM xb2');
    $xb2_sth->execute;

    $dbh->do("CREATE TEMP TABLE tmpxb AS IMPORT(?)",{},$xb_sth);
    $dbh->do("CREATE TEMP TABLE tmpxb2 AS IMPORT(?)",{},$xb2_sth);

    $sth=$dbh->prepare('SELECT iD,xPhrase FROM tmPxb WHERE iD IS NOT NULL');
    $sth->execute;
    my $str='';
    while (my $r=$sth->fetch) { $str.="@$r^"; }

    $sth=$dbh->prepare('SELECT * FROM tmPxb2');
    $sth->execute;
    my $str2='';
    while (my $r=$sth->fetch) { $str2.="@$r^"; }

    $xb_dbh->do("DROP TABLE xb");
    $xb_dbh->do("DROP TABLE xb2");
    return ($str eq '1 foo^' and $str2 eq '2 bar^');
}


########################
# CREATE TABLE AS SELECT
########################
$dbh->do("CREATE TEMP TABLE tbl_copy AS SELECT * FROM aoa");
$sth = $dbh->prepare("SELECT * FROM tbl_copy");
$sth->execute;
ok( '1~9^2~8^' eq query2str($sth),'CREATE TABLE AS SELECT' );

sub query2str {
    my($sth)=@_;
    $sth->execute;
    my $str='';
    while (my $r=$sth->fetch) {
        $str .= sprintf "%s^",join('~',map { defined $_ ? $_ : 'undef' } @$r);
    }
    return $str unless $DEBUG;
    printf "%s\n",join',',@{$sth->{NAME}};
    print "<$str>\n";
    return $str;
}
__END__
