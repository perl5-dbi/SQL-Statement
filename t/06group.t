#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More;
#use lib  qw( ../lib );
use vars qw($DEBUG);
eval { require DBI; require DBD::File; };
if ($@) {
        plan skip_all => "No DBI or DBD::File available";
}
else {
    plan tests => 3;
}
use SQL::Statement; printf "SQL::Statement v.%s\n", $SQL::Statement::VERSION;
my $dbh=DBI->connect('dbi:File(RaiseError=1):');
$dbh->do($_) for <DATA>;

my $sth=$dbh->prepare("
    SELECT SUM(sales), MAX(sales) FROM biz
");
ok('2700~1000^' eq query2str($sth),'AGGREGATE FUNCTIONS WITHOUT GROUP BY');

$sth=$dbh->prepare("
    SELECT class,SUM(sales), MAX(sales) FROM biz GROUP BY class
");
ok('Car~2000~1000^Truck~700~400^' eq query2str($sth),'GROUP BY one column');

$sth=$dbh->prepare("
    SELECT color,class,SUM(sales), MAX(sales) FROM biz GROUP BY color,class
");
ok('White~Car~1000~1000^Blue~Car~500~500^White~Truck~700~400^Red~Car~500~500^'
 eq query2str($sth),'GROUP BY several columns');

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
CREATE TEMP TABLE biz (class TEXT, color TEXT, sales INTEGER)
INSERT INTO biz VALUES ('Car'  ,'White',1000)
INSERT INTO biz VALUES ('Car'  ,'Blue' ,500 )
INSERT INTO biz VALUES ('Truck','White',400 )
INSERT INTO biz VALUES ('Car'  ,'Red'  ,500 )
INSERT INTO biz VALUES ('Truck','White',300 )
