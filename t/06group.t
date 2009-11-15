#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More;
use lib  qw( ../lib );
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
    SELECT region,SUM(sales), MAX(sales) FROM biz GROUP BY region
");
ok('West~2000~1000^East~700~700^' eq query2str($sth),'GROUP BY one column');

$sth=$dbh->prepare("
    SELECT region,store,SUM(sales), MAX(sales) FROM biz GROUP BY region,store
");
ok('West~Los Angeles~1500~1000^West~San Diego~500~500^East~Boston~700~700^'
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
CREATE TEMP TABLE biz (region TEXT, store TEXT, sales INTEGER)
INSERT INTO biz VALUES ('West','Los Angeles',1000 )
INSERT INTO biz VALUES ('West','San Diego'  ,500  )
INSERT INTO biz VALUES ('West','Los Angeles',500  )
INSERT INTO biz VALUES ('East','Boston'     ,700  )
