#!/usr/bin/perl -w
$|=1;
use strict;
use Test::More tests => 23;
use lib qw' ./ ./t ';
use SQLtest;

$parser = new_parser();
$parser->{PrintError}=0;
$parser->{RaiseError}=1;
do_($_)for split/;\n/,"
    CREATE TABLE tbl (c1 INT, c2 CHAR, c3 CHAR);
    INSERT INTO tbl VALUES(1,'Seattle',200);
    INSERT INTO tbl VALUES(2,'Portland',300);
    INSERT INTO tbl VALUES(3,'Boston',600);
    INSERT INTO tbl VALUES(4,'Chicago',100)
";
ok(100  ==  fetchStr("SELECT MIN(c3) FROM tbl   "), 'min'  );
ok(600  ==  fetchStr("SELECT MAX(c3) FROM tbl   "), 'max'  );
ok(1200 ==  fetchStr("SELECT SUM(c3) FROM tbl   "), 'sum'  );
ok(4    ==  fetchStr("SELECT COUNT(c3) FROM tbl "), 'count');
ok(300  ==  fetchStr("SELECT AVG(c3) FROM tbl   "), 'avg'  );
my $date = fetchStr("SELECT CURRENT_DATE");
my $time = fetchStr("SELECT CURRENT_TIME");
ok("$date $time" eq fetchStr("SELECT CURRENT_TIMESTAMP"),'current_date/time/timestamp');
ok(3 == fetchStr("SELECT CHAR_LENGTH('foo')"),'char_length');
ok(2 == fetchStr("SELECT POSITION('a','bar')"),'position');
ok('a' eq fetchStr("SELECT LOWER('A')"),'lower');
ok('A' eq fetchStr("SELECT UPPER('a')"),'upper');
ok('AB' eq fetchStr("SELECT CONCAT('A','B')"),'concat good');
ok(!fetchStr("SELECT CONCAT('A',NULL)"),'concat bad');
ok('z' eq fetchStr("SELECT COALESCE(NULL,'z')"),'coalesce');
ok('z' eq fetchStr("SELECT NVL(NULL,'z')"),'nvl');
ok('PDX' eq fetchStr(q{SELECT DECODE(c2,'Portland','PDX','defualt') FROM tbl WHERE c1=2}),'decode');
ok('fun' eq fetchStr(q{SELECT REPLACE('zfunkY','s/z(.+)ky/$1/i')}),'replace');
ok('fun' eq fetchStr(q{SELECT SUBSTITUTE('zfunkY','s/z(.+)ky/$1/i')}),'substitute');
ok('fun' eq fetchStr(q{SELECT SUBSTR('zfunkY',2,3)}),'substr');
#ok(''  eq  fetchStr("SELECT c1 FROM tbl WHERE SUBSTRING(c2 FROM 1 FOR 1)='P'"), 'substring'  );
ok('fun' eq fetchStr(q{SELECT TRIM(' fun ')}),'trim');
ok(1 == fetchStr("SELECT SOUNDEX('jeff','jeph')"),'soundex match');
ok(0 == fetchStr("SELECT SOUNDEX('jeff','quartz')"),'soundex no match');
ok(1 == fetchStr("SELECT REGEX('jeff','/EF/i')"),'regex match');
ok(0 == fetchStr("SELECT REGEX('jeff','/zzz/')"),'regex no match');
