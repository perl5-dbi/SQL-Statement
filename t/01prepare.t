#!perl -w
use strict;
$|=1;
use lib '../lib';
use SQL::Statement;
use Test::More tests => 100;
print "[SQL::Statement $SQL::Statement::VERSION]\n";
my $parser = SQL::Parser->new('ANSI',{RaiseError=>1});
my $count;
my @data;
for (<DATA>) {
    chomp;
    last if /^#/;
    next if /^\s*\/\*/;
    next if /^\s*$/;
    push @data,$_;
}
for my $sql(@data) {
    ok( my $stmt = SQL::Statement->new($sql,$parser) );
    #
    # NOTE: RaiseError is on so the program will die here
    #       if the SQL can't be parsed
    #
}
__DATA__
  /* DROP TABLE */
DROP TABLE foo
DROP TABLE foo CASCADE
DROP TABLE foo RESTRICT
  /* DELETE */
DELETE FROM foo
DELETE FROM foo WHERE id < 7
  /* UPDATE */
UPDATE foo SET bar = 7
UPDATE foo SET bar = 7 WHERE id > 7
  /* INSERT */
INSERT INTO foo VALUES ( 'baz', 7, NULL )
INSERT INTO foo (col1,col2,col7) VALUES ( 'baz', 7, NULL )
  /* CREATE TABLE */
CREATE TABLE foo ( id INT )
CREATE LOCAL TEMPORARY TABLE foo (id INT)
CREATE LOCAL TEMPORARY TABLE foo (id INT) ON COMMIT DELETE ROWS
CREATE LOCAL TEMPORARY TABLE foo (id INT) ON COMMIT PRESERVE ROWS
CREATE GLOBAL TEMPORARY TABLE foo (id INT)
CREATE GLOBAL TEMPORARY TABLE foo (id INT) ON COMMIT DELETE ROWS
CREATE GLOBAL TEMPORARY TABLE foo (id INT) ON COMMIT PRESERVE ROWS
CREATE TABLE foo ( id INTEGER, phrase VARCHAR(40) )
CREATE TABLE foo ( id INTEGER UNIQUE, phrase VARCHAR(40) UNIQUE )
CREATE TABLE foo ( id INTEGER PRIMARY KEY, phrase VARCHAR(40) UNIQUE )
CREATE TABLE foo ( id INTEGER PRIMARY KEY, phrase VARCHAR(40) NOT NULL )
CREATE TABLE foo ( id INTEGER NOT NULL, phrase VARCHAR(40) NOT NULL )
CREATE TABLE foo ( id INTEGER UNIQUE NOT NULL, phrase VARCHAR(40) )
  /* JOINS */
SELECT Lnum,Llet,Ulet FROM zLower NATURAL INNER JOIN zUpper
SELECT Lnum,Llet,Ulet FROM zLower NATURAL LEFT JOIN zUpper
SELECT Lnum,Llet,Ulet FROM zLower NATURAL RIGHT JOIN zUpper
SELECT Lnum,Llet,Ulet FROM zLower NATURAL FULL JOIN zUpper
SELECT Lnum,Llet,Ulet FROM zLower INNER JOIN zUpper ON Lnum = Unum
SELECT Lnum,Llet,Ulet FROM zLower LEFT JOIN zUpper ON Lnum = Unum
SELECT Lnum,Llet,Ulet FROM zLower RIGHT JOIN zUpper ON Lnum = Unum
SELECT Lnum,Llet,Ulet FROM zLower FULL JOIN zUpper ON Lnum = Unum
SELECT Lnum,Llet,Ulet FROM zLower INNER JOIN zUpper USING(num)
SELECT Lnum,Llet,Ulet FROM zLower LEFT JOIN zUpper USING(num)
SELECT Lnum,Llet,Ulet FROM zLower RIGHT JOIN zUpper USING(num)
SELECT Lnum,Llet,Ulet FROM zLower FULL JOIN zUpper USING(num)
SELECT Lnum,Llet,Ulet FROM zLower,zUpper WHERE Lnum = Unum
SELECT * FROM zLower NATURAL INNER JOIN zUpper
SELECT * FROM zLower NATURAL LEFT JOIN zUpper
SELECT * FROM zLower NATURAL RIGHT JOIN zUpper
SELECT * FROM zLower NATURAL FULL JOIN zUpper
SELECT * FROM zLower INNER JOIN zUpper ON Lnum = Unum
SELECT * FROM zLower LEFT JOIN zUpper ON Lnum = Unum
SELECT * FROM zLower RIGHT JOIN zUpper ON Lnum = Unum
SELECT * FROM zLower FULL JOIN zUpper ON Lnum = Unum
SELECT * FROM zLower INNER JOIN zUpper USING(num)
SELECT * FROM zLower LEFT JOIN zUpper USING(num)
SELECT * FROM zLower RIGHT JOIN zUpper USING(num)
SELECT * FROM zLower FULL JOIN zUpper USING(num)
SELECT * FROM zLower,zUpper WHERE Lnum = Unum
  /* SELECT COLUMNS */
SELECT id, phrase FROM foo
SELECT * FROM foo
SELECT DISTINCT * FROM foo
SELECT ALL * FROM foo
SELECT A.*,B.* FROM A,B WHERE A.id=B.id
  /* SET FUNCTIONS */
SELECT MAX(foo) FROM bar
SELECT MIN(foo) FROM bar
SELECT AVG(foo) FROM bar
SELECT SUM(foo) FROM bar
SELECT COUNT(foo) FROM foo
SELECT COUNT(*) FROM foo
SELECT SUM(DISTINCT foo) FROM bar
SELECT SUM(ALL foo) FROM bar
  /* ORDER BY */
SELECT * FROM foo ORDER BY bar
SELECT * FROM foo ORDER BY bar, baz
SELECT * FROM foo ORDER BY bar DESC
SELECT * FROM foo ORDER BY bar ASC
  /* LIMIT */
SELECT * FROM foo LIMIT 5
SELECT * FROM foo LIMIT 0, 5
SELECT * FROM foo LIMIT 5, 10
  /* STRING FUNCTIONS */
SELECT * FROM foo WHERE UPPER(phrase) = 'bar'
SELECT * FROM foo WHERE LOWER(phrase) = 'bar'
SELECT * FROM foo WHERE TRIM( str ) = 'bar'S
SELECT * FROM foo WHERE TRIM( LEADING FROM str ) = 'bar'
SELECT * FROM foo WHERE TRIM( TRAILING FROM str ) = 'bar'
SELECT * FROM foo WHERE TRIM( BOTH FROM str ) = 'bar'
SELECT * FROM foo WHERE TRIM( LEADING ';' FROM str ) = 'bar'
SELECT * FROM foo WHERE TRIM( UPPER(phrase) ) = 'bar'
SELECT * FROM foo WHERE TRIM( LOWER(phrase) ) = 'bar'
SELECT * FROM foo WHERE blat= SUBSTRING(bar FROM 3 FOR 6)
SELECT * FROM foo WHERE blat= SUBSTRING(bar FROM 3)
UPDATE foo SET bar='baz', bop=7, bump=bar+8, blat=SUBSTRING(bar FROM 3 FOR 6)
  /* TABLE NAME ALIASES */
SELECT * FROM test as T1
SELECT * FROM test T1
SELECT T1.id, T2.num FROM test as T1 JOIN test2 as T2 USING(id)
SELECT id FROM test as T1 WHERE T1.num < 7
SELECT id FROM test as T1 ORDER BY T1.num
SELECT a.x,b.y FROM foo AS a, bar b WHERE a.baz = b.bop ORDER BY a.blat
  /* NUMERIC EXPRESSIONS */
SELECT * FROM foo WHERE 1 = 0 AND baz < (6*foo+11-r)
  /* CASE OF IDENTIFIERS */
SELECT ID, phRase FROM tEst AS tE WHERE te.id < 3 ORDER BY TE.phrasE
  /* PARENS */
SELECT * FROM ztable WHERE NOT data IN ('one','two')
SELECT * from ztable WHERE (aaa > 'AAA')
SELECT * from ztable WHERE  sev = 50 OR sev = 60
SELECT * from ztable WHERE (sev = 50 OR sev = 60)
SELECT * from ztable WHERE sev IN (50,60)
SELECT * from ztable WHERE rc > 200 AND ( sev IN(50,60) )
SELECT * FROM ztable WHERE data NOT IN ('one','two')
SELECT * from ztable WHERE (aaa > 'AAA') AND (zzz < 'ZZZ')
SELECT * from ztable WHERE (sev IN(50,60))
  /* NOT */
SELECT * FROM foo WHERE NOT bar = 'baz' AND bop = 7 OR NOT blat = bar
SELECT * FROM foo WHERE NOT bar = 'baz' AND NOT bop = 7 OR NOT blat = bar
SELECT * FROM foo WHERE NOT bar = 'baz' AND NOT bop = 7 OR blat IS NOT NULL
