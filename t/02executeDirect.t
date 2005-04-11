#!/usr/bin/perl -w
$|=1;
use strict;
use lib qw' ./ ./t ';
use SQLtest;
use Test::More tests => 24;
my $DEBUG;

eval { $parser = SQL::Parser->new() };
ok(!$@,'$parser->new');
$parser->{PrintError}=0;
$parser->{RaiseError}=1;
for my $sql(<DATA>) {
    my $stmt;
    eval { $stmt = SQL::Statement->new($sql,$parser) };
    ok(!$@,'$stmt->new');
    ok('SELECT' eq $stmt->command,'$stmt->command');
    ok(1 == scalar $stmt->params,'$stmt->params');
    ok('C' eq $stmt->tables(1)->name,'$stmt->tables');
    ok('A' eq $stmt->columns(0)->name,'$stmt->columns');
    ok('A' eq join('',$stmt->column_names),'$stmt->column_names');
    ok('DESC' eq $stmt->order(0)->{desc},'$stmt->order');
    ok('AND' eq $stmt->where->op,'$stmt->where->op');
    ok(0== $stmt->where->neg,'$stmt->where->neg');
    ok('C' eq $stmt->where->arg1->arg1->name,'$stmt->where->arg1');
    ok(7 == $stmt->where->arg2->arg2,'$stmt->where->arg2');
    ok(2 == $stmt->limit,'$stmt->limit');
    ok(5 == $stmt->offset,'$stmt->offset');

    next unless $DEBUG;
    printf "Command      %s\n",$stmt->command;
    printf "Num Pholders %s\n",scalar $stmt->params;
    printf "Columns      %s\n",join',',map{$_->name}$stmt->columns;
    printf "Tables       %s\n",join',',$stmt->tables;
    printf "Where op     %s\n",join',',$stmt->where->op;
    printf "Limit        %s\n",$stmt->limit;
    printf "Offset       %s\n",$stmt->offset;
    printf "Order Cols   %s\n",join',',map{$_->column}$stmt->order;
}
my $stmt = SQL::Statement->new("INSERT a VALUES(3,7)",$parser);
ok(7 == $stmt->row_values(1)->{value},'$stmt->row_values');
ok( ref($parser->structure) eq 'HASH','structure');
ok( $parser->command eq 'INSERT','command');
ok( SQL::Statement->new("SELECT DISTINCT c1 FROM tbl",$parser),'distinct');
my $cache={};
for my $sql(split /\n/,
"   CREATE TABLE a (b INT, c CHAR)
    INSERT INTO a VALUES(1,'abc')
    INSERT INTO a VALUES(2,'ayc')
    SELECT b FROM a WHERE c LIKE '%b%' "
) {
    # print "<$sql>\n";
    $stmt = SQL::Statement->new($sql,$parser);
    eval { $stmt->execute($cache) };
    warn $@ if $@;
    ok(!$@,'$stmt->execute '.$stmt->command);
    next unless $stmt->command eq 'SELECT';
    ok( ref($stmt->where_hash) eq 'HASH','$stmt->where_hash');
    while (my $row=$stmt->fetch) {
        ok(1==$row->[0],'$stmt->fetch');
    }
}
__DATA__
SELECT a FROM b JOIN c WHERE c=? AND e=7 ORDER BY f DESC LIMIT 5,2
