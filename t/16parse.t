#!/usr/bin/perl -w
use strict;
use warnings;
use lib qw(t);

use Test::More;
use Test::Deep;

use SQL::Statement;
use SQL::Parser;

my($stmt,$cache)=(undef,{});
my $p = SQL::Parser->new();
my $sql1 = 'SELECT * FROM x JOIN y ON x.a = y.b AND x.c = y.d';
ok(cmp_parse($sql1,$sql1), 'JOIN with AND');

my $sql2 = 'SELECT * FROM x JOIN y ON x.a = y.b OR x.c = y.d';
ok(cmp_parse($sql2,$sql2), 'JOIN with OR');

my $sql3 = 'SELECT * FROM x JOIN y ON (x.a = y.b) OR (x.c = y.d)';
ok(cmp_parse($sql3,$sql3), 'JOIN with OR with ()s');

done_testing();

sub cmp_parse {
    my ($sql_given,$sql_want) = @_;
    my($stmt_given,$stmt_want);

    eval {
        $stmt_given = SQL::Statement->new($sql_given,$p);
        $stmt_want  = SQL::Statement->new($sql_want,$p);
    };

    return 0 if $@;

    foreach (qw(
        command
        columns
        column_aliases
        tables
    )) {
        return 0 if !eq_deeply($stmt_given->{$_}, $stmt_want->{$_});
    }

    return 1;
}

