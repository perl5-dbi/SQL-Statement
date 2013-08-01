package DBI::Test::SQL::Statement::List;

use strict;
use warnings;

use parent qw(DBI::Test::List);

sub test_cases
{
    return map { "SQL::Statement::" . $_ } qw(error);
}

1;
