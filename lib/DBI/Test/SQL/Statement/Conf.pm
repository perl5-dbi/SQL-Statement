package DBI::Test::SQL::Statement::Conf;

use strict;
use warnings;

use parent qw(DBI::Test::Conf);

=pod

use Carp qw(croak);
use Config;

BEGIN
{
    eval { require DBI::SQL::Nano; };
}

my %conf = (
             (
               -f $INC{'DBI/SQL/Nano.pm'}
               ? (
                   nano => {
                             category   => "SQL Engine",
                             cat_abbrev => "z",
                             abbrev     => "n",
                             init_stub  => qq(\$ENV{DBI_SQL_NANO} = 1;),
                             match      => {
                                        general   => qq(require DBI;),
                                        namespace => [""],
                                      },
                             name => "Unmodified Test",
                           }
                 )
               : ()
             )
           );

sub conf { %conf; }

=cut

sub conf { () }

1;
