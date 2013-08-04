package SQL::Statement::Test;

use DBI::Mock      ();
use SQL::Statement ();

package    # hide from CPAN
  SQL::Statement::Test::db;

use base 'DBI::db';

sub parser
{
    return $_[0]->{parser};
}

sub prepare
{
    my ( $dbh, $sql, $attribs ) = @_;

    my $sth = $dbh->SUPER::prepare( $sql, $attribs );
    $sth->{sql_stmt} and return $sth;

    defined $dbh->{sql_parser_object} or $dbh->{sql_parser_object} = SQL::Parser->new( 'ANSI', {
                   RaiseError => $dbh->FETCH("RaiseError"),
                   PrintError => $dbh->FETCH("PrintError"),

	} );
    delete $sth->{sql_stmt};
    $sth->{sql_stmt} = SQL::Statement->new( $sql, $dbh->{sql_parser_object} );
    $sth->{sql_stmt} or return;
    $sth->{sql_stmt}->{errstr} and return;

    return $sth;
}

sub errstr
{
    $_[0]->isa("DBI::Mock::db") or return $_[0]->SUPER::errstr;
    $_[0]->{sql_parser_object}->{struct}->{errstr};
}

package    # hide from CPAN
  SQL::Statement::Test::st;

use base 'DBI::st';

sub parser
{
    return $_[0]->{dbh}->{sql_parser_object};
}

sub errstr
{
    $_[0]->isa("DBI::Mock::st") or return $_[0]->SUPER::errstr;
    $sth->{sql_stmt}->{errstr};
}

sub command
{
    my $sth = $_[0];
    return $sth->{sql_stmt}->command();
}

sub execute
{
    my $sth = shift;
    if ( $sth->isa("DBI::Mock::st") )
    {
        my $params = @_ ? ( $sth->{sql_params} = [@_] ) : $sth->{sql_params};
	$sth->{dbh}->set_err(0, undef);
	my $result;
	eval {
	    $result = $sth->{sql_stmt}->execute( $sth, $params );
	};
	$@ and return $sth->{dbh}->{sql_parser_object}->do_err($@);
	$sth->{sql_stmt}->{errstr} and return $sth->{dbh}->{sql_parser_object}->do_err($sth->{sql_stmt}->{errstr});
	return $result;
    }

    return $sth->SUPER::execute(@_);
}

sub selectrow_array
{
    my $sth = shift;
    $sth->do(@_);
    my $result = $sth->fetchrow_arrayref();
    return wantarray ? @$result : $result->[0];
}

sub col_names
{
    return $_[0]->{sql_stmt}->{NAME};
}

sub all_cols
{
    return $_[0]->{sql_stmt}->{all_cols};
}

sub tbl_names
{
    my $sth = $_[0];
    my @tables = sort map { $_->name() } $sth->{sql_stmt}->tables();
    return \@tables;
}

sub columns
{
    my ( $sth, @args ) = @_;
    return $sth->{sql_stmt}->columns(@args);
}

sub tables
{
    my ( $sth, @args ) = @_;
    return $sth->{sql_stmt}->tables(@args);
}

sub row_values
{
    my ( $sth, @args ) = @_;
    return $sth->{sql_stmt}->row_values(@args);
}

sub where_hash
{
    my $sth = $_[0];
    return $sth->{sql_stmt}->where_hash();
}

sub where
{
    my $sth = $_[0];
    return $sth->{sql_stmt}->where();
}

sub params
{
    my $sth = $_[0];
    return $sth->{sql_stmt}->params();
}

sub limit
{
    my $sth = $_[0];
    return $sth->{sql_stmt}->limit();
}

sub offset
{
    my $sth = $_[0];
    return $sth->{sql_stmt}->offset();
}

sub order
{
    my ( $sth, @args ) = @_;
    return $sth->{sql_stmt}->order(@args);
}

sub fetch_row
{
    my $sth = $_[0];
    return $sth->fetch();
}

sub fetch_rows
{
    my $sth = $_[0];
    return $sth->fetchall_arrayref();
}

sub fetchall_hashref
{
    my $sth = shift;
    return $sth->fetchall_hashref(@_);
}

sub rows { -1 }

1;
