package SQL::Statement::Term;

our $VERSION = '1.21_1';

use Scalar::Util qw(weaken);

sub new
{
    my $class = shift;
    my $owner = shift;

    my %instance = ( OWNER => $owner );

    my $self = bless( \%instance, $class );
    weaken( $self->{OWNER} );

    return $self;
}

sub value($) { Carp::confess( sprintf( q{pure virtual function '%s->value' called}, ref( $_[0] ) || __PACKAGE__ ) ); }

package SQL::Statement::ConstantTerm;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Term);

sub new
{
    my $class = shift;
    my $owner = shift;
    my $value = shift;

    my $self = $class->SUPER::new($owner);
    $self->{VALUE} = $value;

    return $self;
}

sub value($$) { return $_[0]->{VALUE}; }

package SQL::Statement::ColumnValue;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Term);

use Params::Util qw(_INSTANCE);

sub new
{
    my $class = shift;
    my $owner = shift;
    my $value = shift;

    my $self = $class->SUPER::new($owner);
    $self->{VALUE} = $value;

    return $self;
}

sub value($)
{
    my $self = $_[0];
    my $eval = $_[1];
    unless ( defined( $self->{TMPVAL} ) )
    {
        my ( $tbl, $col ) = $self->{OWNER}->full_qualified_column_name( $self->{VALUE} );
        $self->{TMPVAL}      = $tbl . $self->{OWNER}->{dlm} . $col;
        $self->{TABLE_NAME}  = $tbl;
        $self->{COLUMN_NAME} = $col;
    }

    # with TempEval: return $eval->column($self->{TABLE_NAME}, $self->{COLUMN_NAME});

    if ( _INSTANCE( $eval, 'SQL::Eval' ) )
    {
        my $table = $eval->{tables}->{ $self->{TABLE_NAME} };
        return $table->column($self->{COLUMN_NAME});

        # return $line->[ $table->{col_nums}->{ $self->{COLUMN_NAME} } ];
    }
    else
    {
        return undef unless ( defined( $eval->{rowpos} ) );
        my $line = $eval->{table}->[ $eval->{rowpos} - 1 ];

        return $line->[ $eval->{col_nums}->{ $self->{TMPVAL} } ];
    }
}

1;
