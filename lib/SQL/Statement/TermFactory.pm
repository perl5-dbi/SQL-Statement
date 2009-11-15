package SQL::Statement::TermFactory;

require SQL::Statement::Term;
require SQL::Statement::Operation;
require SQL::Statement::Placeholder;
require SQL::Statement::Function;

use Data::Dumper;
use Params::Util qw(_HASH _ARRAY0);
use Scalar::Util qw(blessed weaken);

our $VERSION = '1.21_4';

my %oplist = (
               '='       => 'Equal',
               '<>'      => 'NotEqual',
               'AND'     => 'And',
               'OR'      => 'Or',
               '<='      => 'LowerEqual',
               '>='      => 'GreaterEqual',
               '<'       => 'Lower',
               '>'       => 'Greater',
               'LIKE'    => 'Like',
               'RLIKE'   => 'Rlike',
               'CLIKE'   => 'Clike',
               'IN'      => 'Contains',
               'BETWEEN' => 'Between',
               'IS'      => 'Is',
             );

sub new
{
    my $class = shift;
    my $owner = shift;
    my $self  = bless( { OWNER => $owner, }, $class );

    weaken( $self->{OWNER} );

    return $self;
}

sub buildCondition
{
    my ( $self, $pred ) = @_;
    my $term;

    if ( _ARRAY0($pred) )
    {
        $term = [ map { $self->buildCondition($_) } @{$pred} ];
    }
    elsif ( defined( $pred->{op} ) )
    {
        my $op = uc( $pred->{op} );
        if ( $op eq 'USER_DEFINED' && !$pred->{arg2} )
        {
            $term = SQL::Statement::ConstantTerm->new( $self->{OWNER}, $pred->{arg1}->{value} );
        }
        elsif ( defined( $oplist{$op} ) )
        {
            my $cn    = 'SQL::Statement::Operation::' . $oplist{$op};
            my $left  = $self->buildCondition( $pred->{arg1} );
            my $right = $self->buildCondition( $pred->{arg2} );
            $term = $cn->new( $self->{OWNER}, $op, $left, $right );
        }
        elsif ( defined( $self->{OWNER}->{opts}->{function_names}->{$op} ) )
        {
            my $left  = $self->buildCondition( $pred->{arg1} );
            my $right = $self->buildCondition( $pred->{arg2} );

            $term = SQL::Statement::Function::UserFunc->new( $self->{OWNER}, $op,
                                                             $self->{OWNER}->{opts}->{function_names}->{$op},
                                                             [ $left, $right ] );
        }
        else
        {
            return $self->{OWNER}->do_err( sprintf( q{Unknown operation '%s'}, $pred->{op} ) );
        }

        if ( $pred->{neg} )
        {
            $term = SQL::Statement::Operation::Neg->new( $self->{OWNER}, 'NOT', $term );
        }
    }
    elsif ( defined( $pred->{type} ) )
    {
        my $type = uc( $pred->{type} );
        if ( ( $type eq 'STRING' ) || ( $type eq 'NUMBER' ) )
        {
            $term = SQL::Statement::ConstantTerm->new( $self->{OWNER}, $pred->{value} );
        }
        elsif ( $type eq 'NULL' )
        {
            $term = SQL::Statement::ConstantTerm->new( $self->{OWNER}, undef );
        }
        elsif ( $type eq 'COLUMN' )
        {
            $term = SQL::Statement::ColumnValue->new( $self->{OWNER}, $pred->{value} );
        }
        elsif ( $type eq 'PLACEHOLDER' )
        {
            $term = SQL::Statement::Placeholder->new( $self->{OWNER}, $pred->{argnum} );
        }
        elsif ( $type eq 'FUNCTION' )
        {
            my @params = map { blessed($_) ? $_ : $self->buildCondition($_) } @{ $pred->{value} };

            if ( $pred->{name} eq 'numeric_exp' )
            {
                $term = SQL::Statement::Function::NumericEval->new( $self->{OWNER}, $pred->{str}, \@params );
            }
            elsif ( $pred->{name} eq 'str_concat' )
            {
                $term = SQL::Statement::Function::StrConcat->new( $self->{OWNER}, \@params );
            }
            elsif ( $pred->{name} eq 'TRIM' )
            {
                $term = SQL::Statement::Function::Trim->new( $self->{OWNER}, $pred->{trim_spec}, $pred->{trim_char},
                                                             \@params );
            }
            elsif ( $pred->{name} eq 'SUBSTRING' )
            {
                my $start = $self->buildCondition( $pred->{start} );
                my $length = $self->buildCondition( $pred->{length} ) if ( _HASH( $pred->{length} ) );
                $term = SQL::Statement::Function::SubString->new( $self->{OWNER}, $start, $length, \@params );
            }
            else
            {
                $term =
                  SQL::Statement::Function::UserFunc->new( $self->{OWNER}, $pred->{name}, $pred->{subname}, \@params );
            }
        }
        else
        {
            return $self->{OWNER}->do_err( sprintf( q{Unknown type '%s'}, $pred->{type} ) );
        }
    }
    else
    {
        return $self->{OWNER}->do_err( sprintf( q~Unknown predicate '{%s}'~, Dumper($pred) ) );
    }

    return $term;
}

sub DESTROY
{
    my $self = $_[0];
    undef $self->{OWNER};
}

1;
