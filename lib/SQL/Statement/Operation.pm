package SQL::Statement::Operation;

use vars qw(@ISA);
require Carp;

require SQL::Statement::Term;

our $VERSION = '1.21_4';

@ISA = qw(SQL::Statement::Term);

sub new
{
    my $class     = shift;
    my $owner     = shift;
    my $operation = shift;
    my $leftTerm  = shift;
    my $rightTerm = shift;

    my $self = $class->SUPER::new($owner);
    $self->{OP}    = $operation;
    $self->{LEFT}  = $leftTerm;
    $self->{RIGHT} = $rightTerm;

    return $self;
}

sub operate($)
{
    Carp::confess(
          sprintf( q{pure virtual function 'operate' called on %s for %s}, ref( $_[0] ) || __PACKAGE__, $_[0]->{OP} ) );
}

sub DESTROY
{
    my $self = $_[0];

    undef $self->{OP};
    undef $self->{LEFT};
    undef $self->{RIGHT};

    $self->SUPER::DESTROY();
}

sub value($) { return $_[0]->operate( $_[1] ); }

package SQL::Statement::Operation::Neg;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation);

sub operate($)
{
    return !$_[0]->{LEFT}->value( $_[1] );
}

package SQL::Statement::Operation::And;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation);

sub operate($) { return $_[0]->{LEFT}->value( $_[1] ) && $_[0]->{RIGHT}->value( $_[1] ); }

package SQL::Statement::Operation::Or;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation);

sub operate($) { return $_[0]->{LEFT}->value( $_[1] ) || $_[0]->{RIGHT}->value( $_[1] ); }

package SQL::Statement::Operation::Is;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation);

sub operate($)
{
    my $self  = $_[0];
    my $left  = $self->{LEFT}->value( $_[1] );
    my $right = $self->{RIGHT}->value( $_[1] );
    my $expr;

    if ( defined($right) )
    {
        $expr = defined($left) ? $left && $right : 0;    # is true / is false
    }
    else
    {
        $expr = !defined($left) || ( $left eq '' );      # FIXME I don't like that '' IS NULL
    }

    return $expr;
}

package SQL::Statement::Operation::Contains;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation);
use Scalar::Util qw(looks_like_number);

sub operate($)
{
    my ( $self, $eval ) = @_;
    my $left  = $self->{LEFT}->value($eval);
    my @right = map { $_->value($eval); } @{ $self->{RIGHT} };
    my $expr  = 0;

    foreach my $r (@right)
    {
        last if $expr |= ( looks_like_number($left) && looks_like_number($r) ) ? $left == $r : $left eq $r;
    }

    return $expr;
}

package SQL::Statement::Operation::Between;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation);
use Scalar::Util qw(looks_like_number);

sub operate($)
{
    my ( $self, $eval ) = @_;
    my $left  = $self->{LEFT}->value($eval);
    my @right = map { $_->value($eval); } @{ $self->{RIGHT} };
    my $expr  = 0;

    if ( looks_like_number($left) && looks_like_number( $right[0] ) && looks_like_number( $right[1] ) )
    {
        $expr = ( $left >= $right[0] ) && ( $left <= $right[1] );
    }
    else
    {
        $expr = ( $left ge $right[0] ) && ( $left le $right[1] );
    }

    return $expr;
}

package SQL::Statement::Operation::Equality;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation);

require Carp;
use Scalar::Util qw(looks_like_number);

sub operate($)
{
    my $self  = $_[0];
    my $left  = $self->{LEFT}->value( $_[1] );
    my $right = $self->{RIGHT}->value( $_[1] );
    return 0 unless ( defined($left) && defined($right) );
    return ( looks_like_number($left) && looks_like_number($right) )
      ? $self->numcmp( $left, $right )
      : $self->strcmp( $left, $right );
}

sub numcmp($)
{
    Carp::confess(
           sprintf( q{pure virtual function 'numcmp' called on %s for %s}, ref( $_[0] ) || __PACKAGE__, $_[0]->{OP} ) );
}

sub strcmp($)
{
    Carp::confess(
           sprintf( q{pure virtual function 'strcmp' called on %s for %s}, ref( $_[0] ) || __PACKAGE__, $_[0]->{OP} ) );
}

package SQL::Statement::Operation::Equal;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation::Equality);

sub numcmp($$) { return $_[1] == $_[2]; }
sub strcmp($$) { return $_[1] eq $_[2]; }

package SQL::Statement::Operation::NotEqual;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation::Equality);

sub numcmp($$) { return $_[1] != $_[2]; }
sub strcmp($$) { return $_[1] ne $_[2]; }

package SQL::Statement::Operation::Lower;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation::Equality);

sub numcmp($$) { return $_[1] < $_[2]; }
sub strcmp($$) { return $_[1] lt $_[2]; }

package SQL::Statement::Operation::Greater;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation::Equality);

sub numcmp($$) { return $_[1] > $_[2]; }
sub strcmp($$) { return $_[1] gt $_[2]; }

package SQL::Statement::Operation::LowerEqual;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation::Equality);

sub numcmp($$) { return $_[1] <= $_[2]; }
sub strcmp($$) { return $_[1] le $_[2]; }

package SQL::Statement::Operation::GreaterEqual;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation::Equality);

sub numcmp($$) { return $_[1] >= $_[2]; }
sub strcmp($$) { return $_[1] ge $_[2]; }

package SQL::Statement::Operation::Regexp;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation);

sub right($)
{
    my $self  = $_[0];
    my $right = $self->{RIGHT}->value( $_[1] );

    unless ( defined( $self->{PATTERNS}->{$right} ) )
    {
        $self->{PATTERNS}->{$right} = $right;
        $self->{PATTERNS}->{$right} =~ s/%/.*/g;
        $self->{PATTERNS}->{$right} = $self->regexp( $self->{PATTERNS}->{$right} );
    }

    return $self->{PATTERNS}->{$right};
}

sub regexp($)
{
    Carp::confess(
           sprintf( q{pure virtual function 'regexp' called on %s for %s}, ref( $_[0] ) || __PACKAGE__, $_[0]->{OP} ) );
}

sub operate($)
{
    my $self  = $_[0];
    my $left  = $self->{LEFT}->value( $_[1] );
    my $right = $self->right( $_[1] );

    return 0 unless ( defined($left) && defined($right) );
    return $left =~ m/^$right$/s;
}

package SQL::Statement::Operation::Like;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation::Regexp);

sub regexp($)
{
    my $right = $_[1];
    return qr/^$right$/s;
}

package SQL::Statement::Operation::Clike;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation::Regexp);

sub regexp($)
{
    my $right = $_[1];
    return qr/^$right$/si;
}

package SQL::Statement::Operation::Rlike;

use vars qw(@ISA);
@ISA = qw(SQL::Statement::Operation::Regexp);

sub regexp($)
{
    my $right = $_[1];
    return qr/$right$/;
}

1;
