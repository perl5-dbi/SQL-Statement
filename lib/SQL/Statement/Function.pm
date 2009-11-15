package SQL::Statement::Function;

require SQL::Statement::Term;
@ISA = qw(SQL::Statement::Term);

our $VERSION = '1.21_4';

sub DESTROY
{
    my $self = $_[0];

    undef $self->{PARAMS};

    $self->SUPER::DESTROY();
}

package SQL::Statement::Function::UserFunc;

use vars qw(@ISA);

require Carp;
use Params::Util qw(_INSTANCE);

use SQL::Statement::Functions;

@ISA = qw(SQL::Statement::Function);

sub new
{
    my $class  = shift;
    my $owner  = shift;
    my $name   = shift;
    my $subnm  = shift;
    my $params = shift;

    my $self = $class->SUPER::new($owner);

    my ( $pkg, $sub ) = $subnm =~ m/^(.*::)([^:]+$)/;
    if ( !$sub )
    {
        $sub = $subnm;
        $pkg = 'main';
    }
    $pkg =~ s/::$//g;
    $pkg = 'main' unless ($pkg);

    $self->{SUB}    = $sub;
    $self->{PKG}    = $pkg;
    $self->{NAME}   = $name;
    $self->{PARAMS} = $params;

    unless ( UNIVERSAL::can( $pkg, $sub ) )
    {
        unless ( 'main' eq $pkg )
        {
            my $mod = $pkg;
            $mod =~ s|::|/|g;
            $mod .= '.pm';
            eval { require $mod; } unless ( defined( $INC{$mod} ) );
            return $owner->do_er($@) if ($@);
        }

        $pkg->can($sub) or return $owner->do_er( "Can't find subroutine $pkg" . "::$sub" );
    }

    return $self;
}

sub value($)
{
    my $self   = $_[0];
    my $eval   = $_[1];
    my $pkg    = $self->{PKG};
    my $sub    = $self->{SUB};
    my @params = map { $_->value($eval); } @{ $self->{PARAMS} };
    return $pkg->$sub( $self->{OWNER}, @params );                   # FIXME is $pkg just a string?
}

package SQL::Statement::Function::NumericEval;

use vars qw(@ISA);

use Params::Util qw(_NUMBER);

@ISA = qw(SQL::Statement::Function);

sub new
{
    my $class  = shift;
    my $owner  = shift;
    my $expr   = shift;
    my $params = shift;

    my $self = $class->SUPER::new($owner);

    $self->{EXPR}   = $expr;
    $self->{PARAMS} = $params;

    return $self;
}

sub value($)
{
    my ( $self, $eval ) = @_;
    my $expr = $self->{EXPR};
    my @vals = map { _INSTANCE( $_, 'SQL::Statement::Term' ) ? $_->value($eval) : $_ } @{ $self->{PARAMS} };
    foreach my $val (@vals)
    {
        return $self->do_err(qq~Bad numeric expression '$val'!~)
          unless ( defined( _NUMBER($val) ) );
    }
    $expr =~ s/\?(\d+)\?/$vals[$1]/g;
    $expr =~ s/\s//g;
    $expr =~ s/^([\)\(+\-\*\/\%0-9]+)$/$1/;    # untaint
    return eval $expr;
}

package SQL::Statement::Function::Trim;

use vars qw(@ISA);

BEGIN { @ISA = qw(SQL::Statement::Function); }

sub new
{
    my $class  = shift;
    my $owner  = shift;
    my $spec   = shift || 'BOTH';
    my $char   = shift || ' ';
    my $params = shift;

    my $self = $class->SUPER::new($owner);

    $self->{PARAMS} = $params;
    $self->{TRIMFN} = sub { my $s = $_[0]; $s =~ s/^$char*//g; return $s; }
      if ( $spec =~ m/LEADING/ );
    $self->{TRIMFN} = sub { my $s = $_[0]; $s =~ s/$char*$//g; return $s; }
      if ( $spec =~ m/TRAILING/ );
    $self->{TRIMFN} = sub { my $s = $_[0]; $s =~ s/^$char*//g; $s =~ s/$char*$//g; return $s; }
      if ( $spec =~ m/BOTH/ );

    return $self;
}

sub value($)
{
    my $val = $_[0]->{PARAMS}->[0]->value( $_[1] );
    $val = &{ $_[0]->{TRIMFN} }($val);
    return $val;
}

package SQL::Statement::Function::SubString;

use vars qw(@ISA);

@ISA = qw(SQL::Statement::Function);

sub new
{
    my $class  = shift;
    my $owner  = shift;
    my $start  = shift;
    my $length = shift;
    my $params = shift;

    my $self = $class->SUPER::new($owner);

    $self->{START}  = $start;
    $self->{LENGTH} = $length;
    $self->{PARAMS} = $params;

    return $self;
}

sub value($)
{
    my $val    = $_[0]->{PARAMS}->[0]->value( $_[1] );
    my $start  = $_[0]->{START}->value( $_[1] ) - 1;
    my $length = defined( $_[0]->{LENGTH} ) ? $_[0]->{LENGTH}->value( $_[1] ) : length($val) - $start;
    return substr( $val, $start, $length );
}

package SQL::Statement::Function::StrConcat;

use vars qw(@ISA);

@ISA = qw(SQL::Statement::Function);

sub new
{
    my $class  = shift;
    my $owner  = shift;
    my $params = shift;

    my $self = $class->SUPER::new($owner);

    $self->{PARAMS} = $params;

    return $self;
}

sub value($)
{
    my $rc = '';
    foreach my $val ( @{ $_[0]->{PARAMS} } )
    {
        $rc .= $val->value( $_[1] );
    }
    return $rc;
}

1;
