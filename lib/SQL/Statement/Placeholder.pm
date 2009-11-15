package SQL::Statement::Placeholder;

use vars qw(@ISA);
require Carp;

require SQL::Statement::Term;

our $VERSION = '1.21_1';

@ISA = qw(SQL::Statement::Term);

sub new
{
    my $class = shift;
    my $owner = shift;
    my $argnum = shift;

    my $self = $class->SUPER::new( $owner );
    $self->{ARGNUM} = $argnum;

    return $self;
}

sub value($)
{
# from S::S->get_row_value():
#        my $val = (
#                         $self->{join}
#                      or !$eval
#                      or ref($eval) =~ /Statement$/
#                  ) ? $self->params($arg_num) : $eval->param($arg_num);

    # let's see where us will lead taking from params every time
    # XXX later: return $_[0]->{OWNER}->{params}->[$_[0]->{ARGNUM}];
    return $_[0]->{OWNER}->{params}->[$_[0]->{OWNER}->{argnum}++];
}

1;
