package SQL::Statement::Util;

use vars qw($VERSION);
$VERSION = '1.21_1';

sub type
{
    my ($self) = @_;
    return 'function' if $self->isa('SQL::Statement::Util::Function');
    return 'column'   if $self->isa('SQL::Statement::Util::Column');
}

package SQL::Statement::Util::Column;
use base 'SQL::Statement::Util';

use Params::Util qw(_ARRAY _HASH0 _STRING);

sub new
{
    my $class        = shift;
    my $col_name     = shift;
    my $table_name   = shift;
    my $term         = shift;
    my $display_name = shift || $col_name;

    # print " $col_name !\n";
    if ( $col_name && ( ( $col_name =~ m/^(".+")\.(.*)$/ ) || ( $col_name =~ m/^([^.]*)\.(.*)$/ ) ) )
    {
        $table_name = $1;
        $col_name   = $2;
    }
    elsif ( _ARRAY($table_name) && ( scalar( @{$table_name} ) == 1 ) )
    {
        $table_name = $table_name->[0];
    }

    my %instance = (
                     name         => $col_name,
                     table        => $table_name,
                     display_name => $display_name,
                     term         => $term,
                   );

    my $self = bless( \%instance, $class );

    return $self;
}

sub value($)     { $_[0]->{term}->value( $_[1] ); }
sub term()       { $_[0]->{term} }
sub display_name { $_[0]->{display_name} }
sub name         { $_[0]->{name} }
sub table        { $_[0]->{table} }

package SQL::Statement::Util::Function;
use base 'SQL::Statement::Util';

sub new
{
    my ( $class, $name, $sub_name, $args ) = @_;
    my ( $pkg, $sub ) = $sub_name =~ /^(.*::)([^:]+$)/;
    if ( !$sub )
    {
        $pkg = 'main';
        $sub = $sub_name;
    }
    $pkg = 'main' if $pkg eq '::';
    $pkg =~ s/::$//;
    my %newfunc = (
                    name     => $name,
                    sub_name => $sub,
                    pkg_name => $pkg,
                    args     => $args,
                    type     => 'function',
                  );
    return bless \%newfunc, $class;
}
sub name     { shift->{name} }
sub pkg_name { shift->{pkg_name} }
sub sub_name { shift->{sub_name} }
sub args     { shift->{args} }

sub validate
{
    my ($self) = @_;
    my $pkg    = $self->pkg_name;
    my $sub    = $self->sub_name;
    $pkg =~ s,::,/,g;
    eval { require "$pkg.pm" }
      unless $pkg eq 'SQL/Statement/Functions'
          or $pkg eq 'main';
    die $@ if $@;
    $pkg =~ s,/,::,g;
    die "Can't find subroutine $pkg" . "::$sub\n" unless $pkg->can($sub);
    return 1;
}

sub run
{
    use SQL::Statement::Functions;

    my ($self) = shift;
    my $sub    = $self->sub_name;
    my $pkg    = $self->pkg_name;
    return $pkg->$sub(@_);
}

1;
