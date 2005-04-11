package SQL::Statement::Util;
sub type {
    my($self)=@_;
    return 'function' if $self->isa('SQL::Statement::Util::Function');
    return 'column'   if $self->isa('SQL::Statement::Util::Column');
}
=pod

=head1 Objects & Methods for accessing parsed SQL Statements

=head2 Column Object

 Column->name()           # column name in upper-case
 Column->display_name()   # column alias or name in user-supplied case
 Column->table()          # name of table column belongs to, if known
 Column->function()       # a Function object, if it's a computed column

=head3 Column->name()

=head3 Column->display_name()

=head3 Column->table()

=head3 Column->function()

=head2 Function Object

=cut

package SQL::Statement::Util::Column;
use base 'SQL::Statement::Util';
sub new {
    my $class = shift;
    my $col_name = shift;
    my $tables = shift;
    my $display_name = shift || $col_name;
    my $function = shift;
    my $table_name = $col_name;
    #my @c = caller 0; print $c[2];
    if (ref $col_name eq 'HASH') {
        $tables   = [ $col_name->{"table"} ];
        $col_name = $col_name->{"column"}  ;
    }
    # print " $col_name !\n";
    my $num_tables = scalar @{ $tables };
    if ($table_name && (
           $table_name =~ /^(".+")\.(.*)$/
        or $table_name =~ /^([^.]*)\.(.*)$/
        )) {
            $table_name = $1;
            $col_name = $2;
    }
    elsif ($num_tables == 1) {
        $table_name = $tables->[0];
    }
    else {
        undef $table_name;
    }
    my $self = {
        name         => $col_name,
        table        => $table_name,
        display_name => $display_name,
        function     => $function,
    };
    return bless $self, $class;
}
sub function     { shift->{"function"} }
sub display_name { shift->{"display_name"} }
sub name         { shift->{"name"} }
sub table        { shift->{"table"} }

package SQL::Statement::Util::Function;
use base 'SQL::Statement::Util';
sub new {
    my($class,$name,$sub_name,$args) = @_;
    my($pkg,$sub) = $sub_name =~ /^(.*::)([^:]+$)/;
    if (!$sub) {
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
    return bless \%newfunc,$class;
}
sub name     { shift->{name}     }
sub pkg_name { shift->{pkg_name} }
sub sub_name { shift->{sub_name} }
sub args     { shift->{args}     }
sub validate {
    my($self) = @_;
    my $pkg = $self->pkg_name;
    my $sub = $self->sub_name;
    $pkg =~ s~::~/~g;
    eval { require "$pkg.pm" }
         unless $pkg eq 'SQL/Statement/Functions' or $pkg eq 'main';
    die $@ if $@;
    $pkg =~ s~/~::~g;
    die "Can't find subroutine $pkg"."::$sub\n" unless $pkg->can($sub);
    return 1;
}
sub run {
  use SQL::Statement::Functions;
  
    my($self) = shift;
    my $sub = $self->sub_name;
    my $pkg = $self->pkg_name;
    return $pkg->$sub(@_);
}
=pod

Value objects
   placeholders, columns, functions, etc.

=head2 SQL::Statement::Func Objects

A new S::S::Func object is created for each function once per prepare, after which, the following methods are available:

 name()     # function's name, e.g. UPPER
 alias()    # function's alias, e.g. c1
 args()     # an arrayref of value objects
 class()    # name of package holding func's subroutine, e.g. Bar::MyFuncs
 subname()  # name of func's subroutine, e.g. SQL_FUNCTION_UPPER

At this point, only the name of the class and sub are known, it isn't known whether there actually is a corresponding subroutine in the class.  Value objects in the args arrayref may also be unknown at this point: placeholders and column names have not yet been replaced with values.

Each Func object is validated once per execute, during open_tables().  Validation requires the function's package and checks for the existence of a routine named with the function's subname.  An error will be generated if the subroutine can't be found.

 validate() # checks if the function corresponds to an available subroutine


=cut
1;
