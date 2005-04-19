package SQL::Statement::Util;
sub type {
    my($self)=@_;
    return 'function' if $self->isa('SQL::Statement::Util::Function');
    return 'column'   if $self->isa('SQL::Statement::Util::Column');
}

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
1;
