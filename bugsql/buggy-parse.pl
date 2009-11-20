use SQL::Statement;
use Data::Dumper;

my $parser = SQL::Parser->new();
$parser->{RaiseError}=0;
$parser->{PrintError}=1;

my @sqls = (
"INSERT INTO aaa VALUES ('aaa','bbb','ccc'),
('sss','ddd','eee')",
"INSERT INTO aaa VALUES ('aaa','bbb','ccc'),
(222,'ddd','eee')",
"INSERT INTO aaa VALUES ('aaa','bbb',111)",
"INSERT INTO aaa VALUES ('aaa','bbb','ccc'),
('sss','ddd',222)",
"INSERT INTO aaa VALUES (222,'bbb','ccc'),
('sss','ddd','eee')" );

foreach my $sql (@sqls)
{
my $stmt = SQL::Statement->new($sql,$parser);
my @rowValues = $stmt->row_values();

print Dumper(\@rowValues);
}

# but if fails when last or first member of inserted data is numeric

my $sql = "INSERT INTO aaa VALUES ('aaa','bbb''111),
('sss','ddd','eee')";
# or
my $sql = "INSERT INTO aaa VALUES ('aaa','bbb','ccc'),
(222,'ddd','eee')";

# Other examples seems to work well:
my $sql = "INSERT INTO aaa VALUES ('aaa','bbb',111)";
my $sql = "INSERT INTO aaa VALUES ('aaa','bbb','ccc'),
('sss','ddd',222)";
my $sql = "INSERT INTO aaa VALUES (222,'bbb','ccc'),
('sss','ddd','eee')";
