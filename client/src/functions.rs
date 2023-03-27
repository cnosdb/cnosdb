//! Functions that are query-able and searchable via the `\h` command
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;

use crate::Result;

#[derive(Debug)]
pub enum Function {
    Select,
    Explain,
    Show,
    CreateTable,
    CreateTableAs,
    DescribeTable,
    DescribeDatabase,
    Insert,
    DropTable,
}

const ALL_FUNCTIONS: [Function; 9] = [
    Function::CreateTable,
    Function::CreateTableAs,
    Function::DropTable,
    Function::Explain,
    Function::Insert,
    Function::Select,
    Function::DescribeTable,
    Function::DescribeDatabase,
    Function::Show,
];

impl Function {
    pub fn function_details(&self) -> Result<&str> {
        let details = match self {
            Function::Select => {
                r#"
Command:     SELECT
Description: retrieve rows from a table or view
Syntax:
SELECT [ ALL | DISTINCT [ ON ( expression [, ...] ) ] ]
    [ * | expression [ [ AS ] output_name ] [, ...] ]
    [ FROM from_item [, ...] ]
    [ WHERE condition ]
    [ GROUP BY [ ALL | DISTINCT ] grouping_element [, ...] ]
    [ HAVING condition ]
    [ WINDOW window_name AS ( window_definition ) [, ...] ]
    [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select ]
    [ ORDER BY expression [ ASC | DESC | USING operator ] [ NULLS { FIRST | LAST } ] [, ...] ]
    [ LIMIT { count | ALL } ]
    [ OFFSET start [ ROW | ROWS ] ]

where from_item can be one of:

    [ ONLY ] table_name [ * ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
                [ TABLESAMPLE sampling_method ( argument [, ...] ) [ REPEATABLE ( seed ) ] ]
    [ LATERAL ] ( select ) [ AS ] alias [ ( column_alias [, ...] ) ]
    with_query_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
    [ LATERAL ] function_name ( [ argument [, ...] ] )
                [ WITH ORDINALITY ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
    [ LATERAL ] function_name ( [ argument [, ...] ] ) [ AS ] alias ( column_definition [, ...] )
    [ LATERAL ] function_name ( [ argument [, ...] ] ) AS ( column_definition [, ...] )
    [ LATERAL ] ROWS FROM( function_name ( [ argument [, ...] ] ) [ AS ( column_definition [, ...] ) ] [, ...] )
                [ WITH ORDINALITY ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
    from_item [ NATURAL ] join_type from_item [ ON join_condition | USING ( join_column [, ...] ) [ AS join_using_alias ] ]

and grouping_element can be one of:

    ( )
    expression
    ( expression [, ...] )

and with_query is:

    with_query_name [ ( column_name [, ...] ) ] AS [ [ NOT ] MATERIALIZED ] ( select | values | insert | update | delete )

TABLE [ ONLY ] table_name [ * ]"#
            }
            Function::Explain => {
                r#"
Command:     EXPLAIN
Description: show the execution plan of a statement
Syntax:
EXPLAIN [ ANALYZE ] statement
"#
            }
            Function::Show => {
                r#"
Command:     SHOW
Description: show the value of a run-time parameter
Syntax:
SHOW name
"#
            }
            Function::CreateTable => {
                r#"
Command:     CREATE TABLE
Description: define a new table
Syntax:
CREATE [ EXTERNAL ]  TABLE table_name ( [
  { column_name data_type }
    [, ... ]
] )
"#
            }
            Function::CreateTableAs => {
                r#"
Command:     CREATE TABLE AS
Description: define a new table from the results of a query
Syntax:
CREATE TABLE table_name
    [ (column_name [, ...] ) ]
    AS query
    [ WITH [ NO ] DATA ]
"#
            }
            Function::DescribeDatabase => {
                r#"
Command:     Describe
Description: describe database
Syntax:
DESCRIBE DATABASE database_name
"#
            }
            Function::DescribeTable => {
                r#"
Command:     Describe
Description: describe table
Syntax:
DESCRIBE TABLE [database_name.] table_name
"#
            }
            Function::Insert => {
                r#"
Command:     INSERT
Description: create new rows in a table
Syntax:
INSERT INTO table_name [ ( column_name [, ...] ) ]
    { VALUES ( { expression } [, ...] ) [, ...] }
"#
            }
            Function::DropTable => {
                r#"
Command:     DROP TABLE
Description: remove a table
Syntax:
DROP TABLE [ IF EXISTS ] name [, ...]
"#
            }
        };
        Ok(details)
    }
}

impl FromStr for Function {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s.trim().to_uppercase().as_str() {
            "SELECT" => Self::Select,
            "EXPLAIN" => Self::Explain,
            "SHOW" => Self::Show,
            "CREATE TABLE" => Self::CreateTable,
            "CREATE TABLE AS" => Self::CreateTableAs,
            "INSERT" => Self::Insert,
            "DROP TABLE" => Self::DropTable,
            _ => return Err(()),
        })
    }
}

impl fmt::Display for Function {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Function::Select => write!(f, "SELECT"),
            Function::Explain => write!(f, "EXPLAIN"),
            Function::Show => write!(f, "SHOW"),
            Function::CreateTable => write!(f, "CREATE TABLE"),
            Function::CreateTableAs => write!(f, "CREATE TABLE AS"),
            Function::DescribeTable => write!(f, "DESCRIBE TABLE"),
            Function::DescribeDatabase => write!(f, "DESCRIBE DATABASE"),
            Function::Insert => write!(f, "INSERT"),
            Function::DropTable => write!(f, "DROP TABLE"),
        }
    }
}

pub fn display_all_functions() -> Result<()> {
    println!("Available help:");
    let array = StringArray::from(
        ALL_FUNCTIONS
            .iter()
            .map(|f| format!("{}", f))
            .collect::<Vec<String>>(),
    );
    let schema = Schema::new(vec![Field::new("Function", DataType::Utf8, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])?;
    println!("{}", pretty_format_batches(&[batch]).unwrap());
    Ok(())
}
