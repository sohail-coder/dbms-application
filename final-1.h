/********************************************************************
db.h - This file contains all the structures, defines, and function
	prototype for the db.exe program.
*********************************************************************/
#include <stdio.h>
#define MAX_IDENT_LEN   16
#define MAX_NUM_COL			16
#define MAX_TOK_LEN			32
#define KEYWORD_OFFSET	10
#define STRING_BREAK		" (),<>="
#define NUMBER_BREAK		" ),"
#define MAX_STRING_LEN 255
#define MAX_NUM_CONDITION 2
/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def
{
	char		col_name[MAX_IDENT_LEN+4];
	int			col_id;                   /* Start from 0 */
	int			col_type;
	int			col_len;
	int 		not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
   Minimum of 1 column in a table - therefore minimum size of
	 1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def
{
	int				tpd_size;
	char			table_name[MAX_IDENT_LEN+4];
	int				num_columns;
	int				cd_offset;
	int       tpd_flags;
} tpd_entry;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
   table is defined the tpd_list is 48 bytes.  When there is 
	 at least 1 table, then the tpd_entry (36 bytes) will be
	 overlapped by the first valid tpd_entry. */
typedef struct tpd_list_def
{
	int				list_size;
	int				num_tables;
	int				db_flags;
	tpd_entry	tpd_start;
}tpd_list;

/* This token_list definition is used for breaking the command
   string into separate tokens in function get_tokens().  For
	 each token, a new token_list will be allocated and linked 
	 together. */
typedef struct t_list
{
	char	tok_string[MAX_TOK_LEN];
	int		tok_class;
	int		tok_value;
	struct t_list *next;
} token_list;
typedef struct table_file_header_def {
    int file_size;
    int record_size;
    int num_records;
    int record_offset;
    int file_header_flag;
    tpd_entry *tpd_ptr;
} table_file_header;
typedef struct row_condition_def {
    int col_id;
    int op_type;
    int int_data_value;
    char string_data_value[MAX_STRING_LEN + 1];
} row_condition;
typedef struct col_item_def {
    bool is_null;
    int int_val;
    char string_val[MAX_STRING_LEN + 1];
    token_list *token;
    int col_id;
    int type;
} col_item;
typedef struct row_item_def {
    int num_fields;
    col_item_def *value_ptrs[MAX_NUM_COL];
    int sorting_col_id;
    struct row_item_def *next;
} row_item;
typedef struct col_info_def {
    char name[MAX_IDENT_LEN + 1];
    token_list *token;
} col_info;
typedef struct row_predicate_def {
    int type;
    int num_conditions;
    row_condition conditions[2];

} row_predicate;


/* This enum defines the different classes of tokens for 
	 semantic processing. */
typedef enum t_class
{
	keyword = 1,	// 1
	identifier,		// 2
	symbol, 			// 3
	type_name,		// 4
	constant,		  // 5
  function_name,// 6
	terminator,		// 7
	error			    // 8
  
} token_class;

/* This enum defines the different values associated with
   a single valid token.  Use for semantic processing. */
typedef enum t_value
{
	T_INT = 10,		// 10 - new type should be added above this line
	T_VARCHAR,		    // 11 
	T_CHAR,		    // 12       
	K_CREATE, 		// 13
	K_TABLE,			// 14
	K_NOT,				// 15
	K_NULL,				// 16
	K_DROP,				// 17
	K_LIST,				// 18
	K_SCHEMA,			// 19
  K_FOR,        // 20
	K_TO,				  // 21
  K_INSERT,     // 22
  K_INTO,       // 23
  K_VALUES,     // 24
  K_DELETE,     // 25
  K_FROM,       // 26
  K_WHERE,      // 27
  K_UPDATE,     // 28
  K_SET,        // 29
  K_SELECT,     // 30
  K_ORDER,      // 31
  K_BY,         // 32
  K_DESC,       // 33
  K_IS,         // 34
  K_AND,        // 35
  K_OR,         // 36 - new keyword should be added below this line
  F_SUM,        // 37
  F_AVG,        // 38
	F_COUNT,      // 39 - new function name should be added below this line
	S_LEFT_PAREN = 70,  // 70
	S_RIGHT_PAREN,		  // 71
	S_COMMA,			      // 72
  S_STAR,             // 73
  S_EQUAL,            // 74
  S_LESS,             // 75
  S_GREATER,          // 76
	IDENT = 85,			    // 85
	INT_LITERAL = 90,	  // 90
  STRING_LITERAL,     // 91
	EOC = 95,			      // 95
	INVALID = 99		    // 99
} token_value;

/* This constants must be updated when add new keywords */
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 30

/* New keyword must be added in the same position/order as the enum
   definition above, otherwise the lookup will be wrong */
char *keyword_table[] = 
{
  "int", "varchar", "char", "create", "table", "not", "null", "drop", "list", "schema",
  "for", "to", "insert", "into", "values", "delete", "from", "where", 
  "update", "set", "select", "order", "by", "desc", "is", "and", "or",
  "sum", "avg", "count"
};

/* This enum defines a set of possible statements */
typedef enum s_statement
{
  INVALID_STATEMENT = -199,	// -199
	CREATE_TABLE = 100,				// 100
	DROP_TABLE,								// 101
	LIST_TABLE,								// 102
	LIST_SCHEMA,							// 103
  INSERT,                   // 104
  DELETE,                   // 105
  UPDATE,                   // 106
  SELECT                    // 107
} semantic_statement;

/* This enum has a list of all the errors that should be detected
   by the program.  Can append to this if necessary. */
typedef enum error_return_codes
{
	INVALID_TABLE_NAME = -399,	// -399
	DUPLICATE_TABLE_NAME,				// -398
	TABLE_NOT_EXIST,						// -397
	INVALID_TABLE_DEFINITION,		// -396
	INVALID_COLUMN_NAME,				// -395
	DUPLICATE_COLUMN_NAME,			// -394
	COLUMN_NOT_EXIST,						// -393
	MAX_COLUMN_EXCEEDED,				// -392
	INVALID_TYPE_NAME,					// -391
	INVALID_COLUMN_DEFINITION,	// -390
	INVALID_COLUMN_LENGTH,			// -389
  INVALID_REPORT_FILE_NAME,		// -388
  /* Must add all the possible errors from I/U/D + SELECT here */
	INVALID_COLUMN_DATA,	//-387
	INVALID_CONDITION_OPERAND,  // -386
	INVALID_CONDITION,          // -385
	FILE_OPEN_ERROR = -299,			// -299
	DBFILE_CORRUPTION,					// -298
	MEMORY_ERROR							  // -297
} return_codes;

/* Set of function prototypes */
int get_token(char *command, token_list **tok_list);
void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value);
int do_semantic(token_list *tok_list);
int sem_create_table(token_list *t_list);
int sem_drop_table(token_list *t_list);
int sem_list_tables();
int sem_list_schema(token_list *t_list);
int sem_insert_schema(token_list *t_list);
int create_table_file(tpd_entry tab_entry, cd_entry cd_entries[]);
int sem_select_schema(token_list *t_list);
int sem_update_schema(token_list *t_list);
int sem_delete_schema(token_list *t_list);
void free_row_item(row_item *row, bool to_last);
int save_records_to_file(table_file_header *const tab_header,
                         row_item *const rows_head);
/*
	Keep a global list of tpd - in real life, this will be stored
	in shared memory.  Build a set of functions/methods around this.
*/
tpd_list	*g_tpd_list;
int initialize_tpd_list();
int add_tpd_to_list(tpd_entry *tpd);
int drop_tpd_from_list(char *tabname);
tpd_entry* get_tpd_from_list(char *tabname);
int validate_columns(int n_columns, col_item col_items[], cd_entry cd_entries[]);
int load_table_file(tpd_entry *tab_entry, table_file_header **table_header);
int get_file_size(FILE *fhandle);
int update_row(cd_entry cd_entries[], int num_cols, row_item *c_row,
                    char record_bytes[]);
int copy_columns_as_bytes(cd_entry cd_entries[], col_item *col_items[],
                          int num_cols, char record_bytes[],
                          int num_record_bytes);
						  void print_border(cd_entry *sorted_cd_entries[], int num_values);
void print_column_names(cd_entry *sorted_cd_entries[],
                              col_info col_infos[], int num_values);
int get_cd_entry_index(cd_entry cd_entries[], int num_cols, char *col_name);
bool eval_condition(cd_entry cd_entries[], row_condition *condition_ptr, col_item *col_item_ptr);
int records_comparator(const void *arg1, const void *arg2);
void print_record(cd_entry *sorted_cd_entries[], int num_cols,
                      row_item *row);
int column_width(cd_entry *col_entry);
bool is_row_filtered(cd_entry cd_entries[], int num_cols, row_item *row_ptr,
                     row_predicate *predicate_ptr);
int parse_where_clauses(token_list *&cur, bool &has_where_clause, cd_entry cd_entries[], tpd_entry *tab_entry, row_predicate &row_filter);
void sort_records(row_item rows[], int num_records, cd_entry *cd_entries_list_ptr,
                  bool is_desc);
inline void get_cd_entries(tpd_entry *tab_entry, cd_entry **pp_cd_entry) {
    *pp_cd_entry = (cd_entry *)(((char *)tab_entry) + tab_entry->cd_offset);
}
inline void repeat_print_char(char c, int times) {
    for (int i = 0; i < times; i++) {
        printf("%c", c);
    }
}