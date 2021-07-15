import os


# requirements :
# 2 database SQL dumps
# 2 text files : dev_queries.txt and prod_queries.txt
# after running this script the source db must be cleaned up before running this script again

def null_out_ids(query, auto_incremented_tables):
    """
    This for setting the primary key as NULL
    :param query: "INSERT INTO" SQL query
    :param auto_incremented_tables: list of tables that have an AUTO_INCREMENT PRIMARY KEY column
    :return: (old primary key, table name, new query)
    """
    final_q = "eh"
    val_id = query[query.index('(') + 1: query.index(',')]
    table = query.split()[2][1:].rstrip('`')
    if val_id.isnumeric() and table in auto_incremented_tables:
        final_q = query[:query.index('(') + 1] + "NULL" + query[query.index(','):]
    return val_id, table, final_q


def get_singular_from_plural_table_name(name):
    """
    This is for getting a possible column name that references the table "name"
    :param name: table name
    :return: column name
    """
    name = name.strip()
    if name.endswith('s'):
        return name[: len(name) - 1]
    elif "people" in name:
        name.replace('people', 'person')
    return name


def write_list_to_file(my_list, filename):
    """
    This is for writing a list to  file
    :param my_list: list to write to file
    :param filename: target file name
    :return:
    """
    with open(filename, 'w', encoding="utf-8") as f:
        for item in my_list:
            f.write("%s\n" % item)


def get_merge_queries(source, dest, filename):
    """
    This function finds the updates that need to be done to the dest db from the source db.
    By updates, we mean adding new data entries (INSERT INTO queries)
    This function, then, prepares the new queries that update the dest db and writes them to file filename
    :param source: database sql dump file that we want to get updates from
    :param dest: database sql dump file that we want to update
    :param filename: file that we want to save the new "INSERT INTO" queries into
    :return:
    """
    pwd = os.getcwd()
    source_file = source
    dest_file = dest

    missing = 0
    create_table_statements = {}  # table_name : [[col, type] ]
    auto_incremented_tables = []  # table names of tables having an auto incremented id

    nulled_insert_queries = []  # [table_name, id] : table_name => name of table that we set its the primary key to NULL
    #                                                id => old primary key value
    all_insert_queries = []  # all insert into queries
    normal_insert_queries = []  # insert into queries that are not affected by any foreign keys that we set NULL
    #                             these queries can be ran without needing any ordering or checking
    nulled_insert_queries_and_foreign_keys = {}  # {table_name : [id, id_pos, query_containing_id]}
    #                                              table_name => name of table that we set its primary key to NULL
    #                                              id => old primary key value
    #                                              id_pos => position of foreign key
    #                                              query_containing_id => INSERT INTO query containing the foreign key reference

    print('\033[91m' + "List of statements that exist in " + source + " but are missing from " + dest + '\033[0m')
    # first check big insert into statements
    with open(source_file, encoding="utf8") as src:
        for src_line in src:
            src_line = src_line.strip()
            if src_line.startswith('INSERT INTO') and not src_line.endswith('*/') and not src_line.startswith(
                    '/*') and not src_line.startswith('//') and not src_line.startswith(
                '--') and not src_line.startswith('*'):

                # clean up statement
                if src_line.endswith(';'):
                    src_line = src_line[:len(src_line) - 1]
                queries = src_line.split('),(')
                loop_queries = src_line.split('),(')
                with open(dest_file, encoding="utf8") as dst:
                    # loop dst file
                    in_create_statement = False
                    tablename = ''
                    for dst_line in dst:
                        if dst_line.startswith(") ENGINE="):
                            # end of create statement
                            in_create_statement = False
                            continue
                        if in_create_statement:
                            # collect table columns in dictionary
                            properties = dst_line.strip().split()
                            properties_len = len(properties)
                            if properties_len > 2 and "AUTO_INCREMENT" in properties[
                                properties_len - 1] and tablename not in auto_incremented_tables:
                                auto_incremented_tables.append(tablename)
                            col = properties[0][1:].rstrip('`')
                            col_type = properties[1]
                            create_table_statements[tablename].append([col, col_type.split('(')[0]])
                            continue
                        if dst_line.startswith("CREATE TABLE"):
                            # start of create statement
                            tablename = dst_line.split('`')[1]
                            create_table_statements[tablename] = []
                            in_create_statement = True
                            continue

                        if dst_line.startswith('INSERT INTO') and not dst_line.endswith(
                                '*/') and not dst_line.startswith('/*') and not dst_line.startswith(
                            '//') and not dst_line.startswith('--') and not dst_line.startswith('*'):
                            for src_query in loop_queries:
                                if src_query not in queries:
                                    continue
                                # clean up
                                query = src_query.strip()
                                if len(query) == 0:
                                    queries.remove(src_query)
                                    continue
                                if query.strip() in dst_line:
                                    queries.remove(src_query)
                                if len(queries) == 0:
                                    break
                            if len(queries) == 0:
                                break
                if len(queries) != 0:
                    missing = missing + len(queries)
                    for index, miss in enumerate(queries):
                        q = ''
                        if index == 0 and loop_queries[0] == miss:
                            q = miss
                        else:
                            if miss.startswith('(') and miss.endswith(')'):
                                q = loop_queries[0].split("VALUES ")[0] + "VALUES " + miss
                            elif miss.startswith('('):
                                q = loop_queries[0].split("VALUES ")[0] + "VALUES " + miss + ')'
                            elif miss.endswith(')'):
                                q = loop_queries[0].split("VALUES ")[0] + "VALUES " + '(' + miss
                            else:
                                q = loop_queries[0].split("VALUES ")[0] + "VALUES " + '(' + miss + ')'
                        null_id, null_table, null_q = null_out_ids(q, auto_incremented_tables)
                        if null_q != "eh":
                            nulled_insert_queries.append([null_table, null_id])
                            normal_insert_queries.append(null_q)
                            all_insert_queries.append(null_q)
                        else:
                            normal_insert_queries.append(q)
                            all_insert_queries.append(q)

    # loop nulled out ids
    for insert_query in nulled_insert_queries:
        # loop the insert statements with the non nulled out ids
        for normal_insert_query in all_insert_queries:
            id_pos_in_normal_insert_query = -1
            normal_insert_query_values = normal_insert_query.split('(')[1].rstrip(')').split(',')
            # find position of id in normal insert statement
            if insert_query[1] in normal_insert_query_values:
                id_pos_in_normal_insert_query = normal_insert_query_values.index(insert_query[1])
            else:
                continue
            # found id
            if id_pos_in_normal_insert_query != -1:
                normal_insert_query_table_name = normal_insert_query.split()[2][1:].rstrip('`')
                # check that column name in normal table is the singular form of the nulled out table
                singular_table_null = get_singular_from_plural_table_name(insert_query[0])
                col_name_normal = \
                    create_table_statements[normal_insert_query_table_name][id_pos_in_normal_insert_query][0]
                # print(insert_query)
                # print(normal_insert_query)
                # print(col_name_normal)
                # print(singular_table_null)
                # print(create_table_statements[normal_insert_query_table_name])
                if (singular_table_null == col_name_normal) or (col_name_normal in singular_table_null):
                    # print("First if ")
                    # check that both positions are equal (correct column)
                    # print("AAAAAA " + str(col_name_normal in singular_table_null))
                    if (
                            (singular_table_null == col_name_normal)
                            and
                            (create_table_statements[normal_insert_query_table_name].index([singular_table_null, 'int'])
                             == id_pos_in_normal_insert_query)
                    ) or (
                            ([singular_table_null.split("_")[0], 'int'] in create_table_statements[
                                normal_insert_query_table_name])
                            and
                            (create_table_statements[
                                 normal_insert_query_table_name].index(
                                [singular_table_null.split("_")[0], 'int']) == id_pos_in_normal_insert_query)
                    ):
                        # print("Second if ")
                        # foreign key detected
                        if normal_insert_query in normal_insert_queries:
                            normal_insert_queries.remove(normal_insert_query)
                        # save findings
                        if insert_query[0] in nulled_insert_queries_and_foreign_keys:
                            nulled_insert_queries_and_foreign_keys[insert_query[0]].append(
                                [insert_query[1], id_pos_in_normal_insert_query, normal_insert_query])
                        else:
                            nulled_insert_queries_and_foreign_keys[insert_query[0]] = [insert_query[1],
                                                                                       id_pos_in_normal_insert_query,
                                                                                       normal_insert_query]

    # print("missing : " + str(missing))
    # print("create_table_statements : " + str(len(create_table_statements)))
    # print("auto_incremented_tables : " + str(len(auto_incremented_tables)))
    # print("nulled_insert_queries : " + str(len(nulled_insert_queries)))
    # print("nulled_insert_queries_and_foreign_keys : " + str(len(nulled_insert_queries_and_foreign_keys)))
    # print("final_insert_queries : " + str(len(final_insert_queries)))
    # print("all_insert_queries : " + str(len(all_insert_queries)))
    # print("normal_insert_queries : " + str(len(normal_insert_queries)))
    # for k in nulled_insert_queries_and_foreign_keys:
    #    print(str(len(nulled_insert_queries_and_foreign_keys[k])))
    # print('\033[91m' + "create_table_statements : " + '\033[0m')
    # print(create_table_statements)
    # print('\033[91m' + "auto_incremented_tables : " + '\033[0m')
    # print(auto_incremented_tables)
    # print('\033[91m' + "nulled_insert_queries : " + '\033[0m')
    # print(nulled_insert_queries)
    # print('\033[91m' + "normal_insert_queries : " + '\033[0m')
    # print(normal_insert_queries)
    # print('\033[91m' + "all : " + '\033[0m')
    # print(all_insert_queries)
    # print('\033[91m' + "nulled_insert_queries_and_foreign_keys : " + '\033[0m')
    # print(nulled_insert_queries_and_foreign_keys)
    # print('\033[91m' + "all_insert_queries : " + '\033[0m')
    # print(all_insert_queries)

    print("Check file '" + filename + "' for full list")
    print('\033[91m' + "--TOTAL : " + str(missing) + '\033[0m')
    # fix this later
    write_list_to_file(all_insert_queries, filename)


# This saves all the missing insert into queries in a file
get_merge_queries("tn_dev.sql", "tn_prod.sql", "dev_queries.txt")
#get_merge_queries("tn_prod.sql", "tn_dev.sql", "prod_queries.txt")

# PLAN OF ACTION FOR PRIMARY KEYS :
# get table name frm insert into statement
# find the create table statement for that table
# get the next line (the id line)
# check for auto increment
# if exists ==> null id
# else ==> leave it as it is

# PLAN OF ACTION FOR FOREIGN KEYS :
# any id that we null out ==> save in a list along with table name!!
# loop list of nulled out ids
# loop insert into statements file to look for said id
# when found :
# get the tablename of that insert into query
# get the position of the id in the insert into values
# get the create statement of the new table
# tablename is plural ==> make it singular ( people ==> person) ==> this is the column name
# find the column position in the create table statement (line number)
# compare the 2 positions
# if != ==> do nothing
# if == remove insert into statement from list and save it in the dictionary
