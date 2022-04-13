import IndexManager
import BufferManager
import os
import re

tables_c = {}
path_c = ''
indexs_c = {}

class table_instance():   
    def __init__(self, name, p_key = 0):
        self.table_name = name
        self.primary_key = p_key
        self.columns = []

class column:
    def __init__(self, name, unique, type = 'char', length = 20):
        self.column_name = name
        self.is_unique = unique
        self.type = type
        self.length = length

def __initialize__(_path):
    global tables_c
    global path_c
    global indexs_c
    path_c = _path
    if not os.path.exists(os.path.join(path_c, 'DBfiles/Catalog_Files')):
        os.makedirs(os.path.join(path_c, 'DBfiles/Catalog_Files'))
        tables_c['sys'] = table_instance('sys')

        # print(tables_c['sys'].primary_key)

        indexs_c['sys_index'] = {'table':'sys','column':'username'}
        tables_c['sys'].columns.append(column('username', True))
        tables_c['sys'].columns.append(column('password', False)) 
        
        BufferManager.c_store(os.path.join(path_c, 'DBfiles/Catalog_Files'))
    tables_c,indexs_c = BufferManager.c_load(os.path.join(path_c, 'DBfiles/Catalog_Files'))
    # print(indexs_c)
    # print(tables_c)
    # for table in tables_c.items():
    #     print(table)

def __finalize__():
    BufferManager.c_store(os.path.join(path_c, 'DBfiles/Catalog_Files'))



def check_types_of_table(table,values):
    cur_table = tables_c[table]
    if len(cur_table.columns) != len(values):
        raise Exception("Error: Column count of table '%s' doesn't match value count" % (table))
    # print(values)
    for index,i in enumerate(cur_table.columns):
        # print(index)
        if i.type == 'int':
            value = int(values[index])
        elif i.type == 'float':
            value = float(values[index])
        
        else:
            value = values[index].strip().strip("'")
            if len(value) > i.length:
                raise Exception("Error : Length of column '%s' can't be longer than %d." % (i.column_name,i.length))
        # print(index, value)
        if i.is_unique:
            IndexManager.check_unique(table,index,value)
        # print('check_type finished', tables_c[table].columns[index].column_name, tables_c[table].columns[index].is_unique)

def exists_table(table):
    for key in tables_c.keys():
        if key == table:
            raise Exception("ERROR : Table '%s' already exists." % (table))

def not_exists_table(table):
    for key in tables_c.keys():
        if key == table:
            return
    raise Exception("ERROR : Table '%s' doesn't exist." % table)

def not_exists_index(index):
    for key in indexs_c.keys():
        if key == index:
            return
    raise Exception("ERROR : Index '%s' doesn't exist." % index)

def exists_index(index):
    for key in indexs_c.keys():
        if key == index:
            raise Exception("ERROR : Index '%s' already exists."%index)

def create_table(table,statement):
    global tables_c
    if re.search('primary key *\(',statement):
        primary_place = re.search('primary key *\(',statement).end()
        primary_place_end = re.search('\)',statement[primary_place:]).start()
    else :
        raise Exception("ERROR : Primary key is needed to specify when creating a table")
    # print('primary')
    primary_key = statement[primary_place:][:primary_place_end].strip()
    # unique_col = []
    # if re.search('unique *\(',statement):
    #     unique_place = re.search('unique *\(',statement).end()
    #     unique_place_end = re.search('\)',statement[unique_place:]).start()
    #     print(unique_place, unique_place_end)

    cur_table = table_instance(table,primary_key)
    lists = statement.split(',')
    columns = []
    for cur_column_statement in lists:
        cur_column_statement = cur_column_statement.strip()
        cur_lists = cur_column_statement.split(' ')
        if cur_lists[0] != 'unique' and cur_lists[0] != 'primary':
            # print('type')
            is_unique = False
            type = 'char'
            column_name = cur_lists[0]
            if re.search('unique',concat_list(cur_lists[1:])) or column_name == primary_key:
                is_unique = True
            if re.search('char',concat_list(cur_lists[1:])):
                length_start = re.search('\(',concat_list(cur_lists[1:])).start()+1
                length_end = re.search('\)', concat_list(cur_lists[1:])).start()
                length = int(concat_list(cur_lists[1:])[length_start:length_end])

            elif re.search('int', concat_list(cur_lists[1:])):
                length = 0
                type = 'int'
            elif re.search('float', concat_list(cur_lists[1:])):
                length = 0
                type = 'float'
            else:
                raise Exception("ERROR : Unknown type for %s. Try 'varchar' or 'int' or 'float'.\n" % column_name)
            columns.append(column(column_name,is_unique,type,length))
        elif cur_lists[0] == 'unique':
            # print('in unique')
            unique_place = re.search('unique *\(',concat_list(cur_lists)).end()
            unique_place_end = re.search('\)', concat_list(cur_lists)).start()
            unique_column = concat_list(cur_lists)[unique_place:unique_place_end]
            # print(unique_column)
            # print(unique_column)
            for col in columns:
                if col.column_name == unique_column:
                    col.is_unique = True
        # else :
        #     print(cur_lists[0])
    # print(1)
    cur_table.columns = columns
    seed = False
    for index,__column in enumerate(cur_table.columns):
        if __column.column_name == cur_table.primary_key:
            cur_table.primary_key = index
            seed = True
            break
    if seed == False:
        raise Exception("ERROR : Primary key '%s' not exists."
                        % cur_table.primary_key)

    tables_c[table] = cur_table


def drop_table(table):
    tables_c.pop(table)

def drop_index(index):
    indexs_c.pop(index)
    print("Index '%s' deleted." % index)

def create_index(index_name,table,column):
    indexs_c[index_name] = {'table':table,'column':column}

def check_select_statement(table,conditions,__columns):
    columns = []
    for i in tables_c[table].columns:
        columns.append(i.column_name)
    if conditions != '':
        conditions = re.sub('and|or',',',conditions)
        conditions_lists = conditions.split(',')
        for i in conditions_lists:
            if i.strip().split(' ')[0] not in columns:
                raise Exception("ERROR : Unknown column '%s' in 'where clause'" % i.strip().split(' ')[0])
    if __columns == '*':
        return
    else:
        __columns_list = __columns.split(',')
        for i in __columns_list:
            if i.strip() not in columns:
                raise Exception("ERROR : Unknown column '%s' in 'field list'" % i.strip())


def concat_list(lists):
    statement = ''
    for i in lists:
        statement = statement + i
    return statement

