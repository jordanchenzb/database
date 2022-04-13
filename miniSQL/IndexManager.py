import CatalogManager
import BufferManager
import os
# import math

tables_i = {}
path_i = ''
N=21

def __initialize__(_path):
    global tables_i
    global path_i
    path_i = _path
    if not os.path.exists(os.path.join(path_i,'DBfiles/Index_Files')):
        os.makedirs(os.path.join(path_i,'DBfiles/Index_Files'))
        tables_i['sys'] = BufferManager.node(True, ['root'], [['root','123456'], ''])
        BufferManager.i_store(os.path.join(path_i,'DBfiles/Index_Files/tables.sql'))
    tables_i = BufferManager.i_load(os.path.join(path_i,'DBfiles/Index_Files/tables.sql'))

def __finalize__():
    BufferManager.i_store(os.path.join(path_i,'DBfiles/Index_Files/tables.sql'))

def create_table(table):
    tables_i[table] = BufferManager.node(True,[],[])

def delete_table(table):
    tables_i.pop(table)

def insert_into_table(table,values):
    
    # print('in index insert_into_table')# for test
    
    for index, _column in enumerate(CatalogManager.tables_c[table].columns):
        if _column.type == 'int':
            values[index] = int(values[index])
        elif _column.type == 'char':
            values[index] = values[index].strip().strip("'")
        elif _column.type == 'float':
            values[index] = float(values[index])
    _node = tables_i[table]
    _primary_key = CatalogManager.tables_c[table].primary_key
    
    while _node.is_leaf==False: #find leaf
        flag = False
        for index, key in enumerate(_node.keys):
            if key>values[_primary_key]:
                _node = _node.blocks[index]
                flag = True
                break
        if flag==False:
            _node = _node.blocks[-1]
    # print(1)
    # print(_node.keys)
    if len(_node.keys)==0:
        _node.keys.append(values[_primary_key])
        _node.blocks.append(values)
        _node.blocks.append('')
        # print('Query OK. 1 row affected.')
        return

    if len(_node.keys) < N-1:
        # print(2)
        insert_leaf(_node, values[_primary_key], values)
        # print(3)
    else:
        insert_leaf(_node, values[_primary_key], values)
        new_node = BufferManager.node(True, [], [])
        new_node.keys = _node.keys[int((N+1)/2):]
        new_node.blocks = _node.blocks[int((N+1)/2):]
        _node.keys = _node.keys[0:int((N+1)/2)]
        _node.blocks = _node.blocks[0:int((N+1)/2)]
        _node.blocks.append(new_node)
        insert_parent(table, _node, new_node.keys[0], new_node)

def delete_from_table(table, statement):
    if len(statement)==0:
        # print(0)
        tables_i[table] = BufferManager.node(True, [], [],'')
        print("All entrys in table '%s' deleted," % table,end='')
    else:
        # print(statement)
        columns = {}
        for index,col in enumerate(CatalogManager.tables_c[table].columns):
            columns[col.column_name] = index
        __primary_key = CatalogManager.tables_c[table].primary_key

         

        conditions = []
        tmp = []
        pos = 1
        for i in statement:
            if i == 'and':
                conditions.append(tmp)
                tmp = []
                pos = 1
                continue
            if pos == 1:
                tmp.append(columns[i])
            elif pos == 3:
                if CatalogManager.tables_c[table].columns[tmp[0]].type == 'char':
                    tmp.append(i.strip().replace("'", ''))
                elif CatalogManager.tables_c[table].columns[tmp[0]].type == 'int':
                    tmp.append(int(i))
                elif CatalogManager.tables_c[table].columns[tmp[0]].type == 'float':
                    tmp.append(float(i))
            else:
                tmp.append(i)
            pos = pos + 1
        conditions.append(tmp)
        times = 0
        
        # print(conditions)
        
        while True:
            # nodes = find_leaf_with_condition(table, conditions[0][0], conditions[0][2], conditions[0][1])
            nodes = []
            for col in conditions:
                    nodes = find_leaf_with_condition(table,col[0],col[2],col[1])
                    break
            if len(nodes) == 0:
                break
            flag = False
            for _node in nodes:
                if flag == True:
                    break
                for index,leaf in enumerate(_node.blocks[0:-1]):
                    if check_conditions(leaf,conditions):
                        _node.blocks.pop(index)
                        _node.keys.pop(index)
                        maintain_B_plus_tree_after_delete(table,_node)
                        times = times + 1
                        flag = True
                        break
            if flag == False:
                break
        print("Query OK,  %d rows affected." % (times),end='')


def maintain_B_plus_tree_after_delete(table,_node):
    global N
    if _node.parent == '' and len(_node.blocks) == 1:
        tables_i[table] = _node.blocks[0]
        
        # print(tables_i[table])
        if tables_i[table] == '':
            # print("''")
            tables_i[table] = BufferManager.node(True, [],[])
        else:
            tables_i[table].parent = ''
        return
    elif ((len(_node.blocks) < int((N+1)/2) and _node.is_leaf == False) or (len(_node.keys) < int((N-1)/2) and _node.is_leaf == True) ) and _node.parent != '':
        previous = False
        other_node = BufferManager.node(True,[],[])
        K = ''
        __index = 0
        for index, i in enumerate(_node.parent.blocks):
            if i == _node:
                # print(index, len(_node.parent.blocks))
                if index == len(_node.parent.blocks) - 1:
                    # print(1)
                    # print(len(_node.parent.blocks))
                    # print(_node.is_leaf)
                    # print(len(_node.blocks))
                    if (len(_node.parent.blocks)>1):
                        other_node = _node.parent.blocks[-2]
                    else:
                        # _node.is_leaf = True
                        # _node.parent = ''
                        # tables_i[table] = _node
                        # return
                        maintain_B_plus_tree_after_delete(table, _node.parent)
                        return
                    # print(2)
                    previous = True
                    K = _node.parent.keys[index - 1]
                else:
                    K = _node.parent.keys[index]
                    other_node = _node.parent.blocks[index + 1]
                    __index = index + 1
                break

        if (other_node.is_leaf == True and len(other_node.keys)+len(_node.keys) < N) or \
            (other_node.is_leaf == False and len(other_node.blocks) +
            len(_node.blocks) <= N):
            if previous == True:
                if other_node.is_leaf == False:
                    other_node.blocks = other_node.blocks + _node.blocks
                    other_node.keys = other_node.keys + [K] + _node.keys
                    for _node__ in _node.blocks:
                        _node__.parent = other_node
                else:
                    other_node.blocks = other_node.blocks[0:-1]
                    other_node.blocks = other_node.blocks + _node.blocks
                    other_node.keys = other_node.keys + _node.keys
                _node.parent.blocks = _node.parent.blocks[0:-1]
                _node.parent.keys = _node.parent.keys[0:-1]
                maintain_B_plus_tree_after_delete(table,_node.parent)
            else:
                if other_node.is_leaf == False:
                    _node.blocks = _node.blocks + other_node.blocks
                    _node.keys = _node.keys + [K] + other_node.keys
                    for _node__ in other_node.blocks:
                        _node__.parent = _node
                else:
                    _node.blocks = _node.blocks[0:-1]
                    _node.blocks = _node.blocks + other_node.blocks
                    _node.keys = _node.keys + other_node.keys
                _node.parent.blocks.pop(__index)
                _node.parent.keys.pop(__index-1)
                maintain_B_plus_tree_after_delete(table,_node.parent)
        else:
            if previous == True:
                if other_node.is_leaf == True:
                    _node.keys.insert(0,other_node.keys.pop(-1))
                    _node.blocks.insert(0,other_node.blocks.pop(-2))
                    _node.parent.keys[-1] = _node.keys[0]
                else:
                    __tmp = other_node.blocks.pop(-1)
                    __tmp.parent = _node
                    _node.blocks.insert(0,__tmp)
                    _node.keys.insert(0,_node.parent.keys[-1])
                    _node.parent.keys[-1] = other_node.keys.pop(-1)
            else:
                if other_node.is_leaf == True:
                    _node.keys.append(other_node.keys.pop(0))
                    _node.blocks.insert(-1,other_node.blocks.pop(0))
                    _node.parent.keys[__index-1] = other_node.keys[0]
                else:
                    __tmp = other_node.blocks.pop(0)
                    __tmp.parent = _node
                    _node.blocks.append(__tmp)
                    _node.keys.append(other_node.keys.pop())
                    _node.parent.keys[__index-1] = other_node.keys[0]


def create_index(index_name,table,column):
    pass



def check_conditions(leaf,conditions):
    for cond in conditions:
        # cond <-> column op value
        __value = leaf[cond[0]]
        if cond[1] == '<':
            if not (__value < cond[2]):
                return False
        elif cond[1] == '<=':
            if not (__value <= cond[2]):
                return False
        elif cond[1] == '>':
            if not (__value > cond[2]):
                return False
        elif cond[1] == '>=':
            if not (__value >= cond[2]):
                return False
        elif cond[1] == '<>' or cond[1]=='!=':
            if not (__value != cond[2]):
                return False
        elif cond[1] == '=':
            if not (__value == cond[2]):
                return False
        else:
            raise Exception("ERROR : Unknown Op.")
    return True

def check_equal(table, column, value):
    # print('check_equal')
    equals = []
    header = tables_i[table]
    leaf_node = header
    # print(1)
    while not leaf_node.is_leaf:
        leaf_node = leaf_node.blocks[0]
    while len(leaf_node.blocks):
        for block in leaf_node.blocks[0:-1]:
            if block[column] == value:
                equals.append(block)
        if leaf_node.blocks[-1] == '':
            break
        leaf_node = leaf_node.blocks[-1]
    return equals
        


def select_from_table(table, __conditions='', __columns=''):
    results = []
    columns = {}
    # print('index')
    for index,col in enumerate(CatalogManager.tables_c[table].columns):
        columns[col.column_name] = index
    __primary_key = CatalogManager.tables_c[table].primary_key
    # __primary_key = 0
    # columns = {'num': 0, 'val': 1}
    # print("before if")
    if len(tables_i[table].keys) == 0:
        pass
    else:
        if __conditions != '':
            
            conditions = []
            statement = __conditions.split(' ')
            position = 1
            tmp = []
            for i in statement:
                if i == 'and':
                    conditions.append(tmp)
                    tmp = []
                    position = 1
                    continue
                if position == 1:
                    tmp.append(columns[i])
                elif position == 3:
                    if CatalogManager.tables_c[table].columns[tmp[0]].type == 'char':
                        tmp.append(i.strip().strip("'"))
                    elif CatalogManager.tables_c[table].columns[tmp[0]].type == 'int':
                        tmp.append(int(i))
                    elif CatalogManager.tables_c[table].columns[tmp[0]].type == 'float':
                        tmp.append(float(i))
                else:
                    tmp.append(i)
                position = position + 1
            conditions.append(tmp)
            # nodes = find_leaf_with_condition(table, conditions[0][0], conditions[0][2], conditions[0][1])
            # for col in conditions:
            #     if col[0] == __primary_key:
            #         nodes = find_leaf_with_condition(table, col[0], col[2], col[1])
            #         break

            nodes = []
            head_node = tables_i[table]
            first_leaf_node = head_node
            while first_leaf_node.is_leaf != True:
                first_leaf_node = first_leaf_node.blocks[0]
            while True:
                for pointer in first_leaf_node.blocks[0:-1]:
                    nodes.append(pointer)
                if first_leaf_node.blocks[-1] == '':
                    break
                first_leaf_node = first_leaf_node.blocks[-1]

            for pointer in nodes:
                # for pointer in __node.blocks[0:-1]:
                if check_conditions(pointer,conditions):
                    results.append(pointer)
        else:
            # print("''")
            first_node = tables_i[table]
            while first_node.is_leaf != True:
                first_node = first_node.blocks[0]
            # print(first_node.keys)
            while True:
                if len(first_node.blocks):
                    for i in first_node.blocks[0:-1]:
                        results.append(i)
                        # print(i)
                else:
                    break
                # print(first_node.blocks[-1].keys,first_node.blocks[-1].blocks)
                if type(first_node.blocks[-1]) == BufferManager.node:
                    
                    first_node = first_node.blocks[-1]
                else:
                    break
    # print('after if')
    if __columns == '*':
        __columns_list = list(columns.keys())
        __columns_list_num = list(columns.values())
    else:
        __columns_list_num = [columns[i.strip()] for i in __columns.split(',')]
        __columns_list = [i.strip() for i in __columns.split(',')]

    print('+'+'-' * ((17 * len(__columns_list_num) + 1)-2)+'+')
    for i in __columns_list:
        if len(str(i)) > 14:
            output = str(i)[0:14]
        else:
            output = str(i)
        print('|',output.center(15),end='')
    print('|')
    print('+'+'-' * ((17 * len(__columns_list_num) + 1)-2)+'+')
    for i in results:
        for j in __columns_list_num:
            if len(str(i[j])) > 14:
                output = str(i[j])[0:14]
            else:
                output = str(i[j])
            print('|',output.center(15) ,end='')
        print('|')
    print('+'+'-' * ((17 * len(__columns_list_num) + 1)-2)+'+')
    print("%d rows in set," % len(results),end='')



    
def insert_leaf(_node, value, block):
    index = 0
    for index, key in enumerate(_node.keys):
        if key==value:
            raise Exception('Primary key already exists!')
        elif key>value:
            _node.blocks.insert(index, block)
            _node.keys.insert(index, value)
            return
    _node.blocks.insert(index+1, block)
    _node.keys.insert(index+1, value)

def insert_parent(table, _node, _key, new_node):
    if _node.parent == '':
        p_node = BufferManager.node(False, [], [])
        p_node.blocks.append(_node)
        p_node.blocks.append(new_node)
        p_node.keys.append(_key)
        _node.parent = new_node.parent = p_node
        tables_i[table] = p_node
    else:
        p_node = _node.parent
        if len(p_node.blocks)<N:
            flag = False
            for index, key in enumerate(p_node.keys):
                if key > _key:
                    p_node.keys.insert(index, _key)
                    p_node.blocks.insert(index+1, new_node)
                    flag = True
                    break
            if flag == False:
                p_node.keys.append(_key)
                p_node.blocks.append(new_node)
            new_node.parent = p_node
        else:
            flag = False
            for index, key in enumerate(p_node.keys):
                if key > _key:
                    p_node.keys.insert(index, _key)
                    p_node.blocks.insert(index+1, new_node)
                    flag = True
                    break
            if flag == False:
                p_node.keys.append(_key)
                p_node.blocks.append(new_node)
            new_p_node = BufferManager.node(False, [], [])
            new_p_node.keys = p_node.keys[int((N+1)/2)+1:]
            new_p_node.blocks = p_node.blocks[int((N+1)/2)+1:]
            for _node in new_p_node.blocks:
                _node.parent = new_p_node
            p_key = p_node.keys[int((N+1)/2)]
            p_node.keys = p_node.keys[0:int((N+1)/2)]
            p_node.blocks = p_node.blocks[0:int((N+1)/2)]
            insert_parent(table, p_node, p_key, new_p_node)

def exist_user(username,password):
    # nodes = find_leaf_with_condition('sys', 0, username, '=')
    # for _node in nodes:
    #     for ptr in _node.blocks[0:-1]:
    #         if ptr[0] == username and ptr[1] == password:
    #             return True

    users = (check_equal('sys', 0, username))
    for user in users:
        if user[1] == password:
            return True

    return False

def check_unique(table,column,value):
    columns = []
    
    for col in CatalogManager.tables_c[table].columns:
        columns.append(col)
    # print('check unique')
    # print(table, column, value)
    # if len(find_leaf_with_condition(table,column,value,'=')):
    if len(check_equal(table, column, value)):
        # print("not_unique")
        raise Exception("ERROR : Duplicate entry '%s' for key '%s.%s'." % (value, table, columns[column].column_name))
    # else:
        # print(columns[column].column_name)
    # print('after check')



def find_leaf_with_condition(table, column, value, condition):
    __primary_key = CatalogManager.tables_c[table].primary_key
    # __primary_key = 0
    head_node = tables_i[table]
    first_leaf_node = head_node
    lists = []
    if len(first_leaf_node.blocks) == 0:
        return lists
    while first_leaf_node.is_leaf != True:
        first_leaf_node = first_leaf_node.blocks[0]
    
    # print(first_leaf_node.blocks)
    if False:#__primary_key == column and condition not in['<>','!=']:
        
        # print(column, condition, value)

        while not head_node.is_leaf:
            seed = False
            for index, key in enumerate(head_node.keys):
                if key > value:

                    print(head_node.keys,key, value)
                    print(len(head_node.blocks), index)

                    head_node = head_node.blocks[index]
                    seed = True
                    break
            
            

            if seed == False:
                head_node = head_node.blocks[-1]
        
        print(value)
        print(head_node.blocks, head_node.keys)
        
        if condition == '=':
            for pointer in head_node.blocks[0:-1]:
                if pointer[column] == value:
                    lists.append(head_node)
        elif condition == '<=':
            cur_node = first_leaf_node
            while True:
                if cur_node != head_node:
                    lists.append(cur_node)
                    cur_node = cur_node.blocks[-1]
                else:
                    break
            for pointer in head_node.blocks[0:-1]:
                if pointer[column] <= value:
                    lists.append(head_node)
                    break
        elif condition == '<':
            cur_node = first_leaf_node
            while True:
                if cur_node != head_node:
                    lists.append(cur_node)
                    cur_node = cur_node.blocks[-1]
                else:
                    break
            for pointer in head_node.blocks[0:-1]:
                if pointer[column] < value:
                    lists.append(head_node)
                    break
        elif condition == '>':
            for pointer in head_node.blocks[0:-1]:
                if pointer[column] > value:
                    lists.append(head_node)
                    break
            while True:
                head_node = head_node.blocks[-1]
                if head_node != '':
                    lists.append(head_node)
                else:
                    break
        elif condition == '>=':
            for pointer in head_node.blocks[0:-1]:
                if pointer[column] >= value:
                    lists.append(head_node)
                    break
            while True:
                head_node = head_node.blocks[-1]
                if head_node != '':
                    lists.append(head_node)
                else:
                    break
        else:
            raise Exception("ERROR : Unknown Op.")

    else:
        while True:
            for pointer in first_leaf_node.blocks[0:-1]:
                if condition == '=':
                    if pointer[column] == value:
                        lists.append(first_leaf_node)
                        break
                elif condition == '<':
                    if pointer[column] < value:
                        lists.append(first_leaf_node)
                        break
                elif condition == '<=':
                    if pointer[column] <= value:
                        lists.append(first_leaf_node)
                        break
                elif condition == '>':
                    if pointer[column] > value:
                        lists.append(first_leaf_node)
                        break
                elif condition == '>=':
                    if pointer[column] >= value:
                        lists.append(first_leaf_node)
                        break
                elif condition == '<>' or condition == '!=':
                    if pointer[column] != value:
                        lists.append(first_leaf_node)
                        break
                else:
                    raise Exception("ERROR : Unknown Op.")
            if first_leaf_node.blocks[-1] == '':
                break
            first_leaf_node = first_leaf_node.blocks[-1]
    return lists