import json
import os
import IndexManager
import CatalogManager 

# tables={}
__last_leaf_pointer = ''

class node:
    def __init__(self, is_leaf, keys, blocks, parent = ''):
        self.is_leaf = is_leaf
        self.keys = keys
        self.blocks = blocks
        self.parent = parent

def i_load(_path):
    tables = IndexManager.tables_i
    with open(_path) as f:
        tables_r=json.loads(f.read())
        # print(tables_r)
    for table in tables_r.items():
        # if len(table[1]['keys']) == 0:
        #     tables[table[0]] = node(True, [], [])
        #     continue
        tables[table[0]] = node(table[1]['is_leaf'],table[1]['keys'],table[1]['blocks'])
        if tables[table[0]].is_leaf==False:
            tables[table[0]].blocks = recursive_load(table[1]['blocks'],tables[table[0]])
    return tables

def recursive_load(_blocks, _parent):#__lasg_leaf_pointer is what for?
    global __last_leaf_pointer
    lists = []
    for block in _blocks:
        _node = node(block['is_leaf'], block['keys'], block['blocks'], _parent)
        lists.append(_node)
        if _node.is_leaf==False:
            _node.blocks = recursive_load(block['blocks'], _node)
        else:
            if __last_leaf_pointer == '':
                __last_leaf_pointer = _node
            else:
                __last_leaf_pointer.blocks.append(_node)
                __last_leaf_pointer = _node
    return lists

def i_store(_path):
    tables_s = {}
    # print(IndexManager.tables_i)
    for table in IndexManager.tables_i.items():
        # print(table[1].blocks)
        tables_s[table[0]] = recursive_store(table[1])
        # print(0)
    with open (_path,'w') as f:
        json_tables = json.dumps(tables_s)
        f.write(json_tables)

def recursive_store(_node):
    snode = {}
    snode['is_leaf'] = _node.is_leaf
    snode['keys'] = _node.keys
    if _node.is_leaf and len(_node.blocks)!=0:
        # print(_node.blocks[-1]=='')
        if  _node.blocks[-1] != '':
            # print("''")
            snode['blocks'] = _node.blocks[0:-1]
        else :
            snode['blocks'] = _node.blocks
    elif _node.is_leaf and len(_node.keys)==0:
        snode['blocks'] = []
    else:
        snode['blocks'] = []
        for block in _node.blocks:
            snode['blocks'].append(recursive_store(block))
    return snode
    
def c_load(_path):
    tables = {}
    indexs = {}
    with open(os.path.join(_path, 'Tables_Catalog.sql')) as f:
        json_tables = json.loads(f.read())
        for table in json_tables.items():
            _table = CatalogManager.table_instance(table[0], table[1]['primary_key'])
            columns = []
            for _column in table[1]['columns'].items():
                columns.append(CatalogManager.column(_column[0], _column[1][0], _column[1][1], _column[1][2]))
            _table.columns = columns
            tables[table[0]] = _table
    with open(os.path.join(_path, 'Indexs_Catalog.sql')) as f:
        json_indexs = json.loads(f.read())
        for index in json_indexs.items():
            indexs[index[0]] = index[1]
    return (tables, indexs)

def c_store(_path):
    tables = CatalogManager.tables_c
    indexs = CatalogManager.indexs_c
    tables_s = {}
    for table in tables.items():
        _table = {}
        _table['primary_key'] = table[1].primary_key
        columns = {}
        for _column in table[1].columns:
            columns[_column.column_name] = [_column.is_unique, _column.type, _column.length]
        _table['columns'] = columns
        tables_s[table[0]] = _table
    json_tables = json.dumps(tables_s)
    with open(os.path.join(_path, 'Tables_Catalog.sql'),'w') as f:
        f.write(json_tables)
    with open(os.path.join(_path, 'Indexs_Catalog.sql'), 'w') as f:
        f.write(json.dumps(indexs))

# def __prints(table):
#     node = tables[table]
#     __do_print(node)

# def __do_print(node):
#     if node.is_leaf == True:
#         print('leaf')
#         print(node.keys)
#         print(node.blocks)
#         if node.parent != '':
#             print('parent:',node.parent.keys)
#     else:
#         print('node')
#         print(node.keys)
#         if node.parent != '':
#             print('parent:',node.parent.keys)
#         for i in node.blocks:
#             __do_print(i)


if __name__ == '__main__':
    i_load()
    # __prints('student')
