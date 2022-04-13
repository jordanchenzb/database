import CatalogManager
import IndexManager
import time
import re

__root = True
def select(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    # lists = args.split(' ')
    start_from = re.search('from', args).start()
    end_from = re.search('from', args).end()
    columns = args[0:start_from].strip()
    if re.search('where', args):
        start_where = re.search('where', args).start()
        end_where = re.search('where', args).end()
        table = args[end_from+1:start_where].strip()
        conditions = args[end_where+1:].strip()
    else:
        table = args[end_from+1:].strip()
        conditions = ''
    CatalogManager.not_exists_table(table)
    # print('exist_finished')
    CatalogManager.check_select_statement(table,conditions,columns)
    # print('check_finished')
    IndexManager.select_from_table(table,conditions,columns)
    time_end = time.time()
    print(" time elapsed : %fs." % (time_end-time_start))
    print()


def create(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    if lists[0] == 'table':
        start_on = re.search('table', args).end()
        start = re.search('\(', args).start()
        end = find_last(args,')')
        table = args[start_on:start].strip()
        statement = args[start + 1:end].strip()
        CatalogManager.exists_table(table)
        IndexManager.create_table(table)
        CatalogManager.create_table(table,statement)

    elif lists[0] == 'index':
        index_name = lists[1]
        if lists[2] != 'on':
            raise Exception("ERROR : Unkown syntax for command 'create index',try 'on' after it.")
        start_on = re.search('on',args).start()
        start = re.search('\(',args).start()
        end = find_last(args, ')')
        table = args[start_on:start].strip()
        column = args[start+1:end].strip()
        CatalogManager.exists_index(index_name)
        CatalogManager.create_index(index_name,table,column)
        IndexManager.create_index(index_name,table,column)

    else:
        raise Exception("ERROR : Unrknown object for command 'create',try 'table' or 'index'.")
    time_end = time.time()
    print("Qeury OK. Table '%s' created , time elapsed : %fs."
          % (table,time_end - time_start))

def drop(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    if args[0:5] == 'table':
        table = args[6:].strip()
        if table == 'sys':
            raise Exception("ERROR : Delete operation to 'sys' is not permitted. ")
        CatalogManager.not_exists_table(table)
        CatalogManager.drop_table(table)
        IndexManager.delete_table(table)
        time_end = time.time()
        print("Query OK. Table '%s' deleted, time elapsed : %fs." % (table,time_end - time_start))

    elif args[0:5] == 'index':
        index = args[6:].strip()
        CatalogManager.not_exists_index(index)
        CatalogManager.drop_index(index)

    else:
        raise Exception("ERROR : Unkown object for command 'drop', try 'table' or 'index'.")


def insert(args):
    # print(1)
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    if lists[0] != 'into':
        raise Exception("ERROR : Unknown command for 'insert', try 'into'.")
    table = lists[1]
    if table == 'sys' and __root  == False:
        raise Exception("ERROR : Modification to 'sys' is forbidden , you are not root.")
    if not re.search('values', args):
        raise Exception("ERROR : Unknown command for 'insert', try 'values'.")
    if re.search('\(',args):
        value = args[re.search('\(',args).start()+1:find_last(args,')')]
    else:
        raise Exception("ERROR : Missing values to insert")
    values = value.split(',')
    # print(2)
    CatalogManager.not_exists_table(table)
    # print('not_exist finished')
    CatalogManager.check_types_of_table(table,values)
    # print('check finished')
    IndexManager.insert_into_table(table,values)
    time_end = time.time()
    print('Query OK. 1 row affected.')
    print(" time elapsed : %fs." % (time_end-time_start))

def delete(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    if lists[0] != 'from':
        raise Exception("ERROR : Unknown object for command 'delete',try 'from'.")
    table = lists[1]
    if table == 'sys' and __root  == False:
        raise Exception("ERROR : Can't modify 'sys' table, you are not root.")
    CatalogManager.not_exists_table(table)
    if len(lists) == 2:
        IndexManager.delete_from_table(table,[])
    else:
        IndexManager.delete_from_table(table,lists[3:])
    time_end = time.time()
    print(" time elapsed : %fs." % (time_end-time_start))

def find_last(string,str):
    last_position=-1
    while True:
        position=string.find(str,last_position+1)
        if position==-1:
            return last_position
        last_position=position


def process(comand):
        if comand == '':
            return
        if comand[0] == '#':
            return
        if comand.split(' ')[0].replace('\u200b','') == 'insert':
            try:
                insert(comand[6:])
                # f.write(comand+'\n')
            except Exception as e:
                print(str(e))
        elif comand.split(' ')[0] == 'select':
            try:
                select(comand[6:])
            except Exception as e:
                print(str(e))
        elif comand.split(' ')[0] == 'delete':
            try:
                delete(comand[6:])
            except Exception as e:
                print(str(e))
        elif comand.split(' ')[0] == 'drop':
            try:
                drop(comand[4:])
            except Exception as e:
                print(str(e))
        elif comand.split(' ')[0] == 'create':
            try:
                create(comand[6:])
            except Exception as e:
                print(str(e))
        # __finalize__()
        
        return comand.split(' ')[3]

def exec_from_file(args):
    time_start = time.time()
    input = args.split(' ')
    filename = input[0]
    if len(input)==3:
        
        start = int(input[1])
        end = int(input [2])
    elif len(input)==1:
        start = 0
        end = -1
    else:
        raise Exception("ERROR : Syntax error")
    f = open(filename)
    text = f.read()
    f.close()
    comands = text.split(';')
    if end>=0:
        comands = [i.strip().replace('\n','') for i in comands][start:end]
    else:
        comands = [i.strip().replace('\n','') for i in comands][start:]
    
    p = [process(comand) for comand in comands]
    
    count = 0
    
    # __finalize__()
    time_end = time.time()
    # print('insert %d '%count)
    print("In read file total time elapsed : %fs." % (time_end - time_start))

