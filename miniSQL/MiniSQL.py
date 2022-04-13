from cmd import Cmd
import API
import CatalogManager
import IndexManager
import sys
import time
import os
# from multiprocessing.dummy import Pool as ThreadPool
# from numba import jit


class miniSQL(Cmd):
    def __init__(self):
        Cmd.__init__(self)
        self.prompt = '\n' + '(%s)' % sys.argv[2] + 'MiniSQL > '
        self.intro = "Welcome to the MiniSQL Server.\n"+\
                "Server version: 1.0.0\n\n"+\
                "Copyright 2021 @ J0RDAN with his patner from ZJU. \n"\
                "Tpye 'help' or 'help name' for help \n"
    
    def do_readfile(self, args):
        try:
            API.exec_from_file(args)
            # __finalize__()
        except Exception as e:
            print(str(e))
    
    def do_select(self,args):
        # API.select(args.replace(';', ''))
        try:
            API.select(args.replace(';',''))
        except Exception as e:
            print(str(e))

    def do_create(self,args):
        try:
            API.create(args.replace(';',''))
        except Exception as e:
            print(str(e))

    def do_drop(self,args):
        try:
            API.drop(args.replace(';',''))
        except Exception as e:
            print(str(e))

    def do_insert(self,args):
        try:
            API.insert(args.replace(';',''))
        except Exception as e:
            print(str(e))

    def do_delete(self,args):
        try:
            API.delete(args.replace(';',''))
        except Exception as e:
            print(str(e))

    def do_commit(self,args):
        time_start = time.time()
        __finalize__()
        time_end = time.time()
        print('Modifications has been commited to local files,',end='')
        print(" time elapsed : %fs." % (time_end - time_start))

    def do_quit(self,args):
        __finalize__()
        print('Goodbye.')
        sys.exit()

    def emptyline(self):
        pass

    def default(self, line):
        print('ERROR : Unrecognized command : %s' % line)

    def help_commit(self):
        print()
        text = "The server will not automatically write changes to files until quit.\n \
                If unfortunately exit the server unnormally, everything will be lost.\n \
                Type 'commit' to update the local files\n"
        print(text)

    def help_quit(self):
        print()
        print('Quit the program and write changes to local file.')

    def help_select(self):
        print()
        print("select * from student;")
        print("select num from student where num >= 2 and num < 10 and gender = 'male';")

    def help_create(self):
        print()
        print("create table student (ID int, name char(10), gender char(10), enroll_date char(10), primary key(ID));")
        print("create table test_s (s int, ss char (20), sss char(30) unique, primary key (ss), unique (s))")

    def help_drop(self):
        print()
        print("drop table student;")

    def help_insert(self):
        print("insert into student values ( 1,'Alan','male','2017.9.1');")

    def help_delete(self):
        print()
        print("delete from students")
        print("delete from student where enroll_date = '2009.10.2';")
    def help_readfile(self):
        print()
        print("readfile [file_path]")


def __initialize__():
    CatalogManager.__initialize__(os.getcwd())
    IndexManager.__initialize__(os.getcwd())

def __finalize__():
    CatalogManager.__finalize__()
    IndexManager.__finalize__()

if __name__ == '__main__':
    errortext = \
"Login syntax :\n"+\
"(python) MiniSQL -u [username] -p [password] \n"+\
"Login operators : \n"+\
"-u username\tusername for MiniSQL.\n"+\
"-p password\tpassword for MiniSQL.\n"

    if len(sys.argv) < 5:
        print('ERROR : Unsupported syntax, please login.\n',errortext)
        sys.exit()
    
    if sys.argv[1] != '-u' or sys.argv[3] != '-p':
        print('ERROR : Unsupported syntax, please login.\n',errortext)
        sys.exit()
    __initialize__()
    if sys.argv[2] == 'root' and sys.argv[4] == '123456':
        API.__root = True
    elif IndexManager.exist_user(username=sys.argv[2],password=sys.argv[4]):
        API.__root = False
    else:
        print('ERROR : Username or password incorrect , check and login again.\n',errortext)
        sys.exit()
    
    try:
        # os.system('cls')
        msql = miniSQL()
        # miniSQL.prompt = '\n' + '(%s)' % sys.argv[2] + 'MiniSQL > '
        miniSQL().cmdloop()
    except:
        exit()
