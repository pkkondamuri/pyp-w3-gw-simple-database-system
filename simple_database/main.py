import os
from simple_database.config import BASE_DB_FILE_PATH
from simple_database.exceptions import ValidationError
import datetime


class Database(object):
    def __init__(self,name):
        self.path = os.path.join(BASE_DB_FILE_PATH,name)
        if not os.path.exists(self.path):
            os.makedirs(self.path)
    
    def create_table(self,table_name,columns = []):
        table = Table(os.path.join(self.path,table_name),columns)
        setattr(self,table_name,table)
    
    def show_tables(self):
        
        return os.listdir(self.path)

class Table(object):
    def __init__(self,name,columns):
        self.name = name 
        if not os.path.exists(name):
            
            self.columns = columns
            with open(self.name, mode ='w') as fp:
                fp.write(str(self.columns))
        else:
            with open(self.name) as fp:
                self.columns = eval(fp.readline())
        
    def describe(self):
        with open(self.name) as fp:
            return eval(fp.readline())
    
    def insert(self,*args):
        if len(args) != len(self.columns):
            raise ValidationError ("Invalid amount of field")
        for ind in range(len(args)):
            if type(args[ind]).__name__ != self.columns[ind]['type']:
                raise ValidationError('Invalid type of field "{0}": Given "{1}", expected "{2}"'.format(self.columns[ind]['name'],type(args[ind]).__name__,self.columns[ind]['type']))
        with open(self.name, mode ='a') as fp:
            fp.write("\n"+(str(args)))
            
    def count(self):
        with open(self.name) as fp:
            
            cnt = 0
            for line in fp:
                print (line)
                cnt += 1
            return cnt-1
            
    def query(self,**kwargs):

        with open(self.name) as fp:
            next(fp)
            for line in fp:
                entry = eval(line)
                for ind in range(len(entry)):
                    setattr(self,self.columns[ind]['name'],entry[ind])
                nomatch=0
                for key in kwargs:
                    if getattr(self, key) != kwargs[key]:
                        nomatch += 1
                        key = None
                if not nomatch:
                    yield self
    def all(self):
        with open(self.name) as fp:
            next(fp)
            for line in fp:
                entry = eval(line)
                for ind in range(len(entry)):
                    setattr(self,self.columns[ind]['name'],entry[ind])
                yield self

def create_database(db_name):
    path = os.path.join(BASE_DB_FILE_PATH,db_name)
    if os.path.exists(path):
        raise ValidationError('Database with name "{}" already exists.'.format(db_name))
    else:
        db = Database(db_name)
        return db
    
def connect_database(db_name):
    db = Database(db_name)
    for table in db.show_tables():
        path = os.path.join(BASE_DB_FILE_PATH,db_name,table)
        setattr(db,table,Table(path,[]))
    return db