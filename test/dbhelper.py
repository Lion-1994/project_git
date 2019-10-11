"""
db.field('sname,ssex,sclass).where(ssex='男').orderby('sname desc).select()
db.where(ssex='男').orderby('sname desc).field('sname,ssex,sclass).select()
"""

import os

import pymysql

import settings

class Dbhelper:
    def __init__(self,table):
        self.table = table  # 表名
        self.conn = pymysql.Connect(**settings.parameters) #连接数据库
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)  #游标
        self.params = self.__init_param()   #初始化字典参数
        self.__cache_fields()

    def __del__(self):
        self.cursor.close()
        self.conn.close()
    # 生成缓存字段
    def __cache_fields(self):
        # 判断缓存目录是否存在
        if not os.path.exists('cache'):
            os.mkdir('cache')
        path = "cache/" + self.table   #缓存文件的路径
        if os.path.exists(path):
            with open(path) as fp:
                self.params['fields'] = fp.read()
        else: #缓存文件不存在
            sql = "desc " + self.table
            res = self.cursor.execute(sql)
            if res>0:
                records = self.cursor.fetchall()
                value = ''
                for rec in records:
                    # print(rec['Field'])
                    value += rec['Field'] + ','
                # print(records)
                value = value.rstrip(',')
                with open(path,'w') as fp:
                    fp.write(value)
                self.params['fields'] = value
    def __init_param(self):
         return  {
                'fields': '*',
                'table': self.table,
                'limit': '',
                'where': '',
                'groupby': '',
                'having': '',
                'orderby': ''
            }
    # 条件之间是逻辑与的关系
    def where(self,**kwargs):
        """
        where:" where uid=1"
        :param args: {'uid':2,username:'tom',password:'dddddd}
                     {'uid__nin':2}  uid  gt  2
        :return:
        """
        if len(kwargs) <= 0:
            return self
        self.__add_quote(kwargs)
        ops = {  #运算符对照表
            'gt':'>',
            'ge':'>=',
            'lt':'<',
            'le':'<=',
            'ne':'!=',
            'in':'in',
            'nin':'not in'

        }
        # print(kwargs)
        for key in kwargs:
            keys = key.split('__')
            if len(keys)>1: #运算符不是等号
                op = keys[1]  #运算符
                if self.params['where']:
                    self.params['where'] += " and " + keys[0] + ops[op] + kwargs[key]
                else:
                    self.params['where'] = " where " + keys[0] + ops[op] + kwargs[key]
            else:
                if self.params['where']:
                    self.params['where'] += " and " + keys[0] + '=' + kwargs[key]
                else:
                    self.params['where'] = " where " + keys[0] + '=' + kwargs[key]


        # if self.params['where']:  # where有值
        #     self.params['where'] += " and " + " and ".join([key +"="+ value for key,value in kwargs.items()])
        # else:  # where没有值
        #     self.params['where'] = " where " +" and ".join([key +"="+ value for key,value in kwargs.items()])
        print(self.params)
        return self
    def __add_quote(self,data):
        """

        :param data: 参数字典
        :return:
        """
        for key in data:
            if isinstance(data[key],str): # 是字符串两边添加单引号
                data[key] = "'" + pymysql.escape_string(data[key]) + "'"
            else: # 不是字符串，转换为字符串
                data[key] = str(data[key])
        return self
    # 条件之间是逻辑或的关系
    def whereor(self,*args):
        print('where')
        self.params['where'] = 'Where ssex="男"'
        return self
    def orderby(self,*args):
        if len(args) <=0:
            return self
        self.params['orderby'] = ' order by ' + ','.join(args)
        return self
    def limit(self,*args):
        if len(args)<=0:
            return self
        my_args = [ str(value) for value in args]
        self.params['limit'] = " limit " + ','.join(my_args)
        return  self
    def groupby(self,*args):
        if len(args) <=0:
            return self
        self.params['groupby'] = " group by " +",".join(args)
        return self
    def having(self,**kwargs):
        if len(kwargs) <= 0:
            return self
        self.__add_quote(kwargs)
        # print(kwargs)
        if self.params['having']:  # having有值
            self.params['having'] += " and " + " and ".join([key +"="+ value for key,value in kwargs.items()])
        else:  # where没有值
            self.params['having'] = " having " +" and ".join([key +"="+ value for key,value in kwargs.items()])
        # print(self.params)
        return self
    def  fields(self,value):
        """

        :param value: 形如：'sid,name,sex'
        :return:
        """
        value = value.strip()
        if not value:
            return self
        self.params['fields'] = str(value)
        return self

    def select(self):
        sql = "SELECT {fields} FROM {table} {where}  {groupby} {having} {orderby} {limit}"
        sql = sql.format(**self.params)
        print(sql)
        return self.query(sql)
    def query(self,sql):
        self.params = self.__init_param() # 参数字典初始化
        try:
            res = self.cursor.execute(sql)
            if res > 0:
                return  self.cursor.fetchall()
            else:
                return None
        except Exception as e:
            print(e)
            return None
        return None

    # insert 一条就记录
    def insert(self, data):
        """

        :param data: 字典，代表一条记录，键是字段名
        :return:
        """
        # 1 如果字典的值是字符串，两边添加单引号
        self.__add_quote(data)

        # 2.生成字段列表和值列表
        keys = ''
        values = ''
        for key, value in data.items():
            keys += key + ','
            values += value + ','
        keys = keys.rstrip(',')
        values = values.rstrip(',')
        self.params['fields'] = keys
        self.params['value'] = values

        sql = "INSERT INTO {table} ({fields})  VALUES({value})".format(**self.params)
        print(sql)
        return self.execute(sql)

    def update(self, data):
        """

        :param data: 字典
        :return:
        """
        self.__add_quote(data)
        self.params['value'] = ','.join([key + "=" + value for key, value in data.items()])
        sql = "UPDATE {table} SET {value} {where}".format(**self.params)
        return self.execute(sql)

    def delete(self):
        sql = "DELETE FROM {table} {where}".format(**self.params)
        return self.execute(sql)

    def execute(self, sql):
        self.sql = sql
        self.__init_param()
        try:
            res = self.cursor.execute(sql)
            if res > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print(e)
            self.conn.rollback()
            return False
if __name__ == "__main__":
    db = Dbhelper('student')
    # print(db.select())
    # db.where(username='tom',password='123').where(uid=2)
    # db.orderby('username asc','uid desc').select()
    # db.limit(5)
    # data = db.limit(0,1).select()
    # db.limit('2')
    # data = db.fields('sex ,count(*) ').groupby('sex').select()
    # print(data)
    # data = db.where(sid__ne=2).orderby('sid desc').select()  #{'sid__ne':2}
    # print(data)
    # db.insert({'name':'hello1','sex':'男'})
    # db.where(sid=9).delete()
    # db.where(sid=8).update({'gid':30})

    # db.query("")