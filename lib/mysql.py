#-- coding:utf-8 --
import json
import pymysql

# Database返回数据库连接
# 描述了一个db的处理器，将db中的符合条件的表（有主键）提取出来，写到redis中去

class Mysql():
    def __init__(self,host,user,password,port,charactor,dblist):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.dblist = list(dblist)
        self.charset = charactor

    def db_connect(self,db):
        try:
            self.conn = pymysql.connect(
                host = self.host,
                user = self.user,
                database = db,
                password = self.password,
                charset = self.charset,
            )
        except Exception as e:
            print('mysql__db_connect error :',e)           
            exit()

        return self.conn

    def __close(self):
        self.conn.close()

    def db_select(self,dbconnect,sql):
        cursor = dbconnect.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

    def db_update(self):
        pass

    def db_insert(self):
        pass

    def db_delete(self):
        pass


    def get_db_table_column_info(self,excludeTable='',includeTable=['all']):
        '''
            查询information_schema.COLUMNS得到有主键的表，然后剔除配置中要排除的表，再获取表的字段
            有主键的表的各个字段 
        '''
        #print(excludeTable,includeTable)
        db_table_column_info = {}
        for db in self.dblist:
            db_table_column_info[db] = {}
            conn = self.db_connect(db)
            if conn:
                print('mysql connect success...')
                table_sql = 'select TABLE_NAME from information_schema.COLUMNS where COLUMN_KEY = \'PRI\' AND TABLE_SCHEMA = \'{0}\';'.format(db)
                table_in_db = self.db_select(conn,table_sql)
                for table in table_in_db:
                    table = table[0]
                    # 根据配置文件过滤
                    if includeTable[0] == 'all':
                        pass
                    elif table not in includeTable:
                        continue

                    if table in excludeTable:
                        continue

                    db_table_column_info[db][table] = {}
                    table_create_sql = 'select COLUMN_NAME from information_schema.COLUMNS where TABLE_NAME = \'{0}\';'.format(table)
                    table_pri_sql = 'select COLUMN_NAME from information_schema.COLUMNS where TABLE_NAME = \'{0}\' and COLUMN_KEY = \'PRI\';'.format(table)
                    table_create_result =  self.db_select(conn,table_create_sql)
                    table_pri_result = self.db_select(conn,table_pri_sql)
                    table_columns = []
                    for column in table_create_result:
                        table_columns.append(column[0])
                    db_table_column_info[db][table]['prikey'] = table_pri_result[0][0]
                    db_table_column_info[db][table]['columns'] = table_columns
        db_table_column_info = json.dumps(db_table_column_info)
        self.__close()
        return db_table_column_info