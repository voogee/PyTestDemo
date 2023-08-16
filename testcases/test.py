import pymongo as pymongo
import pymysql
from loguru import logger

class MongoToMysql:
    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, mysql_host, mysql_port, mysql_user,
                 mysql_password, mysql_db,table_name=None,set_max_length=False,batch_size=10000,table_description=''):
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_db = mysql_db
        self.table_name = table_name
        self.set_max_length = set_max_length
        self.batch_size = batch_size
        self.table_description = table_description
        self.data_types = self.get_mongo_data_types()
        self.create_mysql_table(self.data_types,set_max_length= self.set_max_length,table_description=self.table_description)
        self.push_data_to_mysql(self.batch_size)

    def get_mongo_data_types(self):
        logger.debug('正在获取mongo中字段的类型！')
        client = pymongo.MongoClient(host=self.mongo_host, port=self.mongo_port)
        db = client[self.mongo_db]
        db.authenticate('yapi', 'yapi123456')
        collection = db[self.mongo_collection]
        data_types = {}
        dict_temp = {}
        for i in collection.find():
            dict_temp.update(i)
        for i in dict_temp.keys():
            dict_temp[i] = type(dict_temp[i]).__name__
        return dict_temp


    def check_mysql_table_exists(self):
        logger.debug('检查是否存在该表，有则删之！')
        conn = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user,
                               password=self.mysql_password, db=self.mysql_db)
        cursor = conn.cursor()
        sql = f"DROP TABLE IF EXISTS {self.mongo_collection}"
        cursor.execute(sql)
        conn.commit()
        conn.close()

    def get_max_length(self, field):
        logger.debug(f'正在获取字段 {field} 最大长度......')
        client = pymongo.MongoClient(host=self.mongo_host, port=self.mongo_port)
        db = client[self.mongo_db]
        db.authenticate('yapi', 'yapi123456')
        collection = db[self.mongo_collection]
        max_length = 0
        fieldnew = str(field.split('`')[1])

        # for item in collection.find({}):
        #     w = str(item.get(fieldnew))
        #     if item.get(fieldnew) is not None and len(w)>max_length:
        #         max_length= len(w)

        for item in collection.find({},{fieldnew:1,'_id':0}):
            value = str(item.get(fieldnew))
            if isinstance(value, str) and len(value) > max_length:
                max_length = len(value)

        # print(fieldnew + "------" + str(max_length))
        return max_length

    def create_mysql_table(self, data_types,set_max_length,table_description):
        logger.debug('正在mysql中创建表结构！')
        self.check_mysql_table_exists()
        conn = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user,
                               password=self.mysql_password, db=self.mysql_db)
        cursor = conn.cursor()
        if self.table_name:
            table_name = self.table_name
        else:
            table_name = self.mongo_collection
        fields = []
        for field, data_type in data_types.items():
            field = "`" + field + "`"
            if data_type == 'int':
                fields.append(f"{field} INT")
            elif data_type == 'float':
                fields.append(f"{field} FLOAT")
            elif data_type == 'bool':
                fields.append(f"{field} BOOLEAN")
            else:
                if set_max_length:
                    fields.append(f"{field} TEXT)")
                else:
                    max_length = self.get_max_length(field)
                    fields.append(f"{field} VARCHAR({max_length + 200})")
        fields_str = ','.join(fields)
        sql = f"CREATE TABLE {table_name} (id INT PRIMARY KEY AUTO_INCREMENT,{fields_str}) COMMENT='{table_description}'"
        cursor.execute(sql)
        conn.commit()
        conn.close()


    def data_handle(self,data):
        par = ""
        if len(data) >0:
            for i in data:
                par = par + str(i.get("name")) + "=" +str(i.get("example")) + "&"
            return par[:-1]
        return ""


    def push_data_to_mysql(self, batch_size=10000):
        logger.debug('--- 正在准备从mongo中每次推送10000条数据到mysql ----')
        client = pymongo.MongoClient(host=self.mongo_host, port=self.mongo_port)
        db = client[self.mongo_db]
        db.authenticate('yapi', 'yapi123456')
        collection = db[self.mongo_collection]
        conn = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user,
                               password=self.mysql_password, db=self.mysql_db)
        cursor = conn.cursor()
        if self.table_name:
            table_name = self.table_name
        else:
            table_name = self.mongo_collection
        # table_name = self.mongo_collection
        data = []
        count = 0
        for item in collection.find():
            count += 1
            row = []
            for field, data_type in self.data_types.items():
                if field != "req_query":
                    value = item.get(field)
                    if value is None:
                        row.append(None)
                    elif data_type == 'int':
                        row.append(str(item.get(field, 0)))
                    elif data_type == 'float':
                        row.append(str(item.get(field, 0.0)))
                    elif data_type == 'bool':
                        row.append(int(item.get(field, False)))
                    else:
                        row.append(str(item.get(field, '')))
                else:
                    row.append(self.data_handle(item.get(field)))
            data.append(row)
            if count % batch_size == 0:
                placeholders = ','.join(['%s'] * len(data[0]))
                sql = f"INSERT INTO {table_name} VALUES (NULL,{placeholders})"
                cursor.executemany(sql, data)
                conn.commit()
                data = []
                logger.debug(f'--- 已完成推送：{count} 条数据！ ----')
        if data:

            placeholders = ','.join(['%s'] * len(data[0]))
            sql = f"INSERT INTO {table_name} VALUES (NULL,{placeholders})"
            cursor.executemany(sql, data)
            conn.commit()
            logger.debug(f'--- 已完成推送：{count} 条数据！ ----')
        conn.close()


if __name__ == '__main__':
    """MySQL"""
    mongo_host = '152.136.136.83'
    mongo_port = 27017
    mongo_db = 'yapi'
    mongo_collection = 'interface'
    """MongoDB"""
    mysql_host = '152.136.136.83'
    mysql_port = 3306
    mysql_user = 'ligang'
    mysql_password = 'test123456'
    mysql_db = 'db_test'

    table_description = ''  # 表描述

    mongo_to_mysql = MongoToMysql(mongo_host, mongo_port, mongo_db, mongo_collection, mysql_host, mysql_port,
                                  mysql_user, mysql_password, mysql_db,table_description=table_description)


