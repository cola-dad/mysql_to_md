#### 背景
开发过程中需要整理数据库设计文档，上传到svn，因为编写markdown文档时，基本上都是复制粘贴，并且开发过程中数据库表结构还会变动，维护起来比较麻烦。因此开发此工具，一键生成数据库设计文档。
该工具直接读取数据库里面的表结构来生成文档，如果数据库里的表结构字段类型和注释不完善，需要自己去修改。
目前已支持的数据库类型有：mysql、clickhouse

#### 使用

```shell
# 帮助函数
➜  go run main.go -h
flag needs an argument: -h
Usage: mysql_to_md [options...]
--help  This help text
-s      dial select.     default mysql
-h      host.     default 127.0.0.1
-u      username. default root
-p      password. default root
-d      database. default mysql
-P      port.     default 3306
-c      charset.  default utf8
-o      output.   default current location
-t      tables.   default all table and support ',' separator for filter, every item can use regexp
```
#### 简单使用
 - 导出数据库类型为mysql，test数据库所有表的md文档
```
go run main.go -h 127.0.0.1 -u root -p 123456 -d test -P 3306
```
- 导出数据库类型为clickhouse,test数据库的所有表的md文档
```
 go run main.go -h 127.0.0.1 -u root -p 123456 -d test -P 10009 -s clickhouse
```