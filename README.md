### How to run example
Run this class ```org.example.DirectUseOfJdbcRelation```
```shell
Setup followign env variable ot run examples
database=my_db
table=my_table 
password=my_password (we assume the user name is root - you can change the code)
```

Also create the table with following:
```sql
CREATE TABLE `my_table` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

```