#!/usr/bin/env sh

echo "Preparing MySQL configs"
mysql -u root -e 'create database mysql_async_tests;'
mysql -u root -e "create table mysql_async_tests.transaction_test (id varchar(255) not null, primary key (id))"
mysql -u root -e "GRANT ALL PRIVILEGES ON *.* TO 'mysql_async'@'localhost' IDENTIFIED BY 'root' WITH GRANT OPTION";
mysql -u root -e "GRANT ALL PRIVILEGES ON *.* TO 'mysql_async_old'@'localhost' WITH GRANT OPTION";
mysql -u root -e "UPDATE mysql.user SET Password = OLD_PASSWORD('do_not_use_this'), plugin = 'mysql_old_password' where User = 'mysql_async_old'; flush privileges;";
mysql -u root -e "GRANT ALL PRIVILEGES ON *.* TO 'mysql_async_nopw'@'localhost' WITH GRANT OPTION";

echo "preparing postgresql configs"

PGDATA=/etc/postgresql/9.1/main
PGUSER=postgres
SCRIPTSDIR=`dirname $0`

psql -d "postgres" -c 'create database netty_driver_test;' -U $PGUSER
psql -d "postgres" -c 'create database netty_driver_time_test;' -U $PGUSER
psql -d "postgres" -c "alter database netty_driver_time_test set timezone to 'GMT'" -U $PGUSER
psql -d "netty_driver_test" -c "create table transaction_test ( id varchar(255) not null, constraint id_unique primary key (id))" -U $PGUSER
psql -d "postgres" -c "CREATE USER postgres_md5 WITH PASSWORD 'postgres_md5'; GRANT ALL PRIVILEGES ON DATABASE netty_driver_test to postgres_md5;" -U $PGUSER
psql -d "postgres" -c "CREATE USER postgres_cleartext WITH PASSWORD 'postgres_cleartext'; GRANT ALL PRIVILEGES ON DATABASE netty_driver_test to postgres_cleartext;" -U $PGUSER
psql -d "postgres" -c "CREATE USER postgres_kerberos WITH PASSWORD 'postgres_kerberos'; GRANT ALL PRIVILEGES ON DATABASE netty_driver_test to postgres_kerberos;" -U $PGUSER
psql -d "netty_driver_test" -c "CREATE TYPE example_mood AS ENUM ('sad', 'ok', 'happy');" -U $PGUSER

sudo chmod 666 $PGDATA/pg_hba.conf

echo "pg_hba.conf goes as follows"
cat "$PGDATA/pg_hba.conf"

sudo echo "local    all             all                                     trust"    >  $PGDATA/pg_hba.conf
sudo echo "host     all             postgres           127.0.0.1/32         trust"    >> $PGDATA/pg_hba.conf
sudo echo "host     all             postgres_md5       127.0.0.1/32         md5"      >> $PGDATA/pg_hba.conf
sudo echo "host     all             postgres_cleartext 127.0.0.1/32         password" >> $PGDATA/pg_hba.conf
sudo echo "host     all             postgres_kerberos  127.0.0.1/32         krb5"     >> $PGDATA/pg_hba.conf

echo "pg_hba.conf is now like"
cat "$PGDATA/pg_hba.conf"

sudo chmod 600 $PGDATA/pg_hba.conf

sudo chmod 666 $PGDATA/postgresql.conf

echo "previous certificate"

sudo chmod 666 /var/ramfs/postgresql/9.1/main/server.crt /var/ramfs/postgresql/9.1/main/server.key
sudo cat "/var/ramfs/postgresql/9.1/main/server.crt"
sudo cp $SCRIPTSDIR/server.crt /var/ramfs/postgresql/9.1/main/
sudo cp $SCRIPTSDIR/server.key /var/ramfs/postgresql/9.1/main/
sudo chown $PGUSER /var/ramfs/postgresql/9.1/main/server.crt /var/ramfs/postgresql/9.1/main/server.key
sudo chmod 600 /var/ramfs/postgresql/9.1/main/server.crt /var/ramfs/postgresql/9.1/main/server.key

sudo /etc/init.d/postgresql restart
