# encoding: utf-8
require 'sequel/adapters/jdbc'

Sequel::JDBC::DATABASE_SETUP[:access] = proc do |db|
  require 'sequel/adapters/jdbc/access'
  db.extend(Sequel::JDBC::Access::DatabaseMethods)
  com.hxtt.sql.access.AccessDriver
end