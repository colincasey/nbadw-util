# encoding: utf-8

require 'rubygems'
require 'nbadw/util/cli'

src = 'jdbc:access:////home/colin/master.mdb'
#src = 'jdbc:mysql://localhost/nb_aquatic_data_warehouse_dev?user=developer&password=developer'
dst = 'jdbc:postgresql://localhost/test?user=developer&password=developer'
#dst = 'jdbc:mysql://localhost/test?user=developer&password=developer'
#src = 'mysql://developer:developer@localhost/nb_aquatic_data_warehouse_dev'
#dst = 'postgres://developer:developer@localhost/test'

NBADW::Util::Cli.new([], :verify_data => true, :except => ["ef data before mm/100"]).copy(src, dst)

#db = Sequel.connect(src, :single_threaded => true)
#puts "connected"
##db.fetch("SELECT *, recno() FROM [AUXUSERDBNONRESTRICTEDACTIVITYIDS] WHERE BETWEEN (recno(), 1, 100)") do |row|
##  puts 'p'
##end
#
#db[:auxUserDBNonRestrictedActivityIds].limit(5, 0).each do |r|
#  r.inspect
#end
#
#db[:auxUserDBNonRestrictedActivityIds].limit(5, 5).each do |r|
#  r.inspect
#end
#
#puts 'done'