require 'sequel'
require 'sequel/extensions/schema_dumper'
require 'sequel/extensions/migration'
require 'nbadw/util/progress_bar'
require 'sequel/schema_dumper_patch'
if defined?(JRUBY_VERSION)
  require 'sequel/jdbc_access_adapter'
end

module NBADW
  module Util
    class CopyDatabaseTask
      attr_reader :source, :destination, :page_size, :except
      
      def initialize(src, dest, options = {})
        @source = Sequel.connect(src, :single_threaded => true)
        @destination = Sequel.connect(dest, :single_threaded => true)
        @page_size = options[:page_size] || :unlimited
        @verify_data = !!options[:verify_data]
        @except = options[:except] || []
      end

      def self.start(src, dest, options = {})
        print "Initializing copy operation"
        task = new(src, dest, options)
        begin
          task.copy
        rescue Exception => e
          puts "...fail!!!"
          puts "Reason: #{e.message}"
          raise e
        end
      end
      
      def copy
        puts "..."
        puts "#{source.tables.length} tables, #{format_number(total_records(source))} records"
        copy_schema
        copy_data
        copy_indexes
        verify_data if verify_data?
        puts "...copy completed"
      end

      def copy_schema
        begin
          run_callback :before_copy_schema
        
          tables = source.tables
          progress = ProgressBar.new("Schema copy", tables.length)

          tables.each do |t|
            next if except.include?(t.to_s)
            args = { :table => t, :schema => source.dump_table_schema(t.to_sym, :indexes => false) }
            run_callback :before_create_table, args
            migration = "Class.new(Sequel::Migration) do \n def up \n #{args[:schema]} \n end \n end"
            eval(migration).apply(destination, :up)
            run_callback :after_create_table, args
            progress.inc(1)
          end

          run_callback :after_copy_schema
        ensure
          progress.finish if progress
        end
      end

      def copy_data
        run_callback :before_copy_data

        progress = ProgressBar.new("Data copy", source.tables.size)
        begin
          source.tables.each do |table_name|
            next if except.include?(table_name.to_s)
            src_table = source[table_name.to_sym]
            dst_table = destination[table_name.to_sym]
            args = { :table => table_name }
            page_size == :unlimited ? copy_table_without_limit(src_table, dst_table, args) : copy_table_with_limit(src_table, dst_table, args)
            progress.inc(1)
          end
        ensure
          progress.finish
        end

        run_callback :after_copy_data
      end

      def copy_table_without_limit(src_table, dst_table, args = {})
        src_table.each do |row|
          args.merge!({ :row => row })
          run_callback :before_copy_row, args
          dst_table.insert(row)
          run_callback :after_copy_row, args
        end
      end

      def copy_table_with_limit(src_table, dst_table, args = {})
        count = src_table.count
        offset = 0
        while(offset < count) do
          rows = src_table.limit(page_size, offset).all
          rows.each_with_index do |row, i|
            args.merge!({ :row => row, :index => i, :offset => offset })
            run_callback :before_copy_row, args
            dst_table.insert(row)
            run_callback :after_copy_row, args
          end
          offset += rows.size
        end
      end

      def copy_indexes
        begin
          run_callback :before_copy_indexes

          tables = source.tables
          progress = ProgressBar.new("Index copy", tables.length)

          tables.each do |t|
            next if except.include?(t.to_s)
            args = { :table => t, :indexes => source.send(:dump_table_indexes, t.to_sym, :add_index) }
            run_callback :before_add_indexes, args
            migration = "Class.new(Sequel::Migration) do \n def up \n #{args[:indexes]} \n end \n end"
            eval(migration).apply(destination, :up)
            run_callback :after_add_indexes, args
            progress.inc(1)
          end

          run_callback :after_copy_indexes
        ensure
          progress.finish if progress
        end
      end

      def verify_data
        tables = source.tables
        progress = ProgressBar.new("Verify data", tables.length)
        begin
          tables.each do |table_name|
            next if except.include?(table_name.to_s)
            src_table = source[table_name.to_sym]
            dst_table = destination[table_name.to_sym]
            page_size == :unlimited ? verify_table_without_limit(table_name, src_table, dst_table) : verify_table_with_limit(table_name, src_table, dst_table)
            progress.inc(1)
          end
        ensure
          progress.finish if progress
        end
      end

      def verify_table_without_limit(table_name, src_table, dst_table)
        src_table.each do |row|
          row_found = dst_table.filter(row).first
          raise "no matching row found in #{table_name} for #{row.inspect}" unless row_found
          verify_row(table_name, row, row_found)
        end
      end

      def verify_table_with_limit(table_name, src_table, dst_table)
        count = src_table.count
        offset = 0
        while(offset < count) do
          rows = src_table.limit(page_size, offset).all
          rows.each do |row|
            row_found = dst_table.filter(row).first
            raise "no matching row found in #{table_name} for #{row.inspect}" unless row_found
            verify_row(table_name, row, row_found)
          end
          offset += rows.length
        end
      end

      def verify_row(table_name, row1, row2)
        diff = {}
        row1.each do |col, val|
          eql = case val
          when Time then (val - row1[col]).abs < 1  # time fields are sometimes off by very miniscule fractions
          else           val == row1[col]
          end
          diff[col] = "#{val}, #{row2[col]}" unless eql
        end
        raise "row does not match exactly - expected #{row1.inspect}, but was #{row2.inspect} - in table #{table_name}, diff #{diff.inspect}" unless diff.empty?
      end

      def verify_data?
        @verify_data
      end

      def total_records(db)
        db.tables.inject(0) { |total, table_name| total += db[table_name.to_sym].count }
      end

      def format_number(num)
        num.to_s.gsub(/(\d)(?=(\d\d\d)+(?!\d))/, "\\1,")
      end

      # the following is a callback system that helps to handle slight
      # differences when copying between database types
      class << self
        def callbacks
          @callbacks ||= []
        end

        def before(callback, opts = {}, &block)
          add_callback(:before, callback, opts, &block)
        end

        def after(callback, opts = {}, &block)
          add_callback(:after, callback, opts, &block)
        end

        def add_callback(type, callback, opts, &block)
          callback_config = {
            :type     => type,
            :callback => callback,
            :adapter  => opts[:adapter] || :all,
            :for      => opts[:for],
            :logic    => block
          }
          callbacks << callback_config
        end
      end

      # prevent MySQL from changing '0' values on insert since we'd like an exact copy
      before :copy_schema, :adapter => :mysql, :for => :destination do |src, dst, args|
        dst.run("SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';")
      end

      # fix to catch schema dumps for PostgreSQL which set an invalid boolean default
      before :create_table, :adapter => :postgres, :for => :destination do |src, dst, args|
        schema = args[:schema]
        schema = schema.split("\n").collect do |line|
          if line.match(/TrueClass/)
            line = line.sub(/:default=>(\d)/) { |match| ":default=>#{$1 == '0' ? 'true' : 'false'}"  }
          end
          line
        end.join("\n")
        args[:schema] = schema
      end

      # this fixes the string as primary keys
      before :create_table, :adapter => :access, :for => :source do |src, dst, args|
        table =  args[:table].to_s
        pks = src.schema(args[:table]).collect do |col_schema|
          col, opts = col_schema
          opts[:primary_key] ? col_schema : nil
        end.compact

        if pks.size == 1 && pks[0][1][:type] == :string
          col, opts = pks[0]
          schema = args[:schema]
          schema = schema.split("\n").collect do |line|
            line = "  String :#{col}, :size=>#{opts[:column_size] / 2}, :null=>false" if line.match(/primary_key/)
            line = "  primary_key [:#{col}]\nend" if line.match(/^end/)
            line
          end.join("\n")
          args[:schema] = schema
        end
      end

      # When copying from access, convert all BigDecimal columns to Float or lose precision!
      before :create_table, :adapter => :access, :for => :source do |src, dst, args|
        args[:schema] = args[:schema].gsub(/BigDecimal/, 'Float')
      end

      # determines which callbacks to run (is this needlessly complex?)
      def run_callback(full_callback, args = {})
        full_callback.to_s.match(/(before|after)_(.*)/)
        type, callback = $1.to_sym, $2.to_sym
        CopyDatabaseTask.callbacks.each do |callback_config|
          if callback_config[:type] == type && callback_config[:callback] == callback # callback matches
            # which adapters should we check against?
            adapters = [:all] # always check for all...
            if callback_config[:for] == :destination # only destination?
              adapters << destination.database_type.to_sym
            elsif callback_config[:for] == :source   # only source?
              adapters << source.database_type.to_sym
            else                                     # or both?
              adapters << destination.database_type.to_sym
              adapters << source.database_type.to_sym
            end
            # if the adapter matches, run the callback
            if adapters.include?(callback_config[:adapter])
              callback_config[:logic].call(source, destination, args)
            end
          end
        end
      end
    end # CopyDatabaseTask
  end # Util
end # NBADW
