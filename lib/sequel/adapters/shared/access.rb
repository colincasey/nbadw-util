module Sequel
  module Access
    module DatabaseMethods
      AUTO_INCREMENT = 'COUNTER(1,1)'.freeze
      SERVER_VERSION_RE = /^(\d+)\.(\d+)\.(\d+)/.freeze
      SQL_BEGIN = "BEGIN TRANSACTION".freeze
      SQL_COMMIT = "COMMIT TRANSACTION".freeze
      SQL_ROLLBACK = "ROLLBACK TRANSACTION".freeze
      SQL_ROLLBACK_TO_SAVEPOINT = 'ROLLBACK TRANSACTION autopoint_%d'.freeze
      SQL_SAVEPOINT = 'SAVE TRANSACTION autopoint_%d'.freeze
      TEMPORARY = "#".freeze
      
      def database_type
        :access
      end
      
      def upcase_identifiers?
        false
      end
        
      def supports_savepoints?
        true
      end

      def tables
      ts = []
        m = output_identifier_meth
        metadata(:getTables, nil, nil, nil, ['TABLE'].to_java(:string)) do |h|
          h = downcase_hash_keys(h)
          ts << m.call(h[:table_name])
        end
        ts
      end

      def identifier_input_method_default
        :to_s
      end

      # The method to apply to identifiers coming the database by default.
      # Should be overridden in subclasses for databases that fold unquoted
      # identifiers to lower case instead of uppercase, such as
      # MySQL, PostgreSQL, and SQLite.
      def identifier_output_method_default
        :to_s
      end

      private
      def downcase_hash_keys(h)
        lh = {}
        h.each { |k,v| lh[k.to_s.downcase.to_sym] = v }
        lh
      end
      
      # MSSQL uses the COUNTER(1,1) column for autoincrementing columns.
      def auto_increment_sql
        AUTO_INCREMENT
      end
      
      # SQL to start a new savepoint
      def begin_savepoint_sql(depth)
        SQL_SAVEPOINT % depth
      end

      # SQL to BEGIN a transaction.
      def begin_transaction_sql
        SQL_BEGIN
      end
      
      # Commit the active transaction on the connection, does not commit/release
      # savepoints.
      def commit_transaction(conn)
        log_connection_execute(conn, commit_transaction_sql) unless Thread.current[:sequel_transaction_depth] > 1
      end

      # SQL to COMMIT a transaction.
      def commit_transaction_sql
        SQL_COMMIT
      end
      
      # The SQL to drop an index for the table.
      def drop_index_sql(table, op)
        "DROP INDEX #{quote_identifier(op[:name] || default_index_name(table, op[:columns]))} ON #{quote_schema_table(table)}"
      end
      
      # Always quote identifiers in the metadata_dataset, so schema parsing works.
      def metadata_dataset
        ds = super
        ds.quote_identifiers = true
        ds
      end
      
      # SQL to rollback to a savepoint
      def rollback_savepoint_sql(depth)
        SQL_ROLLBACK_TO_SAVEPOINT % depth
      end
      
      # SQL to ROLLBACK a transaction.
      def rollback_transaction_sql
        SQL_ROLLBACK
      end
      
      # SQL fragment for marking a table as temporary
      def temporary_table_sql
        TEMPORARY
      end
      
      # MSSQL has both datetime and timestamp classes, most people are going
      # to want datetime
      def type_literal_generic_datetime(column)
        :datetime
      end

      # MSSQL has both datetime and timestamp classes, most people are going
      # to want datetime
      def type_literal_generic_time(column)
        column[:only_time] ? :time : :datetime
      end
      
      # MSSQL doesn't have a true boolean class, so it uses bit
      def type_literal_generic_trueclass(column)
        :bit
      end
      
      # MSSQL uses image type for blobs
      def type_literal_generic_file(column)
        :image
      end
    end
  
    module DatasetMethods
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      COMMA_SEPARATOR = ', '.freeze
      DELETE_CLAUSE_METHODS = Dataset.clause_methods(:delete, %w'with from output from2 where')
      INSERT_CLAUSE_METHODS = Dataset.clause_methods(:insert, %w'with into columns output values')
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'with limit distinct columns from table_options join where group order having compounds')
      UPDATE_CLAUSE_METHODS = Dataset.clause_methods(:update, %w'with table set output from where')
      WILDCARD = LiteralString.new('*').freeze
      CONSTANT_MAP = {:CURRENT_DATE=>'CAST(CURRENT_TIMESTAMP AS DATE)'.freeze, :CURRENT_TIME=>'CAST(CURRENT_TIMESTAMP AS TIME)'.freeze}

      def identifier_output_method
        :to_s
      end

      # Split out from fetch rows to allow processing of JDBC result sets
      # that don't come from issuing an SQL string.
      def process_result_set(result)
        # get column names
        meta = result.getMetaData
        cols = []
        i = 0
        meta.getColumnCount.times { cols << [output_identifier(meta.getColumnLabel(i+=1)), i] }
        @columns = cols.map{|c| c.at(0)}
        row = {}
        blk = if @convert_types
          lambda{ |n, i|
            begin
#              puts "#{n}=#{(o = result.getObject(i)).nil? ? 'nil' : o}"
              row[n] = convert_type(result.getObject(i))
            rescue
              # XXX: this is because HXTT driver throws an error here
              if n == :column_def && row[:type_name] == 'TIMESTAMP'
                row[:column_def] = ''
              end
            end
          }
        else
          lambda{|n, i| row[n] = result.getObject(i)}
        end
        # get rows
        rsmd = result.get_meta_data
        num_cols = rsmd.get_column_count

        while result.next
          row = {}
          cols.each(&blk)
          yield row
        end
      end

      # MSSQL uses + for string concatenation
      def complex_expression_sql(op, args)
        case op
        when :'||'
          super(:+, args)
        else
          super(op, args)
        end
      end
      
      # MSSQL doesn't support the SQL standard CURRENT_DATE or CURRENT_TIME
      def constant_sql(constant)
        CONSTANT_MAP[constant] || super
      end
      
      # When returning all rows, if an offset is used, delete the row_number column
      # before yielding the row.
      def fetch_rows(sql, &block)
        @opts[:offset] ? super(sql) {|r| r.delete(:"recno()"); yield r} : super(sql, &block)
      end
      
      # MSSQL uses the CONTAINS keyword for full text search
      def full_text_search(cols, terms, opts = {})
        filter("CONTAINS (#{literal(cols)}, #{literal(terms)})")
      end
      
      # MSSQL uses a UNION ALL statement to insert multiple values at once.
      def multi_insert_sql(columns, values)
        [insert_sql(columns, LiteralString.new(values.map {|r| "SELECT #{expression_list(r)}" }.join(" UNION ALL ")))]
      end

      # Allows you to do .nolock on a query
      def nolock
        clone(:table_options => "(NOLOCK)")
      end

      # Include an OUTPUT clause in the eventual INSERT, UPDATE, or DELETE query.
      #
      # The first argument is the table to output into, and the second argument
      # is either an Array of column values to select, or a Hash which maps output
      # column names to selected values, in the style of #insert or #update.
      #
      # Output into a returned result set is not currently supported.
      #
      # Examples:
      #
      #   dataset.output(:output_table, [:deleted__id, :deleted__name])
      #   dataset.output(:output_table, :id => :inserted__id, :name => :inserted__name)
      def output(into, values)
        output = {}
        case values
        when Hash:
            output[:column_list], output[:select_list] = values.keys, values.values
        when Array:
            output[:select_list] = values
        end
        output[:into] = into
        clone({:output => output})
      end

      # An output method that modifies the receiver.
      def output!(into, values)
        mutation_method(:output, into, values)
      end

      # MSSQL uses [] to quote identifiers
      def quoted_identifier(name)
        "[#{name}]"
      end
      
      # Pagination queries (i.e., limit with offset) are supported HXTT
      # with the help of the recno() function which returns the
      # row number of each record
      def select_sql
        return super unless offset = @opts[:offset]        
        if @opts[:select]
          @opts[:select] << :recno.sql_function
        else
          @opts[:select] = [WILDCARD, :recno.sql_function]
        end
        s = unlimited.where("BETWEEN (recno(), #{@opts[:offset] + 1}, #{@opts[:limit] + @opts[:offset]})")
        s.select_sql
      end

      # Microsoft SQL Server does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end
      
      # MSSQL does not support IS TRUE
      def supports_is_true?
        false
      end

      # MSSQL 2005+ supports window functions
      def supports_window_functions?
        true
      end

      private

      # MSSQL can modify joined datasets
      def check_modification_allowed!
        raise(InvalidOperation, "Grouped datasets cannot be modified") if opts[:group]
      end

      # MSSQL supports the OUTPUT clause for DELETE statements.
      # It also allows prepending a WITH clause.
      def delete_clause_methods
        DELETE_CLAUSE_METHODS
      end

      # Handle the with clause for delete, insert, and update statements
      # to be the same as the insert statement.
      def delete_with_sql(sql)
        select_with_sql(sql)
      end
      alias insert_with_sql delete_with_sql
      alias update_with_sql delete_with_sql
      
      # MSSQL raises an error if you try to provide more than 3 decimal places
      # for a fractional timestamp.  This probably doesn't work for smalldatetime
      # fields.
      def format_timestamp_usec(usec)
        sprintf(".%03d", usec/1000)
      end

      # MSSQL supports FROM clauses in DELETE and UPDATE statements.
      def from_sql(sql)
        if (opts[:from].is_a?(Array) && opts[:from].size > 1) || opts[:join]
          select_from_sql(sql)
          select_join_sql(sql)
        end
      end
      alias delete_from2_sql from_sql
      alias update_from_sql from_sql
      
      # MSSQL supports the OUTPUT clause for INSERT statements.
      # It also allows prepending a WITH clause.
      def insert_clause_methods
        INSERT_CLAUSE_METHODS
      end

      # MSSQL uses a literal hexidecimal number for blob strings
      def literal_blob(v)
        blob = '0x'
        v.each_byte{|x| blob << sprintf('%02x', x)}
        blob
      end
      
      # Use unicode string syntax for all strings
      def literal_string(v)
        "N#{super}"
      end
      
      # Use 0 for false on MSSQL
      def literal_false
        BOOL_FALSE
      end

      # Use 1 for true on MSSQL
      def literal_true
        BOOL_TRUE
      end
      
      # The alias to use for the row_number column when emulating OFFSET
      def row_number_column
        :x_sequel_row_number_x
      end

      # MSSQL adds the limit before the columns
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end

      # MSSQL uses TOP for limit
      def select_limit_sql(sql)
        sql << " TOP #{@opts[:limit]}" if @opts[:limit]
      end

      # MSSQL uses the WITH statement to lock tables
      def select_table_options_sql(sql)
        sql << " WITH #{@opts[:table_options]}" if @opts[:table_options]
      end

      # SQL fragment for MSSQL's OUTPUT clause.
      def output_sql(sql)
        return unless output = @opts[:output]
        sql << " OUTPUT #{column_list(output[:select_list])}"
        if into = output[:into]
          sql << " INTO #{table_ref(into)}"
          if column_list = output[:column_list]
            cl = []
            column_list.each { |k, v| cl << literal(String === k ? k.to_sym : k) }
            sql << " (#{cl.join(COMMA_SEPARATOR)})"
          end
        end
      end
      alias delete_output_sql output_sql
      alias update_output_sql output_sql
      alias insert_output_sql output_sql

      # MSSQL supports the OUTPUT clause for UPDATE statements.
      # It also allows prepending a WITH clause.
      def update_clause_methods
        UPDATE_CLAUSE_METHODS
      end
    end
  end
end
