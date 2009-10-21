require 'sequel/adapters/shared/access'

module Sequel
  module JDBC
    class Database
      # Alias the generic JDBC version so it can be called directly later
      alias jdbc_schema_parse_table schema_parse_table
    end
    
    # Database and Dataset instance methods for MSSQL specific
    # support via JDBC.
    module Access
      # Database instance methods for MSSQL databases accessed via JDBC.
      module DatabaseMethods
        PRIMARY_KEY_INDEX_RE = /\Apk__/i.freeze
        
        include Sequel::Access::DatabaseMethods
        
        # Return instance of Sequel::JDBC::MSSQL::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::Access::Dataset.new(self, opts)
        end
        
        private
                
        # Call the generic JDBC version instead of MSSQL version,
        # since the JDBC version handles primary keys.
        def schema_parse_table(table, opts={})
          jdbc_schema_parse_table(table, opts)
        end
        
        # Primary key indexes appear to start with pk__ on MSSQL
        def primary_key_index_re
          PRIMARY_KEY_INDEX_RE
        end
      end
      
      # Dataset class for MSSQL datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Access::DatasetMethods
      end
    end
  end
end
