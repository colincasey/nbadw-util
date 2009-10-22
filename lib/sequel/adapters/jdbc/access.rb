require 'sequel/adapters/shared/access'

module Sequel
  module JDBC    
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
        def schema_parse_table(table, opts={})
          m = output_identifier_meth
          im = input_identifier_meth
          ds = dataset
          schema, table = schema_and_table(table)
          schema ||= opts[:schema]
          schema = im.call(schema) if schema
          table = im.call(table)
          pks, ts = [], []
          metadata(:getPrimaryKeys, nil, schema, table) do |h|
            h = downcase_hash_keys(h)
            pks << h[:column_name]
          end
          metadata(:getColumns, nil, schema, table, nil) do |h|
            h = downcase_hash_keys(h)
            ts << [m.call(h[:column_name]), {:type=>schema_column_type(h[:type_name]), :db_type=>h[:type_name], :default=>(h[:column_def] == '' ? nil : h[:column_def]), :allow_null=>(h[:nullable] != 0), :primary_key=>pks.include?(h[:column_name]), :column_size=>h[:column_size]}]
          end
          ts
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
