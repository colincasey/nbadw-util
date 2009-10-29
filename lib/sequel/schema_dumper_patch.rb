# encoding: utf-8
module Sequel
  class Database
    alias_method :sequel_column_schema_to_ruby_type, :column_schema_to_ruby_type

    def column_schema_to_ruby_type(schema)
      if database_type == :access
        access_column_schema_to_ruby_type(schema)
      else
        sequel_column_schema_to_ruby_type(schema)
      end
    end

    def access_column_schema_to_ruby_type(schema)
      case t = schema[:db_type].downcase
      when /^varchar$/
        # not sure why all varchar columns report double the size they should be
        size = schema[:column_size].to_i / 2
        { :type => String, :size => size == 0 ? 255 : size }
      when /^integer auto_increment$/
        { :type => Integer }
      else
        sequel_column_schema_to_ruby_type(schema)
      end
    end
  end
end
