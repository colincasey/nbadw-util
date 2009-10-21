module NBADW
	module Util
    module AdapterHacks
      HACKS = {
        :mysql => ['column_schema_to_ruby_type', 'tiny_int_default_to_boolean']
      }

      def self.load(adapter)
        (HACKS[adapter.to_sym] || []).each do |r|
          require "nbadw/util/adapter_hacks/#{r}"
        end
      end
    end
  end
end
