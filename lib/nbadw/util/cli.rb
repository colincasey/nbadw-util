# encoding: utf-8
require 'thor'
require 'sequel'
require 'nbadw/util/copy_database_task'

module NBADW
  module Util
    class Cli < Thor
      desc "copy <src_database_url> <dest_database_url>", "Copy a database from source to destination"
      method_options(:page_size => 1000, :verify_data => :boolean)
      def copy(src, dest)
        verify_database_url(src)
        verify_database_url(dest)
        CopyDatabaseTask.start(src, dest, options)
      end

      private
      def verify_database_url(url)
				db = Sequel.connect(url)
				db.test_connection
				db.disconnect
			rescue Object => e
				puts "Failed to connect to database #{url}:\n  #{e.class} -> #{e}"
				exit 1
			end
    end
  end
end
