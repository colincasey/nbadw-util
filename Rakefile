# encoding: utf-8
require 'rubygems'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |s|
    s.name = "nbadw"
    s.summary = %Q{NB Aquatic Data Warehouse - Models and Database Utilities}
    s.email = "casey.colin@gmail.com"
    s.homepage = "http://github.com/colincasey/nbadw"
    s.description = "Database models, migrations, and utilities for the New Brunswick Aquatic Data Warehouse"
    s.authors = ["Colin Casey"]

    s.add_dependency 'thor', '= 0.9.9'
    s.add_dependency 'sequel', '>= 3.4.0'

    s.rubygems_version = '1.3.1'

    s.files = FileList['spec/*.rb'] + FileList['lib/**/*.rb'] + ['README.rdoc', 'LICENSE', 'VERSION.yml', 'Rakefile'] + FileList['bin/*']
    s.executables = ['nbadw']
  end
rescue LoadError => e
  if e.message =~ /jeweler/
    puts "Jeweler not available. Install it with: sudo gem install technicalpickles-jeweler -s http://gems.github.com"
  else
    puts e.message + ' -- while loading jeweler.'
  end
end

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = 'NB Aquatic Data Warehouse'
  rdoc.options << '--line-numbers' << '--inline-source'
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |t|
    t.libs << 'spec'
    t.test_files = FileList['spec/*_spec.rb']
    t.verbose = true
  end
rescue LoadError
  if RUBY_PLATFORM =~ /java/
    puts "RCov is not available. In order to run rcov, you must: sudo gem install jruby-rcov"
  else
    puts "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

begin
  require 'bacon'
  desc "Run all specs; requires the bacon gem"
  task :spec do
    bacon = "bacon \"#{File.dirname(__FILE__)}/spec/*_spec.rb}\""
    if RUBY_PLATFORM =~ /java/
      bacon = "#{Config::CONFIG['bindir']}/jruby -S #{bacon}"
    end
    system bacon
  end
rescue LoadError
  puts "Bacon is not available. In order to run test specs, you must: sudo gem install bacon"
end

task :default => :spec

#
#require 'rake/testtask'
#require 'spec/rake/spectask'
#
#Rake::TestTask.new do |t|
#  t.test_files = FileList['test/**/*.rb']
#end
#
#Spec::Rake::SpecTask.new do |t|
#  t.spec_files = FileList['spec/**/*.rb']
#end