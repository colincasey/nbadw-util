#!/usr/bin/env ruby
# encoding: utf-8

require 'rubygems'
gem 'activerecord', '= 2.2.2'
gem 'thor', '= 0.9.9'
gem 'sequel', '>= 3.0.0', '< 3.1.0'
require 'nbadw/util/cli'

NBADW::Util::Cli.start
