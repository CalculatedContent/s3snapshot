gem 'rdoc'
require 'bundler'
require 'rdoc/task'

Bundler::GemHelper.install_tasks


RDoc::Task.new do |rd|
  rd.main = "README.rdoc"
  rd.rdoc_files.include("README.rdoc", "lib/**/*.rb")
end