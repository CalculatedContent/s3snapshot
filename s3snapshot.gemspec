# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "s3snapshot/version"

Gem::Specification.new do |s|
  s.name        = "s3snapshot"
  s.version     = S3snapshot::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Todd"]
  s.email       = ["foo@bar.com"]
  s.homepage    = ""
  s.summary     = %q{Uploads to s3}
  s.description = %q{see summary}

  s.rubyforge_project = "s3snapshot"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  s.add_dependency "fog", "~>0.8.1"  
  s.add_dependency "thor", "~>0.14.6"
end
