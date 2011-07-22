Gem::Specification.new do |s|
  s.name             = "mongo-locking"
  s.version          = "0.0.2"
  s.platform         = Gem::Platform::RUBY
  s.has_rdoc         = true
  s.extra_rdoc_files = [ "README.md", "LICENSE" ]
  s.summary          = "A mixin DSL for implementing cross-process mutexes/locks using MongoDB."
  s.description      = s.summary
  s.authors          = [ "Jordan Ritter", "Brendan Baldwin", "Yanzhu Du" ]
  s.email            = "jpr5@serv.io"
  s.homepage         = "http://github.com/servio/mongo-locking"

  s.add_dependency "mongo",          "~> 1.3.1"
  s.add_dependency "active_support", "~> 3.0.4"

  s.require_path = 'lib'
  s.files        = %w(LICENSE README.md Rakefile) + Dir["lib/{**/*/**/,}*.rb"]
end
