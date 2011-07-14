task :default do
    Kernel.exec("#{$0}", '-T')
end

task :gem do
    `gem build mongo-locking.gemspec`
end
