desc "Run rspec"
task :spec do
  require "rspec/core/rake_task"

  RSpec::Core::RakeTask.new do |t|
    t.rspec_opts = ['-cfs']
  end
end
