# File derived from NATS.
# Original source: nats/benchmark/sub_perf.rb
#
require File.dirname(__FILE__) + '/../lib/rats/client'

$expected = 100000
$hash = 10000
$sub  = 'test'

STDOUT.sync = true

trap("TERM") { exit! }
trap("INT")  { exit! }


EM::run do
  @rats = RATS.new('amqp://localhost/')
  @rats.on_error { |err| puts "Server Error: #{err}"; exit! }

  received = 1
  @rats.subscribe($sub) {
    ($start = Time.now and puts "Started Receiving..") if (received == 1)
    if ((received+=1) == $expected)
      puts "\nTest completed : #{($expected/(Time.now-$start)).ceil} msgs/sec.\n"
      @rats.stop { EM.stop }
    end
    printf('+') if received.modulo($hash) == 0
  }

  puts "Waiting for #{$expected} messages on [#{$sub}]"
end
