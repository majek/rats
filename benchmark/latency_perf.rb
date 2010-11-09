# File derived from NATS.
# Original source: nats/benchmark/latency_perf.rb
#
require File.dirname(__FILE__) + '/../lib/rats/client'

$loop = 10000
$hash = 1000
$sub  = 'test'

STDOUT.sync = true

$drain = $loop-1

trap("TERM") { exit! }
trap("INT")  { exit! }


EM::run do
  @rats = RATS.new('amqp://localhost/')

  def done
    ms = "%.2f" % (((Time.now-$start)/$loop)*1000.0)
    puts "\nTest completed : #{ms} ms avg request/response latency\n"
    @rats.stop { EM.stop }
  end

  def send_request
    @rats.request('test', [1,2,3]) {
      $drain-=1
      if $drain == 0
        done
      else
        send_request
        printf('+') if $drain.modulo($hash) == 0
      end
    }
  end

  @rats.subscribe('test') { |msg, reply|
    @rats.publish(reply)
  }

  # Send first request when we are connected with subscriber
  @rats.on_connect {
    puts "Sending #{$loop} request/responses"
    $start = Time.now
    send_request
  }

  @rats.on_error { |err|
    puts "Server Error: #{err}"; exit!
  }
end
