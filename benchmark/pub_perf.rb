# File derived from NATS.
# Original source: nats/benchmark/pub_perf.rb
#
require File.dirname(__FILE__) + '/../lib/rats/client'

$loop = 100000
$hash = 10000

$max_outstanding = (512*1024) #512k

$data = '-ok-'
$sub  = 'test'

STDOUT.sync = true

$to_drain = $loop - 1
$flow_trigger = $max_outstanding/4

trap("TERM") { exit! }
trap("INT")  { exit! }

EM::run do
  @rats = RATS.new('amqp://localhost/')
  @rats.on_error { |err| puts "Server Error: #{err}"; exit! }

  $start = Time.now

  def done
    $to_drain-=1
    # Send last message and add closure which gets called when the server has processed
    # the message. This way we know all messages have been processed by the server.
    @rats.publish($sub, $data) {
      puts "\nTest completed : #{($loop/(Time.now-$start)).ceil} msgs/sec.\n"
      @rats.stop { EM.stop }
    }
  end

  def drain
    sent_flow = false
    while ($to_drain > 0) do
      $to_drain-=1
      outstanding_bytes = @rats.client.get_outbound_data_size
      if (outstanding_bytes > $flow_trigger && !sent_flow)
        @rats.publish($sub, $data) { drain }
        sent_flow = true
      else
        @rats.publish($sub, $data)
      end
      printf('+') if $to_drain.modulo($hash) == 0
      break if outstanding_bytes > $max_outstanding
    end
    done if $to_drain == 0
  end

  if false
    EM.add_periodic_timer(0.1) do
      puts "Outstanding data size is " ##{@rats.client.get_outbound_data_size}"
    end
  end

  # kick things off..
  puts "Sending #{$loop} messages of size #{$data.size} bytes on [#{$sub}], outbound buffer maximum is #{$max_outstanding} bytes"
  drain
end
