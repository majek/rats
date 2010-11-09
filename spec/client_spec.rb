require './lib/rats/client'

describe RATS do
  it 'should handle basic request/reply with empty payload' do
    msgs = []
    EM::run do
      rats = RATS.new
      rats.subscribe('exit') { rats.stop {EM.stop} }
      rats.subscribe('x') { |msg| msgs << msg }
      rats.publish('x')
      rats.publish('x', 'body')
      rats.publish('exit')
    end
    msgs.should == ['', 'body']
  end

  it 'should not complain when publishing to nil' do
    msgs = []
    EM::run do
      rats = RATS.new
      rats.subscribe('exit') { rats.stop {EM.stop} }
      rats.subscribe(nil) { |msg| msgs << msg }
      rats.publish(nil)
      rats.publish(nil, 'hello')
      rats.publish('exit')
    end
    msgs.should == ['', 'hello']
  end

  it 'should be able to do unsubscribe' do
    msgs = []
    EM::run do
      rats = RATS.new
      rats.subscribe('exit') { rats.stop {EM.stop} }
      s = rats.subscribe('x') { |msg| msgs << msg }
      rats.publish('x', 'a')
      rats.unsubscribe(s)
      rats.publish('x', 'b')
      rats.publish('exit')
    end
    msgs.should == ['a']
  end

  it 'should handle wildcard subscriptions' do
    msgs = []
    EM::run do
      rats = RATS.new
      rats.subscribe('exit') { rats.stop {EM.stop} }
      s = rats.subscribe('#') { |msg| msgs << msg }
      rats.publish('a', 'a')
      rats.publish('b', 'b')
      rats.publish('exit')
    end
    msgs.should == ['a', 'b', '']
  end

  it 'should receive a response from a request' do
    msgs = []
    EM::run do
      rats = RATS.new
      s = rats.subscribe('a') do |msg, reply_to|
        rats.reply(reply_to, msg + '_r')
      end
      rats.request('a', 'a') { |msg| msgs << msg }
      rats.request('a', 'b') { |msg| msgs << msg }
      rats.request('a', 'c') { rats.stop {EM.stop} }
    end
    msgs.should == ['a_r', 'b_r']
  end


  it 'should be able to handle two connections in one EM block' do
    msgs = []
    EM::run do
      rats = RATS.new
      rats2 = RATS.new nil, :exchange => 'rats2'
      s = rats.subscribe('a') do |msg, reply_to|
        rats.reply(reply_to, msg + '_1')
      end
      s = rats2.subscribe('a') do |msg, reply_to|
        rats2.reply(reply_to, msg + '_2')
      end
      rats.request('a', '1') { |msg| msgs << msg }
      rats2.request('a', '2') { |msg| msgs << msg }
      rats.request('a', '1') { |msg| msgs << msg }
      rats2.request('a', '2') { |msg| msgs << msg }
      rats.request('a', '3') { rats.stop { rats2.stop {EM.stop} } }
    end
    msgs.sort.should == ['1_1', '1_1', '2_2', '2_2']
  end

end


