require 'mq'

class RATS
  attr_reader :client
  def initialize(amqp_url=nil, opts={}, &blk)
    raise 'can only be used from within EM.run{}' unless EM.reactor_running?

    opts = AMQP::Client.parse_amqp_url(amqp_url || 'amqp:///').merge(opts)
    @client = AMQP::connect(opts)
    @client.connection_status do |status|
      case status
      when :connected
        @on_connect.call if @on_connect
      when :disconnected
        @on_reconnect.call if @on_reconnect
      end
    end
    @mq = MQ.new(@client)
    @on_connect = blk

    @exchange_name = opts[:exchange] || 'rats'
    @exchange = amqp_exchange('topic', @exchange_name)
    @empty_exchange = amqp_exchange('direct', '')

    @resp_map = Hash.new
    @resp_qname = "response.#{(rand*2E14).floor}"
    q = @mq.queue(@resp_qname, :auto_delete => true, :exclusive => true)
    q.subscribe do |properties, enc_body|
      body = enc_body[1..-1]
      corr_id = properties.correlation_id
      if @resp_map.has_key?(corr_id)
        blk = @resp_map[corr_id]
        @resp_map.delete(corr_id)
        case blk.arity
        when 0 then blk.call
        else blk.call body
        end
      end
    end
  end

  def on_connect(&blk)
    @on_connect = blk
  end

  def on_reconnect(&blk)
    @on_reconnect = blk
  end

  def on_error(&blk)
  end

  def request(topic, data=nil , &blk)
    corr_id = "#{(rand*2E14).floor}"
    @resp_map[corr_id] = blk
    publish(topic, data, :reply_to => @resp_qname, :correlation_id => corr_id)
  end

  def stop &blk
    @client.close &blk
  end

  def subscribe(topic, &blk)
    q = @mq.queue("#{@exchange_name}.#{(rand*2E14).floor}",
                  :auto_delete => true,
                  :exclusive => true)
    q.bind(@exchange, :key => (topic || '')).subscribe \
    do |properties, enc_body|
      reply_to = [properties.reply_to, properties.correlation_id]
      body = enc_body[1..-1]
      case blk.arity
      when 0 then blk.call
      when 1 then blk.call(body)
      when 2 then blk.call(body, reply_to)
      else blk.call(body, reply_to, properties.routing_key)
      end
    end
    q
  end

  def unsubscribe(q)
    q.unsubscribe
  end

  def publish(topic, body='', opts={}, &blk)
    if topic.class == Array
      # For compatibility
      return self.reply(topic, body, opts, &blk)
    end
    @exchange.publish(' '+body.to_s, # can't handle empty frames.
                      {:key => (topic || ''),
                        :content_type => 'text/plain'}.merge(opts))
    blk.call if blk
  end

  def reply(reply, body='', opts={}, &blk)
    reply_to, corr_id = reply
    @empty_exchange.publish(' '+body.to_s,
                            {:key => reply_to, :correlation_id => corr_id,
                              :content_type => 'text/plain'}.merge(opts))
    blk.call if blk
  end


  private

  def amqp_exchange(exch_type, exch_name)
    @mq.__send__(exch_type, exch_name)
  end
end




module AMQP
  module Client
    def self.parse_amqp_url(amqp_url)
      uri = URI.parse(amqp_url)
      raise("amqp:// uri required!") unless %w{amqp amqps}.include? uri.scheme
      opts = {}
      opts[:user] = URI.unescape(uri.user) if uri.user
      opts[:pass] = URI.unescape(uri.password) if uri.password
      opts[:vhost] = URI.unescape(uri.path) if uri.path
      opts[:host] = uri.host if uri.host
      opts[:port] = uri.port ? uri.port :
                      {"amqp"=>5672, "amqps"=>5671}[uri.scheme]
      opts[:ssl] = uri.scheme == "amqps"
      return opts
    end
  end
end
