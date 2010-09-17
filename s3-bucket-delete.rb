#!/usr/bin/ruby

# Cribbed from http://gist.github.com/13554 (http://github.com/jdance)

require 'rubygems'
require 'bundler/setup'
require 'right_aws'

access_key = 'key'
secret = 'secret'
bucket_to_delete = 'xyz'

num_threads = 20

s3 = RightAws::S3.new(access_key, secret, { :multi_thread => true })

$keys = []
threads = []

def check_keys_size
  size = nil
  Thread.exclusive do
    size = $keys.size
  end
  size
end

num_threads.times do
  threads << Thread.new do
    loop do
      key = nil
      Thread.exclusive do
        Thread.exit if $keys.nil?
        key = $keys.shift
      end
      if key.nil?
        puts "[thread] Thread sleeping, waiting for keys"
        sleep(1)
        redo
      end
      begin
        s3.interface.delete(bucket_to_delete, key)
      rescue RightAws::AwsError
        sleep(1)
        retry
      end
      STDOUT.puts "DELETE #{key}"
      STDOUT.flush
    end
  end
end

s3.interface.incrementally_list_bucket(bucket_to_delete) do |key_data|
  more_keys = key_data[:contents].collect { |node| node[:key] }
  Thread.exclusive do
    $keys.concat(more_keys)
  end
  loop do
    if check_keys_size > num_threads * 50
      puts "[main] Sleeping, plenty of keys"
      sleep(1)
    else
      break
    end
  end
end

loop do
  if check_keys_size == 0
    Thread.exclusive do
      $keys = nil
    end
    break
  else
    sleep(1)
  end
end

threads.each { |t| t.join }

s3.interface.delete_bucket(bucket_to_delete)
