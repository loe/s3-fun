#!/usr/bin/ruby

# Cribbed from http://gist.github.com/13554 (http://github.com/jdance)

require 'rubygems'
require 'right_aws'
require 'ftools'

access_key = 'key'
secret = 'secret'
src = 'abc'
dest = 'xyz'

num_threads = 10 # This is basically the number of simultaneous downloads your machine can handle.

s3 = RightAws::S3.new(access_key, secret, { :multi_thread => true })

src_bucket = s3.bucket(src)

# Make sure the destination exists!
File.makedirs(dest)

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
        src_key = src_bucket.key(key, true)
        if File.exists?(File.join(dest, key)) && File.size(File.join(dest, key)) == src_key.size
          STDOUT.puts "#{src}:#{key} already exists"
          STDOUT.flush
          next
        end
        path = key.split('/')
        file_name = path.pop
        File.makedirs(File.join(dest, path)) # Make the enclosing folder.
        file = File.new(File.join(dest, path, "#{file_name}"), File::CREAT|File::RDWR)
        s3.interface.get(src, key) do |chunk|
          file.write(chunk)
        end
        file.close
      rescue RightAws::AwsError
        sleep(1)
        retry
      end
      STDOUT.puts "#{src}:#{key} => #{dest}/#{key}"
      STDOUT.flush
    end
  end
end

s3.interface.incrementally_list_bucket(src) do |key_data|
  more_keys = key_data[:contents].collect { |node| node[:key] }
  Thread.exclusive do
    $keys.concat(more_keys)
  end
  loop do
    if check_keys_size > num_threads * 5
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
