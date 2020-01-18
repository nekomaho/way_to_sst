begin
  require 'bundler/inline'
rescue
  put 'Need bundler 1.10 or later'
end

gemfile(true) do
  source 'https://rubygems.org'
end

require 'readline'
require 'singleton'

Location = Struct.new(:file_name, :pos)

class DB
  include Singleton

  attr_reader :db_name

  def initialize
    @db_name = 'db/db'
  end

  def name
    db_name
  end
end

class Interactive
  class << self
    def execute
      new.execute
    end
  end

  def initialize
    @hash = {}
    @db = DB.instance

    if File.file?('db/index')
      File.open('db/index','r') do |f|
        puts 'load index file'
        @hash = Marshal.load(f)
      end
    end
  end

  def execute
    while buf = Readline.readline("> ", true)
      command, args = buf.split(' ', 2)
      case command
      when 'read'
        read(args)
      when 'read_all'
        read_all
      when 'write'
        write(args)
      when 'clear'
        clear
      when 'quit'
        quit
        break
      else
        puts 'command: read, write, read_all, clear, quit'
      end
    end
  end

  def read(args)
    location = @hash[args]
    # キャッシュから取る場合
    if !location.nil?
      File.open(location.file_name,'r') do |f|
        pos = location.pos
        f.seek(pos)
        key, value = f.readline.split(',',2)
        puts 'use in memory hash map'
        return puts value
      end
    end

    # db以下にある全てのファイルから探す
    Dir.glob('db/db*').each do |file_name|
      File.open(file_name,'r') do |f|
        f.reverse_each do |r|
          key, value = r.split(',',2)
          return puts value if key == args
        end
      end
    end
  end

  def write(args)
    key, value = args.split(':', 2)
    return puts 'write key:value' if key.nil? || value.nil?
    File.open(@db.name,'a') do |f|
      len = f.write("#{key},#{value}\n")
      @hash[key] = Location.new('db/db', f.pos-len)
    end
  end

  def read_all
    File.open(@db.name,'r') do |f|
      f.each do |r|
        key, value = r.split(',',2)
        puts "#{key}:#{value}"
      end
    end
  end

  def clear
    File.open(@db.name,'w') do |f|
      f = nil
    end
    @hash = {}
  end

  def quit
    File.open('db/index','w') do |f|
      Marshal.dump(@hash, f)
    end
  end
end

Interactive.execute
