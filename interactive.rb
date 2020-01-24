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
require 'fileutils'

Location = Struct.new(:file_name, :pos)

class FileSegment
  attr_accessor :hash_index

  def initialize
    @hash_index = {}
  end
end

class DB
  include Singleton

  attr_reader :count,:file_segment

  def initialize
    @count = 0
    @file_segments = [FileSegment.new]
  end

  def name
    "db/db#{count}"
  end

  def divide
    return if File.size?(name) < 30
    puts 'change db file'
    @count += 1
    @file_segments.push(FileSegment.new)
  end

  def index(key, value)
    @file_segments.last.hash_index[key] = value
  end

  def search(key)
    index = count
    @file_segments.reverse_each do |file_segment|
      if file_segment.hash_index.has_key?(key)
        puts "db/db#{index}, #{file_segment.hash_index[key]}"
        return Location.new("db/db#{index}", file_segment.hash_index[key])
      end
      index -= 1
    end
    nil
  end

  def dump_index
    @file_segments.each_with_index do |file_segment, i|
      File.open("db/index#{i}",'w') do |f|
        Marshal.dump(file_segment.hash_index, f)
      end
    end
  end

  def load_index
    files = Dir.glob('db/index*')
    return if files.count == 0
    @file_segments = [] # indexが存在しない場合はfile segmentsも存在していないので初期化する
    Dir.glob('db/index*').reverse_each do |file_name|
      File.open(file_name,'r') do |f|
        file_seg = FileSegment.new
        file_seg.hash_index = Marshal.load(f)
        @file_segments.push(file_seg)
      end
    end
    @count = files.count - 1
  end

  def clear
    @count = 0
    @file_segments = [FileSegment.new]
  end

  def read_all
    Dir.glob('db/db*').each do |file_name|
      File.open(file_name,'r') do |f|
        f.each do |r|
          key, value = r.split(',',2)
          puts "#{file_name}:#{key}:#{value}"
        end
      end
    end
  end

  def all_index
    index = 0
    @file_segments.each do |file|
      file.hash_index.each do |key, value|
        puts "db#{index}:#{key}:#{value}"
      end
      index += 1
    end
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

    @db.load_index
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
      when 'all_index'
        all_index
      when 'quit'
        quit
        break
      else
        puts 'command: read, write, read_all, clear, quit'
      end
    end
  end

  def read(args)
    location = @db.search(args)
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
      @db.index(key, f.pos-len)
    end

    @db.divide
  end

  def read_all
    @db.read_all
  end

  def all_index
    @db.all_index
  end

  def clear
    Dir.glob('db/db*').each do |file_name|
      FileUtils.rm(file_name)
    end
    Dir.glob('db/index*').each do |file_name|
      FileUtils.rm(file_name)
    end
    @db.clear
  end

  def quit
    @db.dump_index
  end
end

Interactive.execute
