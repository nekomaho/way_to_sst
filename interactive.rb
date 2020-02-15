begin
  require 'bundler/inline'
rescue
  put 'Need bundler 1.10 or later'
end

gemfile(true) do
  source 'https://rubygems.org'
  gem 'ruby-avl'
end

# monkey patch For ruby-avl
module AVLTree
  class BSTree
    def search(item)
      search_node(item, @root)
    end

    private

    def search_node(item, node)
      if !node.is_a?(Node)
        return nil
      elsif compare(item, node.data) < 0
        search_node(item, node.left)
      elsif compare(item, node.data) > 0
        search_node(item, node.right)
      else
        return node.data
      end
    end
  end
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

class DBData
  attr_reader :key, :value

  def initialize(key, value)
    @key = key
    @value = value
  end

  def compare_to(other_data)
    key <=> other_data.key
  end
end

class DB
  include Singleton

  attr_reader :count,:file_segment,:in_memory_data

  def initialize
    @count = 0
    @file_segments = [FileSegment.new]
    @in_memory_data = AVLTree::AVLTree.new
  end

  def name
    "db/db#{count}"
  end

  def read(args)
    # memory上にデータがないかをまず見る
    hit_data = in_memory_data.search(DBData.new(args, nil))
    if hit_data
      puts 'use in memory data'
      return puts hit_data.value
    end

    location = search(args)
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

  FLUSH_DEPTH = 2
  def write(key, value)
    # 重複する場合は一回消してから入れる
    hit_data = in_memory_data.search(DBData.new(key, nil))
    in_memory_data.remove_item(hit_data) if hit_data
    in_memory_data.insert_item(DBData.new(key, value))

    # 木の深さがFLUSH_DEPTHになったらDiskに書き出す
    if in_memory_data.depth_of_tree == FLUSH_DEPTH
      File.open(name,'a') do |f|
        AVLTree::BSTreeTraversal.new.in_order_array(in_memory_data.root).each do |data|
          len = f.write("#{data.key},#{data.value}\n")
          index(data.key, f.pos-len)
        end
      end

      # 書き込み終わったらmemoryデータを消去する
      in_memory_data = AVLTree::AVLTree.new
      divide
    end
  end

  def divide
    return if File.size?(name) < 30
    puts 'change db file'
    compact_1_file(@count)
    @count += 1
    @file_segments.push(FileSegment.new)
  end

  def dump_data
    return if in_memory_data.number_of_nodes == 0
    File.open(name,'a') do |f|
      AVLTree::BSTreeTraversal.new.in_order_array(in_memory_data.root).each do |data|
        len = f.write("#{data.key},#{data.value}\n")
        index(data.key, f.pos-len)
      end
    end
  end

  def dump_index
    @file_segments.each_with_index do |file_segment, i|
      next if file_segment.hash_index.empty?
      File.open("db/index#{i}",'w') do |f|
        puts "db/index#{i}:#{file_segment.hash_index}"
        Marshal.dump(file_segment.hash_index, f)
      end
    end
  end

  def load_index
    files = Dir.glob('db/index*')
    return if files.count == 0
    @file_segments = [] # indexが存在しない場合はfile segmentsも存在していないので初期化する
    Dir.glob('db/index*').sort.each do |file_name|
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
    Dir.glob('db/db*').sort.each do |file_name|
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

  def current_db
    puts "file:db/#{count}"
  end

  def compact
    # currentよりも1少ないファイルたちを全部対象にする
    files = Dir.glob('db/db*').sort
    files.delete("db/db#{count}")

    # 古い順にファイルを開いてハッシュにキーを入れる
    # 同じキー値は新しい物で上書きされていく
    compaction_hash = {}
    files.each do |file|
      File.open(file,'r') do |f|
        compaction_hash = f.each_with_object(compaction_hash) do |disp, hash|
          key, value = disp.split(',',2)
          puts "before: #{key}:#{value}"
          hash[key] = value
        end
      end
    end

    # コンパクションが終わったファイル郡のうち最も新しいファイルに対してコンパクションした結果を書き込む
    file_segment = FileSegment.new
    File.open(files.last,'w') do |f|
      compaction_hash.each do |key, value|
        len = f.write("#{key},#{value}")
        file_segment.hash_index[key] = f.pos-len
      end
    end

    puts "after: hash:#{compaction_hash}"

    # インメモリインデックスを更新する
    file_num = files.last.match(/db\/db([0-9]+)/)[1].to_i
    @file_segments[file_num] = file_segment
    file_num.times do |num|
      # コンパクションされてまとめられた古いファイルのインデックスは削除する
      puts "delete index:#{@file_segments[num].hash_index}"
      @file_segments[num].hash_index = {}
    end

    # 不要なファイル郡を削除する
    files.pop # 最後のファイルは消さない
    files.each do |file|
      num = file.match(/db\/db([0-9]+)/)[1].to_i
      FileUtils.rm(file)
      FileUtils.rm("db/index#{num}")
    end
  end

  private

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

  def compact_1_file(file_num)
    puts "file compaction db/db#{file_num}"
    file_name = "db/db#{file_num}"
    compaction_hash = {}
    File.open(file_name,'r') do |f|
      compaction_hash = f.each_with_object({}) do |r, hash|
        key, value = r.split(',',2)
        hash[key] = value
      end
    end

    FileUtils.rm(file_name)
    file_segment = FileSegment.new

    compaction_hash.each do |key,value|
      File.open(file_name,'a') do |f|
        len = f.write("#{key},#{value}")
        file_segment.hash_index[key] = f.pos-len
      end
    end

    @file_segments[file_num] = file_segment
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
      when 'current'
        current_db
      when 'compact'
        compact
      when 'quit'
        quit
        break
      else
        puts 'command: read, write, read_all, all_index, current, clear, compact, quit'
      end
    end
  end

  def read(args)
    @db.read(args)
  end

  def write(args)
    key, value = args.split(':', 2)
    return puts 'write key:value' if key.nil? || value.nil?
    @db.write(key,value)
  end

  def read_all
    @db.read_all
  end

  def all_index
    @db.all_index
  end

  def current_db
    @db.current_db
  end

  def compact
    @db.compact
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
    @db.dump_data
    @db.dump_index
  end
end

Interactive.execute
