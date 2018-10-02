require 'sqlite3'
require 'pg'

module Selection
  
  def find(*ids)
    if ids.length == 1
      find_one(ids.first)
    else
      sql = <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id IN (#{ids.join(",")});
SQL
      if defined?(connection.exec)
        rows = connection.exec(sql)
      else
        rows = connection.execute(sql)
      end
      
      rows_to_array(rows)
    end
  end
    
  
  def find_one(id)
    if defined?(connection.exec)
    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE id = #{id[1]};
SQL
      row = connection.exec(sql).first
    else
      sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE id = #{id};
SQL
    
      row = connection.get_first_row(sql)
    end
    
    init_object_from_row(row)
  end
    
    
  def find_by(attribute, value)
    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
SQL
    
    if defined?(connection.exec)
      rows = connection.exec(sql)
    else
      rows = connection.execute(sql)
    end
    
    rows_to_array(rows)
  end
    
    
  def take(num=1)
    if num > 1
      sql =  <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        ORDER BY random()
        LIMIT #{num};
SQL
      
      if defined?(connection.exec)
        rows = connection.exec(sql)
      else
        rows = connection.execute(sql)
      end
      
      rows_to_array(rows)
    else
      take_one
    end
  end
    
    
  def take_one
    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY random()
      LIMIT 1;
SQL
    
    if defined?(connection.exec)
      row = connection.exec(sql).first
    else
      row = connection.get_first_row(sql)
    end
    
    init_object_from_row(row)
  end
    
    
  def first
    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id ASC LIMIT 1;
SQL
    
    if defined?(connection.exec)
      row = connection.exec(sql).first
    else
      row = connection.get_first_row(sql)
    end
    
    init_object_from_row(row)
  end
    
    
  def last
    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id DESC LIMIT 1;
SQL
    
    if defined?(connection.exec)
      row = connection.exec(sql).first
    else
      row = connection.get_first_row(sql)
    end
    
    init_object_from_row(row)
  end
    
  
  def all
    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table};
SQL
    
    if defined?(connection.exec)
      rows = connection.exec(sql)
    else
      rows = connection.execute(sql)
    end
    
    rows_to_array(rows)
  end

  
  def where(*args)
    if args.count > 1
      expression = args.shift
      params = args
    else
      case args.first
      when String
        expression = args.first
      when Hash
        expression_hash = BlocRecord::Utility.convert_keys(args.first)
        expression = expression_hash.map {|key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
      end
    end

    sql = <<-SQL
       SELECT #{columns.join ","} FROM #{table}
       WHERE #{expression};
SQL
    
    if defined?(connection.exec)
       rows = connection.exec(sql, params)
    else
      rows = connection.execute(sql, params)
    end
    
     rows_to_array(rows)
  end
  
  
  def order(*args)
    if args.count > 1
      order = args.join(",")
    else
      order = args.first.to_s
    end
    
    sql = <<-SQL
      SELECT * FROM #{table}
      ORDER BY #{order};
SQL
    if defined?(connection.exec)
      rows = connection.exec(sql)
    else
      rows = connection.execute(sql)
    end
    
    rows_to_array(rows)
  end
  
  
  def join(*args)
    if args.count > 1
      joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id"}.join(" ")
      sql = <<-SQL
        SELECT * FROM #{table} #{joins}
SQL
        if defined?(connection.exec)
          rows = connection.exec(sql)
        else
          rows = connection.execute(sql)
        end
    else
      case args.first
      when String
        sql = <<-SQL
          SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)};
SQL
        if defined?(connection.exec)
          rows = connection.exec(sql)
        else
          rows = connection.execute(sql)
        end
      when Symbol
        sql = <<-SQL
          SELECT * FROM #{table}
          INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
SQL
        if defined?(connection.exec)
          rows = connection.exec(sql)
        else
          rows = connection.execute(sql)
        end
      end
    end
    
    rows_to_array(rows)
  end
  
  
  private
  
    
  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end
    
  
  def rows_to_array(rows)
    collection = BlocRecord::Collection.new
    rows.each { |row| collection << new(Hash[columns.zip(row)]) }
    collection
  end
  
end