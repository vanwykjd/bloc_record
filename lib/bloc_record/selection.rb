require 'sqlite3'

module Selection
  
  
  def find(*ids)
    if ids.length == 1
      find_one(ids.first)
    else
      ids.each { |id|
        BlocRecord::Utility.int_data_type?(id)
      }
        rows = connection.execute(<<-SQL)
          SELECT #{columns.join ","} FROM #{table}
          WHERE id IN (#{ids.join(",")});
        SQL

        rows_to_array(rows)
    end
  end
    
  
  def find_one(id)
    BlocRecord::Utility.int_data_type?(id)
    
    row = connection.get_first_row(<<-SQL)
      SELECT #{columns.join ","} FROM #{table}
      WHERE id = #{id};
    SQL
    
    init_object_from_row(row)
  end
    
    
  def find_by(attribute, value)
    rows = connection.execute(<<-SQL)
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL
    
    rows_to_array(rows)
  end
    
    
  def take(num=1)
    if BlocRecord::Utility.int_data_type?(num) && num > 0
      if num > 1
        rows = connection.execute(<<-SQL)
          SELECT #{columns.join ","} FROM #{table}
          ORDER BY random()
          LIMIT #{num};
        SQL

        rows_to_array(rows)
      else
        take_one
      end
    else
      raise ArgumentError.new("Invalid Entry: '#{num}' >> Entry must be larger than 0.") 
    end
  end
    
    
  def take_one
    row = connection.get_first_row(<<-SQL)
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY random()
      LIMIT 1;
    SQL
    
    init_object_from_row(row)
  end
    
    
  def first
    row = connection.get_first_row(<<-SQL)
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id ASC LIMIT 1;
    SQL
    
    init_object_from_row(row)
  end
    
    
  def last
    row = connection.get_first_row(<<-SQL)
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id DESC LIMIT 1;
    SQL
    
    init_object_from_row(row)
  end
    
  
  def all
    rows = connection.execute(<<-SQL)
      SELECT #{columns.join ","} FROM #{table};
    SQL
    
    rows_to_array(rows)
  end
    
    
  def find_each(options = {}, &block)   
    start_index = options[:start] || options[:offset] || options.values[0] || 0
    limit_amount = options[:batch_size] || options[:limit] || options.values[1] 
    
    if limit_amount.nil? 
      batch = all
    else
      batch = batch_size(start_index, limit_amount)
    end
    
    batch.each { |record| 
      yield record
    }
  end
  
    
  def batch_size(start_index, limit_amount)
    rows = connection.execute (<<-SQL)
        SELECT #{columns.join ","} FROM #{table}
        ORDER BY id ASC
        LIMIT #{limit_amount}
        OFFSET #{start_index};
    SQL
    
    rows_to_array(rows)
  end
  
    
  def find_in_batches(options = {}, &block)
    start_index = options[:start] || options[:offset] || options.values[0] || 0
    limit_amount = options[:batch_size] || options[:limit] || options.values[1] 
    batch = batch_size(start_index, limit_amount)
    
    yield batch
  end
    
    
    
  private
    
  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end
    
  
  def rows_to_array(rows)
    rows.map { |row| new(Hash[columns.zip(row)]) }
  end
    
  
  def method_missing(m, *args, &block) 
    if m.match?(/find_by/)
      attribute = m.to_s[8..-1]
      value = args[0]
    
      unless columns.index("#{attribute}").nil?
         return find_by(attribute, value)
      end
      
      return "Invalid Argument: #{m} >> There is no such column: '#{attribute}' -- please try again."  
    else
      return "Invalid Method: #{m} >> The method called does not exist -- please try again."  
    end  
  end
    
  
    
end