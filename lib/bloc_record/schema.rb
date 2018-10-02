
require 'sqlite3'
require 'pg'
require 'bloc_record/utility'


module Schema
  def table
    BlocRecord::Utility.underscore(name)
  end
  
  def schema
    unless @schema
      @schema = {}
      if defined?(connection.table_info)
        connection.table_info(table) do |col|
          @schema[col["name"]] = col["type"]
        end
      elsif defined?(connection.exec)
        column_names = connection.exec("SELECT * FROM information_schema.columns WHERE table_schema = 'public' AND  table_name = '#{table}'")
        column_names.each do |col|
          @schema[col['column_name']] = col['data_type']
        end
      end
    end
    @schema
  end
  
  def columns
    schema.keys
  end
  
  def attributes
    columns - ["id"]
  end
  
  def count
    if defined?(connection.exec)
      connection.exec(<<-SQL)[0][0]
        SELECT COUNT(*) FROM #{table}
      SQL
    elsif defined?(connection.execute)
      connection.execute(<<-SQL)[0][0]
        SELECT COUNT(*) FROM #{table}
      SQL
    end
  end
end
