require 'sqlite3'
require 'bloc_record/utility'
require 'pg'

module Schema
  def table
    BlocRecord::Utility.underscore(name)
  end
  
=begin
  def schema
    unless @schema
      @schema = {}

      connection.table_info(table) do |col|
        p col
        @schema[col["name"]] = col["type"]
      end
    end
    @schema
  end
=end
  def schema
    unless @schema
      @schema = {}
      column_names = connection.query("SELECT * FROM information_schema.columns WHERE table_schema = 'public' AND  table_name = '#{table}'")
      column_names.each do |col|
        @schema[col['column_name']] = col['data_type']
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
    connection.exec(<<-SQL)[0][0]
      SELECT COUNT(*) FROM #{table}
    SQL
  end
end