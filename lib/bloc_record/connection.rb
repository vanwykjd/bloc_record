require 'sqlite3'
require 'pg'
 
module Connection
  def connection
    
    case BlocRecord.database_platform
      when :sqlite3
        @connection ||= SQLite3::Database.new(BlocRecord.database_filename)
      when :pg
        @connection ||= PG::Connection.new(:dbname => BlocRecord.database_filename)
    end
    
  end
end