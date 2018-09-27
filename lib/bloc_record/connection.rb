require 'sqlite3'
require 'pg'
 
module Connection
  def connection
    
    case BlocRecord.database_platform
      when :sqlite3
        @connection ||= SQLite3::Database.new(BlocRecord.database_filename)

      when :pg
        @connection ||= PG::Connection.open(:dbname => 'address_bloc', :user => 'postgres')

    end
    
  end
  
  def self.execute(sql)
    case BlocRecord.database_platform
      when :sqlite3
        SQLite3::Connection.execute(sql)
      when :pg
        PG::Connection.exec(sql)

    end
  end
  
end