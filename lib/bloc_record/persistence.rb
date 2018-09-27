require 'sqlite3'
require 'bloc_record/schema'
require 'pg'


module Persistence
  
  def self.included(base)
    base.extend(ClassMethods)
  end
  
  
  def save
    self.save! rescue false
  end
  
  
  def save!
    unless self.id
      self.id = self.class.create(BlocRecord::Utility.instance_variables_to_hash(self)).id
      BlocRecord::Utility.reload_obj(self)
      return true
    end
    
    fields = self.class.attributes.map { |col| "#{col}=#{BlocRecord::Utility.sql_strings(self.instance_variable_get("@#{col}"))}"}.join(",")
    
    self.class.connection.exec <<- SQL
      UPDATE #{self.class.table}
      SET #{fields}
      WHERE id = #{self.id};
    SQL
    
    true
  end
  
  
  def update_attribute(attribute, value)
    self.class.update(self.id, { attribute => value })
  end
  
  
  def update_attributes(updates)
    self.class.update(self.id, updates)
  end
  
  
  def destroy
    self.class.destroy(self.id)
  end
  
  
  module ClassMethods
    
    def create(attrs)
      attrs = BlocRecord::Utility.convert_keys(attrs)
      attrs.delete "id"
      vals = attributes.map { |key| BlocRecord::Utility.sql_strings(attrs[key]) }
      
      connection.exec <<-SQL
        INSERT INTO #{table} (#{attributes.join ","})
        VALUES (#{vals.join ","});
SQL

      data = Hash[attributes.zip attrs.values]
      data["id"] = connection.exec("SELECT currval(pg_get_serial_sequence('#{table}','id'));")[0][0]
      new(data)
    end
    
    
    def update(ids, updates)
      updates = BlocRecord::Utility.convert_keys(updates)
      updates.delete "id"
      updates_array = updates.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}" }
      
      if ids.class == Fixnum || Serial
        where_clause = "WHERE id = #{ids}"
      elsif ids.class == Array
        where_clause = ids.empty? ? ";" : "WHERE id IN (#{ids.join(",")});"
      else
        where_clause = ";"
      end
      
      connection.exec <<-SQL
        UPDATE #{table}
        SET #{updates_array * ","} #{where_clause}
SQL
      
      true
    end
    
    
    def update_all(updates)
      update(nil, updates)
    end
    
    
    def destroy(*id)
      if id.length > 1
        where_clause = "WHERE id IN (#{id.join(",")});"
      else
        where_clause = "WHERE id = #{id.first[1]};"
      end
      
      connection.exec <<-SQL
        DELETE FROM #{table} #{where_clause}
SQL
      
      true
    end
    
    
    def destroy_all(conditions_hash=nil)
      if conditions_hash && !conditions_hash.empty?
        conditions_hash = BlocRecord::Utility.convert_keys(conditions_hash)
        conditions = conditions_hash.map {|key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
        
        connection.exec <<-SQL
          DELETE FROM #{table}
          WHERE #{conditions};
SQL
      else
        connection.exec <<-SQL
          DELETE FROM #{table}
SQL
      end
      
      true
    end
    
    

  end
  
end