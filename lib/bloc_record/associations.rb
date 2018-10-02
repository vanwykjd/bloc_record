require 'sqlite3'
require 'active_support/inflector'
require 'pg'

module Associations
  
  def has_many(association)
    define_method(association) do
      if defined?(self.class.connection.exec)
        rows = self.class.connection.exec <<-SQL
        SELECT * FROM #{association.to_s.singularize}
        WHERE #{self.class.table}_id = #{self.id.last}
SQL
      else
        rows = self.class.connection.execute <<-SQL
        SELECT * FROM #{association.to_s.singularize}
        WHERE #{self.class.table}_id = #{self.id}
SQL
      end
     
      class_name = association.to_s.classify.constantize
      collection = BlocRecord::Collection.new
      
      rows.each do |row|
        collection << class_name.new(Hash[class_name.columns.zip(row)])
      end
      
      collection
    end
  end
  
  
  def has_one(association)
    define_method(association) do
      association_name = association.to_s

      
      if defined?(self.class.connection.exec)
        row = self.class.connection.exec(sql).first
      else
        row = self.class.connection.get_first_row <<-SQL
         SELECT * FROM #{association_name}
         WHERE id = #{self.send(association_name + "_id")}
SQL
      end
      
      class_name = association_name.classify.constantize
      
      if row
        data = Hash[class_name.columns.zip(row)]
        class_name.new(data)
      end
    
    end
  end
  
  
  def belongs_to(association)
    define_method(association) do
      association_name = association.to_s
      if defined?(self.class.connection.exec)
        row = self.class.connection.exec <<-SQL
        SELECT * FROM #{association_name}
        WHERE id = #{self.send(association_name + "_id").last}
        LIMIT 1;
SQL
      else
        row = self.class.connection.get_first_row <<-SQL
        SELECT * FROM #{association_name}
        WHERE id = #{self.send(association_name + "_id")}
SQL
      end
      class_name = association_name.classify.constantize
      
      if row
        data = Hash[class_name.columns.zip(row)]
        class_name.new(data)
      end
      
    end
  end
  
end