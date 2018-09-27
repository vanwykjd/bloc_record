require 'sqlite3'
require 'active_support/inflector'
require 'pg'

module Associations
  
  def has_many(association)
    define_method(association) do
      rows = self.class.connection.exec <<-SQL
        SELECT * FROM #{association.to_s.singularize}
        WHERE #{self.class.table}_id = #{self.id}
SQL
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
      row = self.class.connection.exec(<<-SQL).first.values
        SELECT * FROM #{association_name}
        WHERE #{self.class.table}_id = #{self.id}
SQL
      
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
      target_id = self.send(association_name + "_id")[1]
      row = self.class.connection.exec(<<-SQL).first.values
        SELECT * FROM #{association_name}
        WHERE id = #{target_id}
SQL
      
      class_name = association_name.classify.constantize
      
      if row
        data = Hash[class_name.columns.zip(row)]
        class_name.new(data)
      end
      
    end
  end
  
end