require 'readline'
require_relative 'my_sqlite_request'

class MySqlite
    def initialize
        @table_name     = nil
        @select_columns = []
        @where_column   = nil
        @where_criteria = nil
        @data_values    = {}
        @request_type   = :none
        @column_on_db_a = nil
        @column_on_db_b = nil
        @join_table_a   = nil
        @with_table_b   = nil
        @insert_values  = {}  
        @update_params  = {}
    end

    def get_table_name(query)
        query.each do |element|
            if(element.upcase=="FROM" || element.upcase=="INTO" || element.upcase=="UPDATE")
                @table_name=query[query.find_index(element) + 1]
                break
            end
        end
        self
    end

    def get_selected_columns(query)
        query=query.map(&:upcase)
        for i in (query.find_index("SELECT") + 1)..(query.find_index("FROM") - 1)
            @select_columns << query[i].downcase
        end
        @select_columns=@select_columns.join.to_s if(@select_columns.length==1)
        p "selected #{@select_columns}"
    end

    def get_where_params(query)
        query.each do |element|
            if(element.upcase=="WHERE")
                @where_column=query[query.find_index(element) + 1].split('=').first
                @where_criteria=query[query.find_index(element) + 1].split('=').last
                break
            end
        end
        p @where_column
        p @where_criteria
    end

    def get_join_params(query)
        split1=[]
        split2=[]
        query.each do |element|
            if(element.upcase=="ON")
                split1=query[query.find_index(element) + 1].split('=').first
                split2=query[query.find_index(element) + 1].split('=').last
                @column_on_db_a=split1.split('.').last
                @column_on_db_b=split2.split('.').last
            elsif(element.upcase=="JOIN")
                @with_table_b=query[query.find_index(element) + 1]
            end
        end
    end

    def get_order_params(query)
        
    end
=begin
    def get_insert_values(query)
        keys = process_data(@table_name)
        keys = keys[0].keys
        p keys
        query.each do |element|
            if(element.upcase=="VALUES")

            end
        end
    end
=end
    def get_update_params(query)

    end

    def run_select(query)
        request = MySqliteRequest.new
        request = request.from(@table_name)
        request = request.select(@select_columns)
        request = request.where(@where_column, @where_criteria) if(query.map(&:upcase).include? "WHERE")
        request = request.join(@column_on_db_a, @with_table_b, @column_on_db_b) if(query.map(&:upcase).include? "JOIN")
        p request.run
    end

    def run_insert(query)
        request = MySqliteRequest.new
        request = request.insert(@table_name)
        request = request.values(@insert_values)
        request.run
    end

    def run_update(query)
        request = MySqliteRequest.new
        request = request.update(@table_name)
        request = request.set(@update_params)
        request = request.where(@where_column,@where_criteria)
        request.run
    end

    def run_delete(query)
        request = MySqliteRequest.new
        request = request.delete
        request = request.from(@table_name)
        request = request.where(@where_column,@where_criteria)
        request.run
    end

    def get(query)
        get_selected_columns(query)
        get_table_name(query)
        get_where_params(query)
        get_join_params(query)
        #get_insert_values(query)
        #get_update_params(query)
        @self
    end

    def run_request(query)
        get(query)
        if(query[0].upcase=="SELECT")
            p "select"
            run_select(query)
        elsif(query[0].upcase=="INSERT")
            p "insert"
            run_insert(query)
        elsif(query[0].upcase=="UPDATE")
            p "update"
            run_update(query)
        elsif(query[0].upcase=="DELETE")
            p "delete"
            run_delete(query)
        end
        @self
    end


    def run
        while query=Readline.readline("my_sqlite_cli > ", true)
            query=query.split
            p query
            #for i in 0..query.length do
             #   query[i]=query[i].split(",")
            #end
            p query
            p "\n"
            if(query.join=="quit")
                exit
            elsif
                run_request(query)
            end
            initialize
        end
    end
end

def _main()
    request=MySqlite.new
    request.run
end

_main()