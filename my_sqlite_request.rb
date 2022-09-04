require 'csv'


class MySqliteRequest
    def initialize
        @request_type   = :none
        @table_name     = nil
        @where_params   = []
        @select_columns = []
        @order          = :asc
        @order_column   = nil
        @insert_attr    = {}
        @update_values  = {}
        @table_data     = []
        @select_res     = []
    end

    def from(table_name)
        @table_name=table_name
        self
    end

    def select(columns)
        if(columns.is_a?(Array))
            @select_columns+=columns.collect {|elem| elem.to_s}
        else
            @select_columns << columns.to_s       
        end
        self.req_type(:select)
        self
    end

    def where(column_name, criteria)
        @where_params << [column_name,criteria]
        self
    end

    def join(column_on_db_a, filename_db, column_on_db_b)
            File.truncate("new_join_db.csv", 0)
            table_a=process_data(@table_name)
            table_b=process_data(filename_db)
            table_a.each do |key1|
                table_b.each do |key2|
                    if key1[column_on_db_a]==key2[column_on_db_b]
                        key1.merge!(key2)
                    end
                end
            end
            File.open("new_join_db.csv", 'a') do |f|
                f.puts table_a[0].keys.join(',')
                table_a.each do |a|
                    f.puts a.values.join(',')
                end
            end
            @table_name="new_join_db.csv"
        self
    end

    def order(order,column_name)
        @order=order
        @order_column=column_name
        self
    end

    def insert(table_name)
        @table_name=table_name
        self.req_type(:insert)
        self
    end

    def values(data)
        if @request_type==:insert
            @insert_attr=data
        else
            raise "Wrong type of reques to call values()"
        end
        self
    end

    def update(table_name)
        @table_name=table_name
        self.req_type(:update)
        self
    end

    def set(data)
        @update_values=data
        self
    end

    def delete
        self.req_type(:delete)
        self
    end

    def req_type(type)
        @request_type=type
    end

    def process_data(filename)
        result=[]
        CSV.parse(File.open(filename), headers: true).each do |row|
            result << row.to_hash
        end
        result
    end

    def print_select
        puts "select attribute   =  #{@select_columns}"
        puts "where params       =  #{@where_params}"
    end

    def print_insert
        puts "insert attributes  =  #{@insert_attr}"
    end

    def print_update
        puts "update attributes  =  #{@update_values}"
        puts "where params       =  #{@where_params}"
    end

    def print_delete
    
    end

    def print
        puts "request type       =  #{@request_type}"
        puts "table_name         =  #{@table_name}"
        if(@request_type==:select)
            print_select
        elsif(@request_type==:insert)
                print_insert
        elsif(@request_type==:update)
                print_update
        elsif(@request_type==:delete)
                print_delete
        end
    end

    def run
        print
        if(@request_type==:select)
            _run_select
        elsif(@request_type==:insert)
            _run_insert
        elsif(@request_type==:update)
            _run_update
        elsif(@request_type==:delete)
            _run_delete
        end
    end

    def _run_select
        CSV.parse(File.read(@table_name), headers: true).each do |row|
            if @where_params.empty?
                if @select_columns==["*"]
                    @select_res << row.to_hash
                else
                    @select_res << row.to_hash.slice(*@select_columns)
                end
            else
                @where_params.each do |where_attr|
                    if row[where_attr[0]] == where_attr[1]
                        if @select_columns==["*"]
                            @select_res << row.to_hash
                        else
                            @select_res << row.to_hash.slice(*@select_columns)
                        end
                    end
                end
            end
        end
        #if(@order==:asc)
         #   @select_res.sort_by! {|k| k[@order_column]}
        ##   @select_res.sort_by! {|k| k[@order_column]}
          #  @select_res.reverse!
        #end
        @select_res
    end

    def _run_insert
        File.open(@table_name, 'a') do |f|
            f.puts @insert_attr.values.join(',')
        end
        p "new row successfully added"
    end

    def _run_update
        flag=0
        @table_data=process_data(@table_name)
        @table_data.each do |key,value|
            @where_params.each do |where_attr|
                if key[where_attr[0]]==where_attr[1]
                    key.merge!(@update_values)
                    flag=1
                end
            end
        end
        if(flag!=1)
            puts "The #{@where_params[0][0]}, #{@where_params[0][1]} doesn't exist"
        end
        File.truncate(@table_name, 66)
        File.open(@table_name, 'a') do |f|
                f.puts "\n"
            @table_data.each do |a|
                f.puts a.values.join(',')
            end
        end
    end

    def _run_delete
        flag=0
        i=0
        @table_data=process_data(@table_name)
        @table_data.each do |key,value|
            @where_params.each do |where_attr|
                if key[where_attr[0]]==where_attr[1]
                    j=i
                    @table_data.delete_at(j)
                    flag=1
                end
            end
            i+=1
        end
        if(flag!=1)
            puts "The #{@where_params[0][0]}, #{@where_params[0][1]} doesn't exist"
        end
        File.truncate(@table_name, 66)
        File.open(@table_name, 'a') do |f|
            f.puts "\n"
            @table_data.each do |a|
                f.puts a.values.join(',')
            end
        end
    end
end
=begin
def _main()
    request = MySqliteRequest.new
    #request = request.from('nba_player_data.csv')
    request = request.from("nba_player_data.csv")
    request = request.select(["name", "position"])
    request = request.where("name", "Paul Cloyd")
    #request = request.order(:asc, "name")
    #request = request.join("name","join.csv","Player")
    p request.run
end

_main()
=end
