require 'csv'

class MySqliteRequest
    # constructor
    def initialize
        @type_of_request = :none # select update delete or insert
        @table_name      = nil # file name
        @where_params    = []
        @select_columns  = [] # which column to look for, can be more than one (the one which we are gonna print)
        @order_params    = nil
        @join_params     = nil
        @insert_att      = nil
    end

    # a method to set the table for the query
    # takes one argument: a table name
    # e.g., from('filename.csv')
    def from(table_name) # name of the table filename.csv
        @table_name = table_name
        self
    end

    # a method to set the query type and arguments
    # takes one argument: either a column name or an array of columns
    # e.g., select(['col_1', 'col_2'])
    def select(column_name)
        @type_of_request = :select

        if column_name.is_a?(Array)
            @select_columns += column_name.collect { |elem| elem.to_s }
        else 
            @select_columns << column_name.to_s
        end
        self
    end

    # a method to specify query conditions
    # takes two arguments: a column name and its value
    # e.g., where('col_country', 'Spain')
    # can be called several times to add multiple conditions
    def where(column_name, criteria)
        @where_params << [column_name, criteria]
        self
    end

    # a method to join another table to select records
    # takes three arguments: a column in the original table to join on, a second table name, and a column on the second table
    # e.g.: join('col_process_id', 'processes.csv', 'process_id')
    def join(column_on_db_a, filename_db_b, column_on_db_b)
        unless @type_of_request == :select
            raise "Invalid query type to use join()"
        end

        @join_params = {
          'table' => filename_db_b,
          'col_a' => column_on_db_a,
          'col_b' => column_on_db_b,
        }
        self
    end

    # a method to specify the order of the resulting records
    # takes two arguments: an order of sorting and a column name to sort records by
    # e.g., order(:asc, 'col_year')
    def order(order, column_name)
        unless @type_of_request == :select
            raise "Invalid query type to use order()"
        end

        @order_params = {
          'dir' => order,
          'column' => column_name,
        }
        self
    end

    # a method to set the query type and arguments
    # takes one argument: a table name
    # e.g., insert('filename.csv')
    def insert(table_name)
        @type_of_request = :insert
        @table_name = table_name
        self
    end

    # a method to specify values to insert or update
    # takes one argument: a hash of data in the (key => value) format
    # values({"name" => "Don Adams"})
    def values(data)
        unless @type_of_request == :insert || @type_of_request == :update
            raise "Invalid query type to use values() or set()"
        end

        @insert_att = data
        self
    end

    # an alias for the values() method
    # both are valid because Qwasar tests messed them up
    def set(data)
        values(data)
    end

    # a method to set the query type and arguments
    # takes one argument: a table name
    # e.g., update('filename.csv')
    def update(table_name)
        @type_of_request = :update
        @table_name = table_name
        self
    end

    # a method to set the query type and arguments
    # takes no arguments
    def delete
        @type_of_request = :delete
        self
    end
    
    # a method to execute the query
    # takes no arguments
    def run
        #_debug_out
        _validate_query
        if @type_of_request == :select
            _run_select
        elsif @type_of_request == :insert
            _run_insert
        elsif @type_of_request == :update
            _run_update
        elsif @type_of_request == :delete
            _run_delete
        end
    end

    private

    def _debug_out
        puts "query type: #{@type_of_request}"
        puts "table name: #{@table_name}"
        if @type_of_request == :select
            puts "select columns: #{@select_columns}"
            puts "criteria: #{@where_params}"
            puts "join: #{@join_params}"
        elsif @type_of_request == :insert
            puts "data to insert: #{@insert_att}"
        elsif @type_of_request == :update
            puts "data to update: #{@insert_att}"
            puts "criteria: #{@where_params}"
        elsif @type_of_request == :delete
            puts "data to delete: #{@where_params}"
        end
    end

    def _validate_query
        if @type_of_request == :none
            raise "Invalid query"
        end
        if @table_name == nil
            raise "Table name has not been provided"
        end
        unless File.exist?(@table_name)
            raise "Table #{@table_name} doesn't exist"
        end

        if @type_of_request == :select
            if @order_params != nil && !@select_columns.include?('*') && !@select_columns.include?(@order_params['column'])
                raise "To order the result by #{@order_params['column']}, add this column to selection"
            end
            if @join_params != nil
                if @join_params['table'] == nil
                    raise "Joined table name has not been provided"
                end
                unless File.exist?(@join_params['table'])
                    raise "Joined table #{@join_params['table']} doesn't exist"
                end
                # todo: validate that when using join, selected columns either use a dot notation or are just *
            end
        end

        if @type_of_request == :insert || @type_of_request == :update
            if @insert_att == nil
                raise "No data to #{@type_of_request}"
            end
        end
    end
    
    def _run_select
        result = []
        where_count = @where_params.length
        data = CSV.read(@table_name, headers: true)
        join = @join_params == nil ? nil : CSV.read(@join_params['table'], headers: true)

        data.each do |row|
            satisfied = 0
            if where_count > 0
                @where_params.each do |where_attr|
                    if row[where_attr[0]] && row[where_attr[0]] == (where_attr[1])
                        satisfied += 1
                    end
                end
            end
            if satisfied == where_count
                joined = nil
                if join != nil && row[@join_params['col_a']] != nil
                    match = join.find { |jr| jr[@join_params['col_b']] == row[@join_params['col_a']] }
                    if match != nil then joined = match.to_h end
                end

                if @select_columns.include?('*')
                    if joined == nil
                        result << row.to_h
                    else
                        joined.delete(@join_params['col_b'])
                        result << row.to_h.merge(joined)
                    end
                else
                    result << row.to_h.slice(*@select_columns)
                end
            end
        end

        if @order_params != nil && result.length > 1
            result = result.sort_by { |row| row[@order_params['column']] }
            if @order_params['dir'] == :desc
                result = result.reverse
            end
        end

        _print_output(result)
    end

    def _run_insert
        data = CSV.read(@table_name, headers: true)
        data << @insert_att
        _write_csv(data)
    end

    def _run_delete
        data = CSV.read(@table_name, headers: true)
        headers = data.headers

        @where_params.each do |data_to_delete|
            data = data.reject do |row|
                if row[data_to_delete[0]] && row[data_to_delete[0]].include?(data_to_delete[1])
                    row[data_to_delete[0]].include?(data_to_delete[1])
                end
            end
        end

        _write_csv(data, headers)
    end

    def _run_update
        where_count = @where_params.length
        data = CSV.read(@table_name, headers: true)

        data.each do |row|
            satisfied = 0
            if where_count > 0
                @where_params.each do |where_attr|
                    if row[where_attr[0]] && row[where_attr[0]].include?(where_attr[1])
                        satisfied += 1
                    end
                end
            end
            if satisfied == where_count
                @insert_att.each do |update_column, update_value|
                    row[update_column] = update_value
                end
            end
        end

        _write_csv(data)
    end

    def _print_output(output_request)
        if output_request != nil
            output_request.each do |row|
                output = row.map { |_,value| "#{value}" }.join("|")
                puts output
            end
        end
    end

    def _write_csv(data, headers = data.headers)
        CSV.open(@table_name, 'w', write_headers: true, headers: headers) do |csv|
            data.each { |row| csv << row }
        end
    end

end

def _main
=begin
    request = MySqliteRequest.new
    request = request.from('nba_player_data.csv')
    request = request.select('name')
    request.run

    request = MySqliteRequest.new
    request = request.from('nba_player_data.csv')
    request = request.select('name')
    request = request.where('college', 'University of California')
    request.run

    request = MySqliteRequest.new
    request = request.from('nba_player_data.csv')
    request = request.select('name')
    request = request.where('college', 'University of California')
    request = request.where('year_start', '1997')
    request.run

    request = MySqliteRequest.new
    request = request.insert('nba_player_data.csv')
    request = request.values('name' => 'Alaa Abdelnaby', 'year_start' => '1991', 'year_end' => '1995', 'position' => 'F-C', 'height' => '6-10', 'weight' => '240', 'birth_date' => "June 24, 1968", 'college' => 'Duke University')
    request.run

    request = MySqliteRequest.new
    request = request.update('nba_player_data.csv')
    request = request.values('name' => 'Alaa Renamed')
    request = request.where('name', 'Alaa Abdelnaby')
    request.run

    request = MySqliteRequest.new
    request = request.delete()
    request = request.from('nba_player_data.csv')
    request = request.where('name', 'Alaa Abdelnaby')
    request.run
=end
end

_main