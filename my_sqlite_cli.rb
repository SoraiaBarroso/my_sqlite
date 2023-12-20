require "readline"
require_relative 'my_sqlite_request'
require 'csv'

class MySqliteQueryCli

    def read_input
        line = Readline.readline("my_sqlite_cli> ", true)
        if line == nil
            p Readline::HISTORY.to_a
        end
        line
    end
    
    def handle_where(input)
        where_request = input.split('WHERE')
        where_params = where_request[1].scan(/\S+/)
        col = where_params[0]
        criteria = where_params[2].delete('"\'')

        return [col, criteria]
    end

    # UPDATE students SET email = 'jane@janedoe.com', blog = 'https://blog.janedoe.com' WHERE name = 'Mila'
    def parse_update(input)
        request = MySqliteRequest.new

        table_name = input.match(/UPDATE\s+(\S+)/)
        table = table_name[1] + ".csv"
        where_params = handle_where(input)

        #set hash
        set_values = input.match(/SET\s+(.+)\s+WHERE/)[1].split(',').map(&:strip).map { |value| value.gsub(/['']/, '') }
        hash_values = {}

        set_values.each do |item|
            key, value = item.split(' = ')
            hash_values[key] = value
        end
        
        request = request.update(table)
        request = request.set(hash_values)
        request = request.where(where_params[0], where_params[1])
        request.run
    end

    # INSERT INTO students VALUES (John,john@johndoe.com,A,https://blog.johndoe.com)
    def parse_insert(input)

        request = MySqliteRequest.new
        table_name = input.match(/INTO\s+(\S+)/)
        table = table_name[1] + ".csv"

        # get everything after values divide by ',' and remove '()'
        values = input.match(/VALUES\s(.+)/)[1].split(',').map(&:strip).map { |value| value.gsub(/[()]/, '') }

        # convert values into hash 
        file_columns = CSV.read(table, headers: true).headers
        hash_values = {}

        values.each_with_index do |val, index|
            break if index >= file_columns.length

            hash_values[file_columns[index]] = val
        end

        request = request.insert(table)
        request = request.values(hash_values)
        request.run
    end

    # SELECT * FROM students
    # SELECT name,email FROM students WHERE name = 'Mila'
    def parse_select(input)
        request = MySqliteRequest.new
        # get data between SELECT and FROM
        columns_select = input.match(/SELECT\s+(.+)\s+FROM/)[1].split(/[\s,]+/).map(&:strip)
        table_name = input.match(/FROM\s+(\S+)/)
        
        table = table_name[1] + ".csv"
        request = request.from(table)

        if columns_select.length > 1
            request = request.select([columns_select[0], columns_select[1]]) #columns to select
        else
            request = request.select(columns_select[0]) #columns to select
        end
       
        if input.include?("WHERE")
            where_params = handle_where(input)
            request = request.where(where_params[0], where_params[1]) 
        end

        request.run # print output
    end

    # DELETE FROM students WHERE name = 'John'
    def parse_delete(input)
        request = MySqliteRequest.new

        table_name = input.match(/FROM\s+(\S+)/)
        table = table_name[1] + ".csv"
        where_params = handle_where(input)

        request = request.delete()
        request = request.from(table)
        request = request.where(where_params[0], where_params[1])

        request.run
    end

    def parse(arguments)        
        if arguments.include?("SELECT")
            parse_select(arguments)
        elsif arguments.include?("INSERT")
            parse_insert(arguments)
        elsif arguments.include?("DELETE")
            parse_delete(arguments)
        elsif arguments.include?("UPDATE")
            parse_update(arguments)
        else
            puts "Invalid input"
        end
    end

    def run         
        puts "MySQLite version 0.1 20XX-XX-XX"
        while commands = read_input
            if commands == 'quit'
                break
            else
                #execute commandss
                parse(commands)
            end
        end       
    end

end

MySqliteQueryCli.new.run