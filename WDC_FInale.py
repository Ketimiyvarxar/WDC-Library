import requests


# database connection object
class dbc:
    # initalizing our dbc by providing it with the service endpoint, from which we can get a datacube
    def __init__(self, url):
    
       # Initializes a new dbc instance which is used to manage connections and send queries to a WCPS server.

      
        if not isinstance(url, str):
            raise TypeError("Value entered must be a string.")
        self.server_url = url
    
    def send_query(self, wcps_query):
        
       # Sends a WCPS query to the server and retrieves the response.

     
        if not isinstance(wcps_query, str):
            raise TypeError("Value entered must be a string.")
        # getting a response from the server
        try:
            # 'verify=False' is used to skip SSL certificate verification;
            response = requests.post(self.server_url, data = {'query': wcps_query}, verify = False)
            if response.status_code == 200:
                return response
            else:
                raise ValueError("Not correct query")
        except:
            raise Exception("Something is wrong...")
         # General exception handling to catch potential issues like network 




# function needed for converting a byte string to the list of numbers
def byte_to_list(byte_str):
    
   # Converts a byte string into a list of floats. Useful for parsing numeric data returned from a server.


    decoded_str = byte_str.decode('utf-8') # decode the byte string
    str_list = decoded_str.split(',') # split numbers separated by comma
    num_list = [float(num) for num in str_list] # create a list of numbers
    return num_list


# datacube object
class dco:
    # initializing the dco
    def __init__(self, dbc_being_used):
        
       # Initializes a dco instance which manages WCPS queries using a dbc object for server communication.

        if not isinstance(dbc_being_used, dbc):
            raise TypeError("dbc instance not passed")
        
        self.DBC = dbc_being_used
        # default values
        self.vars = []
        self.Subsets = []
        self.aggregation = None
        self.format = None
        self.aggregation_condition = None
        self.filter_condition = None
        self.var_names = []
        self.transformation = None
        self.encode_as = None
        
    def reset(self):
      
       # Resets the attributes of the dco instance to their default values, except for the dbc connection
     
        self.vars = []
        self.Subsets = []
        self.aggregation = None
        self.format = None
        self.count_condition = None
        self.filter_condition = None
        self.var_names = []
        self.transformation = None
        self.encode_as = None
        return self

    
    def get_all_var_names(self, string):
        """
        Extracts all variable names from a string where variables are prefixed by '$' and can be
            followed by various delimiters such as spaces, commas, parentheses, etc.

        """
        var_names = []
        index = 0
        while index < len(string):
            start_index = string.find('$', index) # Find the index of the next '$' symbol starting from 'index'
            if start_index == -1:
                break
            
            # Define a list of characters that should terminate the variable name
            delimiters = [' ', ',', '(', ')', '[', ']', '{', '}', ';', '>', '<', '+', '-', '=', '.', 
                         '/', '\\', '|', '!']
            end_index = len(string)
            
            # Enumerate through the substring starting from 'start_index' to find the first delimiter
            for i, char in enumerate(string[start_index:]):
                if char in delimiters:
                    end_index = start_index + i
                    break

            var_names.append(string[start_index:end_index])
            index = end_index
            
        if len(var_names) == 0:
            return None
        return var_names

    
    def initialize_var(self, s):
        """
        Initializes a variable for the datacube object. This method extracts the variable name from 
            the input,checks if it's successfully defined, and then appends the variable along with
            its associated datacube to the respective lists within the dco instance.

        """
        if not isinstance(s, str):
            raise TypeError("Value entered must be a string.")
        # specifying  that s string must be formatted like: '$variable_name in (coverage_name)'
        if not(s.startswith('$') and " in (" in s and s.endswith(')')):
            raise ValueError("The format of variable initialization wasn't correct")
        
        var_name = self.get_all_var_names(s)[0]
        self.vars.append(s)
        #No subset has been defined yet
        self.Subsets.append(None)
        self.var_names.append(var_name)
        return self
    
    def do_vars_exist(self, string):
        """
        Checks whether all variable names extracted from the input string exist in the predefined list of
        variable names of the current instance. This method is useful for validating that variables
        referenced in a string (e.g., a query or command) are all recognized by the system
        before proceeding with further operations.

        """
        # Normalize the string by replacing common whitespace characters with a space
        string = string.replace('\n', ' ')
        string = string.replace('\r', ' ')
        string = string.replace('\t', ' ')
        var_names = self.get_all_var_names(string)
        if var_names != None:
            var_names = set(var_names)
            # Convert the list of variable names to a set for efficient comparison
            if var_names.issubset(set(self.var_names)):
                return True
            else:
                raise ValueError("Variables in a string don't exist")
        else:
            raise ValueError("Variables weren't specified")
        
        
    def subset(self, subset, var_name):
        """
        Adds a subset specification for a specific variable in the datacube object. This method
            identifies the variable by its name, then associates the specified subset with it, updating
            the internal state of the dco instance.

        """
        if not isinstance(subset, str):
            raise TypeError("Value entered must be a string.")
        if not isinstance(var_name, str):
            raise TypeError("Value entered must be a string.")
        if not(var_name in self.var_names):
            raise ValueError("Such variable doesn't exist")
        # index of the variable name in the var_names list
        idx = self.var_names.index(var_name)
        # Update the subset specification at the corresponding index in the Subsets list
        self.Subsets[idx] = subset
        return self
        
    def where(self, filter_condition):
        """
        Sets a filter condition for the datacube query. This method allows specifying conditions that
            filter the data according to certain criteria, similar to a SQL WHERE clause. The specified
            condition will be applied when the query is executed to filter the results.

        """
    
        if not isinstance(filter_condition, str):
            raise TypeError("Value entered must be a string.")
        self.do_vars_exist(filter_condition)
        self.filter_condition = filter_condition
        return self
    

    def set_format(self, output_format):
        """
        Sets the output format for data retrieved from the datacube. This method allows the user to specify 
        the format in which the data should be returned after a query is executed, enabling different
        types of data processing.


        """
        if not isinstance(output_format, str):
            raise TypeError("Value entered must be a string.")
        if not (output_format in ['PNG', 'CSV', 'JPEG']):
            raise ValueError("Entered format doesn't exist")
        self.format = output_format
        return self
    
        
    # aggregation methods
    def min(self, condition = None):
        """
        Configures the datacube to compute the minimum value of the specified data subset when executed.
            An optional condition can specify the subset or criteria for the aggregation.

        """
        #if condition is provided, check if it's a string and if all the variables are pre-defined
        if condition != None:
            if not isinstance(condition, str):
                raise TypeError("Value entered must be a string.")
            self.do_vars_exist(condition)
        self.aggregation_condition = condition
        self.aggregation = 'MIN'
        return self
        
    def max(self, condition = None):
        """
        Configures the datacube to compute the maximum value of the specified data subset when executed.
            An optional condition can specify the subset or criteria for the aggregation.

        """
        #if condition is provided, check if it's a string and if all the variables are pre-defined
        if condition != None:
            if not isinstance(condition, str):
                raise TypeError("Value entered must be a string.")
            self.do_vars_exist(condition)
        self.aggregation_condition = condition
        self.aggregation = 'MAX'
        return self
    
    def avg(self, condition = None):
        """
        Configures the datacube to compute the average value of the specified data subset when executed.
            An optional condition can specify the subset or criteria for the aggregation.

        """
        #if condition is provided, check if it's a string and if all the variables are pre-defined
        if condition != None:
            if not isinstance(condition, str):
                raise TypeError("Value entered must be a string.")
            self.do_vars_exist(condition)
        self.aggregation_condition = condition
        self.aggregation = 'AVG'
        return self
    
    def sum(self, condition = None):
        """
        Configures the datacube to compute the sum of values across the specified data subset when executed.
            An optional condition can specify the subset or criteria for the aggregation.


        """
        #if condition is provided, check if it's a string and if all the variables are pre-defined
        if condition != None:
            if not isinstance(condition, str):
                raise TypeError("Value entered must be a string.")
            self.do_vars_exist(condition)
        self.aggregation_condition = condition
        self.aggregation = 'SUM'
        return self
        
    def count(self, condition = None):
        """
        Configures the datacube to count the number of data points that meet the specified
            condition when executed.

        """
        #if condition is provided, check if it's a string and if all the variables are pre-defined
        if condition != None:
            if not isinstance(condition, str):
                raise TypeError("Value entered must be a string.")
            self.do_vars_exist(condition)
        self.aggregation = 'COUNT'
        self.aggregation_condition = condition
        return self
    
    def replace_variables_with_subsets(self, str_to_transform = None):
        """
        Replaces variables in a given string with their corresponding subsets if defined.
            If no string is provided, it constructs a string representation of
            all variables and their subsets.

        """
        #This distinction is critical as it determines the course of action
        #code will either modifying an existing string or creating a new list of all variables and their subsets.
        if str_to_transform != None:
            expression = str_to_transform
            #iterate over tuples of variables and corresponding subsets
            for var, subset in zip(self.var_names, self.Subsets):
                if subset != None:
                    #replace the variable in the expression with its subset
                    expression = expression.replace(var, f'{var}[{subset}]')
            return expression
        else:
            expression = ''
            for var, subset in zip(self.var_names, self.Subsets):
                if subset != None:
                    # If a subset exists, append the variable and its subset in bracketed form
                    expression += f'''{var}[{subset}] '''
                else:
                    #If no subset exists, simply append the variable
                    expression += f'''{var}'''
            return expression
    
    def transform_data(self, operation):
       
       # Sets a transformation operation to be applied to the datacube when the query is executed.


        if not isinstance(operation, str):
            raise TypeError("Value entered must be a string.")
        self.do_vars_exist(operation)
        self.transformation = operation
        return self
    
    def encode(self, operation):
        """
        Specifies the encoding operation to be applied to the output of the query.

        Parameters:
            operation (str): A string representing the encoding function, such as "encode".

        Returns:
            self: Returns the instance itself, allowing for method chaining.

        Example:
            >>> datacube.encode("
                switch 
                    case $c = 99999 
                        return {red: 255; green: 255; blue: 255} 
                    case 18 > $c
                        return {red: 0; green: 0; blue: 255} 
                    case 23 > $c
                        return {red: 255; green: 255; blue: 0} 
                    case 30 > $c
                        return {red: 255; green: 140; blue: 0} 
                    default return {red: 255; green: 0; blue: 0}
            ")
        """
        # Ensure the operation is a string, convert if not
        if not isinstance(operation, str):
            operation = str(operation)
        # Ensure all variables used in the operation are recognized by the datacube    
        if self.get_all_var_names(operation) != None:
            self.do_vars_exist(operation)
        self.encode_as = operation
        return self
        
            
    # method, which returns the result after using aggregation functions
    def aggregate_data(self):
        
        # Constructs a part of the WCPS query for performing aggregation functions based on the current settings.


        # The `helper_query` contains the conditions and/or
        # subsets that have been applied to the variables in the datacube.
        # helper_query will betargeted specifically for preforming aggregation functions later on
        helper_query = self.replace_variables_with_subsets(self.aggregation_condition)
        if self.aggregation == 'MIN':
            query = f'''min({helper_query})'''
        elif self.aggregation == 'AVG':
            query = f'''avg({helper_query})'''
        elif self.aggregation == 'MAX':
            query = f'''max({helper_query})'''
        elif self.aggregation == 'SUM':
            query = f'''sum({helper_query})'''
        elif self.aggregation == 'COUNT':
            query = f'''count({helper_query})'''
        return query
    
    def return_format(self):
     
       # Determines the format for the output based on the configured settings of the datacube.

  
        if self.format == 'CSV':
            query = "text/csv" # if the desired format of the output is text/csv
        elif self.format == 'PNG':
            query = "image/png" # if the desired format of the output is image/png:
        elif self.format == 'JPEG': 
            query = "image/jpeg" # if the desired format of the output is image/jpeg:
        return query
    
    
    # the function, which converts operations from the dco object to the query
    def to_wcps_query(self):
       
        # Constructs a WCPS query string from the current state of the dco instance.

 
        query = '''for '''
        for var in self.vars:
            query += (var + '\n')
        
        # we check for the usage of 'where' predicate. If it's None, we skip it and put return to our query
        if self.filter_condition != None:
            query += f'''where {self.filter_condition}\n'''
        query += f'''return \n'''
        
        # we check if any of the aggregation functions were used. If they were, we add them to the query and return.
        if self.aggregation != None:
            query += self.aggregate_data()
            return query
        
        # we check if the encoding conditions were specified. If they were, we will add them to 'return'
        if self.encode_as != None:
            helper_query = self.replace_variables_with_subsets(self.encode_as)
        # encoding condition wasn't specified, so we check transformation operation
        elif self.transformation != None:
            helper_query = self.replace_variables_with_subsets(self.transformation)
        # transformation wasn't used as well, so we just return a variable with the corresponding subset
        else:
            helper_query = self.replace_variables_with_subsets()
        
        # if the format was specified, we write encode() to the query
        if self.format != None: # we check whether the format was specified
            query += f'''encode({helper_query}, "{self.return_format()}")'''
        # if the encoding or data transformation were used, we must include encode()
        elif self.encode_as != None or self.transformation != None:
            query += f'''encode({helper_query}, "text/csv")'''
        else:
            query += f'''{helper_query}'''
        return query
    
    
    # executing, when all the operations were added
    def execute(self):
        
       # Executes the constructed WCPS query and processes the response based on the specified format.

 
        wcps_query = self.to_wcps_query() # get a WCPS query
        response = self.DBC.send_query(wcps_query) # pass the WCPS query to the server and get a response
        if self.format == 'CSV': # if the format is CSV, convert binary string to the list of numbers
            data = byte_to_list(response.content) 
            self.reset() # returning the values of the dco instance to default
            return data
        elif self.format == 'PNG': # if the format is PNG, return the image
            self.reset() # returning the values of the dco instance to default
            return response.content
        elif self.format == 'JPEG': # if the format is JPEG, return the image
            self.reset() # returning the values of the dco instance to default
            return response.content
        else:
            self.reset()
            data = byte_to_list(response.content) 
            return data